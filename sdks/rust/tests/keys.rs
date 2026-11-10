//! Unique keys, facts and requirements: declared on aggregates, sent by the worker after every
//! command, checked by the kernel at commit. The kernel tests' scenarios at SDK level.

mod common;

use common::TestKernel;
use kernable::{
    Aggregate, Index, Kernable, NewEvent, Outcome, Reindex, Rejected, Requirement, Submit, Unique,
    UniqueKey, Wait, Worker, domain, reindex_type,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

fn spawn(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

async fn send(kernable: &Kernable, submit: Submit) -> Outcome {
    kernable.submit(submit.wait(Wait::Committed)).await.unwrap()
}

/// The code and message of a rejected outcome, with the key or requirement it names, or
/// `committed`.
fn result(outcome: &Outcome) -> (String, String) {
    match outcome {
        Outcome::Committed(_) => ("committed".into(), String::new()),
        Outcome::Rejected(Rejected {
            code,
            message,
            key,
            requirement,
        }) => {
            let named = match (key, requirement) {
                (Some(key), None) => format!(" [{key}]"),
                (None, Some(q)) => format!(
                    " [{}.{} {} {}]",
                    q.domain, q.aggregate_type, q.aggregate_id, q.fact
                ),
                (None, None) => String::new(),
                both => panic!("a key and a requirement: {both:?}"),
            };
            (code.clone(), format!("{message}{named}"))
        }
    }
}

fn committed() -> (String, String) {
    ("committed".into(), String::new())
}

/// A rejection with `code` and `message`, and the key or requirement the message names.
fn rejected(code: &str, message: &str) -> (String, String) {
    let words: Vec<&str> = message.split(' ').collect();
    let named = match (code, words.as_slice()) {
        ("unique_taken", [_, "key", key, "is", "taken"]) => format!(" [{key}]"),
        ("requirement_missing", [aggregate, id, "does", "not", "hold", fact]) => {
            format!(" [{aggregate} {id} {fact}]")
        }
        _ => String::new(),
    };
    (code.into(), format!("{message}{named}"))
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct Customer {
    id: String,
    email: Option<String>,
    number: Option<String>,
    tags: Vec<String>,
}

/// `crm.Customer`: its email is unique in the tenant, its number across tenants, and every tag
/// in the tenant.
fn customers() -> Aggregate<Customer> {
    let crm = domain("crm");
    let customer = crm.aggregate("Customer", Customer::default());
    customer.apply("Registered", |s, d: Customer, evt, _ctx| {
        *s = Customer {
            id: evt.aggregate_id.clone(),
            ..d
        }
    });
    customer.apply("EmailChanged", |s, d: Value, _evt, _ctx| {
        s.email = d["email"].as_str().map(str::to_owned)
    });
    customer.command("Register", |_, d: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Registered", d)])
    });
    customer.command("ChangeEmail", |_, d: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("EmailChanged", d)])
    });
    customer.command("Touch", |_, _: Value, _cmd, _ctx| Ok(vec![]));
    customer
        .unique(
            "email",
            |s| s.email.as_ref().map(|e| e.to_lowercase()),
            Unique::tenant(),
        )
        .unique("number", |s| s.number.clone(), Unique::global())
        .unique("tag", |s| s.tags.clone(), Unique::default())
        .fact("reachable", |s| s.email.is_some())
}

#[test]
fn an_aggregate_computes_its_keys_and_facts_for_a_state() {
    let customer = customers();
    assert_eq!(
        customer.index(&Customer::default()).unwrap(),
        Index::default()
    );
    let state = Customer {
        id: "k1".into(),
        email: Some("Max@Example.com".into()),
        number: None,
        tags: vec!["a".into(), "b".into(), "a".into()],
    };
    let key = |name: &str, value: &str, global| UniqueKey {
        name: name.into(),
        value: value.into(),
        global,
        subject: None,
    };
    assert_eq!(
        customer.index(&state).unwrap(),
        Index {
            keys: vec![
                key("email", "max@example.com", false),
                key("tag", "a", false),
                key("tag", "b", false),
            ],
            facts: vec!["reachable".into()],
        }
    );

    // A personal key without a subject is not held.
    let people = domain("people").aggregate("Person", Customer::default());
    people.unique(
        "email",
        |s| s.email.clone(),
        Unique::global().personal(|s: &Customer| Some(s.id.clone())),
    );
    let nobody = Customer {
        email: Some("max@example.com".into()),
        ..Customer::default()
    };
    assert_eq!(people.index(&nobody).unwrap().keys, []);
    let max = Customer {
        id: "k1".into(),
        ..nobody
    };
    assert_eq!(
        people.index(&max).unwrap().keys,
        [UniqueKey {
            subject: Some("k1".into()),
            ..key("email", "max@example.com", true)
        }]
    );

    // Requirements come from the state before and the command's data.
    let accounts = domain("funds").aggregate("Account", Customer::default());
    accounts
        .command("Open", |_, _: Value, _cmd, _ctx| Ok(vec![]))
        .requires("Open", |s, d: Value| {
            let vehicle = d["vehicle"].as_str().unwrap_or_default();
            let mut facts = vec![Requirement::new("funds", "Vehicle", vehicle, "open")];
            facts.extend(
                s.tags
                    .iter()
                    .map(|t| Requirement::new("funds", "Tag", t, "valid")),
            );
            facts
        });
    let state = Customer {
        tags: vec!["x".into()],
        ..Customer::default()
    };
    assert_eq!(
        accounts
            .requirements(&state, "Open", json!({ "vehicle": "v1" }))
            .unwrap(),
        [
            Requirement::new("funds", "Vehicle", "v1", "open"),
            Requirement::new("funds", "Tag", "x", "valid"),
        ]
    );
    assert_eq!(accounts.requirements(&state, "Close", ()).unwrap(), []);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_is_unique_in_its_tenant_or_across_tenants_and_a_change_frees_the_old_value() {
    let kernel = TestKernel::new(&["crm.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let worker = kernable.worker(&customers());
    spawn(&worker);
    worker.ready().await;
    let register = |tenant: &str, id: &str, email: &str, number: &str| {
        let data = json!({ "email": email, "number": number });
        let submit = Submit::new("crm", "Customer", id, "Register")
            .tenant(tenant)
            .data(data);
        send(&kernable, submit)
    };
    let email_taken = rejected("unique_taken", "crm.Customer key email is taken");

    assert_eq!(
        result(&register("t1", "k1", "max@example.com", "1").await),
        committed()
    );
    // Normalized by the key function: the same email in another case is taken.
    assert_eq!(
        result(&register("t1", "k2", "MAX@example.com", "2").await),
        email_taken
    );
    // Another tenant may hold the same email, but not the same global number.
    assert_eq!(
        result(&register("t2", "k2", "max@example.com", "2").await),
        committed()
    );
    assert_eq!(
        result(&register("t2", "k3", "eva@example.com", "1").await),
        rejected("unique_taken", "crm.Customer key number is taken")
    );

    // A command without events keeps the keys; a change frees the old value in the same commit.
    let k1 = |command: &str, data: Value| {
        Submit::new("crm", "Customer", "k1", command)
            .tenant("t1")
            .data(data)
    };
    assert_eq!(
        result(&send(&kernable, k1("Touch", json!({}))).await),
        committed()
    );
    assert_eq!(
        result(&register("t1", "k4", "max@example.com", "4").await),
        email_taken
    );
    let changed = send(
        &kernable,
        k1("ChangeEmail", json!({ "email": "m@example.com" })),
    )
    .await;
    assert_eq!(result(&changed), committed());
    assert_eq!(
        result(&register("t1", "k4", "max@example.com", "4").await),
        committed()
    );
    // None frees it too.
    let changed = send(&kernable, k1("ChangeEmail", json!({ "email": null }))).await;
    assert_eq!(result(&changed), committed());
    assert_eq!(
        result(&register("t1", "k5", "m@example.com", "5").await),
        committed()
    );
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Vehicle {
    open: bool,
}

#[derive(Deserialize)]
struct Open {
    vehicle: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_commits_only_while_the_facts_it_requires_are_held() {
    let kernel = TestKernel::new(&["funds.*"], &[]);
    kernel.start().await;
    let funds = domain("funds");
    let vehicle = funds.aggregate("Vehicle", Vehicle::default());
    vehicle
        .apply("Launched", |s, _: Value, _evt, _ctx| s.open = true)
        .apply("WoundUp", |s, _: Value, _evt, _ctx| s.open = false)
        .command("Launch", |_, _: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Launched", ())])
        })
        .command("WindUp", |_, _: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("WoundUp", ())])
        })
        .fact("open", |s| s.open);
    let account = funds.aggregate("Account", false);
    account
        .apply("Opened", |s, _: Value, _evt, _ctx| *s = true)
        .command("Open", |_, _: Open, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Opened", ())])
        })
        .requires("Open", |_, d: Open| {
            Some(Requirement::new("funds", "Vehicle", &d.vehicle, "open"))
        });
    let kernable = kernel.client();
    let worker = kernable.worker(&vehicle).aggregate(&account);
    spawn(&worker);
    worker.ready().await;
    let open = |id: &str| {
        let submit = Submit::new("funds", "Account", id, "Open").data(json!({ "vehicle": "v1" }));
        send(&kernable, submit)
    };
    let vehicle = |command: &str| send(&kernable, Submit::new("funds", "Vehicle", "v1", command));
    let missing = rejected("requirement_missing", "funds.Vehicle v1 does not hold open");

    assert_eq!(result(&open("a1").await), missing);
    assert_eq!(result(&vehicle("Launch").await), committed());
    assert_eq!(result(&open("a1").await), committed());
    assert_eq!(result(&vehicle("WindUp").await), committed());
    assert_eq!(result(&open("a2").await), missing);
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Person {
    id: String,
    email: Option<String>,
}

fn field(d: &Value, name: &str) -> String {
    d[name].as_str().unwrap_or_default().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn erasing_its_subject_frees_a_personal_key() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let identity = domain("identity");
    identity
        .event("PersonErased", 1)
        .erases(|d, _| field(d, "personId"));
    let person = identity.aggregate("Person", Person::default());
    person
        .apply("Registered", |s, d: Value, evt, _ctx| {
            *s = Person {
                id: evt.aggregate_id.clone(),
                email: Some(field(&d, "email")),
            }
        })
        .command("Register", |_, d: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Registered", d)])
        })
        .unique(
            "email",
            |s| s.email.clone(),
            Unique::tenant().personal(|s: &Person| Some(s.id.clone())),
        );
    // Another aggregate's event erases the person.
    let request = identity.aggregate("Request", ());
    request.command("Erase", |_, d: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("PersonErased", d)])
    });
    let kernable = kernel.client().with_domain(&identity);
    let worker = kernable.worker(&person).aggregate(&request);
    spawn(&worker);
    worker.ready().await;
    let register = |id: &str| {
        let data = json!({ "email": "max@example.com" });
        send(
            &kernable,
            Submit::new("identity", "Person", id, "Register").data(data),
        )
    };

    assert_eq!(result(&register("p1").await), committed());
    assert_eq!(
        result(&register("p2").await),
        rejected("unique_taken", "identity.Person key email is taken")
    );
    let erase = Submit::new("identity", "Request", "r1", "Erase").data(json!({ "personId": "p1" }));
    assert_eq!(result(&send(&kernable, erase).await), committed());
    assert_eq!(result(&register("p2").await), committed());
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Member {
    handle: Option<String>,
}

/// `club.Member`, before (`keyed` false) and after it declares its handle unique.
fn members(keyed: bool) -> Aggregate<Member> {
    let member = domain("club").aggregate("Member", Member::default());
    member
        .apply("Joined", |s, d: Value, _evt, _ctx| {
            s.handle = Some(field(&d, "handle"))
        })
        .command("Join", |_, d: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Joined", d)])
        });
    if keyed {
        member.unique("handle", |s| s.handle.clone(), Unique::tenant());
    }
    member
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reindex_stores_the_keys_of_aggregates_from_before_the_key_was_declared() {
    let kernel = TestKernel::new(&["club.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let join = |id: &str| {
        let data = json!({ "handle": "max" });
        send(
            &kernable,
            Submit::new("club", "Member", id, "Join").data(data),
        )
    };
    let before = kernable.worker(&members(false));
    let running = spawn(&before);
    before.ready().await;
    assert_eq!(result(&join("m1").await), committed());
    before.stop();
    running.await.unwrap();

    let after = kernable.worker(&members(true));
    spawn(&after);
    after.ready().await;
    // m1 holds its handle only once it ran a command; a member without events holds nothing.
    let outcomes = kernable
        .reindex(Reindex::new("club", "Member", ["m1", "m0"]))
        .await
        .unwrap();
    let ids: Vec<&str> = outcomes.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["m1", "m0"]);
    for (_, outcome) in &outcomes {
        assert_eq!(result(outcome), committed());
        assert!(outcome.events().is_empty());
    }
    assert_eq!(
        result(&join("m2").await),
        rejected("unique_taken", "club.Member key handle is taken")
    );
    // Another tenant's members are reindexed in theirs.
    let other = kernable
        .reindex(Reindex::new("club", "Member", ["m1"]).tenant("t2"))
        .await
        .unwrap();
    assert_eq!(result(&other[0].1), committed());
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregates_of_one_domain_in_separate_workers_are_each_reindexed() {
    let kernel = TestKernel::new(&["club.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let club = domain("club");
    let member = club.aggregate("Member", Member::default());
    member
        .apply("Joined", |s, d: Value, _evt, _ctx| {
            s.handle = Some(field(&d, "handle"))
        })
        .command("Join", |_, d: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Joined", d)])
        })
        .unique("handle", |s| s.handle.clone(), Unique::tenant());
    let guest = club.aggregate("Guest", Member::default());
    guest
        .apply("Visited", |s, d: Value, _evt, _ctx| {
            s.handle = Some(field(&d, "handle"))
        })
        .command("Visit", |_, d: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Visited", d)])
        })
        .unique("handle", |s| s.handle.clone(), Unique::tenant());
    let workers = [kernable.worker(&member), kernable.worker(&guest)];
    for worker in &workers {
        spawn(worker);
        worker.ready().await;
    }
    // Each aggregate's own `_reindex_<type>` reaches the worker that runs it.
    for (aggregate, id) in [("Member", "m1"), ("Guest", "g1"), ("Member", "m2")] {
        let outcomes = kernable
            .reindex(Reindex::new("club", aggregate, [id]))
            .await
            .unwrap();
        assert_eq!(result(&outcomes[0].1), committed(), "{aggregate}");
    }
    // Keys are per aggregate type: a member and a guest may share a handle.
    let data = json!({ "handle": "max" });
    let join = Submit::new("club", "Member", "m1", "Join").data(&data);
    assert_eq!(result(&send(&kernable, join).await), committed());
    let visit = Submit::new("club", "Guest", "g1", "Visit").data(&data);
    assert_eq!(result(&send(&kernable, visit).await), committed());
    let join = Submit::new("club", "Member", "m2", "Join").data(&data);
    assert_eq!(
        result(&send(&kernable, join).await),
        rejected("unique_taken", "club.Member key handle is taken")
    );
}

#[test]
#[should_panic(expected = "_reindex_Guest: command types starting with _reindex_ are reserved")]
fn command_types_starting_with_reindex_are_reserved() {
    assert_eq!(reindex_type("Member"), "_reindex_Member");
    let member = domain("club").aggregate("Member", Member::default());
    // Plain `_reindex` is a name like any other; another aggregate's built-in one is not.
    member.command("_reindex", |_, _: Value, _cmd, _ctx| Ok(vec![]));
    member.command("_reindex_Guest", |_, _: Value, _cmd, _ctx| Ok(vec![]));
}

#[test]
#[should_panic(expected = "club.Member declares a key handle already")]
fn a_key_name_is_declared_once() {
    members(true).unique("handle", |_| None, Unique::global());
}

#[test]
fn a_fact_may_share_a_keys_name() {
    let member = members(true).fact("handle", |s| s.handle.is_some());
    let state = Member {
        handle: Some("max".into()),
    };
    let index = member.index(&state).unwrap();
    assert_eq!(index.keys[0].name, "handle");
    assert_eq!(index.facts, ["handle"]);
}

#[test]
#[should_panic(expected = "club.Member declares a fact active already")]
fn a_fact_name_is_declared_once() {
    members(false)
        .fact("active", |_| true)
        .fact("active", |_| false);
}

#[test]
fn key_and_fact_names_are_type_names_of_at_most_128_bytes() {
    let long = "k".repeat(129);
    for name in ["", "a.b", "a*", long.as_str()] {
        let declared = std::panic::catch_unwind(|| {
            members(false).fact(name, |_| true);
        });
        assert!(declared.is_err(), "{name:?} is no valid name");
    }
    members(false).fact(&"k".repeat(128), |_| true);
}

#[test]
#[should_panic(expected = "has no command Leave")]
fn requirements_are_declared_for_a_command_of_the_aggregate() {
    members(false).requires("Leave", |_, _: Value| None);
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Fund {
    isin: String,
    open: bool,
    wound_up: bool,
}

/// The rejection of an outcome: its message and the requirement it names.
fn missing(outcome: &Outcome) -> (String, Requirement) {
    match outcome {
        Outcome::Rejected(r) if r.code == "requirement_missing" => {
            (r.message.clone(), *r.requirement.clone().unwrap())
        }
        other => panic!("expected requirement_missing, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_requirement_names_its_aggregate_by_a_key_or_wants_a_fact_absent() {
    let kernel = TestKernel::new(&["funds.*"], &[]);
    kernel.start().await;
    let funds = domain("funds");
    let vehicle = funds.aggregate("Vehicle", Fund::default());
    vehicle
        .apply("Launched", |s, d: Value, _evt, _ctx| {
            s.isin = field(&d, "isin");
            s.open = true;
        })
        .apply("WoundUp", |s, _: Value, _evt, _ctx| {
            s.open = false;
            s.wound_up = true;
        })
        .command("Launch", |_, d: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Launched", d)])
        })
        .command("WindUp", |_, _: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("WoundUp", ())])
        })
        .unique(
            "isin",
            |s| (!s.isin.is_empty()).then(|| s.isin.to_lowercase()),
            Unique::tenant(),
        )
        .fact("open", |s| s.open)
        .fact("woundUp", |s| s.wound_up);
    let account = funds.aggregate("Account", false);
    account
        .apply("Opened", |s, _: Value, _evt, _ctx| *s = true)
        .command("Open", |_, _: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Opened", ())])
        })
        .command("Note", |_, _: Value, _cmd, _ctx| {
            Ok(vec![NewEvent::new("Noted", ())])
        })
        // By the vehicle's ISIN, normalised as its key is.
        .requires("Open", |_, d: Value| {
            let isin = field(&d, "vehicle").to_lowercase();
            Some(Requirement::by_key(
                "funds", "Vehicle", "isin", &isin, "open",
            ))
        })
        // Of the own domain, and absent.
        .requires("Note", |_, d: Value| {
            Some(Requirement::of("Vehicle", &field(&d, "vehicle"), "woundUp").absent())
        });
    let kernable = kernel.client();
    let worker = kernable.worker(&vehicle).aggregate(&account);
    spawn(&worker);
    worker.ready().await;
    let on = |aggregate: &str, id: &str, command: &str, data: Value| {
        send(
            &kernable,
            Submit::new("funds", aggregate, id, command).data(data),
        )
    };

    let open = || on("Account", "a1", "Open", json!({ "vehicle": "VEH-Lyra" }));
    let (message, named) = missing(&open().await);
    assert_eq!(message, "funds.Vehicle with key isin does not hold open");
    assert_eq!(
        (
            named.key.as_deref(),
            named.value,
            named.aggregate_id.as_str()
        ),
        (Some("isin"), None, ""),
        "the key, never its value"
    );
    let launch = json!({ "isin": "VEH-lyra" });
    assert!(on("Vehicle", "v1", "Launch", launch).await.is_committed());
    assert!(open().await.is_committed());

    let note = || on("Account", "a1", "Note", json!({ "vehicle": "v1" }));
    assert!(note().await.is_committed());
    assert!(
        on("Vehicle", "v1", "WindUp", json!({}))
            .await
            .is_committed()
    );
    let (message, named) = missing(&note().await);
    assert_eq!(message, "funds.Vehicle v1 holds woundUp");
    assert_eq!((named.domain.as_str(), named.absent), ("funds", true));
    // A vehicle without events holds nothing.
    let unknown = on("Account", "a2", "Note", json!({ "vehicle": "v9" })).await;
    assert!(unknown.is_committed());
}
