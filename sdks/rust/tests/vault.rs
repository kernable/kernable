//! The vault: personal fields and secrets are declared on the domain; the SDK keeps them out of
//! the signed log, handlers and allowed listeners see them in clear, and an erasing event
//! deletes them for good. Ported from the TypeScript SDK's tests.

mod common;

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::TestKernel;
use kernable::{
    Aggregate, Domain, Error, NewEvent, Outcome, Query, Rejected, StoredEventMeta, Submit, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn sha256(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn field(d: &Value, name: &str) -> String {
    d[name].as_str().unwrap_or_default().to_owned()
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Account {
    email: Option<String>,
    password_hash: String,
}

#[derive(Deserialize)]
struct Register {
    id: String,
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct Registered {
    email: Option<String>,
    #[serde(rename = "passwordHash")]
    password_hash: String,
}

#[derive(Deserialize)]
struct Password {
    password: String,
}

#[derive(Deserialize)]
struct Id {
    id: String,
}

fn identity_domain() -> (Domain, Aggregate<Account>) {
    let identity = domain("identity");
    identity
        .command("RegisterAccount")
        .personal(&["email"], |d, _| field(d, "id"))
        .secret(&["password"]);
    identity
        .event("AccountRegistered", 1)
        .personal(&["email"], |d, _| field(d, "accountId"));
    identity
        .event("AccountErased", 1)
        .erases(|d, _| field(d, "accountId"));
    let account = identity.aggregate("Account", Account::default());
    account.apply("AccountRegistered", |s, data: Registered, _evt, _ctx| {
        *s = Account {
            email: data.email,
            password_hash: data.password_hash,
        }
    });
    account.apply("AccountErased", |s, _: Value, _evt, _ctx| {
        *s = Account::default()
    });
    account.command("RegisterAccount", |_, data: Register, _cmd, _ctx| {
        // The handler gets the secret in clear and keeps only a hash.
        let registered = json!({
            "accountId": data.id,
            "email": data.email,
            "passwordHash": sha256(&data.password),
        });
        Ok(vec![NewEvent::new("AccountRegistered", registered)])
    });
    account.command("Login", |s, data: Password, _cmd, _ctx| {
        if sha256(&data.password) != s.password_hash {
            return Err(Rejected::new("wrong password").code("denied"));
        }
        Ok(vec![NewEvent::new("LoggedIn", ())])
    });
    account.command("EraseAccount", |_, data: Id, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "AccountErased",
            json!({ "accountId": data.id }),
        )])
    });
    identity.command("Login").secret(&["password"]);
    (identity, account)
}

#[tokio::test(flavor = "multi_thread")]
async fn personal_fields_and_secrets_live_in_the_vault_and_an_erasing_event_deletes_them() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let (identity, account) = identity_domain();
    let kernable = kernel.client().with_domain(&identity);
    let emails = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let e = Arc::clone(&emails);
    let accounts = identity.read_model("accounts").on(
        "identity.AccountRegistered",
        move |data: Value, evt, _ctx| {
            e.lock()
                .unwrap()
                .insert(evt.aggregate_id, field(&data, "email"));
            async { Ok(()) }
        },
    );
    let e = Arc::clone(&emails);
    accounts.on("identity.AccountErased", move |_: Value, evt, _ctx| {
        e.lock().unwrap().remove(&evt.aggregate_id);
        async { Ok(()) }
    });
    let e = Arc::clone(&emails);
    accounts.query("identity.GetEmail", move |data: Id, _qry, _ctx| {
        let email = e.lock().unwrap().get(&data.id).cloned();
        async move { Ok(email.ok_or_else(|| Rejected::new("no such account").code("not_found"))?) }
    });
    let worker = kernable.worker(&account);
    let accounts = kernable.listener(&accounts);
    let mut running = vec![
        {
            let w = worker.clone();
            tokio::spawn(async move { w.run().await })
        },
        {
            let a = accounts.clone();
            tokio::spawn(async move { a.run().await })
        },
    ];
    let send = |command_type: &str, data: Value| {
        kernable.submit(Submit::new("identity", "Account", "a1", command_type).data(data))
    };
    let get_email =
        || kernable.query::<String>(Query::new("identity", "GetEmail").data(json!({ "id": "a1" })));
    worker.ready().await;
    accounts.ready().await;

    let registered = send(
        "RegisterAccount",
        json!({ "id": "a1", "email": "max@firma.de", "password": "correct horse" }),
    )
    .await
    .unwrap();
    // The submitter is no reader of what the vault keeps: it gets references only.
    let event = &registered.events()[0];
    assert_eq!(
        event.data,
        json!({ "accountId": "a1", "email": null, "passwordHash": sha256("correct horse") })
    );
    assert_eq!(event.redacted, ["email"]);
    assert_eq!(get_email().await.unwrap(), "max@firma.de");
    assert_eq!(
        send("Login", json!({ "password": "wrong" })).await.unwrap(),
        Outcome::Rejected(Rejected::new("wrong password").code("denied"))
    );
    assert!(
        send("Login", json!({ "password": "correct horse" }))
            .await
            .unwrap()
            .is_committed()
    );

    // Neither the email nor the password is in any file of the kernel in clear.
    for entry in std::fs::read_dir(&kernel.dir).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        for secret in ["max@firma.de", "correct horse"] {
            let found = bytes.windows(secret.len()).any(|w| w == secret.as_bytes());
            assert!(!found, "{secret} is stored in clear in {}", path.display());
        }
    }

    // Erasing: the read model drops the account; a rebuilt one reads null where the email was.
    assert!(
        send("EraseAccount", json!({ "id": "a1" }))
            .await
            .unwrap()
            .is_committed()
    );
    assert!(matches!(get_email().await, Err(Error::Rejected(r)) if r.message == "no such account"));
    let replayed = Arc::new(Mutex::new(Vec::<(Value, StoredEventMeta)>::new()));
    let r = Arc::clone(&replayed);
    let rebuilt = identity.read_model("rebuilt").on(
        "identity.AccountRegistered",
        move |data: Value, evt, _ctx| {
            r.lock().unwrap().push((data, evt));
            async { Ok(()) }
        },
    );
    let rebuilt = kernable.listener(&rebuilt);
    running.push({
        let rebuilt = rebuilt.clone();
        tokio::spawn(async move { rebuilt.run().await })
    });
    rebuilt.ready().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let replayed = replayed.lock().unwrap().clone();
    assert_eq!(replayed.len(), 1);
    assert_eq!(
        (&replayed[0].0["email"], &replayed[0].1.redacted),
        (&Value::Null, &vec!["email".to_owned()])
    );
    for s in [&accounts, &rebuilt] {
        s.stop();
    }
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erased_value_is_in_no_file_of_a_read_models_store() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let (identity, account) = identity_domain();
    let kernable = kernel.client().with_domain(&identity);
    let path = kernel.dir.join("emails.db");
    let emails = identity
        .read_model("emails")
        .store(&format!("sqlite:{}", path.display()), "1")
        .on("AccountRegistered", |data: Value, evt, ctx| async move {
            let store = ctx.store.expect("a store");
            Ok(store.set("emails", &evt.aggregate_id, &field(&data, "email"))?)
        })
        .on("AccountErased", |_: Value, evt, ctx| async move {
            Ok(ctx
                .store
                .expect("a store")
                .remove("emails", &evt.aggregate_id)?)
        });
    let worker = kernable.worker(&account);
    let emails = kernable.listener(&emails);
    let running = [
        {
            let w = worker.clone();
            tokio::spawn(async move { w.run().await })
        },
        {
            let e = emails.clone();
            tokio::spawn(async move { e.run().await })
        },
    ];
    worker.ready().await;
    emails.ready().await;
    let send = |command_type: &str, data: Value| {
        kernable.submit(Submit::new("identity", "Account", "a1", command_type).data(data))
    };
    // Every file of the store: the database, its write-ahead log and its shared memory.
    let stored = || {
        ["", "-wal", "-shm"].into_iter().any(|suffix| {
            let bytes = std::fs::read(format!("{}{suffix}", path.display())).unwrap_or_default();
            bytes.windows(12).any(|w| w == b"max@firma.de")
        })
    };

    let registered = json!({ "id": "a1", "email": "max@firma.de", "password": "pw" });
    assert!(
        send("RegisterAccount", registered)
            .await
            .unwrap()
            .is_committed()
    );
    assert!(stored(), "the read model keeps the email");
    // Settled: the batch with the erasing event is acknowledged.
    let erased = send("EraseAccount", json!({ "id": "a1" })).await.unwrap();
    assert!(erased.is_committed());
    assert!(
        !stored(),
        "the erased email is still in a file of the store"
    );
    worker.stop();
    emails.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_personal_command_field_in_an_event_that_does_not_declare_it_is_rejected() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let (identity, account) = identity_domain();
    identity
        .command("ChangeEmail")
        .personal(&["email"], |_, id| id.to_owned());
    // The event forgets to declare the email personal.
    account.command("ChangeEmail", |_, data: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "EmailChanged",
            json!({ "email": data["email"] }),
        )])
    });
    let kernable = kernel.client().with_domain(&identity);
    let worker = kernable.worker(&account);
    let running = {
        let w = worker.clone();
        tokio::spawn(async move { w.run().await })
    };
    worker.ready().await;
    let send = |command_type: &str, data: Value| {
        kernable.submit(Submit::new("identity", "Account", "a1", command_type).data(data))
    };

    // Declared on the command and on its event: committed.
    let registered = json!({ "id": "a1", "email": "max@firma.de", "password": "pw" });
    assert!(
        send("RegisterAccount", registered)
            .await
            .unwrap()
            .is_committed()
    );
    let changed = send("ChangeEmail", json!({ "email": "moritz@firma.de" }))
        .await
        .unwrap();
    assert_eq!(
        changed,
        Outcome::Rejected(
            Rejected::new(
                "ChangeEmail declares email personal, but EmailChanged does not: it would be \
                 stored in clear (declare it with Domain.event(\"EmailChanged\", …).personal)"
            )
            .code("personal_in_clear")
        )
    );
    // Without the value (null or absent), nothing leaks: committed.
    let cleared = send("ChangeEmail", json!({ "email": null })).await.unwrap();
    assert!(cleared.is_committed(), "{cleared:?}");
    worker.stop();
    running.await.unwrap();
}

#[test]
#[should_panic(expected = "unqualified")]
fn command_definitions_take_unqualified_types() {
    identity_domain().0.command("shop.Register");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erasing_event_is_never_parked() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let (identity, account) = identity_domain();
    let kernable = kernel.client().with_domain(&identity);
    let failures = Arc::new(AtomicUsize::new(0));
    let f = Arc::clone(&failures);
    // Parks at the first failure, but not an erasure: that must get through, or the email
    // would stay in the store.
    let emails = identity
        .reactor("emails")
        .park_after(1)
        .reconnect(Duration::from_millis(20))
        .store("memory:", "1")
        .on("AccountRegistered", |data: Value, evt, ctx| async move {
            let store = ctx.store.expect("a store");
            Ok(store.set("emails", &evt.aggregate_id, &field(&data, "email"))?)
        })
        .on("AccountErased", move |_: Value, evt, ctx| {
            let f = Arc::clone(&f);
            async move {
                if f.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(Rejected::new("not yet").into());
                }
                Ok(ctx
                    .store
                    .expect("a store")
                    .remove("emails", &evt.aggregate_id)?)
            }
        });
    let worker = kernable.worker(&account);
    let emails = kernable.listener(&emails);
    let running = [
        {
            let w = worker.clone();
            tokio::spawn(async move { w.run().await })
        },
        {
            let e = emails.clone();
            tokio::spawn(async move { e.run().await })
        },
    ];
    worker.ready().await;
    emails.ready().await;
    let send = |command_type: &str, data: Value| {
        kernable.submit(Submit::new("identity", "Account", "a1", command_type).data(data))
    };
    let registered = json!({ "id": "a1", "email": "max@firma.de", "password": "pw" });
    assert!(
        send("RegisterAccount", registered)
            .await
            .unwrap()
            .is_committed()
    );
    assert!(
        send("EraseAccount", json!({ "id": "a1" }))
            .await
            .unwrap()
            .is_committed()
    );
    assert_eq!(
        failures.load(Ordering::SeqCst),
        3,
        "tried until it got through"
    );
    let store = emails.opened_store().unwrap();
    assert_eq!(store.get::<String>("emails", "a1").unwrap(), None);
    let (ok, parked) = kernel.cli(&["listeners", "parked", "{store}"]);
    assert!(ok);
    assert_eq!(parked, "", "nothing parked");
    worker.stop();
    emails.stop();
    for r in running {
        r.await.unwrap();
    }
}
