//! Workers: aggregates, snapshots, the request context, event schemas, and crashes of the
//! kernel, ported from the TypeScript SDK's tests.

mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::{TestKernel, eventually};
use kernable::{
    CommandContext, Error, FoldEvent, Kernable, Manifest, NewEvent, Outcome, Query, Rejected,
    RequestContext, StoredEvent, Submit, SubscribeOptions, Timestamp, Wait, Worker, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::Code;

fn spawn(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Account {
    email: Option<String>,
    tags: Vec<String>,
    status: String,
}

#[test]
fn apply_changes_the_state_in_place_or_replaces_it() {
    let crm = domain("Crm");
    let account = crm.aggregate(
        "Account",
        Account {
            status: "new".into(),
            ..Default::default()
        },
    );
    account.apply("Opened", |s, data: serde_json::Value, _evt, ctx| {
        s.email = data["email"].as_str().map(str::to_owned);
        s.tags.push("open".into());
        if !ctx.request.sender_identity.is_empty() {
            s.tags.push(format!("by {}", ctx.request.sender_identity));
        }
    });
    account.apply("Tagged", |s, _: serde_json::Value, evt, _ctx| {
        s.status = format!("{} {} {}", evt.event_type, evt.domain, evt.aggregate_type);
    });
    account.apply("Erased", |s, _: serde_json::Value, _evt, _ctx| {
        s.email = None
    });
    account.apply("Reset", |s, _: serde_json::Value, _evt, _ctx| {
        *s = Account {
            status: "reset".into(),
            ..Default::default()
        }
    });

    let opened = account
        .fold(
            [FoldEvent::new(
                "Opened",
                json!({ "email": "a@example.com" }),
            )],
            None,
        )
        .unwrap();
    assert_eq!(
        opened,
        Account {
            email: Some("a@example.com".into()),
            tags: vec!["open".into()],
            status: "new".into()
        }
    );
    assert_eq!(
        account.initial().tags.len(),
        0,
        "the initial state is untouched"
    );
    let erased = account
        .fold([FoldEvent::new("Erased", ())], Some(opened.clone()))
        .unwrap();
    assert_eq!(erased.email, None);
    assert_eq!(
        opened.email.as_deref(),
        Some("a@example.com"),
        "the state before is untouched"
    );
    let reset = account
        .fold(
            [
                FoldEvent::new("Opened", json!({ "email": "b@example.com" })),
                FoldEvent::new("Reset", ()),
            ],
            None,
        )
        .unwrap();
    assert_eq!(reset.status, "reset");
    // `apply` gets the request context of the causing command.
    let by_alice = account
        .fold(
            [FoldEvent {
                request: RequestContext {
                    sender_identity: "alice".into(),
                    ..Default::default()
                },
                ..FoldEvent::new("Opened", json!({ "email": "c@example.com" }))
            }],
            None,
        )
        .unwrap();
    assert_eq!(by_alice.tags, ["open", "by alice"]);
    // Without meta, `apply` learns the event's type and its own domain and aggregate.
    let tagged = account.fold([FoldEvent::new("Tagged", ())], None).unwrap();
    assert_eq!(tagged.status, "Tagged Crm Account");

    // A command handler can be called directly, e.g. in a unit test.
    account.command("Close", |s: &Account, _: serde_json::Value, cmd, ctx| {
        if s.email.is_none() {
            return Err(Rejected::new("no account").code("not_found"));
        }
        let what = format!("{}.{}.{}", cmd.domain, cmd.aggregate_type, cmd.command_type);
        Ok(vec![NewEvent::new(
            "Closed",
            json!({ "id": ctx.id(), "by": cmd.command_id, "what": what }),
        )])
    });
    let mut ctx = CommandContext::new("close-1", RequestContext::default(), Timestamp(0));
    assert!(matches!(
        account.handle(&Account::default(), "Close", (), &mut ctx),
        Err(Error::Rejected(r)) if r.code == "not_found"
    ));
    assert_eq!(
        account
            .handle(&reset, "Close", (), &mut ctx)
            .map(|e| e.len())
            .ok(),
        None
    );
    let closed = &account.handle(&opened, "Close", (), &mut ctx).unwrap()[0];
    assert_eq!(closed.event_type, "Closed");
    assert_eq!(
        (&closed.data["by"], &closed.data["what"]),
        (&json!("close-1"), &json!("Crm.Account.Close"))
    );
}

#[derive(Clone, Serialize, Deserialize)]
struct Counted {
    n: i64,
    version: u64,
    id: String,
}

#[derive(Serialize, Deserialize)]
struct By {
    by: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshots_shorten_replays_and_a_new_schema_rebuilds_from_the_events() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let applied = Arc::new(AtomicUsize::new(0));

    // v1 keeps the state as a struct, with what `apply` learns about the last event: every
    // increment is by 1, so the version equals n, whether folded from history or a snapshot.
    let v1 = domain("counters")
        .aggregate(
            "Counter",
            Counted {
                n: 0,
                version: 0,
                id: "c1".into(),
            },
        )
        .snapshot_every(3);
    let count = Arc::clone(&applied);
    v1.apply("Incremented", move |s, data: By, evt, _ctx| {
        count.fetch_add(1, Ordering::SeqCst);
        s.n += data.by;
        s.version = evt.version;
        s.id = evt.aggregate_id.clone();
        if (
            evt.event_type.as_str(),
            evt.domain.as_str(),
            evt.aggregate_type.as_str(),
        ) != ("Incremented", "counters", "Counter")
        {
            s.id = format!("{} {} {}", evt.event_type, evt.domain, evt.aggregate_type);
        }
    });
    v1.command("Increment", |s, data: By, cmd, _ctx| {
        assert_eq!(cmd.aggregate_id, "c1");
        if s.version as i64 != s.n || s.id != "c1" {
            return Err(Rejected::new(format!(
                "event meta {} {} {}",
                s.n, s.version, s.id
            )));
        }
        if s.n + data.by > 20 {
            return Err(Rejected::new("limit"));
        }
        Ok(vec![NewEvent::new("Incremented", By { by: data.by })])
    });
    v1.command("Twice", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![
            NewEvent::new("Incremented", By { by: 1 }),
            NewEvent::new("Incremented", By { by: 1 }),
        ])
    });
    // v2 keeps it as a number: old snapshots do not fit.
    let v2 = domain("counters")
        .aggregate("Counter", 0i64)
        .snapshot_every(3)
        .schema("2");
    v2.apply("Incremented", |n, data: By, _evt, _ctx| *n += data.by);
    v2.command("Increment", |n, data: By, _cmd, _ctx| {
        if n + data.by > 20 {
            return Err(Rejected::new("limit"));
        }
        Ok(vec![NewEvent::new("Incremented", By { by: data.by })])
    });
    let increment = |by: i64| {
        kernable.submit(Submit::new("counters", "Counter", "c1", "Increment").data(By { by }))
    };

    let worker = kernable.worker(&v1).reconnect(Duration::from_millis(100));
    let running = spawn(&worker);
    worker.ready().await;
    for _ in 0..10 {
        assert!(increment(1).await.unwrap().is_committed());
    }
    // Without snapshots, folding the history alone would apply 0 + 1 + ... + 9 = 45 events.
    // With one at versions 3, 6 and 9, a command folds at most two events, and taking a
    // snapshot applies its command's event: 3 × (0 + 1 + 2 + 1) + 0 for the tenth = 12.
    assert_eq!(applied.load(Ordering::SeqCst), 12);
    // A snapshot after several events gives each its own version (11 and 12 here).
    let twice = Submit::new("counters", "Counter", "c1", "Twice");
    assert!(kernable.submit(twice).await.unwrap().is_committed());
    assert!(increment(1).await.unwrap().is_committed());
    worker.stop();
    running.await.unwrap();

    let worker = kernable.worker(&v2).reconnect(Duration::from_millis(100));
    let running = spawn(&worker);
    worker.ready().await;
    assert!(!increment(8).await.unwrap().is_committed()); // 13 + 8 > 20: the state is right
    assert!(increment(7).await.unwrap().is_committed());
    assert!(!increment(1).await.unwrap().is_committed()); // 21 > 20
    assert!(increment(0).await.unwrap().is_committed()); // version 15: a v2 snapshot
    assert!(kernel.verify().ends_with("\nOK\n"));
    worker.stop();
    running.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn request_context_reaches_handlers_events_and_follow_up_commands() {
    let kernel = TestKernel::new(&["support.*"], &[]);
    kernel.start().await;
    let service = RequestContext {
        sender_identity: "svc-1".into(),
        attributes: [("channel".to_owned(), "api".to_owned())].into(),
        ..Default::default()
    };
    let kernable = kernable::Kernable::new(kernel.options().context(service.clone())).unwrap();

    let seen = Arc::new(Mutex::new(Vec::<RequestContext>::new()));
    let support = domain("support");
    let ticket = support.aggregate("Ticket", false);
    let applied = Arc::new(Mutex::new(Vec::<RequestContext>::new()));
    let a = Arc::clone(&applied);
    ticket.apply("Opened", move |open, _: serde_json::Value, _evt, ctx| {
        a.lock().unwrap().push(ctx.request.clone());
        *open = true
    });
    let s = Arc::clone(&seen);
    ticket.command("Open", move |_, _: serde_json::Value, _cmd, ctx| {
        s.lock().unwrap().push(ctx.request.clone());
        // Deterministic helpers: the same values on every run of this command.
        let at = ctx.now().millis();
        assert!((at - Timestamp::now().millis()).abs() < 5000 && ctx.deadline().millis() > at);
        Ok(vec![NewEvent::new(
            "Opened",
            json!({ "at": at, "ids": [ctx.id(), ctx.id()] }),
        )])
    });
    let s = Arc::clone(&seen);
    ticket.command("Note", move |_, _: serde_json::Value, _cmd, ctx| {
        s.lock().unwrap().push(ctx.request.clone());
        Ok(vec![NewEvent::new("Noted", ())])
    });

    let replayed = Arc::new(Mutex::new(Vec::new()));
    let (r, k) = (Arc::clone(&replayed), kernable.clone());
    let notes =
        support
            .reactor("notes")
            .on("support.Opened", move |_: serde_json::Value, evt, ctx| {
                let (r, k) = (Arc::clone(&r), k.clone());
                async move {
                    r.lock().unwrap().push((ctx.replaying, ctx.request.clone()));
                    let note = Submit::new("support", "Ticket", &evt.aggregate_id, "Note")
                        .id(format!("note-{}", evt.aggregate_id))
                        .context(ctx.follow())
                        .wait(Wait::Committed);
                    k.submit(note).await?;
                    Ok(())
                }
            });
    let notes = kernable.listener(&notes);
    let worker = kernable
        .worker(&ticket)
        .reconnect(Duration::from_millis(100));
    let running = spawn(&worker);
    let listening = {
        let notes = notes.clone();
        tokio::spawn(async move { notes.run().await })
    };
    worker.ready().await;
    notes.ready().await;

    let opened = kernable
        .submit(
            Submit::new("support", "Ticket", "t1", "Open")
                .id("open-t1")
                .context(RequestContext {
                    sender_session: "s-9".into(),
                    trace_id: "tr-1".into(),
                    ..service.clone()
                }),
        )
        .await
        .unwrap();
    let event = &opened.events()[0];
    let ids = &event.data["ids"];
    let derived = |n| kernable_core::signing::derived_id("open-t1", n);
    assert_eq!(ids, &json!([derived(0), derived(1)]));

    let request = RequestContext {
        sender_session: "s-9".into(),
        trace_id: "tr-1".into(),
        correlation_id: "open-t1".into(),
        ..service.clone()
    };
    assert_eq!(event.request, request);
    // The reactor's follow-up keeps sender and correlation and names its cause.
    let follow_up = RequestContext {
        causation_id: "open-t1".into(),
        ..request.clone()
    };
    assert_eq!(*seen.lock().unwrap(), [request.clone(), follow_up.clone()]);
    assert_eq!(*replayed.lock().unwrap(), [(false, request.clone())]);
    // The Note command folded the Opened event with the request that caused it.
    assert_eq!(*applied.lock().unwrap(), std::slice::from_ref(&request));

    let events = Arc::new(Mutex::new(Vec::<StoredEvent>::new()));
    let e = Arc::clone(&events);
    let subscription = kernable.subscribe(SubscribeOptions::default(), move |event| {
        e.lock().unwrap().push(event);
        async { Ok(()) }
    });
    eventually(|| events.lock().unwrap().len() >= 2).await;
    let last = events.lock().unwrap()[1].position;
    eventually(|| subscription.position() == last).await;
    assert_eq!(subscription.position(), last);
    subscription.stop().await;
    let requests: Vec<_> = events
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.request.clone())
        .collect();
    assert_eq!(requests, [request, follow_up]);

    // An expected version that no longer matches rejects the command.
    let stale = kernable
        .submit(
            Submit::new("support", "Ticket", "t1", "Note").context(RequestContext {
                expected_version: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(matches!(stale, Outcome::Rejected(r) if r.code == "version_mismatch"));

    // A request's own context replaces the client's: an anonymous one does not act as the service.
    let anonymous = kernable
        .submit(Submit::new("support", "Ticket", "t1", "Note").context(RequestContext::default()))
        .await
        .unwrap();
    let event = &anonymous.events()[0];
    assert_eq!(
        event.request,
        RequestContext {
            correlation_id: event.command_id.clone(),
            ..Default::default()
        }
    );
    assert!(kernel.verify().contains("\nOK"));
    worker.stop();
    notes.stop();
    running.await.unwrap();
    listening.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_event_schema_is_upcast_for_aggregates_and_listeners() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "300"]);
    kernel.start().await;
    let kernable = kernel.client();
    let add = |data: serde_json::Value| {
        kernable.submit(
            Submit::new("shop", "Cart", "c1", "AddItem")
                .data(data)
                .wait(Wait::Committed),
        )
    };

    // Version 1 stores a quantity as `qty`.
    let v1 = domain("shop");
    let cart_v1 = v1.aggregate("Cart", 0i64);
    cart_v1.apply("ItemAdded", |n, data: serde_json::Value, _evt, _ctx| {
        *n += data["qty"].as_i64().unwrap()
    });
    cart_v1.command("AddItem", |_, data: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "ItemAdded",
            json!({ "sku": "apple", "qty": data["qty"] }),
        )])
    });

    // Version 2 renames it to `count` and caps a cart at 5 items.
    let v2 = domain("shop");
    v2.event("ItemAdded", 2)
        .upcast(1, |d| json!({ "sku": d["sku"], "count": d["qty"] }));
    let cart_v2 = v2.aggregate("Cart", 0i64);
    cart_v2.apply("ItemAdded", |n, data: serde_json::Value, _evt, _ctx| {
        *n += data["count"].as_i64().unwrap()
    });
    cart_v2.command("AddItem", |n, data: serde_json::Value, _cmd, _ctx| {
        let count = data["count"].as_i64().unwrap();
        if n + count > 5 {
            return Err(Rejected::new("at most 5 items").code("full"));
        }
        Ok(vec![NewEvent::new(
            "ItemAdded",
            json!({ "sku": "pear", "count": count }),
        )])
    });

    let archived = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::clone(&archived);
    let archive = v2.reactor("archive").start(0).order(0).on_raw(
        "shop.*",
        move |data: serde_json::Value, evt, _ctx| {
            a.lock().unwrap().push((evt.schema, data));
            async { Ok(()) }
        },
    );
    let counts = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&counts);
    // The listeners upcast their own domain's events.
    let current = v2.read_model("current").on(
        "shop.ItemAdded",
        move |data: serde_json::Value, _evt, _ctx| {
            c.lock().unwrap().push(data);
            async { Ok(()) }
        },
    );
    let stale = v1
        .read_model("stale")
        .on("shop.ItemAdded", |_: serde_json::Value, _evt, _ctx| async {
            Ok(())
        });

    let old = kernable
        .worker(&cart_v1)
        .reconnect(Duration::from_millis(50));
    let worker = kernable
        .worker(&cart_v2)
        .reconnect(Duration::from_millis(50));
    let outdated = kernable
        .worker(&cart_v1)
        .reconnect(Duration::from_millis(50));
    let mut running = vec![spawn(&old)];
    let (archive, current, stale) = (
        kernable.listener(&archive),
        kernable.listener(&current),
        kernable.listener(&stale),
    );
    for l in [&archive, &current, &stale] {
        let l = l.clone();
        running.push(tokio::spawn(async move { l.run().await }));
    }
    old.ready().await;
    for l in [&archive, &current, &stale] {
        l.ready().await;
    }
    assert!(add(json!({ "qty": 3 })).await.unwrap().is_committed());
    old.stop();

    running.push(spawn(&worker));
    worker.ready().await;
    // The v1 event is upcast before `apply`: 3 + 3 > 5.
    assert_eq!(
        add(json!({ "count": 3 })).await.unwrap(),
        Outcome::Rejected(Rejected::new("at most 5 items").code("full"))
    );
    let added = add(json!({ "count": 2 })).await.unwrap();
    assert_eq!(added.events()[0].schema, 2);

    // An outdated worker is refused: it would write version 1 next to version 2.
    running.push(spawn(&outdated));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), outdated.ready())
            .await
            .is_err()
    );
    outdated.stop();
    // So is one that writes it at version 1 without applying it: its domain declares it.
    let declared = domain("shop");
    declared.event("ItemAdded", 1);
    let writer = declared.aggregate("Cart", 0i64);
    writer.command("AddItem", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("ItemAdded", json!({ "sku": "fig" }))])
    });
    let blind = kernable
        .worker(&writer)
        .reconnect(Duration::from_millis(50));
    running.push(spawn(&blind));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), blind.ready())
            .await
            .is_err()
    );
    blind.stop();

    eventually(|| counts.lock().unwrap().len() >= 2).await;
    assert_eq!(
        *counts.lock().unwrap(),
        [
            json!({ "sku": "apple", "count": 3 }),
            json!({ "sku": "pear", "count": 2 })
        ]
    );
    assert_eq!(
        *archived.lock().unwrap(),
        [
            (1, json!({ "sku": "apple", "qty": 3 })),
            (2, json!({ "sku": "pear", "count": 2 }))
        ]
    );
    // The listener that knows only version 1 stopped before the version 2 event.
    assert_eq!(stale.position(), 1);
    for w in [&old, &worker, &outdated] {
        w.stop();
    }
    for l in [&archive, &current, &stale] {
        l.stop();
    }
    for r in running {
        r.await.unwrap();
    }
}

#[test]
#[should_panic(expected = "from 1")]
fn event_schemas_start_at_version_1() {
    domain("shop").event("ItemAdded", 0);
}

#[test]
#[should_panic(expected = "unqualified")]
fn event_types_are_unqualified() {
    domain("shop").event("shop.ItemAdded", 2);
}

#[test]
#[should_panic(expected = "between 1 and 1")]
fn upcasts_start_below_the_current_version() {
    domain("shop").event("ItemAdded", 2).upcast(2, |d| d);
}

#[test]
fn domain_names_follow_the_rules() {
    domain("Shop");
    domain("order-mgmt_2");
    assert!(std::panic::catch_unwind(|| domain("1shop")).is_err());
    assert!(std::panic::catch_unwind(|| domain("sh op")).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn kernel_crash_mid_command_worker_reconnects_and_commits_exactly_once() {
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let (open, gate) = std::sync::mpsc::channel::<()>();
    let gate = Mutex::new(Some((entered_tx, gate)));

    let counter = domain("counters").aggregate("Counter", 0i64);
    counter.apply("Incremented", |n, _: serde_json::Value, _evt, _ctx| *n += 1);
    counter.command("Increment", move |_, _: serde_json::Value, _cmd, _ctx| {
        if let Some((entered, gate)) = gate.lock().unwrap().take() {
            entered.send(()).unwrap();
            let _ = gate.recv(); // still running when the kernel dies
        }
        Ok(vec![NewEvent::new("Incremented", ())])
    });

    let kernel = TestKernel::new(&["counters.*"], &["--lease-ttl-ms", "500"]);
    kernel.start().await;
    let kernable = kernable::Kernable::new(kernel.options().tenant("t1")).unwrap();
    let worker = kernable
        .worker(&counter)
        .reconnect(Duration::from_millis(100));
    let running = spawn(&worker);
    let increment =
        |id: &str| kernable.submit(Submit::new("counters", "Counter", "c1", "Increment").id(id));
    worker.ready().await;

    let lost = {
        let kernable = kernable.clone();
        tokio::spawn(async move {
            kernable
                .submit(Submit::new("counters", "Counter", "c1", "Increment").id("inc-1"))
                .await
        })
    };
    tokio::task::spawn_blocking(move || entered.recv().unwrap())
        .await
        .unwrap();
    kernel.kill();
    assert!(lost.await.unwrap().is_err(), "the client sees the crash");

    kernel.start().await;
    open.send(()).unwrap(); // the stale execution finishes into a dead session
    let retried = increment("inc-1").await.unwrap(); // ... and resubmits with the same ID
    assert_eq!(
        retried
            .events()
            .iter()
            .map(|e| e.version)
            .collect::<Vec<_>>(),
        [1]
    );
    let next = increment("inc-2").await.unwrap();
    assert_eq!(
        next.events().iter().map(|e| e.version).collect::<Vec<_>>(),
        [2]
    );
    assert!(kernel.verify().lines().any(|l| l == "OK"));
    worker.stop();
    running.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_without_a_read_model_are_refused() {
    let kernel = TestKernel::new(&["shop.*"], &["--query-timeout-ms", "200"]);
    kernel.start().await;
    let kernable = kernel.client();
    let answer: Result<serde_json::Value, _> = kernable.query(Query::new("shop", "GetCart")).await;
    assert!(answer.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_need_a_registered_aggregate_and_a_membership_drops_those_it_no_longer_lists() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", 0i64);
    cart.command("AddItem", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("ItemAdded", ())])
    });
    let wishlist = shop.aggregate("Wishlist", 0i64);
    wishlist.command("Wish", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Wished", ())])
    });
    async fn submit(k: &Kernable, aggregate: &str, command: &str) -> Result<Outcome, Error> {
        k.submit(Submit::new("shop", aggregate, "x1", command))
            .await
    }
    let is_not_found = |r: &Result<Outcome, Error>| matches!(r, Err(Error::Status(s)) if s.code() == Code::NotFound);

    // No worker ever registered the cart and no membership lists it: the kernel refuses its
    // commands.
    let v1 = Kernable::new(kernel.options().service("shop")).unwrap();
    let refused = submit(&v1, "Cart", "AddItem").await;
    assert!(is_not_found(&refused), "{refused:?}");

    // A manifest needs a service: from itself or from the connection.
    let no_service = kernel.client().apply(Manifest::new().worker(&cart)).await;
    assert!(
        matches!(no_service, Err(Error::Config(_))),
        "{no_service:?}"
    );

    // v1 of the service runs both aggregates.
    v1.apply(
        Manifest::new()
            .worker(&cart)
            .worker(&wishlist)
            .metadata("version", "1"),
    )
    .await
    .unwrap();
    let worker = v1.worker(&cart).aggregate(&wishlist);
    let running = spawn(&worker);
    worker.ready().await;
    assert!(submit(&v1, "Cart", "AddItem").await.unwrap().is_committed());
    assert!(
        submit(&v1, "Wishlist", "Wish")
            .await
            .unwrap()
            .is_committed()
    );
    worker.stop();
    running.await.unwrap();

    // v2 has no wishlist any more: once no worker runs it, it is forgotten, while the cart's
    // commands still run.
    let v2 = Kernable::new(kernel.options().service("shop")).unwrap();
    v2.apply(
        Manifest::new()
            .service("shop")
            .worker(&cart)
            .metadata("version", "2"),
    )
    .await
    .unwrap();
    let worker = v2.worker(&cart);
    let running = spawn(&worker);
    worker.ready().await;
    let mut refused = submit(&v2, "Wishlist", "Wish").await;
    for _ in 0..100 {
        if is_not_found(&refused) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        refused = submit(&v2, "Wishlist", "Wish").await;
    }
    assert!(is_not_found(&refused), "{refused:?}");
    assert!(submit(&v2, "Cart", "AddItem").await.unwrap().is_committed());
    worker.stop();
    running.await.unwrap();
}
