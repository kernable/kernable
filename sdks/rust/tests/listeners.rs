//! Listeners: requests from handlers, settling, stores and middleware, ported from the
//! TypeScript SDK's tests.

mod common;

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::{TestKernel, eventually};
use kernable::{
    Check, CheckKind, CheckOutcome, Error, Kernable, ListenerHandle, Manifest, MiddlewareOptions,
    NewEvent, Outcome, Query, QueryOutcome, Rejected, RequestContext, Submit, Wait, Worker, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

fn spawn_worker(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

fn spawn(listener: &ListenerHandle) -> tokio::task::JoinHandle<()> {
    let listener = listener.clone();
    tokio::spawn(async move { listener.run().await })
}

#[derive(Deserialize)]
struct Id {
    id: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reactors_settling_submit_waits_for_the_read_models_before_it_not_for_itself() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "2000"]);
    kernel.start().await;
    let kernable = kernel.client();

    let shop = domain("shop");
    let order = shop.aggregate("Order", false);
    order.apply(
        "Confirmed",
        |confirmed, _: serde_json::Value, _evt, _ctx| *confirmed = true,
    );
    order.command("Place", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Placed", ())])
    });
    order.command("Confirm", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Confirmed", ())])
    });
    let confirmed = Arc::new(Mutex::new(HashSet::new()));
    let (c, k) = (Arc::clone(&confirmed), kernable.clone());
    let orders =
        shop.read_model("orders")
            .on("shop.Confirmed", move |_: serde_json::Value, evt, _ctx| {
                c.lock().unwrap().insert(evt.aggregate_id);
                async { Ok(()) }
            });
    let c = Arc::clone(&confirmed);
    orders.query("shop.IsConfirmed", move |data: Id, _qry, _ctx| {
        let (c, k) = (Arc::clone(&c), k.clone());
        async move {
            if data.id == "self" {
                return Ok(k
                    .query::<bool>(Query::new("shop", "IsConfirmed").data(json!({ "id": "x" })))
                    .await?);
            }
            Ok(c.lock().unwrap().contains(&data.id))
        }
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (s, k) = (Arc::clone(&seen), kernable.clone());
    let confirmations =
        shop.reactor("confirmations")
            .on("shop.Placed", move |_: serde_json::Value, evt, _ctx| {
                let (s, k) = (Arc::clone(&s), k.clone());
                async move {
                    let started = Instant::now();
                    // Settles on the read model (order 0), not on this reactor (order 10).
                    let confirm = Submit::new("shop", "Order", &evt.aggregate_id, "Confirm")
                        .id(format!("confirm-{}", evt.aggregate_id));
                    k.submit(confirm).await?;
                    let answer: bool = k
                        .query(
                            Query::new("shop", "IsConfirmed")
                                .data(json!({ "id": evt.aggregate_id })),
                        )
                        .await?;
                    s.lock().unwrap().push(answer);
                    s.lock()
                        .unwrap()
                        .push(started.elapsed() < Duration::from_secs(1));
                    Ok(())
                }
            });

    let worker = kernable.worker(&order);
    let (orders, confirmations) = (
        kernable.listener(&orders),
        kernable.listener(&confirmations),
    );
    let running = [spawn_worker(&worker), spawn(&orders), spawn(&confirmations)];
    worker.ready().await;
    orders.ready().await;
    confirmations.ready().await;
    kernable
        .submit(Submit::new("shop", "Order", "o1", "Place"))
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [true, true]);

    // A query handler asking its own read model is refused at once: the inner query fails, so
    // the handler fails and the outer query is refused.
    let refused = kernable
        .query::<bool>(Query::new("shop", "IsConfirmed").data(json!({ "id": "self" })))
        .await;
    assert!(
        matches!(&refused, Err(Error::Rejected(r)) if r.message.contains("answered only by orders itself")),
        "{refused:?}"
    );
    worker.stop();
    orders.stop();
    confirmations.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_listener_still_counts_a_settling_submit_reports_not_settled() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "300"]);
    kernel.start().await;
    let kernable = kernel.client();
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", 0i64);
    cart.command("Add", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Added", ())])
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    let audit = shop
        .reactor("audit")
        .on("shop.Added", move |_: serde_json::Value, evt, _ctx| {
            s.lock().unwrap().push(evt.aggregate_id);
            async { Ok(()) }
        });
    let audit = kernable.listener(&audit);
    let worker = kernable.worker(&cart);
    let running = [spawn_worker(&worker), spawn(&audit)];
    let add =
        |id: &str, wait: Wait| kernable.submit(Submit::new("shop", "Cart", id, "Add").wait(wait));
    worker.ready().await;
    audit.ready().await;
    assert!(add("c1", Wait::Settled).await.unwrap().is_committed());
    assert_eq!(*seen.lock().unwrap(), ["c1"]);

    audit.stop();
    let refused = add("c2", Wait::Settled).await;
    assert!(
        matches!(&refused, Err(Error::NotSettled(events)) if events.len() == 1),
        "the command itself was committed: {refused:?}"
    );
    assert!(add("c3", Wait::Committed).await.unwrap().is_committed());
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_membership_decides_which_listeners_count_live_ones_never_count() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "300"]);
    kernel.start().await;
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", 0i64);
    cart.command("Add", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Added", ())])
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mails = || {
        let s = Arc::clone(&seen);
        shop.reactor("mails")
            .on("shop.Added", move |_: serde_json::Value, evt, _ctx| {
                s.lock().unwrap().push(evt.aggregate_id);
                async { Ok(()) }
            })
    };
    let v1 = Kernable::new(kernel.options().service("shop")).unwrap();
    let worker = v1.worker(&cart);
    let mails1 = v1.listener(&mails());
    let auditor = shop
        .reactor("audit")
        .on("shop.Added", |_: serde_json::Value, _evt, _ctx| async {
            Ok(())
        });
    let audit = v1.listener(&auditor);
    v1.apply(
        Manifest::new()
            .worker(&cart)
            .listener(mails1.listener())
            .listener(&auditor)
            .metadata("version", "1"),
    )
    .await
    .unwrap();
    let running = [spawn_worker(&worker), spawn(&mails1), spawn(&audit)];
    async fn add(k: &Kernable, id: &str) -> Result<Outcome, Error> {
        k.submit(Submit::new("shop", "Cart", id, "Add")).await
    }
    worker.ready().await;
    mails1.ready().await;
    audit.ready().await;
    assert!(add(&v1, "c1").await.unwrap().is_committed());
    mails1.stop();
    audit.stop();
    let [worker_run, mails_run, audit_run] = running;
    mails_run.await.unwrap();
    audit_run.await.unwrap();

    // The next version of the service has no audit any more: it counts for nobody, so a
    // settling submit does not wait for it.
    let v2 = Kernable::new(kernel.options().service("shop")).unwrap();
    let mails2 = v2.listener(&mails());
    v2.apply(
        Manifest::new()
            .worker(&cart)
            .listener(mails2.listener())
            .metadata("version", "2"),
    )
    .await
    .unwrap();
    // A live listener counts for nobody, even while it does not get through its events.
    let (release, stuck) = tokio::sync::watch::channel(false);
    let push = v2.listener(&shop.reactor("push").live().on(
        "shop.Added",
        move |_: serde_json::Value, _evt, _ctx| {
            let mut stuck = stuck.clone();
            async move {
                let _ = stuck.wait_for(|released| *released).await;
                Ok(())
            }
        },
    ));
    let again = [spawn(&mails2), spawn(&push)];
    mails2.ready().await;
    push.ready().await;
    let added = add(&v2, "c2").await;
    assert!(
        added.as_ref().is_ok_and(Outcome::is_committed),
        "settled without audit and push: {added:?}"
    );
    // mails resumed where it left off.
    assert_eq!(*seen.lock().unwrap(), ["c1", "c2"]);
    release.send_replace(true);
    mails2.stop();
    push.stop();
    for r in again {
        r.await.unwrap();
    }
    worker.stop();
    worker_run.await.unwrap();

    let live_middleware = std::panic::catch_unwind(|| {
        shop.reactor("x")
            .live()
            .middleware(MiddlewareOptions::new(&["*"]), |_| async { Ok(()) })
    });
    assert!(
        live_middleware.is_err(),
        "a live listener cannot be a middleware"
    );
}

#[derive(Serialize, Deserialize)]
struct By {
    by: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_model_with_a_store_continues_where_it_left_off_and_rebuilds_on_a_new_schema() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let counters = domain("counters");
    let counter = counters.aggregate("Counter", 0i64);
    counter.apply("Incremented", |n, data: By, _evt, _ctx| *n += data.by);
    counter.command("Increment", |_, data: By, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Incremented", By { by: data.by })])
    });
    let url = format!("sqlite:{}", kernel.dir.join("totals.db").display());
    let handled = Arc::new(AtomicUsize::new(0));
    let fail_on = Arc::new(AtomicI64::new(-1));
    let totals = |schema: &str| {
        let (handled, fail_on) = (Arc::clone(&handled), Arc::clone(&fail_on));
        counters
            .read_model("totals")
            .store(&url, schema)
            .reconnect(Duration::from_millis(50))
            .on("counters.Incremented", move |data: By, evt, ctx| {
                let (handled, fail_on) = (Arc::clone(&handled), Arc::clone(&fail_on));
                async move {
                    handled.fetch_add(1, Ordering::SeqCst);
                    let store = ctx.store.expect("a store");
                    let total =
                        store.get::<i64>("totals", &evt.aggregate_id)?.unwrap_or(0) + data.by;
                    store.set("totals", &evt.aggregate_id, &total)?;
                    if total == fail_on.load(Ordering::SeqCst) {
                        return Err("boom".into()); // after writing: the write must not stick
                    }
                    Ok(())
                }
            })
            .query("counters.GetTotal", |data: Id, _qry, ctx| async move {
                let total: Option<i64> = ctx.store.expect("a store").get("totals", &data.id)?;
                Ok(total.ok_or_else(|| Rejected::new("no counter").code("not_found"))?)
            })
    };
    let increment = |id: &str, by: i64| {
        kernable.submit(
            Submit::new("counters", "Counter", id, "Increment")
                .data(By { by })
                .wait(Wait::Committed),
        )
    };
    let total = |id: &str, min_position: u64| {
        kernable.query::<i64>(
            Query::new("counters", "GetTotal")
                .data(json!({ "id": id }))
                .min_position(min_position),
        )
    };

    let worker = kernable.worker(&counter);
    let mut running = vec![spawn_worker(&worker)];
    worker.ready().await;
    let model = kernable.listener(&totals("1"));
    running.push(spawn(&model));
    model.ready().await;
    for by in [1, 2, 3] {
        increment("c1", by).await.unwrap();
    }
    increment("c2", 5).await.unwrap();
    assert_eq!(total("c1", 4).await.unwrap(), 6);
    assert_eq!(handled.load(Ordering::SeqCst), 4);
    model.stop();

    // A new process with the same store: nothing is replayed, the data is there.
    increment("c1", 4).await.unwrap();
    let model = kernable.listener(&totals("1"));
    running.push(spawn(&model));
    model.ready().await;
    assert_eq!(total("c1", 5).await.unwrap(), 10);
    assert_eq!(
        handled.load(Ordering::SeqCst),
        5,
        "only the event after the stored position"
    );
    let all: Vec<(String, i64)> = model.opened_store().unwrap().all("totals").unwrap();
    assert_eq!(all, [("c1".to_owned(), 10), ("c2".to_owned(), 5)]);

    // A failing handler rolls its batch back; delivered again, it counts once.
    fail_on.store(13, Ordering::SeqCst);
    increment("c1", 3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    fail_on.store(-1, Ordering::SeqCst);
    eventually(|| model.position() >= 6).await;
    assert_eq!(total("c1", 6).await.unwrap(), 13);
    model.stop();

    // A new schema clears the store and rebuilds from the first event.
    handled.store(0, Ordering::SeqCst);
    let model = kernable.listener(&totals("2"));
    running.push(spawn(&model));
    model.ready().await;
    assert_eq!(total("c1", 6).await.unwrap(), 13);
    assert_eq!(handled.load(Ordering::SeqCst), 6);
    assert_eq!(total("c2", 0).await.unwrap(), 5);
    model.stop();
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct Ban {
    identity: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_checks_commands_and_queries_from_its_own_state_and_fails_closed() {
    let kernel = TestKernel::new(&["support.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let as_ = |identity: &str| RequestContext {
        sender_identity: identity.into(),
        ..Default::default()
    };

    let support = domain("support");
    let ticket = support.aggregate("Ticket", false);
    ticket.apply("Opened", |open, _: serde_json::Value, _evt, _ctx| {
        *open = true
    });
    ticket.command("Open", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Opened", ())])
    });
    ticket.command("Ban", |_, data: Ban, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Banned", data)])
    });
    // Registered, so the kernel takes it; the middleware fails on it before it runs.
    ticket.command("Crash", |_, _: serde_json::Value, _cmd, _ctx| Ok(vec![]));
    let tickets = Arc::new(Mutex::new(HashSet::new()));
    let t = Arc::clone(&tickets);
    let model = support.read_model("tickets").on(
        "support.Opened",
        move |_: serde_json::Value, evt, _ctx| {
            t.lock().unwrap().insert(evt.aggregate_id);
            async { Ok(()) }
        },
    );
    let t = Arc::clone(&tickets);
    model.query(
        "support.CountTickets",
        move |_: serde_json::Value, _qry, _ctx| {
            let count = t.lock().unwrap().len();
            async move { Ok(count) }
        },
    );

    // Refuses banned identities, from the events it has processed.
    let banned = Arc::new(Mutex::new(HashSet::new()));
    let checked = Arc::new(Mutex::new(Vec::<Check>::new()));
    let b = Arc::clone(&banned);
    let guard = support
        .read_model("guard")
        .on("support.Banned", move |data: Ban, _evt, _ctx| {
            b.lock().unwrap().insert(data.identity);
            async { Ok(()) }
        });
    let (b, c) = (Arc::clone(&banned), Arc::clone(&checked));
    guard.middleware(MiddlewareOptions::new(&["support.*"]), move |check| {
        let banned = b.lock().unwrap().contains(&check.request.sender_identity);
        let crash = check.type_name == "Crash";
        c.lock().unwrap().push(check);
        async move {
            if banned {
                return Err(Rejected::new("banned").code("forbidden").into());
            }
            if crash {
                return Err("boom".into());
            }
            Ok(())
        }
    });

    let worker = kernable
        .worker(&ticket)
        .reconnect(Duration::from_millis(100));
    let (model, guard) = (kernable.listener(&model), kernable.listener(&guard));
    let running = [spawn_worker(&worker), spawn(&model), spawn(&guard)];
    worker.ready().await;
    model.ready().await;
    guard.ready().await;
    let open = |id: &str, who: &str| {
        kernable.submit(Submit::new("support", "Ticket", id, "Open").context(as_(who)))
    };

    assert!(open("t1", "mallory").await.unwrap().is_committed());
    kernable
        .submit(
            Submit::new("support", "Ticket", "t1", "Ban")
                .data(Ban {
                    identity: "mallory".into(),
                })
                .context(as_("admin")),
        )
        .await
        .unwrap();
    // The ban settled, so the middleware has it before the next check.
    assert_eq!(
        open("t2", "mallory").await.unwrap(),
        Outcome::Rejected(Rejected::new("banned").code("forbidden"))
    );
    assert!(open("t3", "alice").await.unwrap().is_committed());
    let first: Vec<_> = checked.lock().unwrap()[..2]
        .iter()
        .map(|c| {
            (
                c.kind,
                c.domain.clone(),
                c.type_name.clone(),
                c.aggregate_id.clone(),
                c.request.sender_identity.clone(),
            )
        })
        .collect();
    assert_eq!(
        first,
        [
            (
                CheckKind::Command,
                "support".into(),
                "Open".into(),
                Some("t1".into()),
                "mallory".into()
            ),
            (
                CheckKind::Command,
                "support".into(),
                "Ban".into(),
                Some("t1".into()),
                "admin".into()
            ),
        ]
    );

    // Queries pass the same chain.
    let count = |who: &str| {
        kernable.query::<usize>(Query::new("support", "CountTickets").context(as_(who)))
    };
    assert_eq!(count("alice").await.unwrap(), 2);
    assert!(matches!(count("mallory").await, Err(Error::Rejected(r)) if r.message == "banned"));
    assert_eq!(
        checked.lock().unwrap().last().map(|c| c.kind),
        Some(CheckKind::Query)
    );

    // A failing middleware refuses, too.
    let crash = kernable
        .submit(Submit::new("support", "Ticket", "t4", "Crash"))
        .await
        .unwrap();
    assert!(matches!(crash, Outcome::Rejected(r) if r.code == "middleware_error"));

    // While the middleware is away, everything it checks is refused.
    guard.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let away = open("t5", "alice").await;
    assert!(
        matches!(&away, Err(Error::Status(s)) if s.message().contains("middleware guard is not connected")),
        "{away:?}"
    );
    worker.stop();
    model.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_after_requests_observes_commands_and_may_refuse_a_querys_response() {
    let kernel = TestKernel::new(&["support.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();

    let support = domain("support");
    let ticket = support.aggregate("Ticket", false);
    ticket.apply("Opened", |open, _: serde_json::Value, _evt, _ctx| {
        *open = true
    });
    ticket.command("Open", |open, _: serde_json::Value, _cmd, _ctx| {
        if *open {
            return Err(Rejected::new("already open").code("invalid_state"));
        }
        Ok(vec![NewEvent::new("Opened", ())])
    });
    let tickets = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let t = Arc::clone(&tickets);
    let model =
        support
            .read_model("tickets")
            .on("Opened", move |_: serde_json::Value, evt, _ctx| {
                t.lock().unwrap().insert(evt.aggregate_id);
                async { Ok(()) }
            });
    let t = Arc::clone(&tickets);
    model.query("ListTickets", move |_: serde_json::Value, _qry, _ctx| {
        let list: Vec<String> = t.lock().unwrap().iter().cloned().collect();
        async move { Ok(list) }
    });

    let seen = Arc::new(Mutex::new(Vec::<Check>::new()));
    let s = Arc::clone(&seen);
    let audit = support
        .read_model("audit")
        .middleware(MiddlewareOptions::new(&["*"]), |check| {
            let banned = check.request.sender_identity == "mallory";
            async move {
                if banned {
                    return Err(Rejected::new("banned").code("forbidden").into());
                }
                Ok(())
            }
        })
        .middleware(MiddlewareOptions::new(&["*"]).after(), move |check| {
            // Only the response of a query can still be refused.
            let hidden = matches!(
                &check.outcome,
                Some(CheckOutcome::Query(QueryOutcome::Answered(data)))
                    if data.as_array().is_some_and(|a| a.contains(&json!("secret")))
            );
            let command = check.kind == CheckKind::Command;
            s.lock().unwrap().push(check);
            async move {
                if hidden {
                    return Err(Rejected::new("hidden").code("hidden").into());
                }
                if command {
                    return Err(Rejected::new("ignored for commands").into());
                }
                Ok(())
            }
        });

    let worker = kernable
        .worker(&ticket)
        .reconnect(Duration::from_millis(100));
    let (model, audit) = (kernable.listener(&model), kernable.listener(&audit));
    let running = [spawn_worker(&worker), spawn(&model), spawn(&audit)];
    worker.ready().await;
    model.ready().await;
    audit.ready().await;
    let after = |n: usize| {
        let seen = Arc::clone(&seen);
        async move {
            eventually(|| seen.lock().unwrap().len() >= n).await;
            seen.lock()
                .unwrap()
                .drain(..)
                .map(|c| (c.kind, c.type_name, c.outcome))
                .collect::<Vec<_>>()
        }
    };
    let open = |id: &str, who: &str| {
        let context = RequestContext {
            sender_identity: who.into(),
            ..Default::default()
        };
        kernable.submit(Submit::new("support", "Ticket", id, "Open").context(context))
    };

    assert!(open("t1", "alice").await.unwrap().is_committed());
    let seen_t1 = after(1).await;
    let [(kind, type_name, Some(CheckOutcome::Command(outcome)))] = &seen_t1[..] else {
        panic!("{seen_t1:?}");
    };
    assert_eq!((*kind, type_name.as_str()), (CheckKind::Command, "Open"));
    assert_eq!(outcome.events()[0].event_type, "Opened");

    // Rejected by the aggregate or refused before: it still hears of it, and refuses nothing.
    assert!(!open("t1", "alice").await.unwrap().is_committed());
    assert!(!open("t2", "mallory").await.unwrap().is_committed());
    let outcomes: Vec<_> = after(2).await.into_iter().map(|(_, _, o)| o).collect();
    assert_eq!(
        outcomes,
        [
            Some(CheckOutcome::Command(Outcome::Rejected(
                Rejected::new("already open").code("invalid_state")
            ))),
            Some(CheckOutcome::Command(Outcome::Rejected(
                Rejected::new("banned").code("forbidden")
            ))),
        ]
    );

    // A query's response passes it, or not.
    let list = || kernable.query::<Vec<String>>(Query::new("support", "ListTickets"));
    assert_eq!(list().await.unwrap(), ["t1"]);
    assert_eq!(
        after(1).await[0].2,
        Some(CheckOutcome::Query(QueryOutcome::Answered(json!(["t1"]))))
    );
    assert!(open("secret", "alice").await.unwrap().is_committed());
    after(1).await;
    assert!(matches!(list().await, Err(Error::Rejected(r)) if r.message == "hidden"));
    assert_eq!(
        after(1).await[0].2,
        Some(CheckOutcome::Query(QueryOutcome::Answered(json!([
            "secret", "t1"
        ]))))
    );

    let twice = std::panic::catch_unwind(|| {
        support
            .read_model("x")
            .middleware(MiddlewareOptions::new(&["*"]), |_| async { Ok(()) })
            .middleware(MiddlewareOptions::new(&["*"]), |_| async { Ok(()) });
    });
    let message = twice.unwrap_err();
    assert!(
        message
            .downcast_ref::<String>()
            .unwrap()
            .contains("already")
    );

    worker.stop();
    model.stop();
    audit.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn types_without_a_domain_are_the_listeners_own_and_its_domains_events_are_upcast() {
    let kernel = TestKernel::new(&["shop.*", "crm.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();

    // The writers are at version 1.
    let cart = domain("shop").aggregate("Cart", ());
    cart.command("Add", |_, data: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Added", json!({ "qty": data["qty"] }))])
    });
    let contact = domain("crm").aggregate("Contact", ());
    contact.command("Create", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Created", json!({ "name": "Max" }))])
    });

    // The listener knows version 2 of its own domain without `with_domain`, and of another
    // domain with it.
    let shop = domain("shop");
    shop.event("Added", 2)
        .upcast(1, |d| json!({ "count": d["qty"] }));
    let crm = domain("crm");
    crm.event("Created", 2)
        .upcast(1, |d| json!({ "fullName": d["name"] }));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let push = |seen: &Arc<Mutex<Vec<_>>>| {
        let s = Arc::clone(seen);
        move |data: serde_json::Value, evt: kernable::StoredEventMeta, _ctx| {
            s.lock()
                .unwrap()
                .push((format!("{}.{}", evt.domain, evt.event_type), data));
            async { Ok(()) }
        }
    };
    let s = Arc::clone(&seen);
    let model = shop
        .read_model("model")
        .with_domain(&crm)
        .on("Added", push(&seen))
        .on("crm.Created", push(&seen))
        .query("Count", move |_: serde_json::Value, qry, _ctx| {
            let count = s.lock().unwrap().len();
            async move { Ok(json!([qry.domain, qry.query_type, count])) }
        });

    let worker = kernable.worker(&cart).aggregate(&contact);
    let model = kernable.listener(&model);
    let running = [spawn_worker(&worker), spawn(&model)];
    worker.ready().await;
    model.ready().await;
    for (domain, aggregate, command) in [("shop", "Cart", "Add"), ("crm", "Contact", "Create")] {
        let submit = Submit::new(domain, aggregate, "x1", command).data(json!({ "qty": 2 }));
        assert!(kernable.submit(submit).await.unwrap().is_committed());
    }
    assert_eq!(
        *seen.lock().unwrap(),
        [
            ("shop.Added".to_owned(), json!({ "count": 2 })),
            ("crm.Created".to_owned(), json!({ "fullName": "Max" })),
        ]
    );
    let answer: serde_json::Value = kernable.query(Query::new("shop", "Count")).await.unwrap();
    assert_eq!(answer, json!(["shop", "Count", 2]));
    model.stop();
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[test]
fn listener_types_are_a_type_a_qualified_type_or_a_domain() {
    let shop = domain("shop");
    let model = shop.read_model("model");
    let on = |t: &'static str| {
        let model = model.clone();
        std::panic::catch_unwind(move || {
            model.on(t, |_: serde_json::Value, _evt, _ctx| async { Ok(()) });
        })
        .is_ok()
    };
    assert!(on("Added") && on("crm.Created") && on("crm.*"));
    assert!(!on("*") && !on("") && !on("crm.a.b") && !on("Add*"));
    let check = |t: &'static str| {
        // A listener has one middleware before requests: each check gets its own.
        let model = shop.read_model("model");
        std::panic::catch_unwind(move || {
            model.middleware(MiddlewareOptions::new(&[t]), |_| async { Ok(()) });
        })
        .is_ok()
    };
    assert!(check("*") && check("Place") && check("crm.Place") && check("crm.*"));
    assert!(!check("crm.") && !check(".x"));
    let query = |t: &'static str| {
        let model = model.clone();
        std::panic::catch_unwind(move || {
            model.query(t, |_: serde_json::Value, _qry, _ctx| async { Ok(()) });
        })
        .is_ok()
    };
    assert!(query("GetCart") && query("crm.GetContact"));
    assert!(!query("crm.*"));
}
