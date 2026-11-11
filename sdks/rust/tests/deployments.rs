//! Deployments: versions of the code side by side. Read models of each answer their own
//! deployment's queries, a membership decides whose workers run the commands, and code meets
//! events a newer deployment wrote.

mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common::{TestKernel, eventually};
use kernable::{
    Error, Kernable, ListenerHandle, Manifest, NewEvent, Outcome, Query, Submit, Wait, Worker,
    domain,
};
use serde_json::{Value, json};

fn spawn_worker(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

fn spawn(listener: &ListenerHandle) -> tokio::task::JoinHandle<()> {
    let listener = listener.clone();
    tokio::spawn(async move { listener.run().await })
}

fn add(id: &str) -> Submit {
    Submit::new("shop", "Cart", id, "Add")
}

#[tokio::test]
async fn an_invalid_deployment_is_refused_at_construction() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    let made = Kernable::new(kernel.options().deployment("v 1"));
    assert!(matches!(made, Err(Error::Config(m)) if m.contains("1 to 128")));
    assert!(Kernable::new(kernel.options().deployment("2026-11-11.1")).is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn read_models_of_two_deployments_run_side_by_side_and_answer_their_own() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let cart = domain("shop").aggregate("Cart", ());
    cart.command("Add", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("ItemAdded", ())])
    });
    let worker = kernable.worker(&cart);
    let working = spawn_worker(&worker);
    worker.ready().await;
    let mut head = 0;
    for id in ["c1", "c2"] {
        let outcome = kernable
            .submit(add(id).wait(Wait::Committed))
            .await
            .unwrap();
        head = outcome.events()[0].position;
    }

    // Each deployment defines the read model anew, as its own process would.
    let carts = |deployment: &'static str| {
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        domain("shop")
            .read_model("carts")
            .on("ItemAdded", move |_: Value, _evt, _ctx| {
                c.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .query("Count", move |_: Value, _qry, _ctx| {
                let n = count.load(Ordering::SeqCst);
                async move { Ok(json!({ "deployment": deployment, "count": n })) }
            })
    };
    let v1 = Kernable::new(kernel.options().deployment("v1")).unwrap();
    let v2 = Kernable::new(kernel.options().deployment("v2")).unwrap();
    let (carts1, carts2) = (v1.listener(&carts("v1")), v2.listener(&carts("v2")));
    let running = [spawn(&carts1), spawn(&carts2)];
    carts1.ready().await;
    carts2.ready().await;

    // Both got every event: v2 built its own from the start.
    let count = |k: Kernable| async move {
        k.query::<Value>(Query::new("shop", "Count").min_position(head))
            .await
            .unwrap()
    };
    assert_eq!(
        count(v1.clone()).await,
        json!({ "deployment": "v1", "count": 2 })
    );
    assert_eq!(
        count(v2.clone()).await,
        json!({ "deployment": "v2", "count": 2 })
    );
    assert_eq!((carts1.position(), carts2.position()), (head, head));

    for handle in [&carts1, &carts2] {
        handle.stop();
    }
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
    working.await.unwrap();
}

/// The shop as one deployment's code knows it: `ItemAdded` at `schema`.
fn shop(schema: u32, runs: Arc<AtomicU64>) -> (kernable::Domain, kernable::Aggregate<()>) {
    let shop = domain("shop");
    if schema > 1 {
        shop.event("ItemAdded", schema).upcast(1, |v| v);
    }
    let cart = shop.aggregate("Cart", ());
    cart.apply("ItemAdded", |_, _: Value, _evt, _ctx| {});
    cart.command("Add", move |_, _: Value, _cmd, _ctx| {
        runs.fetch_add(1, Ordering::SeqCst);
        Ok(vec![NewEvent::new("ItemAdded", json!({ "qty": 1 }))])
    });
    (shop, cart)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_keeps_old_code_off_newer_history_and_its_listeners_skip_or_park_it() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let options = || kernel.options().service("shop");
    let v1 = Kernable::new(options().deployment("v1")).unwrap();
    let v2 = Kernable::new(options().deployment("v2")).unwrap();
    let (runs1, runs2) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let (shop1, cart1) = shop(1, Arc::clone(&runs1));
    let (_, cart2) = shop(2, Arc::clone(&runs2));

    // v1's read model and reactor: they know `ItemAdded` up to schema 1.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    let carts = shop1
        .read_model("carts")
        .on("ItemAdded", move |_: Value, evt, _ctx| {
            s.lock()
                .unwrap()
                .push((evt.aggregate_id.clone(), evt.deployment));
            async { Ok(()) }
        });
    let mails = shop1
        .reactor("mails")
        .park_after(3)
        .on("ItemAdded", |_: Value, _evt, _ctx| async { Ok(()) });
    let (carts, mails) = (v1.listener(&carts), v1.listener(&mails));

    // v2 serves: its workers run the commands.
    v2.apply(Manifest::new().worker(&cart2)).await.unwrap();
    let (worker1, worker2) = (v1.worker(&cart1), v2.worker(&cart2));
    let running = [
        spawn_worker(&worker1),
        spawn_worker(&worker2),
        spawn(&carts),
        spawn(&mails),
    ];
    for w in [&worker1, &worker2] {
        w.ready().await;
    }
    carts.ready().await;
    mails.ready().await;
    let outcome = kernable.submit(add("A")).await.unwrap();
    let written = &outcome.events()[0];
    assert_eq!((written.schema, written.deployment.as_str()), (2, "v2"));
    assert_eq!(
        (runs1.load(Ordering::SeqCst), runs2.load(Ordering::SeqCst)),
        (0, 1)
    );

    // The read model skips the event it cannot read and counts it; the reactor parks it.
    let a = written.position;
    eventually(|| carts.position() >= a).await;
    assert_eq!(carts.skipped(), 1);
    assert!(seen.lock().unwrap().is_empty());
    let mut parked = String::new();
    for _ in 0..100 {
        let (ok, out) = kernel.cli(&["listeners", "parked", "{store}"]);
        assert!(ok);
        parked = out;
        if !parked.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        parked.starts_with(&format!(
            "mails position {a} shop.ItemAdded unknown_event attempts 1 parked "
        )),
        "{parked}"
    );
    assert!(parked.contains("shop.ItemAdded schema 2"), "{parked}");
    assert_eq!(mails.skipped(), 0);

    // Back to v1, applied later: A holds a schema v1 does not read, so the kernel rejects its
    // command before v1's worker sees it; a fresh aggregate works.
    tokio::time::sleep(Duration::from_millis(5)).await;
    v1.apply(Manifest::new().worker(&cart1)).await.unwrap();
    match kernable.submit(add("A")).await.unwrap() {
        Outcome::Rejected(r) => assert_eq!(r.code, "unknown_history", "{r:?}"),
        committed => panic!("{committed:?}"),
    }
    let outcome = kernable.submit(add("B")).await.unwrap();
    let written = &outcome.events()[0];
    assert_eq!((written.schema, written.deployment.as_str()), (1, "v1"));
    assert_eq!(
        (runs1.load(Ordering::SeqCst), runs2.load(Ordering::SeqCst)),
        (1, 1)
    );

    // The read model handles the events it knows after the one it skipped.
    let b = written.position;
    eventually(|| carts.position() >= b).await;
    assert_eq!(*seen.lock().unwrap(), [("B".to_owned(), "v1".to_owned())]);
    assert_eq!(carts.skipped(), 1);

    for w in [&worker1, &worker2] {
        w.stop();
    }
    carts.stop();
    mails.stop();
    for r in running {
        r.await.unwrap();
    }
}

/// The shop as an app of one deployment: a cart and a read model that counts the items.
fn shop_app(
    kernel: &TestKernel,
    deployment: &str,
    switch: kernable::Switch,
) -> kernable::AppHandle {
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", ());
    cart.command("Add", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("ItemAdded", ())])
    });
    let count = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&count);
    shop.read_model("carts")
        .on("ItemAdded", move |_: Value, _evt, _ctx| {
            c.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .query("Count", move |_: Value, _qry, _ctx| {
            let n = count.load(Ordering::SeqCst);
            async move { Ok(n) }
        });
    Kernable::new(kernel.options().service("shop").deployment(deployment))
        .unwrap()
        .app(kernable::App::new().domain(&shop).switch(switch))
        .unwrap()
}

/// Waits up to 10 s for `f`.
async fn within(what: &str, f: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .unwrap_or_else(|_| panic!("{what} within 10 s"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_deployment_warms_up_beside_the_serving_one_before_it_takes_over() {
    use kernable::Switch;
    let kernel = TestKernel::new(&["shop.*"], &["--deployment-switch-lag", "0"]);
    kernel.start().await;
    let kernable = kernel.client();
    let start = |app: &kernable::AppHandle| {
        let app = app.clone();
        tokio::spawn(async move { app.run().await })
    };
    let by = |outcome: Outcome| outcome.events()[0].deployment.clone();

    let v1 = shop_app(&kernel, "v1", Switch::CaughtUp);
    let running1 = start(&v1);
    within("v1 serves", v1.ready()).await;
    for id in ["c1", "c2"] {
        let outcome = kernable.submit(add(id).wait(Wait::Committed)).await;
        assert_eq!(by(outcome.unwrap()), "v1");
    }

    // v2 leaves the switch to an operator: it connects and builds its read model, v1 serves.
    let v2 = shop_app(&kernel, "v2", Switch::Never);
    let running2 = start(&v2);
    within("v2 connects", v2.connected()).await;
    eventually(|| v2.listeners()[0].position() >= 2).await;
    assert_eq!(by(kernable.submit(add("c3")).await.unwrap()), "v1");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), v2.serving())
            .await
            .is_err(),
        "v2 does not take over by itself"
    );

    // v3 takes over by itself once it has caught up.
    let v3 = shop_app(&kernel, "v3", Switch::CaughtUp);
    let running3 = start(&v3);
    within("v3 takes over", v3.ready()).await;
    assert_eq!(by(kernable.submit(add("c4")).await.unwrap()), "v3");

    // An operator switches to v2 (caught up meanwhile): v2's app sees it serve.
    let operator = Kernable::new(kernel.options().deployment("v2")).unwrap();
    let manifest = v2.manifest().clone().deployment("v2");
    for _ in 0..100 {
        match operator.apply(manifest.clone()).await {
            Ok(()) => break,
            Err(e) if e.waits_for_switch() => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("{e}"),
        }
    }
    within("v2 serves", v2.ready()).await;
    assert_eq!(by(kernable.submit(add("c5")).await.unwrap()), "v2");

    for app in [&v1, &v2, &v3] {
        app.stop();
    }
    for running in [running1, running2, running3] {
        running.await.unwrap().unwrap();
    }
}
