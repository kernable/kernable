//! End to end: a kernel on a Unix socket, Rust test workers, signed commands.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use common::*;
use kernable_core::proto::command_result::Outcome;
use tokio::sync::mpsc;
use tonic::Code;

#[tokio::test(flavor = "multi_thread")]
async fn signed_commands_run_end_to_end() {
    let env = start("happy").await;
    spawn_worker(&env).await;
    let client = key(1);

    let placed = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &client),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    let added = submit(&env.addr, signed(command("c2", "AddItem", "o1"), &client))
        .await
        .unwrap();
    assert_eq!(committed_versions(&added), [2]);

    // Resubmitting returns the stored result instead of running again.
    let again = submit(&env.addr, signed(command("c2", "AddItem", "o1"), &client))
        .await
        .unwrap();
    assert_eq!(again, added);

    // Business rejection, signed by the worker.
    let dup = submit(
        &env.addr,
        signed(command("c3", "PlaceOrder", "o1"), &client),
    )
    .await
    .unwrap();
    assert!(matches!(dup.outcome, Some(Outcome::Rejected(r)) if r.code == "invalid_state"));

    // The log the kernel wrote verifies, including a kernel checkpoint.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let report = store.verify(&[id(&env.kernel_key).to_string()]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    assert_eq!(report.events, 2);
    assert!(report.checkpoints >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn unsigned_forged_and_unauthorized_commands_are_refused() {
    let env = start("clients").await;
    spawn_worker(&env).await;

    let unsigned = submit(&env.addr, command("c1", "PlaceOrder", "o1"))
        .await
        .unwrap_err();
    assert_eq!(unsigned.code(), Code::Unauthenticated);

    let mut tampered = signed(command("c2", "PlaceOrder", "o1"), &key(1));
    tampered.payload = b"{\"id\":\"evil\"}".to_vec();
    assert_eq!(
        submit(&env.addr, tampered).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let stranger = signed(command("c3", "PlaceOrder", "o1"), &key(9));
    assert_eq!(
        submit(&env.addr, stranger).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthorized_and_impostor_workers_are_refused() {
    let env = start("workers").await;
    let types = || vec!["orders.PlaceOrder".to_string()];

    let worker = |claimed: u8, signer: u8, command_types: Vec<String>| TestWorker {
        claimed: key(claimed),
        signer: key(signer),
        command_types,
        ..TestWorker::honest()
    };

    let unknown = run_worker(env.addr.clone(), worker(9, 9, types())).await;
    assert_eq!(unknown.code(), Code::PermissionDenied);

    // The worker key owns the orders domain, not billing.
    let other_domain = vec!["billing.DeleteAll".into()];
    let too_much = run_worker(env.addr.clone(), worker(2, 2, other_domain)).await;
    assert_eq!(too_much.code(), Code::PermissionDenied);
    let unqualified = run_worker(env.addr.clone(), worker(2, 2, vec!["PlaceOrder".into()])).await;
    assert_eq!(unqualified.code(), Code::InvalidArgument);

    // Claims the trusted worker key but cannot sign with it.
    let impostor = run_worker(env.addr.clone(), worker(2, 9, types())).await;
    assert_eq!(impostor.code(), Code::Unauthenticated);
}

/// Many concurrent commands on one aggregate across two workers: the kernel queues them,
/// so every command commits, without conflicts, in a gap-free stream.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commands_on_one_aggregate_are_queued() {
    let env = Arc::new(start("concurrent").await);
    spawn_worker(&env).await;
    spawn_worker(&env).await;
    let client = key(1);
    submit(
        &env.addr,
        signed(command("c0", "PlaceOrder", "o1"), &client),
    )
    .await
    .unwrap();

    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for i in 1..=20 {
        let (env, outcomes, cmd) = (
            Arc::clone(&env),
            Arc::clone(&outcomes),
            signed(command(&format!("c{i}"), "AddItem", "o1"), &client),
        );
        tasks.push(tokio::spawn(async move {
            let result = submit(&env.addr, cmd).await.unwrap();
            outcomes.lock().unwrap().push(result.outcome.unwrap());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let outcomes = outcomes.lock().unwrap();
    assert!(
        outcomes.iter().all(|o| matches!(o, Outcome::Committed(_))),
        "{outcomes:?}"
    );
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let versions: Vec<u64> = store
        .read_stream("t1", "orders", "Order", "o1")
        .unwrap()
        .iter()
        .map(|e| e.version)
        .collect();
    assert_eq!(versions, (1..=21).collect::<Vec<_>>());
}

/// `orders.PlaceOrder` and `billing.PlaceOrder` on aggregates both called `Order/o1`: each goes
/// to its own domain's worker and into its own stream.
#[tokio::test(flavor = "multi_thread")]
async fn equal_names_in_two_domains_do_not_collide() {
    let env = start("domains").await;
    env.kernel
        .grant(grant(&key(7), &[("run", &["billing.*"])]))
        .unwrap();
    let (orders_seen, mut orders_rx) = mpsc::unbounded_channel();
    let (billing_seen, mut billing_rx) = mpsc::unbounded_channel();
    start_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::Honest, orders_seen),
    )
    .await;
    let billing = TestWorker {
        claimed: key(7),
        signer: key(7),
        command_types: vec!["billing.PlaceOrder".into()],
        ..TestWorker::with(Behavior::Honest, billing_seen)
    };
    start_worker(env.addr.clone(), billing).await;

    let mut in_billing = command("c2", "PlaceOrder", "o1");
    in_billing.domain = "billing".into();
    let a = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    );
    let b = submit(&env.addr, signed(in_billing, &key(1)));
    let (a, b) = tokio::join!(a, b);
    assert_eq!(committed_versions(&a.unwrap()), [1]);
    assert_eq!(committed_versions(&b.unwrap()), [1]);
    assert_eq!(orders_rx.recv().await.unwrap(), "c1");
    assert_eq!(billing_rx.recv().await.unwrap(), "c2");
    assert!(orders_rx.try_recv().is_err() && billing_rx.try_recv().is_err());

    let store = kernable_store::open_read_only(&env.store).unwrap();
    for domain in ["orders", "billing"] {
        let events = store.read_stream("t1", domain, "Order", "o1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].domain, domain);
    }
}
