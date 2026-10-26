//! Snapshots: workers get the latest one plus the events after it; bad ones are refused.

mod common;

use common::*;
use tokio::sync::mpsc;

fn worker(
    behavior: Behavior,
    executions: mpsc::UnboundedSender<(Option<u64>, usize)>,
) -> TestWorker {
    TestWorker {
        behavior,
        executions: Some(executions),
        ..TestWorker::honest()
    }
}

async fn run(env: &Env, ids: std::ops::Range<u32>) {
    for i in ids {
        let command_type = if i == 0 { "PlaceOrder" } else { "AddItem" };
        let cmd = signed(command(&format!("c{i}"), command_type, "o1"), &key(1));
        committed_versions(&submit(&env.addr, cmd).await.unwrap());
    }
}

fn verify(env: &Env) -> kernable_store::Report {
    let store = kernable_store::open_read_only(&env.store).unwrap();
    store.verify(&id(&env.kernel_key)).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_get_the_latest_snapshot_and_only_the_events_after_it() {
    let env = start("snap-latest").await;
    let (tx, mut executions) = mpsc::unbounded_channel();
    tokio::spawn(run_worker(
        env.addr.clone(),
        worker(Behavior::Snapshots, tx),
    ));

    run(&env, 0..4).await;
    let seen: Vec<_> = std::iter::from_fn(|| executions.try_recv().ok()).collect();
    assert_eq!(seen, [(None, 0), (Some(1), 0), (Some(2), 0), (Some(3), 0)]);

    let report = verify(&env);
    assert!(report.is_ok(), "{:?}", report.problems);
    assert_eq!(report.snapshots, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_snapshot_is_dropped_and_the_command_runs_with_all_events() {
    let env = start("snap-stale").await;
    let (tx, mut executions) = mpsc::unbounded_channel();
    let first = tokio::spawn(run_worker(
        env.addr.clone(),
        worker(Behavior::Snapshots, tx.clone()),
    ));
    run(&env, 0..2).await;
    first.abort();
    while executions.try_recv().is_ok() {}

    // A new worker version cannot read the old snapshot; it gets the full history instead.
    tokio::spawn(run_worker(
        env.addr.clone(),
        worker(Behavior::StaleSnapshots, tx),
    ));
    run(&env, 2..3).await;
    let seen: Vec<_> = std::iter::from_fn(|| executions.try_recv().ok()).collect();
    assert_eq!(seen, [(Some(2), 0), (None, 2)]);
    // Its own new snapshot replaced the old one.
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let latest = store.snapshot("t1", "Order", "o1").unwrap().unwrap();
    assert_eq!(latest.snapshot.unwrap().version, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_snapshot_ends_the_session_and_nothing_is_committed() {
    let env = start("snap-forged").await;
    let (tx, _executions) = mpsc::unbounded_channel();
    let forger = tokio::spawn(run_worker(
        env.addr.clone(),
        worker(Behavior::ForgeSnapshot, tx),
    ));
    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c0", "PlaceOrder", "o1"), &key(1))).await
    });
    let status = forger.await.unwrap();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // An honest worker takes over; the forged snapshot was never stored.
    spawn_worker(&env);
    assert_eq!(committed_versions(&pending.await.unwrap().unwrap()), [1]);
    assert_eq!(verify(&env).snapshots, 0);
}
