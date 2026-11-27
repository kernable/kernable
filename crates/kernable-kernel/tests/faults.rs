//! Fault injection: workers crash, hang or forge, the kernel is stopped mid-flight.
//! In every case each command must end up committed exactly once.

mod common;

use std::{
    process::{Command as Process, Stdio},
    time::{Duration, Instant},
};

use common::*;
use kernable_core::proto::command_result::Outcome;
use tokio::sync::mpsc;
use tonic::Code;

fn stream_versions(env: &Env) -> Vec<u64> {
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let events = store.read_stream("t1", "orders", "Order", "o1").unwrap();
    events.iter().map(|e| e.version).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn crashed_worker_command_is_redelivered_without_waiting_for_the_lease() {
    let env = start_with("crash", 30_000).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let crashed = start_worker(env.addr.clone(), TestWorker::with(Behavior::Crash, seen)).await;

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    crashed.await.unwrap();

    let started = Instant::now();
    spawn_worker(&env).await;
    let result = pending.await.unwrap().unwrap();
    assert_eq!(committed_versions(&result), [1]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(stream_versions(&env), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn hung_worker_loses_its_lease_and_its_late_result_is_discarded() {
    let env = start_with("hung", 200).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let slow = TestWorker::with(Behavior::Slow(Duration::from_millis(600)), seen);
    start_worker(env.addr.clone(), slow).await;

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    spawn_worker(&env).await;

    let result = pending.await.unwrap().unwrap();
    assert_eq!(committed_versions(&result), [1]);
    // Let every late result from the slow worker arrive; none may add events.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(stream_versions(&env), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn forged_result_ends_the_session_and_an_honest_worker_takes_over() {
    let env = start_with("forge", 30_000).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let forger = start_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::ForgeResult, seen),
    )
    .await;

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    assert_eq!(forger.await.unwrap().code(), Code::Unauthenticated);

    spawn_worker(&env).await;
    let result = pending.await.unwrap().unwrap();
    assert_eq!(committed_versions(&result), [1]);
    assert_eq!(stream_versions(&env), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_rejection_ends_the_session_and_an_honest_worker_rejects_instead() {
    let env = start_with("forge-rejection", 30_000).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let forger = start_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::ForgeResult, seen),
    )
    .await;

    // No order o1: the domain rejects AddItem, and the forger signs that with another key.
    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "AddItem", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    let ended = tokio::time::timeout(Duration::from_secs(5), forger)
        .await
        .expect("the forged rejection was accepted");
    assert_eq!(ended.unwrap().code(), Code::Unauthenticated);

    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    start_worker(env.addr.clone(), TestWorker::with(Behavior::Honest, seen)).await;
    assert_eq!(
        seen_rx.recv().await.unwrap(),
        "c1",
        "the command runs again"
    );
    let result = pending.await.unwrap().unwrap();
    assert!(
        matches!(&result.outcome, Some(Outcome::Rejected(r)) if r.code == "invalid_state"),
        "{result:?}"
    );
}

/// X8: the lease ID is not signed. A result under a lease the worker was not given never
/// commits, whatever command that lease is for; the command runs once, on an honest worker.
#[tokio::test(flavor = "multi_thread")]
async fn a_result_under_a_wrong_lease_never_commits() {
    let env = start_with("wrong-lease", 30_000).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let liar = start_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::WrongLease, seen),
    )
    .await;

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(stream_versions(&env).is_empty(), "a wrong lease committed");

    liar.abort();
    spawn_worker(&env).await;
    let result = pending.await.unwrap().unwrap();
    assert_eq!(committed_versions(&result), [1]);
    assert_eq!(stream_versions(&env), [1]);
}

/// SIGTERM: the kernel lets running commands finish for `drain_timeout_ms`, then exits cleanly
/// and writes a final checkpoint. A command still in flight stays pending and runs right after
/// a restart, not only after its 60 s lease expires.
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_checkpoints_and_in_flight_commands_run_right_after_a_restart() {
    let dir = std::env::temp_dir().join(format!("kn-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let sock = dir.join("k.sock");
    let addr = format!("unix:{}", sock.display());
    let store = store_url(&dir);
    // The client and the worker present their grants, as the SDKs do.
    let flags = [
        "--root",
        &id(&root()),
        "--store",
        &store,
        "--listen",
        &addr,
        "--lease-ttl-ms",
        "60000",
        "--checkpoint-interval-ms",
        "3600000",
        "--drain-timeout-ms",
        "500",
    ]
    .map(String::from);
    let mut kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .arg("start")
            .args(["--instance-key", "kernel.key"])
            .args(&flags)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    wait_for(&addr).await;

    // One committed command, so the final checkpoint has something to anchor.
    let honest = start_worker(addr.clone(), TestWorker::honest()).await;
    let c0 = signed(command("c0", "PlaceOrder", "o0"), &key(1));
    assert_eq!(committed_versions(&submit(&addr, c0).await.unwrap()), [1]);
    honest.abort();

    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let slow = TestWorker::with(Behavior::Slow(Duration::from_secs(30)), seen);
    start_worker(addr.clone(), slow).await;
    let submitter = addr.clone();
    tokio::spawn(async move {
        submit(
            &submitter,
            signed(command("c1", "PlaceOrder", "o1"), &key(1)),
        )
        .await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");

    let status = Process::new("kill")
        .args(["-TERM", &kernel.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let started = Instant::now();
    let exit = loop {
        if let Some(exit) = kernel.child.try_wait().unwrap() {
            break exit;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "kernel did not stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(exit.success());

    {
        let store = kernable_store::open(&store).unwrap();
        let report = store.verify(&[id(&key(3)).to_string()]).unwrap();
        assert!(report.is_ok(), "{:?}", report.problems);
        assert!(report.checkpoints >= 1, "no final checkpoint");
        let pending: Vec<String> = store
            .pending()
            .unwrap()
            .into_iter()
            .map(|p| p.command.command_id)
            .collect();
        assert_eq!(pending, ["c1"]);
    }

    // After a restart, c1 runs at once and its result reaches a resubmit.
    drop(kernel);
    let kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .arg("start")
            .args(["--instance-key", "kernel.key"])
            .args(&flags)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    let started = Instant::now();
    let c1 = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    assert_eq!(committed_versions(&submit(&addr, c1).await.unwrap()), [1]);
    assert!(started.elapsed() < Duration::from_secs(5));
    drop(kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_result_that_arrives_before_the_new_holders_never_commits() {
    // A holds the command past its 500 ms lease and answers at 700 ms; B gets it at 500 ms and
    // answers at 900 ms, within its own lease: A's result comes first, under a lease that is no
    // longer its.
    let env = start_with("lease-reversed", 500).await;
    let (seen_a, mut by_a) = mpsc::unbounded_channel();
    let a = TestWorker::with(Behavior::Slow(Duration::from_millis(700)), seen_a);
    start_worker(env.addr.clone(), a).await;
    let addr = env.addr.clone();
    let submitted = tokio::spawn(async move {
        let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
        submit(&addr, place).await.unwrap()
    });
    assert_eq!(by_a.recv().await.unwrap(), "c1", "A holds it");
    let (seen_b, mut by_b) = mpsc::unbounded_channel();
    let b = TestWorker::with(Behavior::Slow(Duration::from_millis(400)), seen_b);
    start_worker(env.addr.clone(), b).await;
    assert_eq!(
        by_b.recv().await.unwrap(),
        "c1",
        "B gets it once A's lease ends"
    );

    let result = submitted.await.unwrap();
    assert_eq!(committed_versions(&result), [1]);
    let read = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(read.head_position().unwrap(), 1, "committed once");
    let report = read.verify(&[id(&env.kernel_key).to_string()]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
}
