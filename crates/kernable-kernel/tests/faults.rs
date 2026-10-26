//! Fault injection: workers crash, hang or forge, the kernel is stopped mid-flight.
//! In every case each command must end up committed exactly once.

mod common;

use std::{
    process::{Child, Command as Process, Stdio},
    time::{Duration, Instant},
};

use common::*;
use kernable_store_sqlite::SqliteStore;
use tokio::sync::mpsc;
use tonic::Code;

fn stream_versions(env: &Env) -> Vec<u64> {
    let store = SqliteStore::open(env.dir.join("k.db")).unwrap();
    let events = store.read_stream("t1", "Order", "o1").unwrap();
    events.iter().map(|e| e.version).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn crashed_worker_command_is_redelivered_without_waiting_for_the_lease() {
    let env = start_with("crash", 30_000).await;
    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let crashed = tokio::spawn(run_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::Crash, seen),
    ));

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    crashed.await.unwrap();

    let started = Instant::now();
    spawn_worker(&env);
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
    tokio::spawn(run_worker(env.addr.clone(), slow));

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    spawn_worker(&env);

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
    let forger = tokio::spawn(run_worker(
        env.addr.clone(),
        TestWorker::with(Behavior::ForgeResult, seen),
    ));

    let addr = env.addr.clone();
    let pending = tokio::spawn(async move {
        submit(&addr, signed(command("c1", "PlaceOrder", "o1"), &key(1))).await
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "c1");
    assert_eq!(forger.await.unwrap().code(), Code::Unauthenticated);

    spawn_worker(&env);
    let result = pending.await.unwrap().unwrap();
    assert_eq!(committed_versions(&result), [1]);
    assert_eq!(stream_versions(&env), [1]);
}

/// Never leave a kernel process behind when a test fails.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

/// SIGTERM: the kernel exits cleanly, writes a final checkpoint and hands in-flight
/// commands back, so they are available immediately and not only after their lease expires.
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_releases_in_flight_commands_and_checkpoints() {
    let dir = std::env::temp_dir().join(format!("kn-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let sock = dir.join("k.sock");
    let addr = format!("unix:{}", sock.display());
    let config = format!(
        "db = \"k.db\"\nlisten = \"{addr}\"\nkernel_key = \"kernel.key\"\nlease_ttl_ms = 60000\n\
         checkpoint_interval_ms = 3600000\n[clients]\n\"{}\" = [\"*\"]\n\
         [workers]\n\"{}\" = [\"PlaceOrder\", \"AddItem\"]\n",
        id(&key(1)),
        id(&key(2)),
    );
    std::fs::write(dir.join("kernable.toml"), config).unwrap();
    let mut kernel = KillOnDrop(
        Process::new(env!("CARGO_BIN_EXE_kernable-kernel"))
            .arg(dir.join("kernable.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for(&addr).await;

    // One committed command, so the final checkpoint has something to anchor.
    let honest = tokio::spawn(run_worker(addr.clone(), TestWorker::honest()));
    let c0 = signed(command("c0", "PlaceOrder", "o0"), &key(1));
    assert_eq!(committed_versions(&submit(&addr, c0).await.unwrap()), [1]);
    honest.abort();

    let (seen, mut seen_rx) = mpsc::unbounded_channel();
    let slow = TestWorker::with(Behavior::Slow(Duration::from_secs(30)), seen);
    tokio::spawn(run_worker(addr.clone(), slow));
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
        .args(["-TERM", &kernel.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let started = Instant::now();
    let exit = loop {
        if let Some(exit) = kernel.0.try_wait().unwrap() {
            break exit;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "kernel did not stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(exit.success());

    let store = SqliteStore::open(dir.join("k.db")).unwrap();
    let report = store.verify(&id(&key(3))).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    assert!(report.checkpoints >= 1, "no final checkpoint");
    // Leasable right away despite the 60 s lease: the kernel handed it back.
    let lease = store.lease(&["PlaceOrder"], 0, 1_000).unwrap();
    assert_eq!(lease.map(|l| l.command.command_id).as_deref(), Some("c1"));
    let _ = std::fs::remove_dir_all(&dir);
}
