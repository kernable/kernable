//! Crash points (`--features crash-points`): the real kernel aborts at each point where it has
//! stored something but not yet told anyone, is restarted, and the workload is resubmitted.
//! Afterwards every command has exactly one outcome, nothing is lost or duplicated, the chain
//! verifies, and the listener has seen every event: the workload's own expectations, and every
//! store and observation invariant (`kernable_store::invariants`).
//!
//! `KERNABLE_CRASH_ROUNDS=<n>` repeats every point n times (`make torture`).

mod common;

use std::{
    path::Path,
    process::{Child, Command as Process, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, Hello, ListenerMessage, Wait,
        command_result::Outcome, listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_kernel::crash::POINTS;
use kernable_store::invariants::{Log, Observed};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

struct Kernel(Child);

impl Drop for Kernel {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_kernel(dir: &Path, flags: &[String], crash_at: Option<&str>) -> Kernel {
    let mut process = Process::new(env!("CARGO_BIN_EXE_kernable"));
    process
        .current_dir(dir)
        .arg("start")
        .args(flags)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(point) = crash_at {
        process.env("KERNABLE_CRASH_AT", point);
    }
    Kernel(process.spawn().unwrap())
}

/// The workload: three orders with items, and one command the domain rejects.
fn workload() -> Vec<Command> {
    let mut commands = Vec::new();
    for i in 0..12 {
        let order = format!("o{}", i % 3);
        let command_type = if i < 3 { "PlaceOrder" } else { "AddItem" };
        commands.push(command(&format!("c{i}"), command_type, &order));
    }
    commands.push(command("c-missing", "AddItem", "nowhere"));
    commands.into_iter().map(|c| signed(c, &key(1))).collect()
}

/// A worker that sends snapshots, reconnecting whenever the kernel goes away. Returns once the
/// kernel registered it the first time: commands for an unregistered aggregate are refused.
async fn worker(addr: &str) {
    let addr = addr.to_owned();
    let (tx, registered) = tokio::sync::oneshot::channel();
    let mut first = Some(tx);
    tokio::spawn(async move {
        loop {
            let worker = TestWorker {
                behavior: Behavior::Snapshots,
                registered: first.take(),
                ..TestWorker::honest()
            };
            run_worker(addr.clone(), worker).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let _ = registered.await;
}

/// A listener that acknowledges every batch and records its connections, batches and
/// acknowledgements, resuming at its stored position whenever the kernel goes away.
fn listener(addr: &str, seen: Arc<Mutex<Observed>>) {
    let addr = addr.to_owned();
    tokio::spawn(async move {
        loop {
            let _ = listen_once(&addr, &seen).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

async fn listen_once(addr: &str, seen: &Mutex<Observed>) -> Result<(), tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: "audit".into(),
        key_id: id(&key(5)),
        order: 0,
        ..Default::default()
    };
    tx.send(send(In::Hello(hello))).await.unwrap();
    let channel = transport::connect(addr)
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(with_grant(ReceiverStream::new(rx), &key(5)))
        .await?
        .into_inner();
    while let Some(msg) = inbound.message().await?.and_then(|m| m.msg) {
        let reply = match msg {
            Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig,
            }),
            Out::Ready(r) => {
                seen.lock()
                    .unwrap()
                    .connected("audit", &[], r.after_position);
                continue;
            }
            Out::Batch(b) => {
                let mut seen = seen.lock().unwrap();
                seen.delivered("audit", &b);
                seen.acked("audit", b.up_to);
                In::Ack(Ack { position: b.up_to })
            }
            _ => continue,
        };
        if tx.send(send(reply)).await.is_err() {
            break;
        }
    }
    Ok(())
}

/// Submits until the kernel answers (it may be restarting).
async fn submit_until_answered(addr: &str, cmd: Command) -> CommandResult {
    let started = Instant::now();
    loop {
        if let Ok(channel) = transport::connect(addr).await
            && let Ok(result) = submit_on(channel, cmd.clone(), Wait::Settled).await
        {
            return result;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "no answer for {}",
            cmd.command_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn crash_and_recover(point: &str, round: u32) {
    let dir = std::env::temp_dir().join(format!("kn-crash-{point}-{round}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let store = store_url(&dir);
    // The client, worker and listener present their grants, as the SDKs do.
    let flags = [
        "--root",
        &id(&root()),
        "--store",
        &store,
        "--listen",
        &addr,
        "--checkpoint-interval-ms",
        "20",
        "--settle-timeout-ms",
        "2000",
    ]
    .map(String::from);

    let mut kernel = start_kernel(&dir, &flags, Some(point));
    wait_for(&addr).await;
    worker(&addr).await;
    let seen = Arc::new(Mutex::new(Observed::default()));
    listener(&addr, Arc::clone(&seen));

    // Run the workload until the kernel crashes at the point.
    let commands = workload();
    for cmd in &commands {
        let channel = transport::connect(&addr).await;
        let result = match channel {
            Ok(channel) => submit_on(channel, cmd.clone(), Wait::Settled).await.ok(),
            Err(_) => None,
        };
        let Some(result) = result else {
            break;
        };
        seen.lock().unwrap().outcome(&result);
    }
    let started = Instant::now();
    let exit = loop {
        if let Some(exit) = kernel.0.try_wait().unwrap() {
            break exit;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{point}: the workload never reached the crash point"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        !exit.success(),
        "{point}: the kernel exited instead of crashing"
    );

    // Restart and finish the workload: resubmitting returns stored outcomes.
    let _ = std::fs::remove_file(dir.join("k.sock"));
    kernel = start_kernel(&dir, &flags, None);
    wait_for(&addr).await;
    let mut outcomes = Vec::new();
    for cmd in &commands {
        let result = submit_until_answered(&addr, cmd.clone()).await;
        seen.lock().unwrap().outcome(&result);
        outcomes.push(result);
    }

    // Every command has exactly the outcome it would have had without the crash.
    for (cmd, result) in commands.iter().zip(&outcomes) {
        match (&result.outcome, cmd.aggregate_id.as_str()) {
            (Some(Outcome::Rejected(r)), "nowhere") => assert_eq!(r.code, "invalid_state"),
            (Some(Outcome::Committed(c)), _) => assert_eq!(c.events.len(), 1, "{point}"),
            (other, _) => panic!("{point}: {} ended as {other:?}", cmd.command_id),
        }
    }
    let read = kernable_store::open_read_only(&store).unwrap();
    for order in ["o0", "o1", "o2"] {
        let versions: Vec<u64> = read
            .read_stream("t1", "orders", "Order", order)
            .unwrap()
            .iter()
            .map(|e| e.version)
            .collect();
        assert_eq!(versions, [1, 2, 3, 4], "{point}: {order}");
    }
    let head = read.head_position().unwrap();
    assert_eq!(head, 12, "{point}: nothing lost, nothing twice");

    // The listener saw every event (at least once) and its stored position caught up.
    let started = Instant::now();
    loop {
        let unique = seen.lock().unwrap().delivered_positions("audit");
        // Opened again each time: a read-only file store is a snapshot of the log.
        let stored = kernable_store::open_read_only(&store)
            .unwrap()
            .listeners()
            .unwrap()
            .iter()
            .find(|l| l.name == "audit")
            .map_or(0, |l| l.position);
        if unique == (1..=head).collect() && stored == head {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{point}: listener saw {unique:?}, stored position {stored}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(read);
    tokio::time::sleep(Duration::from_millis(50)).await; // a checkpoint at the head
    let read = kernable_store::open_read_only(&store).unwrap();
    let ids = seen.lock().unwrap().command_ids();
    let log = Log::read_with(read.as_ref(), &[id(&key(3)).to_string()], &ids).unwrap();
    let mut violations = log.check();
    violations.extend(seen.lock().unwrap().check(&log));
    assert!(violations.is_empty(), "{point}: {violations:#?}");
    drop(read);
    drop(kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_crash_point_recovers_without_loss_or_duplicates() {
    let rounds = std::env::var("KERNABLE_CRASH_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(1);
    for round in 0..rounds {
        for point in POINTS {
            crash_and_recover(point, round).await;
        }
    }
}
