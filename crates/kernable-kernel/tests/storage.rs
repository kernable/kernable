//! Storage faults: the kernel runs on a store that fails a seeded share of its reads and
//! writes, before or after the write took effect, as a full disk, a failed fsync or a lost
//! connection would. Requests may fail meanwhile; nothing may be lost, done twice or corrupted.
//! Once the faults stop, every command resubmitted gets exactly its one outcome, the listener
//! has seen every event, and every store and observation invariant holds.
//!
//! `KERNABLE_FAULT_SEEDS=<n>` runs n seeds (`make torture`).

mod common;

use std::{
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
use kernable_store::{
    Store,
    faults::{Faults, FaultyStore},
    invariants::{Log, Observed},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Three orders with three items each, and one command the domain rejects.
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

/// A worker that reconnects whenever its session ends.
fn worker(addr: &str) {
    let addr = addr.to_owned();
    tokio::spawn(async move {
        loop {
            run_worker(addr.clone(), TestWorker::honest()).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
}

/// A listener that records into `seen` and reconnects whenever its stream ends.
fn listener(addr: &str, seen: Arc<Mutex<Observed>>) {
    let addr = addr.to_owned();
    tokio::spawn(async move {
        loop {
            let _ = listen_once(&addr, &seen).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
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
        from_position: None,
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

/// Submits until the kernel gives an outcome; errors (injected faults) are retried.
async fn submit_until_answered(addr: &str, cmd: &Command, seen: &Mutex<Observed>) -> CommandResult {
    let started = Instant::now();
    loop {
        if let Ok(result) = submit_with(addr, cmd.clone(), Wait::Settled).await {
            seen.lock().unwrap().outcome(&result);
            return result;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "no answer for {}",
            cmd.command_id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run(seed: u64) {
    let faults = Faults::new(seed, 4, 25);
    // Off while the kernel opens: a store that fails then stops it, which is right but not
    // what this test is about.
    faults.arm(false);
    let wrapped = Arc::clone(&faults);
    let env = start_wrapped(
        &format!("storage-{seed}"),
        |config, _| {
            config.submit_timeout_ms = 2_000;
            config.settle_timeout_ms = 500;
        },
        |store| Box::new(FaultyStore::new(store, wrapped)) as Box<dyn Store>,
    )
    .await;
    // The listener registers before the first command, so it must see every event.
    let seen = Arc::new(Mutex::new(Observed::default()));
    listener(&env.addr, Arc::clone(&seen));
    let started = Instant::now();
    while kernable_store::open_read_only(&env.store)
        .unwrap()
        .listeners()
        .unwrap()
        .is_empty()
    {
        assert!(started.elapsed() < Duration::from_secs(5), "no listener");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    worker(&env.addr);
    faults.arm(true);

    // Under faults: every command eventually gets an outcome.
    let commands = workload();
    for cmd in &commands {
        submit_until_answered(&env.addr, cmd, &seen).await;
    }
    let injected = faults
        .failed_before
        .load(std::sync::atomic::Ordering::SeqCst)
        + faults
            .failed_after
            .load(std::sync::atomic::Ordering::SeqCst);
    assert!(injected > 0, "seed {seed}: no fault was injected");

    // Without faults: resubmitting returns the same outcomes, which are the expected ones.
    faults.arm(false);
    for cmd in &commands {
        let result = submit_until_answered(&env.addr, cmd, &seen).await;
        match (&result.outcome, cmd.aggregate_id.as_str()) {
            (Some(Outcome::Rejected(r)), "nowhere") => assert_eq!(r.code, "invalid_state"),
            (Some(Outcome::Committed(c)), _) => {
                assert_eq!(c.events.len(), 1, "seed {seed}: {}", cmd.command_id)
            }
            (other, _) => panic!("seed {seed}: {} ended as {other:?}", cmd.command_id),
        }
    }
    let read = kernable_store::open_read_only(&env.store).unwrap();
    for order in ["o0", "o1", "o2"] {
        let versions: Vec<u64> = read
            .read_stream("t1", "orders", "Order", order)
            .unwrap()
            .iter()
            .map(|e| e.version)
            .collect();
        assert_eq!(versions, [1, 2, 3, 4], "seed {seed}: {order}");
    }
    let head = read.head_position().unwrap();
    assert_eq!(head, 12, "seed {seed}: nothing lost, nothing twice");
    drop(read);

    // The listener catches up and its stored position follows.
    let started = Instant::now();
    loop {
        let unique = seen.lock().unwrap().delivered_positions("audit");
        let stored = kernable_store::open_read_only(&env.store)
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
            started.elapsed() < Duration::from_secs(10),
            "seed {seed}: listener saw {unique:?}, stored position {stored}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await; // a checkpoint at the head

    let read = kernable_store::open_read_only(&env.store).unwrap();
    let log = Log::read(read.as_ref(), &[id(&key(3))]).unwrap();
    let mut violations = log.check();
    violations.extend(seen.lock().unwrap().check(&log));
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_faults_lose_nothing_and_do_nothing_twice() {
    let seeds = std::env::var("KERNABLE_FAULT_SEEDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(3u64);
    for seed in 1..=seeds {
        run(seed).await;
    }
}
