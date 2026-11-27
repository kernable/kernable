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
    process::{Command as Process, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, Hello, ListenerMessage, Parked,
        RevokeRequest, SubmitRequest, SubscribeRequest, VaultValue, Wait, command_result::Outcome,
        command_service_client::CommandServiceClient, event_service_client::EventServiceClient,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient, trust_service_client::TrustServiceClient,
    },
    signing, transport,
};
use kernable_kernel::crash::{POINTS, STATE_POINTS};
use kernable_store::invariants::{Log, Observed};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type Kernel = KillOnDrop;

fn start_kernel(dir: &Path, flags: &[String], crash_at: Option<&str>) -> Kernel {
    let mut process = Process::new(env!("CARGO_BIN_EXE_kernable"));
    process
        .current_dir(dir)
        .arg("start")
        .args(["--instance-key", "kernel.key"])
        .args(flags)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // KERNABLE_TEST_KERNEL_LOG=<file>: every kernel's log appended there, to see what a failed
    // run did.
    if let Ok(path) = std::env::var("KERNABLE_TEST_KERNEL_LOG") {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("the kernel log file");
        process.stderr(log);
    }
    if let Some(point) = crash_at {
        process.env("KERNABLE_CRASH_AT", point);
    }
    KillOnDrop::spawn(&mut process)
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
/// acknowledgements, resuming at its stored position whenever the kernel goes away. Returns
/// once it is registered: a new listener starts at the newest event, so commands sent before
/// would never reach it.
async fn listener(addr: &str, seen: Arc<Mutex<Observed>>) {
    let addr = addr.to_owned();
    let (tx, ready) = tokio::sync::oneshot::channel();
    let mut first = Some(tx);
    tokio::spawn(async move {
        loop {
            let _ = listen_once(&addr, &seen, &mut first).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let _ = ready.await;
}

async fn listen_once(
    addr: &str,
    seen: &Mutex<Observed>,
    ready: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), tonic::Status> {
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
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
                continue;
            }
            Out::Batch(b) => {
                let mut seen = seen.lock().unwrap();
                seen.delivered("audit", &b);
                seen.acked("audit", b.up_to);
                // It gives up on every ItemAdded: the parked set is stored with the position.
                // Up to position 9000 only: a long kill run's listener stays below the kernel's
                // cap of parked events (MAX_PARKED), past which it refuses the ack.
                let parked = b
                    .events
                    .iter()
                    .filter(|e| e.event_type == "ItemAdded" && e.global_position <= 9_000)
                    .map(|e| Parked {
                        position: e.global_position,
                        code: "not_found".into(),
                        message: "no such item".into(),
                        attempts: 1,
                    })
                    .collect();
                In::Ack(Ack {
                    position: b.up_to,
                    parked,
                    skipped: Default::default(),
                })
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
    listener(&addr, Arc::clone(&seen)).await;

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
        if let Some(exit) = kernel.child.try_wait().unwrap() {
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
        let read = kernable_store::open_read_only(&store).unwrap();
        let audit = read
            .listeners()
            .unwrap()
            .into_iter()
            .find(|l| l.name == "audit");
        let stored = audit.as_ref().map_or(0, |l| l.position);
        let parked: Vec<u64> = audit.map_or(Vec::new(), |l| {
            l.parked.iter().map(|p| p.position).collect()
        });
        // Every ItemAdded, once: no parked event lost with a crash, none kept twice.
        let added: Vec<u64> = read
            .read_all(0, 100)
            .unwrap()
            .iter()
            .filter(|e| e.event_type == "ItemAdded")
            .map(|e| e.global_position)
            .collect();
        if unique == (1..=head).collect() && stored == head && parked == added {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{point}: listener saw {unique:?}, stored position {stored}, parked {parked:?} of {added:?}"
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

/// `kill -9` at random moments under load: three clients keep placing orders with two items
/// each, while the kernel dies and restarts `KERNABLE_KILLS` times (5 by default). Afterwards
/// every command has its one expected outcome, nothing is lost or duplicated, the listener has
/// seen every event, and every invariant holds.
#[tokio::test(flavor = "multi_thread")]
async fn random_kills_under_load_lose_nothing() {
    let kills: u32 = std::env::var("KERNABLE_KILLS")
        .ok()
        .and_then(|k| k.parse().ok())
        .unwrap_or(5);
    let dir = std::env::temp_dir().join(format!("kn-kills-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let store = store_url(&dir);
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

    let mut kernel = start_kernel(&dir, &flags, None);
    wait_for(&addr).await;
    worker(&addr).await;
    let seen = Arc::new(Mutex::new(Observed::default()));
    listener(&addr, Arc::clone(&seen)).await;

    // Orders as the clients placed them: each its three commands.
    let placed: Arc<Mutex<Vec<Vec<Command>>>> = Arc::default();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let clients: Vec<_> = (0..3)
        .map(|c| {
            let (addr, seen) = (addr.clone(), Arc::clone(&seen));
            let (placed, stop) = (Arc::clone(&placed), Arc::clone(&stop));
            tokio::spawn(async move {
                let mut n = 0;
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let order = format!("k{c}-{n}");
                    let commands: Vec<Command> = ["PlaceOrder", "AddItem", "AddItem"]
                        .iter()
                        .enumerate()
                        .map(|(i, t)| signed(command(&format!("{order}-{i}"), t, &order), &key(1)))
                        .collect();
                    placed.lock().unwrap().push(commands.clone());
                    for cmd in commands {
                        let result = submit_until_answered(&addr, cmd).await;
                        seen.lock().unwrap().outcome(&result);
                    }
                    n += 1;
                }
            })
        })
        .collect();

    for _ in 0..kills {
        tokio::time::sleep(random_ms(30..300)).await;
        drop(kernel); // SIGKILL, and waits for it
        let _ = std::fs::remove_file(dir.join("k.sock"));
        kernel = start_kernel(&dir, &flags, None);
        wait_for(&addr).await;
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    for client in clients {
        client.await.unwrap();
    }

    // Resubmitted, every command has its outcome: each order has exactly versions 1 to 3.
    let orders = placed.lock().unwrap().clone();
    for cmd in orders.iter().flatten() {
        let result = submit_until_answered(&addr, cmd.clone()).await;
        assert!(
            matches!(&result.outcome, Some(Outcome::Committed(c)) if c.events.len() == 1),
            "{}: {result:?}",
            cmd.command_id
        );
        seen.lock().unwrap().outcome(&result);
    }
    let read = kernable_store::open_read_only(&store).unwrap();
    for commands in &orders {
        let order = &commands[0].aggregate_id;
        let versions: Vec<u64> = read
            .read_stream("t1", "orders", "Order", order)
            .unwrap()
            .iter()
            .map(|e| e.version)
            .collect();
        assert_eq!(versions, [1, 2, 3], "order {order}");
    }
    let head = read.head_position().unwrap();
    assert_eq!(head, 3 * orders.len() as u64, "nothing lost, nothing twice");
    eprintln!("{kills} kills under {} orders", orders.len());
    drop(read);

    let started = Instant::now();
    loop {
        let unique = seen.lock().unwrap().delivered_positions("audit");
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
            started.elapsed() < Duration::from_secs(10),
            "listener saw {unique:?}, stored position {stored}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await; // a checkpoint at the head
    let read = kernable_store::open_read_only(&store).unwrap();
    let log = Log::read(read.as_ref(), &[id(&key(3)).to_string()]).unwrap();
    let mut violations = log.check();
    violations.extend(seen.lock().unwrap().check(&log));
    assert!(violations.is_empty(), "{violations:#?}");
    drop(read);
    drop(kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Waits until the kernel aborted at `point`.
async fn crashed(kernel: &mut Kernel, point: &str) {
    let started = Instant::now();
    let exit = loop {
        if let Some(exit) = kernel.child.try_wait().unwrap() {
            break exit;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{point}: never reached"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(!exit.success(), "{point}: exited instead of crashing");
}

/// A worker that keeps the customer's email in the vault on `PlaceOrder` (subject: the
/// payload's `customer`) and erases the payload's `erase` on `AddItem`, granted to erase.
/// Returns once the kernel registered it the first time.
async fn vault_worker(addr: &str) {
    let addr = addr.to_owned();
    let (tx, registered) = tokio::sync::oneshot::channel();
    let mut first = Some(tx);
    tokio::spawn(async move {
        loop {
            let writes: VaultWriter = Arc::new(|cmd: &Command| {
                let payload: serde_json::Value =
                    serde_json::from_slice(&cmd.payload).unwrap_or_default();
                let field = |f: &str| payload[f].as_str().unwrap_or_default().to_owned();
                match cmd.command_type.as_str() {
                    "PlaceOrder" => (
                        vec![vault_value(&field("customer"), r#""x@example.com""#)],
                        String::new(),
                    ),
                    _ => (Vec::new(), field("erase")),
                }
            });
            let worker = TestWorker {
                writes_vault: Some(writes),
                grant: Some(grant(
                    &key(2),
                    &[("run", &["orders.*"]), ("vault_erase", &["orders.*"])],
                )),
                registered: first.take(),
                ..TestWorker::honest()
            };
            run_worker(addr.clone(), worker).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let _ = registered.await;
}

fn order_with(id: &str, command_type: &str, order: &str, payload: &str) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.payload = payload.as_bytes().to_vec();
    signed(cmd, &key(1))
}

/// Submits `cmd` with the client's vault `values` until the kernel answers.
async fn submit_values(addr: &str, cmd: &Command, values: &[VaultValue]) -> Option<CommandResult> {
    let channel = transport::connect(addr).await.ok()?;
    let request = SubmitRequest {
        command: Some(cmd.clone()),
        wait: Wait::Committed.into(),
        protocol: kernable_core::PROTOCOL,
        listener: String::new(),
        vault: values.to_vec(),
    };
    CommandServiceClient::new(channel)
        .submit(with_grant(request, &key(1)))
        .await
        .ok()
        .map(|r| r.into_inner())
}

async fn revoke(addr: &str, key_id: &str) -> Result<(), tonic::Status> {
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let request = RevokeRequest {
        key_id: key_id.into(),
        since: issued_at,
        issued_at,
        root_sig: Some(signing::sign(
            &root(),
            &signing::revoke_message(key_id, issued_at, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(addr)
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    TrustServiceClient::new(channel)
        .revoke(request)
        .await
        .map(|_| ())
}

/// The values the vault still holds for the vault references of `cmd`'s stored events.
fn vault_values_of(store: &str, vault: &str, command_id: &str) -> usize {
    let read = kernable_store::open_read_only(store).unwrap();
    let refs: Vec<String> = read
        .read_all(0, 100)
        .unwrap()
        .iter()
        .filter(|e| e.command_id == command_id)
        .flat_map(|e| e.vault_refs.clone())
        .collect();
    assert!(!refs.is_empty(), "{command_id} has no vault references");
    kernable_store::vault::Vault::open(
        vault,
        &kernable_kernel::instance::vault_key(key(3).as_bytes()),
    )
    .unwrap()
    .get(&refs)
    .unwrap()
    .len()
}

/// One state crash point: the kernel aborts right after storing a vault value, an erasure, a
/// membership or a revocation, and after the restart that state is in full effect.
async fn state_crash_and_recover(point: &str, round: u32) {
    let dir = std::env::temp_dir().join(format!("kn-state-{point}-{round}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let store = store_url(&dir);
    let vault = format!("sqlite:{}", dir.join("vault.db").display());
    let flags = [
        "--root",
        &id(&root()),
        "--store",
        &store,
        "--listen",
        &addr,
        "--vault-store",
        &vault,
        "--checkpoint-interval-ms",
        "20",
    ]
    .map(String::from);
    let mut kernel = start_kernel(&dir, &flags, Some(point));
    wait_for(&addr).await;
    vault_worker(&addr).await;
    let restart = |kernel: Kernel| {
        drop(kernel);
        let _ = std::fs::remove_file(dir.join("k.sock"));
        start_kernel(&dir, &flags, None)
    };

    match point {
        "vault.stored" => {
            let value = vault_value("k-9", r#""k9@example.com""#);
            let mut cmd = command("cv", "PlaceOrder", "ov");
            cmd.payload = format!(r#"{{"email":{{"$vault":"{}"}}}}"#, value.r#ref).into_bytes();
            cmd.vault_refs = vec![value.r#ref.clone()];
            let cmd = signed(cmd, &key(1));
            let values = [value];
            let _ = submit_values(&addr, &cmd, &values).await;
            crashed(&mut kernel, point).await;
            kernel = restart(kernel);
            wait_for(&addr).await;
            let started = Instant::now();
            let result = loop {
                if let Some(r) = submit_values(&addr, &cmd, &values).await {
                    break r;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "{point}: no answer"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            assert_eq!(committed_versions(&result), [1], "{point}: {result:?}");
            let held = kernable_store::vault::Vault::open(
                &vault,
                &kernable_kernel::instance::vault_key(key(3).as_bytes()),
            )
            .unwrap()
            .get(&cmd.vault_refs)
            .unwrap();
            assert_eq!(held.len(), 1, "{point}: the client's value is kept once");
        }
        "erasure.intended" | "erasure.pending" | "erasure.done" => {
            let place = order_with("c1", "PlaceOrder", "o1", r#"{"customer":"k-1"}"#);
            let placed = submit_until_answered(&addr, place.clone()).await;
            assert_eq!(committed_versions(&placed), [1]);
            assert_eq!(vault_values_of(&store, &vault, "c1"), 1);
            let erase = order_with("c2", "AddItem", "o1", r#"{"erase":"k-1"}"#);
            let _ = submit_with(&addr, erase.clone(), Wait::Committed).await;
            crashed(&mut kernel, point).await;
            kernel = restart(kernel);
            wait_for(&addr).await;
            if point != "erasure.intended" {
                // Committed before the crash: erased on start, before anything is served.
                assert_eq!(
                    vault_values_of(&store, &vault, "c1"),
                    0,
                    "{point}: erased at start"
                );
            }
            let erased = submit_until_answered(&addr, erase).await;
            assert_eq!(committed_versions(&erased), [2], "{point}: {erased:?}");
            assert_eq!(
                vault_values_of(&store, &vault, "c1"),
                0,
                "{point}: the subject is erased"
            );
        }
        "membership.stored" => {
            let shop = membership("shop", &["shop.Cart"], &[], &[]);
            let _ = apply(&addr, shop.clone()).await;
            crashed(&mut kernel, point).await;
            kernel = restart(kernel);
            wait_for(&addr).await;
            let read = kernable_store::open_read_only(&store).unwrap();
            let stored = read.memberships().unwrap();
            assert!(
                stored
                    .iter()
                    .any(|r| r.membership.as_ref().is_some_and(|m| m.service == "shop")),
                "{point}: the membership survived"
            );
            // In effect: commands of the listed aggregate queue although no worker runs it.
            let mut cart = command("cart-1", "AddToCart", "cart-1");
            cart.domain = "shop".into();
            cart.aggregate_type = "Cart".into();
            let queued = submit_with(&addr, signed(cart, &key(1)), Wait::Accepted).await;
            assert!(
                matches!(&queued, Ok(r) if matches!(r.outcome, Some(Outcome::Accepted(_)))),
                "{point}: {queued:?}"
            );
            apply(&addr, shop).await.expect("applied again");
        }
        "revocation.stored" => {
            let _ = revoke(&addr, &id(&key(4))).await;
            crashed(&mut kernel, point).await;
            kernel = restart(kernel);
            wait_for(&addr).await;
            let read = kernable_store::open_read_only(&store).unwrap();
            assert!(
                read.revocations()
                    .unwrap()
                    .iter()
                    .any(|r| r.key_id == id(&key(4))),
                "{point}: the revocation survived"
            );
            let refused = subscribe_as(&addr, &key(4)).await.unwrap_err();
            assert_eq!(refused.code(), tonic::Code::PermissionDenied, "{point}");
            revoke(&addr, &id(&key(4))).await.expect("revoked again");
        }
        other => panic!("no scenario for {other}"),
    }

    tokio::time::sleep(Duration::from_millis(50)).await; // a checkpoint at the head
    let read = kernable_store::open_read_only(&store).unwrap();
    let log = Log::read(read.as_ref(), &[id(&key(3))]).unwrap();
    let violations = log.check();
    assert!(violations.is_empty(), "{point}: {violations:#?}");
    drop(read);
    drop(kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn subscribe_as(addr: &str, key_: &SigningKey) -> Result<(), tonic::Status> {
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let types = vec!["orders.OrderPlaced".to_string()];
    let request = SubscribeRequest {
        after_position: 0,
        event_types: types.clone(),
        issued_at,
        client_sig: Some(signing::sign(
            key_,
            &signing::subscribe_message(0, &types, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(addr).await.unwrap();
    let mut stream = EventServiceClient::new(channel)
        .subscribe(with_grant(request, key_))
        .await?
        .into_inner();
    // A refusal may also come as the stream's first item.
    match tokio::time::timeout(Duration::from_millis(300), stream.message()).await {
        Ok(Err(status)) => Err(status),
        _ => Ok(()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_state_crash_point_recovers_in_full_effect() {
    let rounds = std::env::var("KERNABLE_CRASH_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(1);
    for round in 0..rounds {
        for point in STATE_POINTS {
            state_crash_and_recover(point, round).await;
        }
    }
}
