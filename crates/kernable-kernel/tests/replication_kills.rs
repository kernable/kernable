//! Replication under `kill -9`, with real kernel processes: nothing a client was
//! told is ever lost, a promoted standby or a new primary of a pair serves on, and every store
//! invariant holds at the end.
//!
//! The model test of a pair (P1–P4) runs a seeded sequence of faults: kills of either kernel
//! and of the witness, and pauses (`SIGSTOP`) of either kernel, shorter or longer than the
//! lease, while clients submit and a listener follows the pair. Afterwards nothing a client or
//! the listener was told is missing from the primary's log or differs from it (P1, P3: O1–O4
//! against that log), both kernels hold the same log (P2, P4), and S1–S9 hold on both.
//!
//! `KERNABLE_KILLS=<n>` sets the number of kills (6 by default; `make torture` more);
//! `KERNABLE_PAIR_SEEDS=<a,b,…>` the seeds of the model test (1,2,3 by default),
//! `KERNABLE_PAIR_STEPS=<n>` its faults per seed (8 by default).

mod common;

use std::{
    path::PathBuf,
    process::{Command as Process, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{
        Command, CommandResult, Event, PromoteRequest, Wait, command_result::Outcome,
        replication_service_client::ReplicationServiceClient,
    },
    signing, transport,
};
use kernable_kernel::witness::Witness;
use kernable_store::invariants::{Log, Observed};
use prost::Message;

const LEASE_MS: u64 = 600;

fn kills() -> u32 {
    std::env::var("KERNABLE_KILLS")
        .ok()
        .and_then(|k| k.parse().ok())
        .unwrap_or(6)
}

fn random_ms(range: std::ops::Range<u64>) -> Duration {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).unwrap();
    Duration::from_millis(range.start + u64::from_le_bytes(b) % (range.end - range.start))
}

fn coin() -> bool {
    random_ms(0..2).as_millis() == 1
}

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("kn-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }

    fn sock(&self, name: &str) -> String {
        format!("unix:{}", self.0.join(format!("{name}.sock")).display())
    }

    fn store(&self, name: &str) -> String {
        match std::env::var("KERNABLE_TEST_STORE").as_deref() {
            Ok("file") => format!("file:{}", self.0.join(format!("{name}.log")).display()),
            _ => format!("sqlite:{}", self.0.join(format!("{name}.db")).display()),
        }
    }

    /// Writes `<name>.key` with the seed's key, and `<name>.grant` with `permissions`.
    fn key(&self, name: &str, seed: u8, permissions: &[(&str, &[&str])]) -> PathBuf {
        let path = self.0.join(format!("{name}.key"));
        std::fs::write(&path, hex(key(seed).as_bytes())).unwrap();
        if !permissions.is_empty() {
            let grant = grant(&key(seed), permissions);
            std::fs::write(
                self.0.join(format!("{name}.grant")),
                hex(&grant.encode_to_vec()),
            )
            .unwrap();
        }
        path
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("kept for its logs: {}", self.0.display());
        } else if std::env::var_os("KERNABLE_TEST_LOGS").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// The flags every kernel of these tests starts with: its store, socket and keys in `dir`.
fn flags(dir: &Dir, name: &str, kernel_seed: u8) -> Vec<String> {
    dir.key(&format!("{name}-kernel"), kernel_seed, &[]);
    let at = |file: &str| dir.0.join(file).display().to_string();
    [
        "--root".into(),
        id(&root()),
        "--store".into(),
        dir.store(name),
        "--listen".into(),
        dir.sock(name),
        "--kernel-key".into(),
        at(&format!("{name}-kernel.key")),
        "--snapshots".into(),
        format!("sqlite:{}", at(&format!("{name}-snapshots.db"))),
        "--data-store".into(),
        format!("file:{}", at(&format!("{name}-data"))),
        "--checkpoint-interval-ms".into(),
        "20".into(),
        "--settle-timeout-ms".into(),
        "2000".into(),
    ]
    .to_vec()
}

/// Each process's stderr goes to `<dir>/<n>-<command>.log`, kept when a test fails (or with
/// `KERNABLE_TEST_LOGS`).
fn start(dir: &Dir, args: &[String]) -> KillOnDrop {
    static STARTED: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = STARTED.fetch_add(1, SeqCst);
    let name = format!("{n:03}-{}.log", args.first().map_or("", String::as_str));
    let log = std::fs::File::create(dir.0.join(name)).unwrap();
    std::fs::write(dir.0.join(format!("{n:03}.args")), args.join(" ")).unwrap();
    let stderr = Stdio::from(log);
    KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir.0)
            .args(args)
            .stdout(Stdio::null())
            .stderr(stderr),
    )
}

/// An honest worker on `addr`, connecting again whenever the kernel goes away or refuses it
/// (a standby does).
fn worker(addr: String) {
    tokio::spawn(async move {
        loop {
            run_worker(addr.clone(), TestWorker::honest()).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

/// Submits `cmd` to the first of `addrs` that answers, trying them in turn, as an SDK given
/// both kernels of a pair does.
async fn submit_until_answered(addrs: &[String], cmd: &Command) -> CommandResult {
    let started = Instant::now();
    let mut at = 0;
    loop {
        // Each try is bounded, so a kernel that never answers fails the test, not hangs it.
        if let Ok(channel) = transport::connect(&addrs[at % addrs.len()]).await
            && let Ok(Ok(result)) = tokio::time::timeout(
                Duration::from_secs(5),
                submit_on(channel, cmd.clone(), Wait::Committed),
            )
            .await
        {
            return result;
        }
        at += 1;
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "no answer for {}",
            cmd.command_id
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn events(result: &CommandResult) -> Vec<Event> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.clone(),
        other => panic!("{}: not committed: {other:?}", result.command_id),
    }
}

/// Clients placing orders (three commands each) on `addrs` until `stop`; every answer they get
/// is kept in `told`.
struct Load {
    stop: Arc<AtomicBool>,
    placed: Arc<Mutex<Vec<Vec<Command>>>>,
    told: Arc<Mutex<Vec<CommandResult>>>,
    clients: Vec<tokio::task::JoinHandle<()>>,
}

impl Load {
    fn start(addrs: &[String]) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let placed: Arc<Mutex<Vec<Vec<Command>>>> = Arc::default();
        let told: Arc<Mutex<Vec<CommandResult>>> = Arc::default();
        let clients = (0..3)
            .map(|c| {
                let addrs = addrs.to_vec();
                let (stop, placed, told) =
                    (Arc::clone(&stop), Arc::clone(&placed), Arc::clone(&told));
                tokio::spawn(async move {
                    let mut n = 0;
                    while !stop.load(SeqCst) {
                        let order = format!("k{c}-{n}");
                        let commands: Vec<Command> = ["PlaceOrder", "AddItem", "AddItem"]
                            .iter()
                            .enumerate()
                            .map(|(i, t)| {
                                signed(command(&format!("{order}-{i}"), t, &order), &key(1))
                            })
                            .collect();
                        placed.lock().unwrap().push(commands.clone());
                        for cmd in &commands {
                            let result = submit_until_answered(&addrs, cmd).await;
                            told.lock().unwrap().push(result);
                        }
                        n += 1;
                    }
                })
            })
            .collect();
        Load {
            stop,
            placed,
            told,
            clients,
        }
    }

    /// Stops the clients; returns the orders placed and the answers told.
    async fn finish(self) -> (Vec<Vec<Command>>, Vec<CommandResult>) {
        self.stop.store(true, SeqCst);
        for client in self.clients {
            client.await.unwrap();
        }
        let placed = self.placed.lock().unwrap().clone();
        let told = self.told.lock().unwrap().clone();
        (placed, told)
    }
}

/// Resubmits every order to `addrs`: each command has its one outcome, the one the client was
/// told; every order has exactly versions 1 to 3, nothing is lost or twice, and every invariant
/// holds on `store`.
async fn every_order_once(
    addrs: &[String],
    store: &str,
    placed: &[Vec<Command>],
    told: &[CommandResult],
    kernel_keys: &[String],
) {
    for cmd in placed.iter().flatten() {
        let result = submit_until_answered(addrs, cmd).await;
        assert_eq!(events(&result).len(), 1, "{}", cmd.command_id);
        if let Some(first) = told.iter().find(|t| t.command_id == cmd.command_id) {
            assert_eq!(
                events(&result),
                events(first),
                "{}: the outcome a client was told",
                cmd.command_id
            );
        }
    }
    let read = kernable_store::open_read_only(store).unwrap();
    for commands in placed {
        let order = &commands[0].aggregate_id;
        let versions: Vec<u64> = read
            .read_stream("t1", "orders", "Order", order)
            .unwrap()
            .iter()
            .map(|e| e.version)
            .collect();
        assert_eq!(versions, [1, 2, 3], "order {order}");
    }
    assert_eq!(
        read.head_position().unwrap(),
        3 * placed.len() as u64,
        "nothing lost, nothing twice"
    );
    let log = Log::read(read.as_ref(), kernel_keys).unwrap();
    let violations = log.check();
    assert!(violations.is_empty(), "{violations:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn random_kills_of_primary_and_standby_lose_nothing_a_client_was_told() {
    let dir = Dir::new("repl-kills");
    let mut primary_args = vec!["start".to_owned()];
    primary_args.extend(flags(&dir, "p", 3));
    primary_args.extend(["--replication".into(), "sync".into()]);
    let mut standby_args = vec!["start".to_owned()];
    standby_args.extend(flags(&dir, "s", 8));
    let standby_key = dir.key("standby", 7, &[("replicate", &["*"])]);
    standby_args.extend([
        "--follow".into(),
        dir.sock("p"),
        "--follow-key".into(),
        standby_key.display().to_string(),
        "--primary-kernel-key".into(),
        id(&key(3)),
    ]);
    let primary_addr = vec![dir.sock("p")];

    let mut primary = start(&dir, &primary_args);
    wait_for(&dir.sock("p")).await;
    let mut standby = start(&dir, &standby_args);
    worker(dir.sock("p"));
    let load = Load::start(&primary_addr);
    for _ in 0..kills() {
        tokio::time::sleep(random_ms(50..400)).await;
        if coin() {
            drop(primary); // SIGKILL, and waits for it
            primary = start(&dir, &primary_args);
            wait_for(&dir.sock("p")).await;
        } else {
            drop(standby);
            standby = start(&dir, &standby_args);
        }
    }
    let (placed, told) = load.finish().await;
    assert!(!told.is_empty());

    // The primary is lost for good. P1: the standby holds everything a client was told.
    drop(primary);
    {
        let held = kernable_store::open_read_only(&dir.store("s")).unwrap();
        for result in &told {
            let stored = held
                .result(&result.command_id)
                .unwrap()
                .unwrap_or_else(|| panic!("{} was told, not on the standby", result.command_id));
            assert_eq!(events(&stored), events(result), "{}", result.command_id);
        }
    }

    // Promoted, it serves every order once (P3).
    let issued_at = issued_now();
    let promoted = loop {
        let request = PromoteRequest {
            issued_at,
            sig: Some(signing::sign(&key(1), &signing::promote_message(issued_at))),
            protocol: kernable_core::PROTOCOL,
        };
        let Ok(channel) = transport::connect(&dir.sock("s")).await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        break ReplicationServiceClient::new(channel)
            .promote(request)
            .await;
    };
    promoted.unwrap();
    worker(dir.sock("s"));
    let ids = [id(&key(3)), id(&key(8))];
    every_order_once(&[dir.sock("s")], &dir.store("s"), &placed, &told, &ids).await;
    eprintln!("{} kills under {} orders", kills(), placed.len());
    drop(standby);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pair_under_random_kills_of_its_primary_loses_nothing_a_client_was_told() {
    let dir = Dir::new("pair-kills");
    let state = dir.0.join("witness.state");
    let _witness = start(
        &dir,
        &[
            "witness".into(),
            "--listen".into(),
            dir.sock("w"),
            "--root".into(),
            id(&root()),
            "--state".into(),
            state.display().to_string(),
            "--lease-ms".into(),
            LEASE_MS.to_string(),
        ],
    );
    wait_for(&dir.sock("w")).await;
    let node = |name: &str, kernel_seed: u8, node_seed: u8| {
        let mut args = vec!["start".to_owned()];
        args.extend(flags(&dir, name, kernel_seed));
        let node_key = dir.key(&format!("{name}-node"), node_seed, &[("replicate", &["*"])]);
        args.extend([
            "--witness".into(),
            dir.sock("w"),
            "--node-key".into(),
            node_key.display().to_string(),
            "--standby-timeout-ms".into(),
            LEASE_MS.to_string(),
            "--primary-kernel-key".into(),
            id(&key(3)),
            "--primary-kernel-key".into(),
            id(&key(8)),
        ]);
        args
    };
    let nodes = [("a", node("a", 3, 11), 11u8), ("b", node("b", 8, 12), 12u8)];
    let mut running: Vec<Option<KillOnDrop>> = Vec::new();
    for (name, args, _) in &nodes {
        running.push(Some(start(&dir, args)));
        wait_for(&dir.sock(name)).await;
    }
    let addrs: Vec<String> = nodes.iter().map(|(name, ..)| dir.sock(name)).collect();
    for addr in &addrs {
        worker(addr.clone());
    }
    let primary = || {
        let kept = Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state();
        nodes
            .iter()
            .position(|(.., seed)| id(&key(*seed)) == kept.primary)
    };

    let load = Load::start(&addrs);
    let mut failovers = 0;
    for _ in 0..kills() {
        tokio::time::sleep(random_ms(200..800)).await;
        let Some(at) = primary() else { continue };
        drop(running[at].take()); // the primary's machine is gone
        tokio::time::sleep(random_ms(0..(3 * LEASE_MS))).await;
        if primary() != Some(at) {
            failovers += 1;
        }
        running[at] = Some(start(&dir, &nodes[at].1)); // it comes back
    }
    let (placed, told) = load.finish().await;
    assert!(!told.is_empty());
    let at = primary().expect("a primary");
    let ids = [id(&key(3)), id(&key(8))];
    every_order_once(&addrs, &dir.store(nodes[at].0), &placed, &told, &ids).await;
    eprintln!(
        "{} kills, {failovers} failovers under {} orders",
        kills(),
        placed.len()
    );
    drop(running);
}

// ---- the model test of a pair (P1–P4) ----

/// xorshift: the same seed picks the same faults.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn ms(&mut self, range: std::ops::Range<u64>) -> Duration {
        Duration::from_millis(range.start + self.next() % (range.end - range.start))
    }
}

/// Sends `signal` (`STOP`, `CONT`) to `process`.
fn signal(process: &KillOnDrop, signal: &str) {
    let status = Process::new("kill")
        .args([&format!("-{signal}"), &process.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{signal}");
}

/// A listener of every event that follows the pair, trying its kernels in turn (a standby
/// refuses it), resuming from its stored position; everything it is told goes to `observed`.
/// `ready` is set once it first connected: a new listener starts at the head.
fn audit(
    addrs: Vec<String>,
    observed: Arc<Mutex<Observed>>,
    ready: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) {
    use kernable_core::proto::{
        Ack, ChallengeResponse, Hello, ListenerMessage, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
    };
    tokio::spawn(async move {
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let mut at = 0;
        while !stop.load(SeqCst) {
            at += 1;
            tokio::time::sleep(Duration::from_millis(30)).await;
            let Ok(channel) = transport::connect(&addrs[at % addrs.len()]).await else {
                continue;
            };
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            let hello = Hello {
                protocol: kernable_core::PROTOCOL,
                name: "audit".into(),
                key_id: id(&key(5)),
                ..Default::default()
            };
            tx.send(send(In::Hello(hello))).await.unwrap();
            let Ok(inbound) = ListenerServiceClient::new(channel)
                .listen(with_grant(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                    &key(5),
                ))
                .await
            else {
                continue;
            };
            let mut inbound = inbound.into_inner();
            while let Ok(Ok(Some(msg))) =
                tokio::time::timeout(Duration::from_secs(2), inbound.message()).await
            {
                let reply = match msg.msg {
                    Some(Out::Challenge(c)) => In::ChallengeResponse(ChallengeResponse {
                        sig: signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig,
                    }),
                    Some(Out::Ready(r)) => {
                        observed
                            .lock()
                            .unwrap()
                            .connected("audit", &[], r.after_position);
                        ready.store(true, SeqCst);
                        continue;
                    }
                    Some(Out::Batch(b)) => {
                        let mut observed = observed.lock().unwrap();
                        observed.delivered("audit", &b);
                        // Recorded before it is sent: the kernel cannot know of it earlier.
                        observed.acked("audit", b.up_to);
                        In::Ack(Ack {
                            position: b.up_to,
                            ..Default::default()
                        })
                    }
                    _ => continue,
                };
                if tx.send(send(reply)).await.is_err() {
                    break;
                }
            }
        }
    });
}

async fn until_ready(ready: &AtomicBool) {
    let started = Instant::now();
    while !ready.load(SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the listener never connected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every event of `store`, for comparing two kernels' logs.
fn all_events(store: &str) -> Vec<Event> {
    let read = kernable_store::open_read_only(store).unwrap();
    read.read_all(0, 1_000_000).unwrap()
}

/// One seed: a pair and its witness under `steps` faults picked by `seed`.
async fn pair_model(seed: u64, steps: u32) {
    let dir = Dir::new(&format!("pair-model-{seed}"));
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let state = dir.0.join("witness.state");
    let witness_args: Vec<String> = vec![
        "witness".into(),
        "--listen".into(),
        dir.sock("w"),
        "--root".into(),
        id(&root()),
        "--state".into(),
        state.display().to_string(),
        "--lease-ms".into(),
        LEASE_MS.to_string(),
    ];
    let mut witness = Some(start(&dir, &witness_args));
    wait_for(&dir.sock("w")).await;
    let node = |name: &str, kernel_seed: u8, node_seed: u8| {
        let mut args = vec!["start".to_owned()];
        args.extend(flags(&dir, name, kernel_seed));
        let node_key = dir.key(&format!("{name}-node"), node_seed, &[("replicate", &["*"])]);
        args.extend([
            "--witness".into(),
            dir.sock("w"),
            "--node-key".into(),
            node_key.display().to_string(),
            "--standby-timeout-ms".into(),
            LEASE_MS.to_string(),
            "--primary-kernel-key".into(),
            id(&key(3)),
            "--primary-kernel-key".into(),
            id(&key(8)),
        ]);
        args
    };
    let nodes = [("a", node("a", 3, 11), 11u8), ("b", node("b", 8, 12), 12u8)];
    let mut running: Vec<Option<KillOnDrop>> = Vec::new();
    for (name, args, _) in &nodes {
        running.push(Some(start(&dir, args)));
        wait_for(&dir.sock(name)).await;
    }
    let addrs: Vec<String> = nodes.iter().map(|(name, ..)| dir.sock(name)).collect();
    for addr in &addrs {
        worker(addr.clone());
    }
    let primary = || {
        let kept = Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state();
        nodes
            .iter()
            .position(|(.., seed)| id(&key(*seed)) == kept.primary)
    };
    // A deposed primary stops; its supervisor starts it again, as the standby.
    let revive = |running: &mut [Option<KillOnDrop>]| {
        for (n, process) in running.iter_mut().enumerate() {
            if let Some(p) = process
                && p.child.try_wait().unwrap().is_some()
            {
                *process = Some(start(&dir, &nodes[n].1));
            }
        }
    };

    let observed: Arc<Mutex<Observed>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(false));
    audit(
        addrs.clone(),
        Arc::clone(&observed),
        Arc::clone(&ready),
        Arc::clone(&stop),
    );
    until_ready(&ready).await;
    let load = Load::start(&addrs);
    let mut done = Vec::new();
    for _ in 0..steps {
        tokio::time::sleep(rng.ms(100..600)).await;
        revive(&mut running);
        let Some(p) = primary() else { continue };
        let s = 1 - p;
        // Shorter or longer than the lease: a failover, or none.
        let away = rng.ms(0..3 * LEASE_MS);
        let fault = match rng.next() % 5 {
            0 => {
                drop(running[p].take()); // SIGKILL, and waits for it
                tokio::time::sleep(away).await;
                running[p] = Some(start(&dir, &nodes[p].1));
                "kill the primary"
            }
            1 => {
                drop(running[s].take()); // SIGKILL, and waits for it
                tokio::time::sleep(away).await;
                running[s] = Some(start(&dir, &nodes[s].1));
                "kill the standby"
            }
            2 => {
                let process = running[p].as_ref().expect("running");
                signal(process, "STOP");
                tokio::time::sleep(away).await;
                signal(process, "CONT");
                "pause the primary"
            }
            3 => {
                let process = running[s].as_ref().expect("running");
                signal(process, "STOP");
                tokio::time::sleep(away).await;
                signal(process, "CONT");
                "pause the standby"
            }
            _ => {
                drop(witness.take());
                tokio::time::sleep(away).await;
                witness = Some(start(&dir, &witness_args));
                "kill the witness"
            }
        };
        done.push(format!("{fault} for {} ms", away.as_millis()));
    }
    let (placed, told) = load.finish().await;
    assert!(!told.is_empty(), "seed {seed}: nothing was told");

    // Settle: a primary, its standby in sync with it, the listener at its head.
    let settled = Instant::now();
    let mut last = None;
    let at = loop {
        revive(&mut running);
        if let Some(at) = primary() {
            let head = all_events(&dir.store(nodes[at].0)).len();
            let other = all_events(&dir.store(nodes[1 - at].0)).len();
            let heard = observed.lock().unwrap().delivered_positions("audit").len();
            if head == other && heard >= head && head > 0 {
                break at;
            }
            let delivered = observed.lock().unwrap().delivered_positions("audit");
            let missing: Vec<u64> = (1..=head as u64)
                .filter(|p| !delivered.contains(p))
                .collect();
            last = Some((nodes[at].0, head, other, heard, missing));
        }
        assert!(
            settled.elapsed() < Duration::from_secs(30),
            "seed {seed}: the pair never settled after {done:?}: (primary, its head, the \
             standby's, heard by the listener, not heard) {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    stop.store(true, SeqCst);

    // P1, P3: everything a client or the listener was told is in the primary's log, as told.
    let ids = [id(&key(3)), id(&key(8))];
    let primary_store = dir.store(nodes[at].0);
    {
        let read = kernable_store::open_read_only(&primary_store).unwrap();
        let log = Log::read(read.as_ref(), &ids).unwrap();
        let mut observed = observed.lock().unwrap();
        for result in &told {
            observed.outcome(result);
        }
        let mut violations = log.check();
        violations.extend(observed.check(&log));
        assert!(
            violations.is_empty(),
            "seed {seed} after {done:?}: {violations:#?}"
        );
    }
    // P2, P4: the standby holds the same log, and its own is sound.
    let standby_store = dir.store(nodes[1 - at].0);
    assert_eq!(
        all_events(&standby_store),
        all_events(&primary_store),
        "seed {seed} after {done:?}: the standby's log"
    );
    {
        let read = kernable_store::open_read_only(&standby_store).unwrap();
        let violations = Log::read(read.as_ref(), &ids).unwrap().check();
        assert!(
            violations.is_empty(),
            "seed {seed}, standby: {violations:#?}"
        );
    }
    every_order_once(&addrs, &primary_store, &placed, &told, &ids).await;
    eprintln!("seed {seed}: {} orders, {}", placed.len(), done.join(", "));
    drop((running, witness));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pair_under_seeded_kills_and_pauses_keeps_p1_to_p4() {
    let seeds: Vec<u64> = std::env::var("KERNABLE_PAIR_SEEDS")
        .ok()
        .map(|s| s.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 2, 3]);
    let steps = std::env::var("KERNABLE_PAIR_STEPS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(8);
    for seed in seeds {
        pair_model(seed, steps).await;
    }
}
