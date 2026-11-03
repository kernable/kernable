//! Replication under `kill -9`, with real kernel processes: nothing a client was
//! told is ever lost, a promoted standby or a new primary of a pair serves on, and every store
//! invariant holds at the end.
//!
//! `KERNABLE_KILLS=<n>` sets the number of kills (6 by default; `make torture` more).

mod common;

use std::{
    path::{Path, PathBuf},
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
use kernable_store::invariants::Log;
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
        let _ = std::fs::remove_dir_all(&self.0);
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

fn start(dir: &Dir, args: &[String]) -> KillOnDrop {
    KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir.0)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
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
        if let Ok(channel) = transport::connect(&addrs[at % addrs.len()]).await
            && let Ok(result) = submit_on(channel, cmd.clone(), Wait::Committed).await
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

#[allow(dead_code)]
fn unused(_: &Path) {}
