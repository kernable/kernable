//! The soak: the kernel and an app per SDK (TypeScript, Python, Rust, Go) under continuous load,
//! while faults hit them at random: kills, graceful restarts, stopped processes, a failing
//! network, a full disk, power loss. Every few minutes the faults and the load pause and every
//! invariant is checked (`check.rs`). Ends with a report (`report.json` in the run directory);
//! exits 1 at the first violation, leaving everything in place to look at.
//!
//! ```text
//! kernable-soak run [--minutes 60] [--dir DIR] [--check-every 10]
//!                   [--clients 8] [--rate 0] [--seed N] [--fault-every 45] [--faults a,b,...]
//!                   [--data DIR] [--fill DIR] [--power-loss-cmd CMD] [--pair yes]
//! kernable-soak app    (the Rust app; started by `run`)
//! ```
//!
//! `--rate` limits the load to that many commands per second (0: as fast as they are answered).
//!
//! `--pair yes` runs a pair: two kernels and a witness, each kernel behind a proxy of its
//! own, through which the other kernel and the apps (given both addresses) reach it. The kernel
//! faults hit either kernel, the network faults either proxy, so the pair fails over; each check
//! round reads the primary's store and checks that the standby holds the same (P2).
//!
//! Faults: kernel-kill, kernel-term, kernel-down (killed, back after 6 to 15 s: with a pair,
//! longer than the primary's lease, so the standby takes over), app-kill, app-pause, net-reset,
//! net-down, net-slow, switchover (with a pair: `kernable switchover` asked of both kernels), and
//! with `--fill` (a directory on a small file system that also holds `--data`) disk-full, with
//! `--power-loss-cmd` (run after every process was killed, e.g. to drop a LazyFS cache)
//! power-loss.

mod app;
mod check;
mod load;
mod procs;
mod proxy;

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    sync::{Arc, OnceLock, atomic::Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use kernable::{Kernable, Options};
use kernable_core::signing;
use serde_json::json;

use crate::{
    load::{Identity, Load},
    procs::Proc,
    proxy::Proxy,
};

/// A small, seedable random generator (xorshift64*): faults and load are random, and a seed
/// makes the schedule repeatable (the timing still varies).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    pub fn step(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// In `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.step() % n.max(1)
    }

    /// Between `min` and `max` milliseconds.
    pub fn ms(&mut self, min: u64, max: u64) -> Duration {
        Duration::from_millis(min + self.below(max - min + 1))
    }
}

/// UTC time of day, for logs.
pub fn now() -> String {
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format!("{:02}:{:02}:{:02}", s / 3600 % 24, s / 60 % 60, s % 60)
}

static LOG: OnceLock<PathBuf> = OnceLock::new();

fn say(line: &str) {
    let line = format!("{}  {line}", now());
    println!("{line}");
    if let Some(path) = LOG.get()
        && let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path)
    {
        let _ = writeln!(f, "{line}");
    }
}

struct Args(BTreeMap<String, String>);

impl Args {
    fn parse(args: &[String]) -> Result<Args, String> {
        let mut map = BTreeMap::new();
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            let name = flag
                .strip_prefix("--")
                .ok_or(format!("unexpected {flag}"))?;
            let value = it.next().ok_or(format!("{flag} needs a value"))?;
            map.insert(name.to_owned(), value.clone());
        }
        Ok(Args(map))
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    fn num(&self, name: &str, default: u64) -> Result<u64, String> {
        self.get(name).map_or(Ok(default), |v| {
            v.parse()
                .map_err(|_| format!("--{name}: not a number: {v}"))
        })
    }
}

fn main() -> ExitCode {
    // At least four workers, also on a machine of one core: the harness blocks one now and
    // then (stopping a process), and the load and the audit listener go on meanwhile.
    let workers = std::thread::available_parallelism().map_or(4, |n| n.get().max(4));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("kernable-soak: no runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = runtime.block_on(async {
        match args.first().map(String::as_str) {
            Some("app") => app::run().await.map_err(|e| e.to_string()),
            Some("run") => match Args::parse(&args[1..]) {
                Ok(a) => run(a).await,
                Err(e) => Err(e),
            },
            _ => Err("usage: kernable-soak run [--minutes N] [--dir DIR] ... | kernable-soak app (see src/main.rs)".into()),
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kernable-soak: {e}");
            ExitCode::FAILURE
        }
    }
}

fn cli(kernable: &Path, dir: &Path, args: &[&str]) -> Result<(), String> {
    let out = Command::new(kernable)
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "kernable {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

fn key(path: &Path) -> Result<SigningKey, String> {
    let secret = signing::read_secret(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(SigningKey::from_bytes(&secret))
}

const FAULTS: [&str; 11] = [
    "kernel-kill",
    "kernel-term",
    "kernel-down",
    "app-kill",
    "app-pause",
    "net-reset",
    "net-down",
    "net-slow",
    "disk-full",
    "power-loss",
    "switchover",
];

/// Something to do later: restart or resume a process.
enum Later {
    Start(usize),
    Resume(usize),
}

async fn run(a: Args) -> Result<(), String> {
    let minutes = a.num("minutes", 60)?;
    let check_every = Duration::from_secs(a.num("check-every", 10)? * 60);
    let fault_every = a.num("fault-every", 45)?;
    let clients = a.num("clients", 8)?;
    let rate = a.num("rate", 0)?;
    let seed = a.num(
        "seed",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64),
    )?;
    let backend = a.get("backend").unwrap_or("sqlite").to_owned();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let dir = PathBuf::from(a.get("dir").map_or(format!("soak-{stamp}"), str::to_owned));
    let data = a.get("data").map_or(dir.join("data"), PathBuf::from);
    let fill = a.get("fill").map(PathBuf::from);
    let power_loss = a.get("power-loss-cmd").map(str::to_owned);
    let faults: Vec<String> = match a.get("faults") {
        Some(list) => list.split(',').map(str::to_owned).collect(),
        None => FAULTS
            .iter()
            .filter(|f| {
                (**f != "disk-full" || fill.is_some())
                    && (**f != "power-loss" || power_loss.is_some())
                    && (**f != "switchover" || a.get("pair").is_some_and(|p| p == "yes"))
            })
            .map(|f| (*f).to_owned())
            .collect(),
    };
    if let Some(bad) = faults.iter().find(|f| !FAULTS.contains(&f.as_str())) {
        return Err(format!("unknown fault {bad}"));
    }
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().map_err(|e| e.to_string())?;
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let kernable_bin = a
        .get("kernable")
        .map_or(exe.with_file_name("kernable"), PathBuf::from);
    for d in [&dir, &data, &dir.join("logs")] {
        std::fs::create_dir_all(d).map_err(|e| format!("{}: {e}", d.display()))?;
    }
    let dir = dir.canonicalize().map_err(|e| e.to_string())?;
    let data = data.canonicalize().map_err(|e| e.to_string())?;
    let _ = LOG.set(dir.join("soak.log"));
    let (kport, pport) = (a.num("kernel-port", 17233)?, a.num("proxy-port", 17234)?);
    let pair = a.get("pair").is_some_and(|p| p == "yes");
    // With a pair: kernel B, its proxy and the witness.
    let (kport_b, pport_b, wport) = (kport + 10, pport + 10, kport + 20);
    say(&format!(
        "soak for {minutes} min in {}, {backend}, seed {seed}, faults {}",
        dir.display(),
        faults.join(",")
    ));

    // Keys, a grant for the apps (also used by the soak's clients and audit listener).
    for name in ["root", "kernel", "app", "kernel-b", "node-a", "node-b"] {
        if !dir.join(format!("{name}.key")).exists() {
            cli(&kernable_bin, &dir, &["keys", "new", name])?;
        }
    }
    let types = load::DOMAINS.map(|d| format!("{d}.*")).join(",");
    let run_types = format!("run={types}");
    let mut grant = vec!["grant", "root.key", "app.pub", &run_types];
    let actions = [
        "submit=*",
        "subscribe=*",
        "listen=*",
        "query=*",
        "answer=*",
        "check=*",
        "manage=*",
    ];
    grant.extend(actions);
    cli(&kernable_bin, &dir, &grant)?;
    for node in ["node-a.pub", "node-b.pub"] {
        cli(
            &kernable_bin,
            &dir,
            &["grant", "root.key", node, "replicate=*"],
        )?;
    }
    if pair {
        // Generation 1 of the witness both kernels name, for the witness and both kernels.
        cli(
            &kernable_bin,
            &dir,
            &[
                "witness",
                "generation",
                "root.key",
                "1",
                &format!("127.0.0.1:{wport}"),
            ],
        )?;
    }
    let me = Identity {
        key: Arc::new(key(&dir.join("app.key"))?),
        grant: std::fs::read_to_string(dir.join("app.grant"))
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned(),
    };
    let kernel_key_ids = vec![
        signing::key_id(&key(&dir.join("kernel.key"))?.verifying_key()),
        signing::key_id(&key(&dir.join("kernel-b.key"))?.verifying_key()),
    ];

    let store_of = |name: &str| match backend.as_str() {
        "sqlite" => Ok(format!(
            "sqlite:{}",
            data.join(format!("{name}.db")).display()
        )),
        other => Err(format!("unknown backend {other}")),
    };
    let store = store_of("k")?;
    let store_b = store_of("k-b")?;
    let log = |name: &str| dir.join("logs").join(format!("{name}.log"));
    let url = if pair {
        format!("http://127.0.0.1:{pport},http://127.0.0.1:{pport_b}")
    } else {
        format!("http://127.0.0.1:{pport}")
    };
    let kernel = |name: &str, store: &str, kport: u64, pport: u64, suffix: &str| {
        let mut p = Proc::new(name, &kernable_bin, &dir, log(name)).args(&[
            "start",
            "--root",
            "root.pub",
            "--store",
            store,
            "--listen",
            &format!("127.0.0.1:{kport}"),
            "--checkpoint-interval-ms",
            "1000",
        ]);
        if pair {
            p = p.args(&[
                "--kernel-key",
                &format!("kernel{suffix}.key"),
                "--snapshots",
                &format!(
                    "sqlite:{}",
                    data.join(format!("snapshots{suffix}.db")).display()
                ),
                "--data-store",
                &format!("file:{}", data.join(format!("files{suffix}")).display()),
                "--witness",
                &format!("127.0.0.1:{wport}"),
                "--witness-generation",
                "witness.generation",
                "--node-key",
                if suffix.is_empty() {
                    "node-a.key"
                } else {
                    "node-b.key"
                },
                "--advertise",
                &format!("127.0.0.1:{pport}"),
                "--primary-kernel-key",
                "kernel.pub",
                "--primary-kernel-key",
                "kernel-b.pub",
            ]);
        }
        p
    };
    let mut procs = vec![kernel("kernel", &store, kport, pport, "")];
    let app_env = |p: Proc, sdk: &str| {
        p.env("KERNABLE_URL", &url)
            .env("KERNABLE_KEY", &dir.join("app.key").display().to_string())
            .env(
                "KERNABLE_GRANT",
                &dir.join("app.grant").display().to_string(),
            )
            .env(
                "SOAK_STORE",
                &data.join(format!("{sdk}.db")).display().to_string(),
            )
    };
    let apps = repo.join("crates/kernable-soak/apps");
    procs.push(app_env(
        Proc::new("app-ts", "node", &dir, log("app-ts"))
            .args(&[&apps.join("bank.ts").display().to_string()]),
        "ts",
    ));
    procs.push(
        app_env(
            Proc::new(
                "app-py",
                repo.join("sdks/python/.venv/bin/python"),
                &dir,
                log("app-py"),
            ),
            "py",
        )
        .args(&[&apps.join("bank.py").display().to_string()])
        .env(
            "PYTHONPATH",
            &repo.join("sdks/python").display().to_string(),
        ),
    );
    procs.push(app_env(
        Proc::new("app-rs", &exe, &dir, log("app-rs")).args(&["app"]),
        "rs",
    ));
    // The Go app, built next to this binary (`make soak`: apps/bankgo).
    procs.push(app_env(
        Proc::new(
            "app-go",
            exe.with_file_name("soak-bank-go"),
            &dir,
            log("app-go"),
        ),
        "go",
    ));

    if pair {
        procs.push(kernel("kernel-b", &store_b, kport_b, pport_b, "-b"));
        procs.push(
            Proc::new("witness", &kernable_bin, &dir, log("witness")).args(&[
                "witness",
                "--root",
                "root.pub",
                "--listen",
                &format!("127.0.0.1:{wport}"),
                "--generation",
                "witness.generation",
                "--state",
                "witness.state",
            ]),
        );
    }
    let addr = |p: u64| {
        format!("127.0.0.1:{p}")
            .parse()
            .map_err(|e: std::net::AddrParseError| e.to_string())
    };
    let mut proxies = vec![
        Proxy::start(addr(pport)?, addr(kport)?)
            .await
            .map_err(|e| e.to_string())?,
    ];
    if pair {
        proxies.push(
            Proxy::start(addr(pport_b)?, addr(kport_b)?)
                .await
                .map_err(|e| e.to_string())?,
        );
        procs[6].start().map_err(|e| e.to_string())?;
        wait_for_port(wport, Duration::from_secs(30)).await?;
    }
    procs[0].start().map_err(|e| e.to_string())?;
    wait_for_port(kport, Duration::from_secs(30)).await?;
    if pair {
        // A asks the witness first and becomes the primary; B follows it.
        tokio::time::sleep(Duration::from_secs(2)).await;
        procs[5].start().map_err(|e| e.to_string())?;
        wait_for_port(kport_b, Duration::from_secs(30)).await?;
    }
    for p in &mut procs[1..5] {
        p.start().map_err(|e| e.to_string())?;
    }
    // The primary's store, and the standby's: by the witness's state.
    let witness_state = dir.join("witness.state");
    let stores = |pair: bool| -> (String, Option<String>) {
        if !pair {
            return (store.clone(), None);
        }
        let primary = std::fs::read(&witness_state)
            .ok()
            .and_then(|b| {
                <kernable_core::proto::WitnessState as prost::Message>::decode(b.as_slice()).ok()
            })
            .map(|s| s.primary_address)
            .unwrap_or_default();
        if primary == format!("127.0.0.1:{pport_b}") {
            (store_b.clone(), Some(store.clone()))
        } else {
            (store.clone(), Some(store_b.clone()))
        }
    };
    let load = Load::new(&format!("r{stamp}"));
    load.audit(url.clone(), me.clone());
    // Every listener must be connected before the load starts: a new reactor starts at the
    // newest event, and would miss withdrawals sent before.
    let started = Instant::now();
    loop {
        let primary = stores(pair).0;
        let connected = tokio::task::spawn_blocking(move || {
            kernable_store::open_read_only(&primary)
                .and_then(|read| read.listeners())
                .map_or(0, |l| l.len())
        })
        .await
        .unwrap_or(0);
        // Each app's read model and reactor, and the soak's audit listener.
        if connected >= 9 {
            break;
        }
        if started.elapsed() > Duration::from_secs(120) {
            return Err("the apps' listeners did not all connect in 120 s; see logs/".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    say("everything is connected; load starts");
    // `--rate`: commands per second over all clients (0: as fast as they are answered).
    let every = match rate {
        0 => Duration::ZERO,
        r => Duration::from_secs_f64(clients as f64 / r as f64),
    };
    for n in 0..clients {
        let seed = seed.wrapping_add(n + 1);
        load.client(n as u32, url.clone(), me.clone(), seed, every);
    }
    let client =
        Kernable::new(Options::new(dir.join("app.key")).url(&url)).map_err(|e| e.to_string())?;

    let mut rng = Rng::new(seed);
    let end = Instant::now() + Duration::from_secs(minutes * 60);
    let mut next_check = Instant::now() + check_every;
    let mut next_fault = Instant::now() + rng.ms(2_000, fault_every * 1000);
    let mut later: Vec<(Instant, Later)> = Vec::new();
    let mut injected: BTreeMap<String, u64> = BTreeMap::new();
    let filling = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut exits: BTreeMap<String, u64> = BTreeMap::new();
    let mut rounds = Vec::new();
    // What each check round carries to the next: it reads only the log after it.
    let mut checked = check::Checked::default();
    let failed = loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = Instant::now();
        // Processes that ended on their own are restarted (and counted).
        for p in procs.iter_mut() {
            if let Some(status) = p.exited() {
                *exits.entry(p.name.clone()).or_default() += 1;
                say(&format!(
                    "{} exited by itself ({status}); restarting",
                    p.name
                ));
                later.push((
                    now + Duration::from_millis(500),
                    Later::Start(procs_index(&p.name)),
                ));
            }
        }
        let (due, rest): (Vec<_>, Vec<_>) = later.drain(..).partition(|(at, _)| *at <= now);
        later = rest;
        for (_, action) in due {
            do_later(action, &mut procs);
        }

        let last = now >= end;
        if now >= next_check || last {
            // The faults end first, then the commands still in flight get their answers: a
            // kernel down until later, or a stopped app, would hold them up otherwise.
            load.running.store(false, Ordering::SeqCst);
            for proxy in &proxies {
                proxy.heal();
            }
            for (_, action) in later.drain(..) {
                do_later(action, &mut procs);
            }
            for p in procs.iter_mut() {
                if p.paused {
                    p.resume();
                }
                if p.pid().is_none() {
                    let _ = p.start();
                }
            }
            while filling.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            let waited = Instant::now();
            while load.stats.in_flight.load(Ordering::SeqCst) > 0
                && waited.elapsed() < Duration::from_secs(400)
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            say("check round: waiting until everything is quiet");
            let (primary, standby) = stores(pair);
            say(&format!("check round on {primary}"));
            let report = check::round(
                &primary,
                &kernel_key_ids,
                standby.as_deref(),
                &load,
                &client,
                &mut checked,
            )
            .await;
            say(&format!(
                "check round at head {}: {} violations ({:.0} s)",
                report.head,
                report.violations.len(),
                report.took.as_secs_f64()
            ));
            for v in report.violations.iter().take(50) {
                say(&format!("  VIOLATION {v}"));
            }
            let failed = !report.violations.is_empty();
            rounds.push(json!({
                "at": crate::now(),
                "head": report.head,
                "seconds": report.took.as_secs(),
                "violations": report.violations,
            }));
            write_report(&dir, &a, seed, &rounds, &injected, &exits, &load);
            if failed || last {
                break failed;
            }
            next_check = Instant::now() + check_every;
            next_fault = Instant::now() + rng.ms(2_000, fault_every * 1000);
            load.running.store(true, Ordering::SeqCst);
            continue;
        }

        if now >= next_fault && !faults.is_empty() {
            next_fault = now + rng.ms(2_000, fault_every * 1000);
            let fault = faults[rng.below(faults.len() as u64) as usize].clone();
            *injected.entry(fault.clone()).or_default() += 1;
            let app = 1 + rng.below(4) as usize;
            // With a pair, either kernel and either proxy.
            let k = if pair && rng.below(2) == 1 { 5 } else { 0 };
            let proxy = &proxies[if k == 5 { 1 } else { 0 }];
            match fault.as_str() {
                "kernel-kill" => {
                    procs[k].kill();
                    later.push((now + rng.ms(0, 3_000), Later::Start(k)));
                }
                "kernel-down" => {
                    procs[k].kill();
                    later.push((now + rng.ms(6_000, 15_000), Later::Start(k)));
                }
                "kernel-term" => {
                    procs[k].terminate(Duration::from_secs(10));
                    later.push((Instant::now() + rng.ms(0, 1_000), Later::Start(k)));
                }
                "app-kill" => {
                    procs[app].kill();
                    later.push((now + rng.ms(0, 3_000), Later::Start(app)));
                }
                "app-pause" => {
                    if procs[app].pid().is_some() && !procs[app].paused {
                        procs[app].pause();
                        later.push((now + rng.ms(1_000, 20_000), Later::Resume(app)));
                    }
                }
                "switchover" => {
                    // Asked of both kernels: the standby refuses, the primary hands over and
                    // stops (restarted above, as a process that exits by itself, it follows).
                    let (bin, dir) = (kernable_bin.clone(), dir.clone());
                    let addrs = [format!("127.0.0.1:{pport}"), format!("127.0.0.1:{pport_b}")];
                    tokio::task::spawn_blocking(move || {
                        for addr in addrs {
                            match cli(&bin, &dir, &["switchover", &addr, "app.key"]) {
                                Ok(()) => say(&format!("switchover: {addr} handed over")),
                                Err(e) => say(&format!("switchover: {e}")),
                            }
                        }
                    });
                }
                "net-reset" => proxy.reset(),
                "net-down" => proxy.down(rng.ms(1_000, 20_000)),
                "net-slow" => proxy.slow(rng.ms(20, 400), rng.ms(10_000, 40_000)),
                "disk-full" => {
                    if let Some(fill) = &fill
                        && !filling.swap(true, Ordering::SeqCst)
                    {
                        let (path, hold) = (fill.join("filler"), rng.ms(5_000, 20_000));
                        let filling = Arc::clone(&filling);
                        tokio::task::spawn_blocking(move || {
                            fill_up(&path, hold);
                            filling.store(false, Ordering::SeqCst);
                        });
                    }
                }
                "power-loss" => {
                    for p in procs.iter_mut() {
                        p.kill();
                    }
                    if let Some(cmd) = &power_loss {
                        let status = Command::new("sh").arg("-c").arg(cmd).status();
                        say(&format!("power loss: {cmd}: {status:?}"));
                    }
                    for i in 0..procs.len() {
                        later.push((Instant::now() + rng.ms(500, 3_000), Later::Start(i)));
                    }
                }
                _ => {}
            }
            say(&format!(
                "fault {fault}{}",
                if fault.starts_with("app") {
                    format!(" ({})", procs[app].name)
                } else if fault.starts_with("kernel") || fault.starts_with("net") {
                    format!(" ({})", procs[k].name)
                } else {
                    String::new()
                }
            ));
        }
    };
    for p in procs.iter_mut() {
        p.terminate(Duration::from_secs(5));
    }
    write_report(&dir, &a, seed, &rounds, &injected, &exits, &load);
    if failed {
        Err(format!(
            "violations found; see {}",
            dir.join("report.json").display()
        ))
    } else {
        say("no violations");
        Ok(())
    }
}

fn procs_index(name: &str) -> usize {
    [
        "kernel", "app-ts", "app-py", "app-rs", "app-go", "kernel-b", "witness",
    ]
    .iter()
    .position(|n| *n == name)
    .unwrap_or(0)
}

fn do_later(action: Later, procs: &mut [Proc]) {
    match action {
        Later::Start(i) => {
            if procs[i].pid().is_none()
                && let Err(e) = procs[i].start()
            {
                say(&format!("{}: cannot start: {e}", procs[i].name));
            }
        }
        Later::Resume(i) => {
            if procs[i].paused {
                procs[i].resume();
            }
        }
    }
}

/// Writes `path` until the file system is full, keeps it full for `hold`, then removes it.
fn fill_up(path: &Path, hold: Duration) {
    let Ok(mut f) = std::fs::File::create(path) else {
        return;
    };
    let started = Instant::now();
    let chunk = vec![0u8; 1 << 20];
    let mut written = 0;
    while f.write_all(&chunk).is_ok() {
        written += 1;
    }
    let _ = f.sync_all();
    say(&format!(
        "disk full ({written} MiB in {:.0} s), for {:.0} s",
        started.elapsed().as_secs_f64(),
        hold.as_secs_f64()
    ));
    std::thread::sleep(hold);
    drop(f);
    let _ = std::fs::remove_file(path);
    say("disk free again");
}

async fn wait_for_port(port: u64, limit: Duration) -> Result<(), String> {
    let started = Instant::now();
    while tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .is_err()
    {
        if started.elapsed() > limit {
            return Err(format!(
                "nothing listens on {port} after {limit:?}; see logs/kernel.log"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

fn write_report(
    dir: &Path,
    a: &Args,
    seed: u64,
    rounds: &[serde_json::Value],
    injected: &BTreeMap<String, u64>,
    exits: &BTreeMap<String, u64>,
    load: &Load,
) {
    let s = &load.stats;
    let report = json!({
        "args": a.0,
        "seed": seed,
        "rounds": rounds,
        "faults": injected,
        "exits": exits,
        "commands": {
            "submitted": s.submitted.load(Ordering::SeqCst),
            "committed": s.committed.load(Ordering::SeqCst),
            "rejected": *s.rejected.lock().unwrap(),
            "resubmits": s.resubmits.load(Ordering::SeqCst),
            "errors": *s.errors.lock().unwrap(),
            "unanswered": *s.unanswered.lock().unwrap(),
        },
    });
    let _ = std::fs::write(
        dir.join("report.json"),
        serde_json::to_string_pretty(&report).unwrap_or_default(),
    );
}
