//! The soak: the kernel and an app per SDK (TypeScript, Python, Rust) under continuous load,
//! while faults hit them at random: kills, graceful restarts, stopped processes, a failing
//! network, a full disk, power loss. Every few minutes the faults and the load pause and every
//! invariant is checked (`check.rs`). Ends with a report (`report.json` in the run directory);
//! exits 1 at the first violation, leaving everything in place to look at.
//!
//! ```text
//! kernable-soak run [--minutes 60] [--dir DIR] [--backend sqlite|file] [--check-every 10]
//!                   [--clients 8] [--seed N] [--fault-every 45] [--faults a,b,...]
//!                   [--data DIR] [--fill DIR] [--power-loss-cmd CMD]
//! kernable-soak app    (the Rust app; started by `run`)
//! ```
//!
//! Faults: kernel-kill, kernel-term, app-kill, app-pause, net-reset, net-down, net-slow, and
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

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("app") => app::run().await.map_err(|e| e.to_string()),
        Some("run") => match Args::parse(&args[1..]) {
            Ok(a) => run(a).await,
            Err(e) => Err(e),
        },
        _ => Err("usage: kernable-soak run [--minutes N] [--dir DIR] ... | kernable-soak app (see src/main.rs)".into()),
    };
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

const FAULTS: [&str; 9] = [
    "kernel-kill",
    "kernel-term",
    "app-kill",
    "app-pause",
    "net-reset",
    "net-down",
    "net-slow",
    "disk-full",
    "power-loss",
];

/// Something to do later: restart or resume a process, or free the disk.
enum Later {
    Start(usize),
    Resume(usize),
    Unfill(PathBuf),
}

async fn run(a: Args) -> Result<(), String> {
    let minutes = a.num("minutes", 60)?;
    let check_every = Duration::from_secs(a.num("check-every", 10)? * 60);
    let fault_every = a.num("fault-every", 45)?;
    let clients = a.num("clients", 8)?;
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
    say(&format!(
        "soak for {minutes} min in {}, {backend}, seed {seed}, faults {}",
        dir.display(),
        faults.join(",")
    ));

    // Keys, a grant for the apps (also used by the soak's clients and audit listener).
    for name in ["root", "kernel", "app"] {
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
    let me = Identity {
        key: Arc::new(key(&dir.join("app.key"))?),
        grant: std::fs::read_to_string(dir.join("app.grant"))
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned(),
    };
    let kernel_key_id = signing::key_id(&key(&dir.join("kernel.key"))?.verifying_key());

    let store = match backend.as_str() {
        "sqlite" => format!("sqlite:{}", data.join("k.db").display()),
        "file" => format!("file:{}", data.join("k.log").display()),
        other => return Err(format!("unknown backend {other}")),
    };
    let log = |name: &str| dir.join("logs").join(format!("{name}.log"));
    let url = format!("http://127.0.0.1:{pport}");
    let kernel_listen = format!("127.0.0.1:{kport}");
    let mut procs = vec![
        Proc::new("kernel", &kernable_bin, &dir, log("kernel")).args(&[
            "start",
            "--root",
            "root.pub",
            "--store",
            &store,
            "--listen",
            &kernel_listen,
            "--checkpoint-interval-ms",
            "1000",
        ]),
    ];
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

    procs[0].start().map_err(|e| e.to_string())?;
    wait_for_port(kport, Duration::from_secs(30)).await?;
    let addr = |p: u64| {
        format!("127.0.0.1:{p}")
            .parse()
            .map_err(|e: std::net::AddrParseError| e.to_string())
    };
    let proxy = Proxy::start(addr(pport)?, addr(kport)?)
        .await
        .map_err(|e| e.to_string())?;
    for p in &mut procs[1..] {
        p.start().map_err(|e| e.to_string())?;
    }
    let load = Load::new(&format!("r{stamp}"));
    load.audit(url.clone(), me.clone());
    // Every listener must be connected before the load starts: a new reactor starts at the
    // newest event, and would miss withdrawals sent before.
    let started = Instant::now();
    loop {
        if let Ok(read) = kernable_store::open_read_only(&store)
            && read.listeners().map_or(0, |l| l.len()) >= 7
        {
            break;
        }
        if started.elapsed() > Duration::from_secs(120) {
            return Err("the apps' listeners did not all connect in 120 s; see logs/".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    say("everything is connected; load starts");
    for n in 0..clients {
        load.client(n as u32, url.clone(), me.clone(), seed.wrapping_add(n + 1));
    }
    let client =
        Kernable::new(Options::new(dir.join("app.key")).url(&url)).map_err(|e| e.to_string())?;

    let mut rng = Rng::new(seed);
    let end = Instant::now() + Duration::from_secs(minutes * 60);
    let mut next_check = Instant::now() + check_every;
    let mut next_fault = Instant::now() + rng.ms(2_000, fault_every * 1000);
    let mut later: Vec<(Instant, Later)> = Vec::new();
    let mut injected: BTreeMap<String, u64> = BTreeMap::new();
    let mut exits: BTreeMap<String, u64> = BTreeMap::new();
    let mut rounds = Vec::new();
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
            load.running.store(false, Ordering::SeqCst);
            let waited = Instant::now();
            while load.stats.in_flight.load(Ordering::SeqCst) > 0
                && waited.elapsed() < Duration::from_secs(400)
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            proxy.heal();
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
            say("check round: waiting until everything is quiet");
            let report = check::round(&store, &kernel_key_id, &load, &client).await;
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
            let app = 1 + rng.below(3) as usize;
            match fault.as_str() {
                "kernel-kill" => {
                    procs[0].kill();
                    later.push((now + rng.ms(0, 3_000), Later::Start(0)));
                }
                "kernel-term" => {
                    procs[0].terminate(Duration::from_secs(10));
                    later.push((Instant::now() + rng.ms(0, 1_000), Later::Start(0)));
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
                "net-reset" => proxy.reset(),
                "net-down" => proxy.down(rng.ms(1_000, 20_000)),
                "net-slow" => proxy.slow(rng.ms(20, 400), rng.ms(10_000, 40_000)),
                "disk-full" => {
                    if let Some(fill) = &fill {
                        let filler = fill.join("filler");
                        let f = filler.clone();
                        tokio::task::spawn_blocking(move || fill_up(&f));
                        later.push((now + rng.ms(5_000, 20_000), Later::Unfill(filler)));
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
    ["kernel", "app-ts", "app-py", "app-rs"]
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
        Later::Unfill(path) => {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Writes `path` until the file system is full.
fn fill_up(path: &Path) {
    let Ok(mut f) = std::fs::File::create(path) else {
        return;
    };
    let chunk = vec![0u8; 1 << 20];
    while f.write_all(&chunk).is_ok() {}
    let _ = f.sync_all();
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
