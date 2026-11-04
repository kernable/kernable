//! `kernable node`: one node of a project, run from its `project.toml`.
//!
//! The node runs the kernel and, with a `[backup]` section, the backup follower as child
//! processes of the same (or a named) kernable binary. It restarts a child that exits, backing
//! off up to 30 s, and reads the file again every two seconds (or on SIGHUP): a child whose
//! command changed, or whose binary was replaced, is stopped and started again, so an update is
//! a new binary and nothing else. SIGTERM or SIGINT stops the children in reverse order, each
//! with SIGTERM, then SIGKILL after `stop-seconds`. A kernel crash never takes the node down.
//!
//! ```toml
//! [kernel]
//! binary = "/opt/kernable/1.0.1/kernable"   # optional: default this binary
//! stop-seconds = 30                          # optional
//! flags = { root = "root.pub", store = "sqlite:kernable.db", vault-key = "vault.key" }
//! env = { KERNABLE_LOG = "info" }            # optional
//!
//! [backup]                                   # optional
//! to = "s3://acme-backups/shop"
//! key = "backup.key"
//! backup-key = "backup-encryption.key"
//! follow = "127.0.0.1:7233"                  # optional: default the kernel's --listen
//! segment-seconds = 5                        # optional, as base-hours and keep
//! ```
//!
//! The customer's services are built and run with Podman, one `[[service]]` each:
//!
//! ```toml
//! [node]                                     # optional
//! instance = "node-a"                        # KERNABLE_INSTANCE (default the host name)
//! project = "shop"                           # names the containers (default the directory)
//! podman = "podman"                          # optional
//!
//! [[service]]
//! name = "worker"
//! build = "./worker"                         # a directory with a Containerfile or Dockerfile
//! file = "Containerfile"                     # optional
//! command = ["node", "app.ts"]               # optional: the image's own by default
//! key = "keys/worker.key"                    # optional: and keys/worker.grant next to it
//! env = { SHOP_MAIL = "smtp://mail:25" }     # optional
//! cpus = "1.5"                               # optional, as memory = "512m"
//! stop-seconds = 10                          # optional
//! ```
//!
//! The node builds each service's image at start, when the service changes and on SIGHUP (a
//! service whose image changed restarts), then runs it with `podman run --rm` in the
//! foreground, a child like the kernel: started after the kernel, stopped before it. It runs on
//! the host's network with `KERNABLE_URL` (the kernel's `--listen`), `KERNABLE_INSTANCE`, and,
//! with a key, `KERNABLE_KEY` and `KERNABLE_GRANT` naming the files mounted read-only.
//!
//! The kernel's flags are those of `kernable start`, without their dashes; a `[backup]`
//! section adds `--backup on`. Paths are relative to the file's directory, where the children
//! run.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant, SystemTime},
};

use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
};

use crate::BoxError;

/// A child process as the file describes it; a change restarts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// `kernel`, `backup`, or a service's name.
    pub name: String,
    pub binary: PathBuf,
    /// For a service, the arguments of `podman run` before the image; [`Container::command`]
    /// follows it.
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub stop: Duration,
    /// A service: what to build before it runs.
    pub container: Option<Container>,
}

/// How a service's image is built and named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Container {
    /// The container's name: `kernable-<project>-<service>`.
    pub name: String,
    /// The build directory, absolute.
    pub context: PathBuf,
    pub file: Option<String>,
    pub command: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Project {
    kernel: KernelSection,
    backup: Option<BackupSection>,
    node: Option<NodeSection>,
    #[serde(default)]
    service: Vec<ServiceSection>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct NodeSection {
    instance: Option<String>,
    project: Option<String>,
    podman: Option<PathBuf>,
    metrics_listen: Option<String>,
}

/// What a `project.toml` asks the node for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The children, in start order.
    pub children: Vec<Spec>,
    /// `[node] metrics-listen`.
    pub metrics_listen: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ServiceSection {
    name: String,
    build: PathBuf,
    file: Option<String>,
    #[serde(default)]
    command: Vec<String>,
    key: Option<PathBuf>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cpus: Option<String>,
    memory: Option<String>,
    stop_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct KernelSection {
    binary: Option<PathBuf>,
    stop_seconds: Option<u64>,
    #[serde(default)]
    flags: BTreeMap<String, toml::Value>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct BackupSection {
    to: String,
    key: String,
    backup_key: String,
    follow: Option<String>,
    segment_seconds: Option<u64>,
    base_hours: Option<u64>,
    keep: Option<u64>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// The children `text` (a `project.toml` in `dir`) asks for, in start order; `this` is the
/// running binary.
pub fn specs(text: &str, dir: &Path, this: &Path) -> Result<Vec<Spec>, BoxError> {
    Ok(plan(text, dir, this)?.children)
}

/// What `text` (a `project.toml` in `dir`) asks for; `this` is the running binary.
pub fn plan(text: &str, dir: &Path, this: &Path) -> Result<Plan, BoxError> {
    let project: Project = toml::from_str(text)?;
    let k = project.kernel;
    let binary = k.binary.clone().unwrap_or_else(|| this.to_owned());
    let stop = Duration::from_secs(k.stop_seconds.unwrap_or(30));
    let mut args = vec!["start".to_owned()];
    let mut listen = "127.0.0.1:7233".to_owned();
    for (flag, value) in &k.flags {
        if flag.starts_with('-') {
            return Err(format!("[kernel] flags: write {flag} without its dashes").into());
        }
        let value = match value {
            toml::Value::String(s) => s.clone(),
            toml::Value::Integer(n) => n.to_string(),
            toml::Value::Boolean(b) => b.to_string(),
            other => {
                return Err(format!("[kernel] flags: {flag} is a {}", other.type_str()).into());
            }
        };
        if flag == "listen" {
            listen.clone_from(&value);
        }
        if flag == "dev" {
            if value == "true" {
                args.push("--dev".into());
            }
            continue;
        }
        args.push(format!("--{flag}"));
        args.push(value);
    }
    if project.backup.is_some() && !k.flags.contains_key("backup") {
        args.extend(["--backup".into(), "on".into()]);
    }
    let mut specs = vec![Spec {
        name: "kernel".into(),
        binary: binary.clone(),
        args,
        env: k.env,
        stop,
        container: None,
    }];
    if let Some(b) = project.backup {
        let mut args = vec![
            "backup".to_owned(),
            "--follow".into(),
            b.follow.unwrap_or_else(|| listen.clone()),
            "--key".into(),
            b.key,
            "--to".into(),
            b.to,
            "--backup-key".into(),
            b.backup_key,
        ];
        for (flag, value) in [
            ("--segment-seconds", b.segment_seconds),
            ("--base-hours", b.base_hours),
            ("--keep", b.keep),
        ] {
            if let Some(v) = value {
                args.extend([flag.to_owned(), v.to_string()]);
            }
        }
        specs.push(Spec {
            name: "backup".into(),
            binary,
            args,
            env: b.env,
            stop,
            container: None,
        });
    }
    let node = project.node.unwrap_or_default();
    let metrics_listen = node.metrics_listen.clone();
    let instance = match node.instance {
        Some(i) => i,
        None => host_name(),
    };
    let name = match node.project {
        Some(p) => p,
        None => dir
            .file_name()
            .map(|n| n.to_string_lossy().to_lowercase())
            .unwrap_or_else(|| "project".into()),
    };
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!("[node] project {name}: letters, digits, - and _ only").into());
    }
    let podman = node.podman.unwrap_or_else(|| PathBuf::from("podman"));
    // Services reach the kernel on the host's network.
    let url = match listen.rsplit_once(':') {
        Some(("0.0.0.0" | "[::]" | "", port)) => {
            format!("127.0.0.1:{port}")
        }
        _ => listen.clone(),
    };
    for svc in project.service {
        let valid = (1..=63).contains(&svc.name.len())
            && svc
                .name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !valid || matches!(svc.name.as_str(), "kernel" | "backup") {
            return Err(format!(
                "[[service]] {}: a name has 1 to 63 lowercase letters, digits and -, and is not kernel or backup",
                svc.name
            )
            .into());
        }
        if specs.iter().any(|s| s.name == svc.name) {
            return Err(format!("[[service]] {} is named twice", svc.name).into());
        }
        let container = format!("kernable-{name}-{}", svc.name);
        let mut args = vec![
            "run".to_owned(),
            "--rm".into(),
            "--replace".into(),
            "--name".into(),
            container.clone(),
            "--network".into(),
            "host".into(),
            "--env".into(),
            format!("KERNABLE_URL={url}"),
            "--env".into(),
            format!("KERNABLE_INSTANCE={instance}"),
        ];
        if let Some(key) = &svc.key {
            let key = dir.join(key);
            let grant = key.with_extension("grant");
            for (path, inside, var) in [
                (&key, "/run/kernable/key", "KERNABLE_KEY"),
                (&grant, "/run/kernable/grant", "KERNABLE_GRANT"),
            ] {
                args.extend([
                    "--volume".into(),
                    format!("{}:{inside}:ro,Z", path.display()),
                    "--env".into(),
                    format!("{var}={inside}"),
                ]);
            }
        }
        for (k, v) in &svc.env {
            args.extend(["--env".into(), format!("{k}={v}")]);
        }
        if let Some(cpus) = &svc.cpus {
            args.extend(["--cpus".into(), cpus.clone()]);
        }
        if let Some(memory) = &svc.memory {
            args.extend(["--memory".into(), memory.clone()]);
        }
        specs.push(Spec {
            name: svc.name,
            binary: podman.clone(),
            args,
            env: BTreeMap::new(),
            stop: Duration::from_secs(svc.stop_seconds.unwrap_or(30)),
            container: Some(Container {
                name: container,
                context: dir.join(&svc.build),
                file: svc.file,
                command: svc.command,
            }),
        });
    }
    Ok(Plan {
        children: specs,
        metrics_listen,
    })
}

/// This machine's name, for `KERNABLE_INSTANCE`.
fn host_name() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname(2) writes at most `buf.len()` bytes into the buffer we own.
    let ok = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } == 0;
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let name = String::from_utf8_lossy(&buf[..end]);
    let name: String = name
        .split('.')
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        .collect();
    if ok && !name.is_empty() {
        name
    } else {
        "node".into()
    }
}

/// A child the node runs.
struct Running {
    spec: Spec,
    /// The binary's modification time when it started: a replaced binary restarts it.
    binary_changed: Option<SystemTime>,
    child: Option<Child>,
    started: Instant,
    /// When to start it again (after it exited, or once its image is built).
    restart_at: Option<Instant>,
    backoff: Duration,
    /// A service: the image ID it runs.
    image: Option<String>,
    /// A service's build on its way, in the background.
    building: Option<JoinHandle<Result<String, String>>>,
    /// Times it was started again.
    restarts: u64,
    /// When it last started (ms since epoch), for the metrics.
    started_ms: i64,
}

impl Running {
    fn new(spec: Spec) -> Self {
        Running {
            spec,
            binary_changed: None,
            child: None,
            started: Instant::now(),
            restart_at: Some(Instant::now()),
            backoff: Duration::from_millis(500),
            image: None,
            building: None,
            restarts: 0,
            started_ms: 0,
        }
    }
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Passes the lines of a child's output on to the node's own, prefixed with its name.
fn forward(name: &str, out: Option<impl AsyncRead + Unpin + Send + 'static>, stderr: bool) {
    let Some(out) = out else {
        return;
    };
    let name = name.to_owned();
    tokio::spawn(async move {
        let mut lines = BufReader::new(out).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stderr {
                eprintln!("{name} | {line}");
            } else {
                println!("{name} | {line}");
            }
        }
    });
}

/// Builds a service's image; its ID. The build's own output goes on as the service's.
async fn build(spec: Spec) -> Result<String, String> {
    let c = spec.container.as_ref().expect("a service");
    let mut command = Command::new(&spec.binary);
    command.args(["build", "--quiet", "--tag", &format!("{}:latest", c.name)]);
    if let Some(file) = &c.file {
        command.args(["--file", &c.context.join(file).to_string_lossy()]);
    }
    command
        .arg(&c.context)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    tracing::info!(child = %spec.name, context = %c.context.display(), "building");
    let mut child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", spec.binary.display()))?;
    forward(&spec.name, child.stderr.take(), true);
    let mut id = String::new();
    if let Some(out) = child.stdout.take() {
        let mut lines = BufReader::new(out).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            id = line.trim().to_owned();
        }
    }
    let status = child.wait().await.map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("building {} failed ({status})", spec.name));
    }
    if id.is_empty() {
        return Err(format!("building {} printed no image ID", spec.name));
    }
    Ok(id)
}

fn spawn(spec: &Spec, image: Option<&str>, dir: &Path) -> Result<Child, BoxError> {
    let mut command = Command::new(&spec.binary);
    command.args(&spec.args);
    if let (Some(c), Some(image)) = (&spec.container, image) {
        command.arg(image).args(&c.command);
    }
    command
        .envs(&spec.env)
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", spec.binary.display()))?;
    forward(&spec.name, child.stdout.take(), false);
    forward(&spec.name, child.stderr.take(), true);
    tracing::info!(child = %spec.name, pid = child.id().unwrap_or(0), binary = %spec.binary.display(), "started");
    Ok(child)
}

/// Stops a child: SIGTERM (`podman run` passes it on to the container), then SIGKILL after its
/// stop time; a killed service's container is removed too.
async fn stop(c: &mut Running) {
    let Some(mut child) = c.child.take() else {
        return;
    };
    let (name, stop) = (&c.spec.name, c.spec.stop);
    let child = &mut child;
    if let Some(pid) = child.id() {
        #[cfg(unix)]
        // SAFETY: kill(2) on our own child's pid; a pid that already exited is harmless.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
    }
    match tokio::time::timeout(stop, child.wait()).await {
        Ok(status) => tracing::info!(child = %name, status = ?status.ok(), "stopped"),
        Err(_) => {
            tracing::warn!(child = %name, "did not stop in time: killed");
            let _ = child.kill().await;
            if let Some(container) = &c.spec.container {
                let _ = Command::new(&c.spec.binary)
                    .args(["rm", "--force", &container.name])
                    .output()
                    .await;
            }
        }
    }
}

/// What the metrics endpoint shows of one child.
#[derive(Debug, Clone, Default)]
struct Shown {
    name: String,
    service: bool,
    up: bool,
    restarts: u64,
    started_ms: i64,
    /// A service's CPU seconds and memory, from its container's cgroup (Linux).
    cpu_seconds: Option<f64>,
    memory_bytes: Option<u64>,
}

type Board = Arc<Mutex<Vec<Shown>>>;

/// Runs the node described by the file at `path` until SIGTERM or SIGINT.
pub async fn run(path: &Path) -> Result<(), BoxError> {
    let path = std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let dir = path.parent().unwrap_or(Path::new(".")).to_owned();
    let this = std::env::current_exe()?;
    let read = || -> Result<Plan, BoxError> {
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        plan(&text, &dir, &this).map_err(|e| format!("{}: {e}", path.display()).into())
    };
    let first = read()?;
    let board: Board = Arc::default();
    let _metrics = match &first.metrics_listen {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| format!("[node] metrics-listen {addr}: {e}"))?;
            tracing::info!(%addr, "node metrics at /metrics");
            Some(crate::AbortOnDrop(tokio::spawn(serve_metrics(
                listener,
                Arc::clone(&board),
            ))))
        }
        None => None,
    };
    let metrics_listen = first.metrics_listen.clone();
    let mut children: Vec<Running> = Vec::new();
    let mut last_read = modified(&path);
    apply(&mut children, first.children).await;
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let mut hangup = signal(libc::SIGHUP)?;
    let mut terminate = signal(libc::SIGTERM)?;
    let mut sampled = Instant::now() - Duration::from_secs(60);
    loop {
        let (reread, rebuild) = tokio::select! {
            _ = tick.tick() => (modified(&path) != last_read, false),
            _ = hangup.recv() => (true, true),
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        };
        if reread {
            last_read = modified(&path);
            match read() {
                Ok(plan) => {
                    if plan.metrics_listen != metrics_listen {
                        tracing::warn!(
                            "[node] metrics-listen changed: it applies when the node starts again"
                        );
                    }
                    apply(&mut children, plan.children).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "the project file is invalid: the children run on as they are")
                }
            }
        }
        if rebuild {
            // Built again in the background; a service whose image changed restarts then.
            for c in children.iter_mut() {
                if c.spec.container.is_some() && c.building.is_none() {
                    c.building = Some(tokio::spawn(build(c.spec.clone())));
                }
            }
        }
        for c in &mut children {
            step(c, &dir).await;
        }
        if sampled.elapsed() >= Duration::from_secs(10) {
            sampled = Instant::now();
            show(&board, &children, true).await;
        } else {
            show(&board, &children, false).await;
        }
    }
    // Reverse order: the services and the backup follower before the kernel.
    for c in children.iter_mut().rev() {
        if let Some(b) = c.building.take() {
            b.abort();
        }
        stop(c).await;
    }
    Ok(())
}

/// One look at a child: a finished build, a replaced binary, an exit, a due start.
async fn step(c: &mut Running, dir: &Path) {
    if c.building.as_ref().is_some_and(JoinHandle::is_finished) {
        let result = c.building.take().expect("checked").await;
        match result.unwrap_or_else(|e| Err(e.to_string())) {
            Ok(id) if Some(&id) != c.image.as_ref() => {
                if c.child.is_some() {
                    tracing::info!(child = %c.spec.name, "its image changed: restarting it");
                    stop(c).await;
                }
                c.image = Some(id);
                c.restart_at = Some(Instant::now());
            }
            Ok(_) => {}
            Err(e) if c.image.is_some() => {
                tracing::error!(child = %c.spec.name, error = %e, "the build failed: it runs on as it is")
            }
            Err(e) => {
                c.backoff = (c.backoff * 2).min(Duration::from_secs(30));
                tracing::error!(child = %c.spec.name, error = %e, again_in = ?c.backoff, "cannot build");
                c.restart_at = Some(Instant::now() + c.backoff);
            }
        }
    }
    // A replaced binary: start the child on the new one.
    if c.child.is_some() && modified(&c.spec.binary) != c.binary_changed {
        tracing::info!(child = %c.spec.name, "its binary changed: restarting it");
        stop(c).await;
        c.restart_at = Some(Instant::now());
    }
    if let Some(child) = &mut c.child
        && let Ok(Some(status)) = child.try_wait()
    {
        // Ran for a while: start again soon; failing at once: back off.
        c.backoff = if c.started.elapsed() > Duration::from_secs(60) {
            Duration::from_secs(1)
        } else {
            (c.backoff * 2).min(Duration::from_secs(30))
        };
        tracing::warn!(child = %c.spec.name, %status, again_in = ?c.backoff, "exited");
        c.child = None;
        c.restart_at = Some(Instant::now() + c.backoff);
    }
    if c.child.is_some() || c.restart_at.is_none_or(|at| Instant::now() < at) {
        return;
    }
    if c.spec.container.is_some() && c.image.is_none() {
        // Its image first, in the background.
        if c.building.is_none() {
            c.building = Some(tokio::spawn(build(c.spec.clone())));
        }
        c.restart_at = None;
        return;
    }
    c.binary_changed = modified(&c.spec.binary);
    c.started = Instant::now();
    match spawn(&c.spec, c.image.as_deref(), dir) {
        Ok(child) => {
            if c.started_ms != 0 {
                c.restarts += 1;
            }
            c.started_ms = crate::now_ms();
            c.child = Some(child);
            c.restart_at = None;
        }
        Err(e) => {
            c.backoff = (c.backoff * 2).min(Duration::from_secs(30));
            tracing::error!(child = %c.spec.name, error = %e, again_in = ?c.backoff, "cannot start");
            c.restart_at = Some(Instant::now() + c.backoff);
        }
    }
}

fn signal(kind: libc::c_int) -> Result<tokio::signal::unix::Signal, BoxError> {
    Ok(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::from_raw(kind),
    )?)
}

/// Brings `children` to `specs`: stops those gone or changed (reverse order); those new or
/// changed start with the next look (in order).
async fn apply(children: &mut Vec<Running>, specs: Vec<Spec>) {
    let mut keep = Vec::new();
    for mut c in std::mem::take(children).into_iter().rev() {
        if specs.contains(&c.spec) {
            keep.push(c);
        } else {
            tracing::info!(child = %c.spec.name, "its command changed: restarting it");
            if let Some(b) = c.building.take() {
                b.abort();
            }
            stop(&mut c).await;
        }
    }
    keep.reverse();
    for spec in specs {
        match keep.iter().position(|c| c.spec == spec) {
            Some(i) => children.push(keep.remove(i)),
            None => children.push(Running::new(spec)),
        }
    }
}

/// Puts what the metrics show on the board; `sample`: also the services' CPU and memory.
async fn show(board: &Board, children: &[Running], sample: bool) {
    let old: Vec<Shown> = board.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let mut shown = Vec::with_capacity(children.len());
    for c in children {
        let mut s = Shown {
            name: c.spec.name.clone(),
            service: c.spec.container.is_some(),
            up: c.child.is_some(),
            restarts: c.restarts,
            started_ms: c.started_ms,
            ..Default::default()
        };
        match (sample, &c.spec.container, &c.child) {
            (true, Some(container), Some(_)) => {
                (s.cpu_seconds, s.memory_bytes) = usage(&c.spec.binary, &container.name).await;
            }
            (false, Some(_), Some(_)) => {
                if let Some(o) = old.iter().find(|o| o.name == s.name) {
                    (s.cpu_seconds, s.memory_bytes) = (o.cpu_seconds, o.memory_bytes);
                }
            }
            _ => {}
        }
        shown.push(s);
    }
    *board.lock().unwrap_or_else(PoisonError::into_inner) = shown;
}

/// A container's CPU seconds and memory in bytes, read from its cgroup v2 (what `podman stats`
/// reads); `None` where there is none to read (a Mac, where containers run in a VM).
async fn usage(podman: &Path, container: &str) -> (Option<f64>, Option<u64>) {
    let Ok(out) = Command::new(podman)
        .args(["inspect", "--format", "{{.State.Pid}}", container])
        .output()
        .await
    else {
        return (None, None);
    };
    let pid = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let Ok(cgroup) = std::fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
        return (None, None);
    };
    let Some(path) = cgroup.lines().find_map(|l| l.strip_prefix("0::")) else {
        return (None, None);
    };
    let dir = PathBuf::from("/sys/fs/cgroup").join(path.trim_start_matches('/'));
    let cpu = std::fs::read_to_string(dir.join("cpu.stat"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("usage_usec "))
                .and_then(|v| v.trim().parse::<f64>().ok())
                .map(|us| us / 1e6)
        });
    let memory = std::fs::read_to_string(dir.join("memory.current"))
        .ok()
        .and_then(|s| s.trim().parse().ok());
    (cpu, memory)
}

/// The board in Prometheus' text format.
fn metrics_text(shown: &[Shown]) -> String {
    let now = crate::now_ms();
    let mut out = String::new();
    let mut family = |name: &str, kind: &str, help: &str, rows: Vec<(String, String)>| {
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
        for (child, value) in rows {
            out.push_str(&format!("{name}{{child=\"{child}\"}} {value}\n"));
        }
    };
    let rows = |f: &dyn Fn(&Shown) -> Option<String>| -> Vec<(String, String)> {
        shown
            .iter()
            .filter_map(|s| f(s).map(|v| (s.name.clone(), v)))
            .collect()
    };
    family(
        "kernable_node_child_up",
        "gauge",
        "Whether the child runs (1) or not (0).",
        rows(&|s| Some(u8::from(s.up).to_string())),
    );
    family(
        "kernable_node_child_restarts_total",
        "counter",
        "Times the node started the child again.",
        rows(&|s| Some(s.restarts.to_string())),
    );
    family(
        "kernable_node_child_uptime_seconds",
        "gauge",
        "Seconds since the child last started, while it runs.",
        rows(&|s| {
            s.up.then(|| format!("{:.0}", (now - s.started_ms) as f64 / 1000.0))
        }),
    );
    family(
        "kernable_node_service_cpu_seconds_total",
        "counter",
        "CPU time a service's container used, from its cgroup (Linux).",
        rows(&|s| {
            s.cpu_seconds
                .filter(|_| s.service)
                .map(|v| format!("{v:.3}"))
        }),
    );
    family(
        "kernable_node_service_memory_bytes",
        "gauge",
        "Memory a service's container uses now, from its cgroup (Linux).",
        rows(&|s| s.memory_bytes.filter(|_| s.service).map(|v| v.to_string())),
    );
    out
}

/// Answers `GET /metrics` with the board; anything else with 404.
async fn serve_metrics(listener: tokio::net::TcpListener, board: Board) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let board = Arc::clone(&board);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await
            {
                Ok(Ok(n)) => n,
                _ => return,
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = match request.split_whitespace().take(2).collect::<Vec<_>>()[..] {
                ["GET", "/metrics"] => {
                    let shown = board.lock().unwrap_or_else(PoisonError::into_inner).clone();
                    ("200 OK", metrics_text(&shown))
                }
                _ => ("404 Not Found", "not found\n".to_owned()),
            };
            let reply = format!(
                "HTTP/1.1 {status}\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(reply.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROJECT: &str = r#"
[kernel]
flags = { root = "root.pub", store = "sqlite:kernable.db", listen = "0.0.0.0:7233", max-data-bytes = 1048576 }
env = { KERNABLE_LOG = "info" }

[backup]
to = "s3://acme-backups/shop"
key = "backup.key"
backup-key = "backup-encryption.key"
segment-seconds = 2
"#;

    #[test]
    fn the_file_names_the_kernel_and_its_backup_follower() {
        let specs = specs(PROJECT, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["kernel", "backup"]);
        assert_eq!(
            specs[0].args,
            [
                "start",
                "--listen",
                "0.0.0.0:7233",
                "--max-data-bytes",
                "1048576",
                "--root",
                "root.pub",
                "--store",
                "sqlite:kernable.db",
                "--backup",
                "on"
            ]
        );
        assert_eq!(specs[0].env["KERNABLE_LOG"], "info");
        assert_eq!(
            specs[1].args,
            [
                "backup",
                "--follow",
                "0.0.0.0:7233",
                "--key",
                "backup.key",
                "--to",
                "s3://acme-backups/shop",
                "--backup-key",
                "backup-encryption.key",
                "--segment-seconds",
                "2"
            ]
        );
        assert_eq!(specs[0].binary, Path::new("/bin/kernable"));
    }

    #[test]
    fn a_new_binary_or_flag_changes_the_kernel_and_nothing_else() {
        let before = specs(PROJECT, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        let moved = PROJECT.replace(
            "[kernel]\n",
            "[kernel]\nbinary = \"/opt/kernable/1.0.1/kernable\"\n",
        );
        let after = specs(&moved, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_ne!(before[0], after[0]);
        assert_eq!(after[0].binary, Path::new("/opt/kernable/1.0.1/kernable"));
        let flag = PROJECT.replace("1048576", "2097152");
        let after = specs(&flag, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_ne!(before[0], after[0]);
        assert_eq!(before[1], after[1], "the backup follower runs on");
    }

    #[test]
    fn mistakes_in_the_file_are_named() {
        let (this, dir) = (Path::new("/bin/kernable"), Path::new("/srv/shop"));
        let typo = specs("[kernel]\nflgs = {}\n", dir, this)
            .unwrap_err()
            .to_string();
        assert!(typo.contains("flgs"), "{typo}");
        let dashes = specs("[kernel]\nflags = { \"--root\" = \"r\" }\n", dir, this)
            .unwrap_err()
            .to_string();
        assert!(dashes.contains("without its dashes"), "{dashes}");
        assert!(
            specs("[backup]\nto = \"file:b\"\n", dir, this).is_err(),
            "no kernel"
        );
        let alone = specs("[kernel]\n", dir, this).unwrap();
        assert_eq!(alone.len(), 1);
        assert_eq!(alone[0].args, ["start"]);
    }

    #[test]
    fn a_service_is_built_and_run_next_to_the_kernel_with_its_key() {
        let text = r#"
[kernel]
flags = { listen = "0.0.0.0:7233" }

[node]
instance = "node-a"
project = "shop"

[[service]]
name = "worker"
build = "./worker"
command = ["node", "app.ts"]
key = "keys/worker.key"
env = { SHOP_MAIL = "smtp://mail:25" }
memory = "512m"
"#;
        let specs = specs(text, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        let worker = &specs[1];
        assert_eq!(worker.name, "worker");
        assert_eq!(worker.binary, Path::new("podman"));
        let c = worker.container.as_ref().unwrap();
        assert_eq!(c.name, "kernable-shop-worker");
        assert_eq!(c.context, Path::new("/srv/shop/./worker"));
        assert_eq!(c.command, ["node", "app.ts"]);
        let args = worker.args.join(" ");
        for part in [
            "run --rm --replace --name kernable-shop-worker --network host",
            "--env KERNABLE_URL=127.0.0.1:7233",
            "--env KERNABLE_INSTANCE=node-a",
            "--volume /srv/shop/keys/worker.key:/run/kernable/key:ro,Z --env KERNABLE_KEY=/run/kernable/key",
            "--volume /srv/shop/keys/worker.grant:/run/kernable/grant:ro,Z",
            "--env SHOP_MAIL=smtp://mail:25",
            "--memory 512m",
        ] {
            assert!(args.contains(part), "{part} missing from {args}");
        }
        for bad in ["kernel", "Worker", ""] {
            let named = text.replace("name = \"worker\"", &format!("name = \"{bad}\""));
            assert!(specs_of(&named).is_err(), "{bad}");
        }
        let twice = format!("{text}\n[[service]]\nname = \"worker\"\nbuild = \".\"\n");
        assert!(specs_of(&twice).unwrap_err().to_string().contains("twice"));
    }

    fn specs_of(text: &str) -> Result<Vec<Spec>, BoxError> {
        specs(text, Path::new("/srv/shop"), Path::new("/bin/kernable"))
    }

    #[test]
    fn the_metrics_show_each_child_and_a_service_s_usage() {
        let shown = [
            Shown {
                name: "kernel".into(),
                up: true,
                restarts: 2,
                started_ms: crate::now_ms() - 5_000,
                ..Default::default()
            },
            Shown {
                name: "worker".into(),
                service: true,
                cpu_seconds: Some(1.5),
                memory_bytes: Some(4096),
                ..Default::default()
            },
        ];
        let text = metrics_text(&shown);
        for line in [
            "kernable_node_child_up{child=\"kernel\"} 1",
            "kernable_node_child_up{child=\"worker\"} 0",
            "kernable_node_child_restarts_total{child=\"kernel\"} 2",
            "kernable_node_child_uptime_seconds{child=\"kernel\"} 5",
            "kernable_node_service_cpu_seconds_total{child=\"worker\"} 1.500",
            "kernable_node_service_memory_bytes{child=\"worker\"} 4096",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "{line} missing from\n{text}"
            );
        }
        assert!(
            !text.contains("uptime_seconds{child=\"worker\"}"),
            "not while it is down"
        );
    }

    #[test]
    fn the_node_section_names_the_metrics_address() {
        let text = "[kernel]\n[node]\nmetrics-listen = \"127.0.0.1:7240\"\n";
        let plan = plan(text, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_eq!(plan.metrics_listen.as_deref(), Some("127.0.0.1:7240"));
    }
}
