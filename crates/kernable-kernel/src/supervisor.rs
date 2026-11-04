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
//! The kernel's flags are those of `kernable start`, without their dashes; a `[backup]`
//! section adds `--backup on`. Paths are relative to the file's directory, where the children
//! run. Containers for the customer's processes come later.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use serde::Deserialize;
use tokio::process::{Child, Command};

use crate::BoxError;

/// A child process as the file describes it; a change restarts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// `kernel` or `backup`.
    pub name: String,
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub stop: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Project {
    kernel: KernelSection,
    backup: Option<BackupSection>,
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

/// The children `text` (a `project.toml`) asks for, in start order; `this` is the running binary.
pub fn specs(text: &str, this: &Path) -> Result<Vec<Spec>, BoxError> {
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
    }];
    if let Some(b) = project.backup {
        let mut args = vec![
            "backup".to_owned(),
            "--follow".into(),
            b.follow.unwrap_or(listen),
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
        });
    }
    Ok(specs)
}

/// A child the node runs.
struct Running {
    spec: Spec,
    /// The binary's modification time when it started: a replaced binary restarts it.
    binary_changed: Option<SystemTime>,
    child: Option<Child>,
    started: Instant,
    /// When to start it again after it exited.
    restart_at: Option<Instant>,
    backoff: Duration,
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

fn spawn(spec: &Spec, dir: &Path) -> Result<Child, BoxError> {
    let mut command = Command::new(&spec.binary);
    command
        .args(&spec.args)
        .envs(&spec.env)
        .current_dir(dir)
        .kill_on_drop(true);
    let child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", spec.binary.display()))?;
    tracing::info!(child = %spec.name, pid = child.id().unwrap_or(0), binary = %spec.binary.display(), "started");
    Ok(child)
}

/// Stops `child`: SIGTERM, then SIGKILL after `stop`.
async fn stop(name: &str, child: &mut Child, stop: Duration) {
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
        }
    }
}

/// Runs the node described by the file at `path` until SIGTERM or SIGINT.
pub async fn run(path: &Path) -> Result<(), BoxError> {
    let path = std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let dir = path.parent().unwrap_or(Path::new(".")).to_owned();
    let this = std::env::current_exe()?;
    let read = || -> Result<Vec<Spec>, BoxError> {
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        specs(&text, &this).map_err(|e| format!("{}: {e}", path.display()).into())
    };
    let mut children: Vec<Running> = Vec::new();
    let mut last_read = modified(&path);
    apply(&mut children, read()?, &dir).await;
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    let mut hangup = signal(libc::SIGHUP)?;
    let mut terminate = signal(libc::SIGTERM)?;
    loop {
        let reread = tokio::select! {
            _ = tick.tick() => modified(&path) != last_read,
            _ = hangup.recv() => true,
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        };
        if reread {
            last_read = modified(&path);
            match read() {
                Ok(specs) => apply(&mut children, specs, &dir).await,
                Err(e) => {
                    tracing::error!(error = %e, "the project file is invalid: the children run on as they are")
                }
            }
        }
        for c in &mut children {
            // A replaced binary: start the child on the new one.
            if c.child.is_some() && modified(&c.spec.binary) != c.binary_changed {
                tracing::info!(child = %c.spec.name, "its binary changed: restarting it");
                if let Some(mut child) = c.child.take() {
                    stop(&c.spec.name, &mut child, c.spec.stop).await;
                }
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
            if c.child.is_none() && c.restart_at.is_some_and(|at| Instant::now() >= at) {
                start(c, &dir);
            }
        }
    }
    // Reverse order: the backup follower before the kernel it follows.
    for c in children.iter_mut().rev() {
        if let Some(mut child) = c.child.take() {
            stop(&c.spec.name, &mut child, c.spec.stop).await;
        }
    }
    Ok(())
}

fn signal(kind: libc::c_int) -> Result<tokio::signal::unix::Signal, BoxError> {
    Ok(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::from_raw(kind),
    )?)
}

fn start(c: &mut Running, dir: &Path) {
    c.binary_changed = modified(&c.spec.binary);
    c.started = Instant::now();
    match spawn(&c.spec, dir) {
        Ok(child) => {
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

/// Brings `children` to `specs`: stops those gone or changed (reverse order), starts those new
/// or changed (in order).
async fn apply(children: &mut Vec<Running>, specs: Vec<Spec>, dir: &Path) {
    let mut keep = Vec::new();
    for mut c in std::mem::take(children).into_iter().rev() {
        if specs.contains(&c.spec) {
            keep.push(c);
        } else {
            tracing::info!(child = %c.spec.name, "its command changed: restarting it");
            if let Some(mut child) = c.child.take() {
                stop(&c.spec.name, &mut child, c.spec.stop).await;
            }
        }
    }
    keep.reverse();
    for spec in specs {
        match keep.iter().position(|c| c.spec == spec) {
            Some(i) => children.push(keep.remove(i)),
            None => {
                let mut c = Running {
                    spec,
                    binary_changed: None,
                    child: None,
                    started: Instant::now(),
                    restart_at: None,
                    backoff: Duration::from_millis(500),
                };
                start(&mut c, dir);
                children.push(c);
            }
        }
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
        let specs = specs(PROJECT, Path::new("/bin/kernable")).unwrap();
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
        let before = specs(PROJECT, Path::new("/bin/kernable")).unwrap();
        let moved = PROJECT.replace(
            "[kernel]\n",
            "[kernel]\nbinary = \"/opt/kernable/1.0.1/kernable\"\n",
        );
        let after = specs(&moved, Path::new("/bin/kernable")).unwrap();
        assert_ne!(before[0], after[0]);
        assert_eq!(after[0].binary, Path::new("/opt/kernable/1.0.1/kernable"));
        let flag = PROJECT.replace("1048576", "2097152");
        let after = specs(&flag, Path::new("/bin/kernable")).unwrap();
        assert_ne!(before[0], after[0]);
        assert_eq!(before[1], after[1], "the backup follower runs on");
    }

    #[test]
    fn mistakes_in_the_file_are_named() {
        let this = Path::new("/bin/kernable");
        let typo = specs("[kernel]\nflgs = {}\n", this)
            .unwrap_err()
            .to_string();
        assert!(typo.contains("flgs"), "{typo}");
        let dashes = specs("[kernel]\nflags = { \"--root\" = \"r\" }\n", this)
            .unwrap_err()
            .to_string();
        assert!(dashes.contains("without its dashes"), "{dashes}");
        assert!(
            specs("[backup]\nto = \"file:b\"\n", this).is_err(),
            "no kernel"
        );
        let alone = specs("[kernel]\n", this).unwrap();
        assert_eq!(alone.len(), 1);
        assert_eq!(alone[0].args, ["start"]);
    }
}
