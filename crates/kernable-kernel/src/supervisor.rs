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
//! On Linux every part runs in Podman, in one pod per instance (`kernable-<project>`): the
//! kernel and the backup follower in a base image with the kernable binary mounted, the project
//! directory at its own path; `cpus` and `memory` limit each part, and in `[node]` the instance.
//!
//! Secrets are named in `[[secret]]` (a `file`, or a `command` run at start and on SIGHUP)
//! and mounted read-only at `/run/secrets/<name>` into the services that list them; a new value
//! restarts those. A `[pair]` section (witness, node-key, advertise, peer, kernel-keys) sets the
//! kernel's pair flags and gives the backup follower and the services both kernels' addresses.
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

use sha2::Digest;

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
    /// A child in a container: `binary` is Podman, `args` those of `podman run`.
    pub container: Option<Container>,
    /// A service: the secrets it reads; a new value of one restarts it.
    pub secrets: Vec<String>,
    /// The file whose replacement restarts the child: its binary, or the kernable binary its
    /// container runs.
    pub watch: PathBuf,
}

/// A child's container: its name, its image and the command it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Container {
    /// `kernable-<project>-<child>`.
    pub name: String,
    pub image: Image,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Image {
    /// A service's, built on the node from its directory (absolute).
    Build {
        context: PathBuf,
        file: Option<String>,
    },
    /// The kernel's and the backup follower's: a base image, the kernable binary mounted in.
    Base(String),
}

/// The pod all of an instance's containers join (Linux): one network, one total limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pod {
    /// `kernable-<project>`.
    pub name: String,
    pub cpus: Option<String>,
    pub memory: Option<String>,
    pub network: Network,
}

/// How an instance's pod reaches the network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Network {
    /// A namespace of its own: the kernel listens on a socket in `sockets`, which its services
    /// see read-only; nothing in the pod is reachable from outside, and only the services' own
    /// traffic leaves it. Everything else reaches the kernel through the ingress.
    Own { sockets: PathBuf },
    /// The machine's network (`[node] network = "host"`), for a machine of its own.
    Host,
}

/// The directory of the kernel's socket in the project directory, its file, and where a
/// service finds it.
const SOCKETS: &str = "run";
const SOCKET: &str = "kernel.sock";
const SOCKETS_INSIDE: &str = "/run/kernable/kernel";

/// The kernel's and the backup follower's base image, unless `[kernel] image` names another
/// (one with your KMS's client for `--key-command`, say). Its C library is newer than any
/// supported host's, so a binary built there runs in it.
const BASE_IMAGE: &str = "docker.io/library/debian:trixie-slim";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Project {
    kernel: KernelSection,
    backup: Option<BackupSection>,
    pair: Option<PairSection>,
    node: Option<NodeSection>,
    #[serde(default)]
    service: Vec<ServiceSection>,
    #[serde(default)]
    secret: Vec<SecretSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct SecretSection {
    name: String,
    file: Option<PathBuf>,
    #[serde(default)]
    command: Vec<String>,
}

/// A named secret: its value comes from a file, read at every look, or from a command
/// (a KMS client), run at start and on SIGHUP; it never stands in `project.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Secret {
    pub name: String,
    pub source: SecretSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretSource {
    /// Absolute.
    File(PathBuf),
    Command(Vec<String>),
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct NodeSection {
    instance: Option<String>,
    project: Option<String>,
    podman: Option<PathBuf>,
    metrics_listen: Option<String>,
    /// The instance's total: every part together.
    cpus: Option<String>,
    memory: Option<String>,
    /// `own` (the default) or `host`.
    network: Option<String>,
}

/// What a `project.toml` asks the node for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The children, in start order.
    pub children: Vec<Spec>,
    /// `[node] metrics-listen`.
    pub metrics_listen: Option<String>,
    pub secrets: Vec<Secret>,
    /// Where the node keeps the secrets' values for the services to mount: a directory only
    /// its user may enter, in memory where the system has one (`XDG_RUNTIME_DIR`, `/run`).
    pub secrets_dir: PathBuf,
    /// On Linux: the pod of the instance's containers.
    pub pod: Option<Pod>,
    pub podman: PathBuf,
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
    #[serde(default)]
    secrets: Vec<String>,
    cpus: Option<String>,
    memory: Option<String>,
    stop_seconds: Option<u64>,
    /// A root filesystem the service may write to, for an image that writes outside `/tmp`.
    #[serde(default)]
    writable: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct KernelSection {
    binary: Option<PathBuf>,
    image: Option<String>,
    cpus: Option<String>,
    memory: Option<String>,
    stop_seconds: Option<u64>,
    #[serde(default)]
    flags: BTreeMap<String, toml::Value>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// One node of a pair: the kernel's pair flags, the backup follower's and the services'
/// addresses, from one place.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct PairSection {
    witness: String,
    node_key: String,
    /// Where the other node, the backup follower and the services reach this kernel. Default:
    /// the kernel's `listen`.
    advertise: Option<String>,
    /// The other node's kernel, as it advertises itself.
    peer: String,
    /// Both kernels' kernel keys: either may be the primary.
    kernel_keys: Vec<String>,
    standby_timeout_ms: Option<u64>,
}

/// The kernel flags `[pair]` sets.
const PAIR_FLAGS: [&str; 5] = [
    "witness",
    "node-key",
    "advertise",
    "primary-kernel-key",
    "standby-timeout-ms",
];

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
    cpus: Option<String>,
    memory: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// The children `text` (a `project.toml` in `dir`) asks for, in start order; `this` is the
/// running binary.
pub fn specs(text: &str, dir: &Path, this: &Path) -> Result<Vec<Spec>, BoxError> {
    Ok(plan(text, dir, this)?.children)
}

/// What `text` (a `project.toml` in `dir`) asks for; `this` is the running binary. On Linux
/// every part runs in Podman; elsewhere the kernel and the backup follower are plain
/// processes.
pub fn plan(text: &str, dir: &Path, this: &Path) -> Result<Plan, BoxError> {
    plan_on(text, dir, this, cfg!(target_os = "linux"))
}

/// [`plan`], with every part in Podman or not.
fn plan_on(text: &str, dir: &Path, this: &Path, contained: bool) -> Result<Plan, BoxError> {
    let project: Project = toml::from_str(text)?;
    let node = project.node.unwrap_or_default();
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
    let k = project.kernel;
    let limited = [
        ("[node]", &node.cpus, &node.memory),
        ("[kernel]", &k.cpus, &k.memory),
    ]
    .into_iter()
    .chain(
        project
            .backup
            .as_ref()
            .map(|b| ("[backup]", &b.cpus, &b.memory)),
    )
    .find(|(_, cpus, memory)| cpus.is_some() || memory.is_some());
    if let (false, Some((section, _, _))) = (contained, limited) {
        return Err(format!(
            "{section} cpus and memory need Linux: the kernel runs in Podman there only"
        )
        .into());
    }
    let network = match node.network.as_deref() {
        None | Some("own") => Network::Own {
            sockets: dir.join(SOCKETS),
        },
        Some("host") => Network::Host,
        Some(other) => {
            return Err(format!("[node] network {other}: own or host").into());
        }
    };
    let pod = contained.then(|| Pod {
        name: format!("kernable-{name}"),
        cpus: node.cpus.clone(),
        memory: node.memory.clone(),
        network,
    });
    // The kernel's socket, when the pod has a network of its own.
    let sockets = match &pod {
        Some(Pod {
            network: Network::Own { sockets },
            ..
        }) => Some(sockets.clone()),
        _ => None,
    };
    // Its container sees the project directory and nothing else of the machine.
    let inside = |what: &str, value: &str| -> Result<(), BoxError> {
        let path = ["sqlite:", "file:", "unix:"]
            .iter()
            .find_map(|scheme| value.strip_prefix(scheme))
            .unwrap_or(value);
        if contained && path.starts_with('/') && !Path::new(path).starts_with(dir) {
            return Err(format!(
                "{what} {value}: outside {}, the only directory the kernel's container sees; \
                 move it there or name it relative to it",
                dir.display()
            )
            .into());
        }
        Ok(())
    };
    let binary = k
        .binary
        .as_ref()
        .map_or_else(|| this.to_owned(), |b| dir.join(b));
    let image = k.image.clone().unwrap_or_else(|| BASE_IMAGE.to_owned());
    let stop = Duration::from_secs(k.stop_seconds.unwrap_or(30));
    // The kernel or the backup follower: in a container of the pod, or a plain process.
    let kernable = |child: &str,
                    args: Vec<String>,
                    env: BTreeMap<String, String>,
                    cpus: &Option<String>,
                    memory: &Option<String>| {
        let Some(pod) = &pod else {
            return Spec {
                name: child.into(),
                binary: binary.clone(),
                args,
                env,
                stop,
                container: None,
                secrets: Vec::new(),
                watch: binary.clone(),
            };
        };
        let container = format!("kernable-{name}-{child}");
        let mut run = vec![
            "run".to_owned(),
            "--rm".into(),
            "--replace".into(),
            "--name".into(),
            container.clone(),
            "--pod".into(),
            pod.name.clone(),
            "--volume".into(),
            format!("{}:/usr/local/bin/kernable:ro", binary.display()),
            "--volume".into(),
            format!("{0}:{0}", dir.display()),
            "--workdir".into(),
            dir.display().to_string(),
            // The stores are the container's own files; no label to set on them.
            "--security-opt".into(),
            "label=disable".into(),
        ];
        // The values from Podman's own environment, so they never show in a process list.
        for var in env.keys() {
            run.extend(["--env".into(), var.clone()]);
        }
        harden(&mut run, false);
        limits(&mut run, cpus, memory);
        Spec {
            name: child.into(),
            binary: podman.clone(),
            args: run,
            env,
            stop,
            container: Some(Container {
                name: container,
                image: Image::Base(image.clone()),
                command: std::iter::once("kernable".to_owned()).chain(args).collect(),
            }),
            secrets: Vec::new(),
            watch: binary.clone(),
        }
    };
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
        inside(&format!("[kernel] flags: {flag}"), &value)?;
        if flag == "listen" && sockets.is_some() {
            return Err(format!(
                "[kernel] flags: listen {value}: in the pod's own network the kernel listens on a \
                 socket the node chooses, and the ingress takes outside connections; drop listen, \
                 or set [node] network = \"host\""
            )
            .into());
        }
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
    if let Some(sockets) = &sockets {
        listen = format!("unix:{}", sockets.join(SOCKET).display());
        args.extend(["--listen".into(), listen.clone()]);
    }
    // The other kernel of a pair, for the backup follower and the services: they turn to
    // whichever is the primary.
    let mut peer = None;
    if let (Some(_), Some(_)) = (&project.pair, &sockets) {
        return Err(
            "[pair] in the pod's own network: the kernels of a pair reach each other through \
             the ingress, which is not built yet; set [node] network = \"host\" for now"
                .into(),
        );
    }
    if let Some(pair) = &project.pair {
        if let Some(flag) = PAIR_FLAGS.iter().find(|f| k.flags.contains_key(**f)) {
            return Err(format!("[kernel] flags: {flag} comes from [pair]; drop it").into());
        }
        if pair.kernel_keys.is_empty() {
            return Err("[pair] kernel-keys: name both kernels' kernel keys".into());
        }
        inside("[pair] node-key", &pair.node_key)?;
        let advertise = pair.advertise.clone().unwrap_or_else(|| listen.clone());
        args.extend([
            "--witness".into(),
            pair.witness.clone(),
            "--node-key".into(),
            pair.node_key.clone(),
            "--advertise".into(),
            advertise.clone(),
        ]);
        for key in &pair.kernel_keys {
            inside("[pair] kernel-keys", key)?;
            args.extend(["--primary-kernel-key".into(), key.clone()]);
        }
        if let Some(ms) = pair.standby_timeout_ms {
            args.extend(["--standby-timeout-ms".into(), ms.to_string()]);
        }
        peer = Some((advertise, pair.peer.clone()));
    }
    if project.backup.is_some() && !k.flags.contains_key("backup") {
        args.extend(["--backup".into(), "on".into()]);
    }
    let mut specs = vec![kernable("kernel", args, k.env.clone(), &k.cpus, &k.memory)];
    if let Some(b) = project.backup {
        for (what, value) in [
            ("[backup] key", &b.key),
            ("[backup] backup-key", &b.backup_key),
            ("[backup] to", &b.to),
        ] {
            inside(what, value)?;
        }
        let mut args = vec![
            "backup".to_owned(),
            "--follow".into(),
            b.follow.unwrap_or_else(|| match &peer {
                Some((this, other)) => format!("{this},{other}"),
                None => listen.clone(),
            }),
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
        specs.push(kernable("backup", args, b.env, &b.cpus, &b.memory));
    }
    let metrics_listen = node.metrics_listen.clone();
    let instance = match node.instance {
        Some(i) => i,
        None => host_name(),
    };
    // Services reach the kernel through its socket, or on the host's network, and in a pair
    // the other one too.
    let mut url = match listen.rsplit_once(':') {
        _ if sockets.is_some() => format!("unix:{SOCKETS_INSIDE}/{SOCKET}"),
        Some(("0.0.0.0" | "[::]" | "", port)) => {
            format!("127.0.0.1:{port}")
        }
        _ => listen.clone(),
    };
    if let Some((_, other)) = &peer {
        url = format!("{url},{other}");
    }
    let mut secrets = Vec::new();
    for sec in project.secret {
        if !valid_name(&sec.name) {
            return Err(format!(
                "[[secret]] {}: a name has 1 to 63 lowercase letters, digits and -",
                sec.name
            )
            .into());
        }
        if secrets.iter().any(|s: &Secret| s.name == sec.name) {
            return Err(format!("[[secret]] {} is named twice", sec.name).into());
        }
        let source = match (sec.file, sec.command.is_empty()) {
            (Some(file), true) => SecretSource::File(dir.join(file)),
            (None, false) => SecretSource::Command(sec.command),
            _ => {
                return Err(format!(
                    "[[secret]] {}: give its value either as a file or as a command",
                    sec.name
                )
                .into());
            }
        };
        secrets.push(Secret {
            name: sec.name,
            source,
        });
    }
    let secrets_dir = runtime_dir()
        .join(format!("kernable-{name}"))
        .join("secrets");
    for svc in project.service {
        if !valid_name(&svc.name) || matches!(svc.name.as_str(), "kernel" | "backup") {
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
        ];
        match &pod {
            Some(pod) => args.extend(["--pod".into(), pod.name.clone()]),
            None => args.extend(["--network".into(), "host".into()]),
        }
        args.extend([
            "--env".into(),
            format!("KERNABLE_URL={url}"),
            "--env".into(),
            format!("KERNABLE_INSTANCE={instance}"),
        ]);
        if let Some(sockets) = &sockets {
            args.extend([
                "--volume".into(),
                format!("{}:{SOCKETS_INSIDE}:ro", sockets.display()),
            ]);
        }
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
        // The values from Podman's own environment, so they never show in a process list.
        for var in svc.env.keys() {
            args.extend(["--env".into(), var.clone()]);
        }
        for secret in &svc.secrets {
            if !secrets.iter().any(|s| &s.name == secret) {
                return Err(
                    format!("[[service]] {}: no [[secret]] is named {secret}", svc.name).into(),
                );
            }
            args.extend([
                "--volume".into(),
                format!(
                    "{}:/run/secrets/{secret}:ro,Z",
                    secrets_dir.join(secret).display()
                ),
            ]);
        }
        harden(&mut args, svc.writable);
        limits(&mut args, &svc.cpus, &svc.memory);
        specs.push(Spec {
            name: svc.name,
            binary: podman.clone(),
            args,
            env: svc.env,
            stop: Duration::from_secs(svc.stop_seconds.unwrap_or(30)),
            container: Some(Container {
                name: container,
                image: Image::Build {
                    context: dir.join(&svc.build),
                    file: svc.file,
                },
                command: svc.command,
            }),
            secrets: svc.secrets,
            watch: podman.clone(),
        });
    }
    Ok(Plan {
        children: specs,
        metrics_listen,
        secrets,
        secrets_dir,
        pod,
        podman,
    })
}

/// Most processes a container may have at once.
const PIDS: u32 = 4096;

/// What every container gets: no capabilities, no way to gain privileges (no
/// setuid binary works), a cap on its processes, and a root filesystem it cannot change unless
/// `writable`; Podman mounts a tmpfs on `/tmp`, `/var/tmp` and `/run` of a read-only one.
fn harden(args: &mut Vec<String>, writable: bool) {
    args.extend([
        "--cap-drop".into(),
        "all".into(),
        "--security-opt".into(),
        "no-new-privileges".into(),
        "--pids-limit".into(),
        PIDS.to_string(),
    ]);
    if !writable {
        args.push("--read-only".into());
    }
}

/// `--cpus` and `--memory` for `podman run`.
fn limits(args: &mut Vec<String>, cpus: &Option<String>, memory: &Option<String>) {
    if let Some(cpus) = cpus {
        args.extend(["--cpus".into(), cpus.clone()]);
    }
    if let Some(memory) = memory {
        args.extend(["--memory".into(), memory.clone()]);
    }
}

/// The project file and every file a secret is read from.
fn watched_files(project: &Path, secrets: &[Secret]) -> Vec<PathBuf> {
    let mut files = vec![project.to_owned()];
    for secret in secrets {
        if let SecretSource::File(path) = &secret.source {
            files.push(path.clone());
        }
    }
    files
}

/// A name of a service or a secret: 1 to 63 lowercase letters, digits and `-`.
fn valid_name(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// A directory in memory for this user, where the system has one.
fn runtime_dir() -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        // SAFETY: geteuid(2) cannot fail.
        _ if unsafe { libc::geteuid() } == 0 => PathBuf::from("/run"),
        _ => std::env::temp_dir(),
    }
}

/// The secrets' values as the node last read them: written to `Plan::secrets_dir`, and their
/// hashes stamped on the services that read them.
#[derive(Default)]
struct Secrets {
    held: BTreeMap<String, [u8; 32]>,
}

impl Secrets {
    /// Reads every secret again (a command's only with `commands`, or when it has no value
    /// yet) and writes a changed one. A secret that cannot be read keeps its value; the error
    /// names every one that failed.
    async fn refresh(&mut self, plan: &Plan, dir: &Path, commands: bool) -> Result<(), String> {
        let mut failed = Vec::new();
        for secret in &plan.secrets {
            let value = match &secret.source {
                SecretSource::File(path) => {
                    std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
                }
                SecretSource::Command(_) if !commands && self.held.contains_key(&secret.name) => {
                    continue;
                }
                SecretSource::Command(command) => run_secret_command(command, dir).await,
            };
            let written = value.and_then(|value| {
                let hash: [u8; 32] = sha2::Sha256::digest(&value).into();
                if self.held.get(&secret.name) != Some(&hash) {
                    write_secret(&plan.secrets_dir, &secret.name, &value)
                        .map_err(|e| format!("writing it: {e}"))?;
                    self.held.insert(secret.name.clone(), hash);
                }
                Ok(())
            });
            if let Err(e) = written {
                failed.push(format!("{}: {e}", secret.name));
            }
        }
        match failed.is_empty() {
            true => Ok(()),
            false => Err(format!("secrets not read: {}", failed.join("; "))),
        }
    }

    /// Labels every service with the hashes of the secrets it reads: a new value changes its
    /// command, so it starts again with it.
    fn stamp(&self, children: &mut [Spec]) {
        for spec in children {
            for name in &spec.secrets {
                if let Some(hash) = self.held.get(name) {
                    let short: String = hash[..8].iter().map(|b| format!("{b:02x}")).collect();
                    spec.args
                        .extend(["--label".into(), format!("kernable.secret.{name}={short}")]);
                }
            }
        }
    }
}

/// Runs a secret's command in the project directory; its output, without a final newline, is
/// the value. Waits 30 s at most.
async fn run_secret_command(command: &[String], dir: &Path) -> Result<Vec<u8>, String> {
    let (program, args) = command.split_first().ok_or("an empty command")?;
    let output = Command::new(program)
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(Duration::from_secs(30), output)
        .await
        .map_err(|_| format!("{program} did not finish within 30 s"))?
        .map_err(|e| format!("{program}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let mut value = output.stdout;
    if value.last() == Some(&b'\n') {
        value.pop();
    }
    Ok(value)
}

/// Writes a secret's value into `dir` (created for this user alone), replacing the file whole.
fn write_secret(dir: &Path, name: &str, value: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    let path = dir.join(name);
    let next = dir.join(format!(".{name}.next"));
    // Readable inside the container whatever user it runs as; the directory keeps everyone
    // else on the machine out.
    std::fs::write(&next, value)?;
    std::fs::set_permissions(&next, std::fs::Permissions::from_mode(0o644))?;
    std::fs::rename(&next, &path)
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
    /// SIGHUP came while it was building: build again once that build is done.
    build_again: bool,
    /// Its previous process, being stopped in the background.
    stopping: Option<JoinHandle<()>>,
    /// Times it was started again.
    restarts: u64,
    /// When it last started (ms since epoch), for the metrics.
    started_ms: i64,
}

impl Running {
    fn new(spec: Spec) -> Self {
        // A base image needs no build: Podman pulls it on the first run.
        let image = match spec.container.as_ref().map(|c| &c.image) {
            Some(Image::Base(image)) => Some(image.clone()),
            _ => None,
        };
        Running {
            spec,
            binary_changed: None,
            child: None,
            started: Instant::now(),
            restart_at: Some(Instant::now()),
            backoff: Duration::from_millis(500),
            image,
            building: None,
            build_again: false,
            stopping: None,
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
        use std::io::Write;
        // Bytes, not text: output that is not UTF-8 goes on (lossily) instead of ending the
        // pipe, and a line longer than LINE is cut. A closed stdout or stderr is no reason to
        // stop reading: the child would get SIGPIPE.
        const LINE: u64 = 64 << 10;
        let mut reader = BufReader::new(out);
        let mut line = Vec::new();
        loop {
            line.clear();
            match (&mut reader).take(LINE).read_until(b'\n', &mut line).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let text = String::from_utf8_lossy(&line);
            let text = text.trim_end_matches(['\n', '\r']);
            let _ = if stderr {
                writeln!(std::io::stderr().lock(), "{name} | {text}")
            } else {
                writeln!(std::io::stdout().lock(), "{name} | {text}")
            };
        }
    });
}

/// Builds a service's image; its ID. The build's own output goes on as the service's.
async fn build(spec: Spec) -> Result<String, String> {
    let c = spec.container.as_ref().expect("a service");
    let Image::Build { context, file } = &c.image else {
        return Err(format!("{} is not built", spec.name));
    };
    let mut command = Command::new(&spec.binary);
    command.args(["build", "--quiet", "--tag", &format!("{}:latest", c.name)]);
    if let Some(file) = file {
        command.args(["--file", &context.join(file).to_string_lossy()]);
    }
    command
        .arg(context)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    tracing::info!(child = %spec.name, context = %context.display(), "building");
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
        .kill_on_drop(true)
        // A group of its own: a Ctrl-C in the terminal reaches the node, which stops its
        // children in order, and not every child at once.
        .process_group(0);
    #[cfg(target_os = "linux")]
    // SAFETY: prctl(2) in the child before exec, async-signal-safe: a node that is killed takes
    // its children along instead of leaving them holding ports and stores.
    unsafe {
        command.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", spec.binary.display()))?;
    forward(&spec.name, child.stdout.take(), false);
    forward(&spec.name, child.stderr.take(), true);
    tracing::info!(child = %spec.name, pid = child.id().unwrap_or(0), binary = %spec.binary.display(), "started");
    if spec.container.is_some() && !spec.args.iter().any(|a| a == "--read-only") {
        tracing::warn!(child = %spec.name, "its root filesystem is writable (writable = true)");
    }
    Ok(child)
}

/// Stops a child in the background: SIGTERM (`podman run` passes it on to the container), then
/// SIGKILL after its stop time; a killed service's container is removed too. The node looks
/// after the other children meanwhile; this one starts again only once its process ended.
fn stop(c: &mut Running) {
    let Some(mut child) = c.child.take() else {
        return;
    };
    let (name, stop, binary) = (c.spec.name.clone(), c.spec.stop, c.spec.binary.clone());
    let container = c.spec.container.as_ref().map(|c| c.name.clone());
    let previous = c.stopping.take();
    c.stopping = Some(tokio::spawn(async move {
        if let Some(previous) = previous {
            let _ = previous.await;
        }
        stopped(&name, &mut child, stop, &binary, container.as_deref()).await;
    }));
}

/// Stops a child and waits until it ended: in order, at the node's own end.
async fn stop_now(c: &mut Running) {
    stop(c);
    if let Some(stopping) = c.stopping.take() {
        let _ = stopping.await;
    }
}

async fn stopped(
    name: &str,
    child: &mut Child,
    stop: Duration,
    binary: &Path,
    container: Option<&str>,
) {
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
            if let Some(container) = container {
                let _ = Command::new(binary)
                    .args(["rm", "--force", container])
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
    contained: bool,
    up: bool,
    restarts: u64,
    started_ms: i64,
    /// A container's CPU seconds and memory, from its cgroup (Linux).
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
    let mut first = read()?;
    let mut secrets = Secrets::default();
    secrets
        .refresh(&first, &dir, true)
        .await
        .map_err(|e| format!("{}: {e}", path.display()))?;
    secrets.stamp(&mut first.children);
    let (mut pod, mut podman) = (first.pod.clone(), first.podman.clone());
    let mut _watchdog = None;
    if let Some(p) = &pod {
        make_pod(&podman, p)
            .await
            .map_err(|e| format!("{}: {e}", path.display()))?;
        _watchdog = pod_watchdog(&podman, p);
    }
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
    // The file and the secrets read from files: a change to either is read at the next look.
    let mut watched = watched_files(&path, &first.secrets);
    let mut last_read = watched.iter().map(|p| modified(p)).collect::<Vec<_>>();
    apply(&mut children, first.children).await;
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let mut hangup = signal(libc::SIGHUP)?;
    let mut terminate = signal(libc::SIGTERM)?;
    let mut interrupt = signal(libc::SIGINT)?;
    let mut sampled = Instant::now() - Duration::from_secs(60);
    let usages: Usages = Arc::default();
    let mut sampling: Option<JoinHandle<()>> = None;
    loop {
        let (reread, rebuild) = tokio::select! {
            _ = tick.tick() => {
                (watched.iter().map(|p| modified(p)).collect::<Vec<_>>() != last_read, false)
            }
            _ = hangup.recv() => {
                // The kernel reads its keys again on SIGHUP (and --key-command runs again).
                for c in children.iter().filter(|c| c.spec.name == "kernel") {
                    if let Some(pid) = c.child.as_ref().and_then(Child::id) {
                        // SAFETY: kill(2) on our own child's pid.
                        unsafe { libc::kill(pid as libc::pid_t, libc::SIGHUP) };
                    }
                }
                (true, true)
            }
            _ = terminate.recv() => break,
            _ = interrupt.recv() => break,
        };
        if reread {
            last_read = watched.iter().map(|p| modified(p)).collect();
            match read() {
                Ok(mut plan) => {
                    // A command's secret runs again on SIGHUP only, as --key-command does.
                    if let Err(e) = secrets.refresh(&plan, &dir, rebuild).await {
                        tracing::error!(error = %e, "the services keep the values they have");
                    }
                    secrets.stamp(&mut plan.children);
                    watched = watched_files(&path, &plan.secrets);
                    last_read = watched.iter().map(|p| modified(p)).collect();
                    if plan.pod != pod {
                        // A pod's limits are set when it is made: every part starts again in
                        // a new one.
                        tracing::warn!("the instance's limits changed: every part starts again");
                        for c in children.iter_mut().rev() {
                            if let Some(b) = c.building.take() {
                                b.abort();
                            }
                            stop_now(c).await;
                        }
                        children.clear();
                        if let Some(p) = &plan.pod {
                            if let Err(e) = make_pod(&plan.podman, p).await {
                                tracing::error!(error = %e, "cannot make the instance's pod");
                            }
                            _watchdog = pod_watchdog(&plan.podman, p);
                        }
                        (pod, podman) = (plan.pod.clone(), plan.podman.clone());
                    }
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
            for c in children.iter_mut().filter(|c| built(&c.spec)) {
                if c.building.is_none() {
                    c.building = Some(tokio::spawn(build(c.spec.clone())));
                } else {
                    // The code may have changed after this build started: build once more.
                    c.build_again = true;
                }
            }
        }
        for c in &mut children {
            step(c, &dir).await;
        }
        // The services' CPU and memory, in the background every 10 s: `podman inspect` may take
        // its time, and the node goes on looking after its children meanwhile.
        if sampled.elapsed() >= Duration::from_secs(10)
            && sampling.as_ref().is_none_or(JoinHandle::is_finished)
        {
            sampled = Instant::now();
            let running: Vec<(String, PathBuf, String)> = children
                .iter()
                .filter(|c| c.child.is_some())
                .filter_map(|c| {
                    let container = c.spec.container.as_ref()?;
                    Some((
                        c.spec.name.clone(),
                        c.spec.binary.clone(),
                        container.name.clone(),
                    ))
                })
                .collect();
            let usages = Arc::clone(&usages);
            sampling = Some(tokio::spawn(async move {
                let mut measured = BTreeMap::new();
                for (name, podman, container) in running {
                    measured.insert(name, usage(&podman, &container).await);
                }
                *usages.lock().unwrap_or_else(PoisonError::into_inner) = measured;
            }));
        }
        show(&board, &children, &usages);
    }
    // Reverse order: the services and the backup follower before the kernel, each to the end.
    for c in children.iter_mut().rev() {
        if let Some(b) = c.building.take() {
            b.abort();
        }
        stop_now(c).await;
    }
    if let Some(p) = &pod {
        remove_pod(&podman, &p.name).await;
    }
    Ok(())
}

/// Makes the instance's pod, replacing one a node before left: the host's network, and the
/// instance's total limits.
async fn make_pod(podman: &Path, pod: &Pod) -> Result<(), String> {
    let mut args = vec![
        "pod".to_owned(),
        "create".into(),
        "--replace".into(),
        "--name".into(),
        pod.name.clone(),
    ];
    match &pod.network {
        Network::Own { sockets } => {
            std::fs::create_dir_all(sockets).map_err(|e| format!("{}: {e}", sockets.display()))?;
            // Rootless (one user per instance): pasta, without `host.containers.internal`,
            // which would reach whatever listens on the machine's other addresses. As root,
            // which pasta does not support, Podman's bridge.
            // SAFETY: geteuid(2) cannot fail.
            if unsafe { libc::geteuid() } != 0 {
                args.extend(["--network".into(), "pasta:--map-guest-addr,none".into()]);
            }
        }
        Network::Host => args.extend(["--network".into(), "host".into()]),
    }
    limits(&mut args, &pod.cpus, &pod.memory);
    let made = Command::new(podman)
        .args(&args)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(Duration::from_secs(300), made)
        .await
        .map_err(|_| format!("podman pod create {} took over 300 s", pod.name))?
        .map_err(|e| format!("{}: {e}", podman.display()))?;
    if !output.status.success() {
        return Err(format!(
            "podman pod create {} failed: {}",
            pod.name,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    tracing::info!(pod = %pod.name, cpus = ?pod.cpus, memory = ?pod.memory, "the instance's pod is made");
    Ok(())
}

/// Removes the instance's pod once the node is gone, however it ended: a process of its own
/// that waits for the node's end of a pipe to close. Rootless `podman run` runs the container
/// from a process it executes anew, which the node's death does not reach (a killed node would
/// leave its containers running). Dropped, it ends without removing anything.
fn pod_watchdog(podman: &Path, pod: &Pod) -> Option<Child> {
    let watchdog = Command::new("sh")
        .args(["-c", r#"read _ || exec "$0" pod rm --force --time 10 "$1""#])
        .arg(podman)
        .arg(&pod.name)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        // Out of the node's group: a Ctrl-C meant for the node does not end it.
        .process_group(0)
        .kill_on_drop(true)
        .spawn();
    match watchdog {
        Ok(child) => Some(child),
        Err(e) => {
            tracing::warn!(error = %e, "no watchdog: a killed node would leave its containers");
            None
        }
    }
}

/// Removes the instance's pod once its containers stopped.
async fn remove_pod(podman: &Path, name: &str) {
    let removed = Command::new(podman)
        .args(["pod", "rm", "--force", name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    if !removed.is_ok_and(|s| s.success()) {
        tracing::warn!(pod = %name, "cannot remove the instance's pod");
    }
}

/// Whether the child's image is built on the node (a service's).
fn built(spec: &Spec) -> bool {
    matches!(
        spec.container.as_ref().map(|c| &c.image),
        Some(Image::Build { .. })
    )
}

/// One look at a child: a finished build, a replaced binary, an exit, a due start.
async fn step(c: &mut Running, dir: &Path) {
    if c.building.as_ref().is_some_and(JoinHandle::is_finished) {
        let result = c.building.take().expect("checked").await;
        if std::mem::take(&mut c.build_again) {
            c.building = Some(tokio::spawn(build(c.spec.clone())));
        }
        match result.unwrap_or_else(|e| Err(e.to_string())) {
            Ok(id) if Some(&id) != c.image.as_ref() => {
                if c.child.is_some() {
                    tracing::info!(child = %c.spec.name, "its image changed: restarting it");
                    stop(c);
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
    if c.child.is_some() && modified(&c.spec.watch) != c.binary_changed {
        tracing::info!(child = %c.spec.name, "its binary changed: restarting it");
        stop(c);
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
    // The previous process first ends (it may hold the port or the store).
    if c.stopping.as_ref().is_some_and(|s| !s.is_finished()) {
        return;
    }
    c.stopping = None;
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
    c.binary_changed = modified(&c.spec.watch);
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
    // Processes of changed or removed children still stopping: a child of the same name
    // starts once its old process ended.
    let mut ending: Vec<(String, JoinHandle<()>)> = Vec::new();
    for mut c in std::mem::take(children).into_iter().rev() {
        if specs.contains(&c.spec) {
            keep.push(c);
        } else {
            tracing::info!(child = %c.spec.name, "its command changed: restarting it");
            if let Some(b) = c.building.take() {
                b.abort();
            }
            stop(&mut c);
            if let Some(stopping) = c.stopping.take() {
                ending.push((c.spec.name.clone(), stopping));
            }
        }
    }
    keep.reverse();
    for spec in specs {
        match keep.iter().position(|c| c.spec == spec) {
            Some(i) => children.push(keep.remove(i)),
            None => {
                let mut c = Running::new(spec);
                if let Some(i) = ending.iter().position(|(name, _)| *name == c.spec.name) {
                    c.stopping = Some(ending.remove(i).1);
                }
                children.push(c);
            }
        }
    }
    // A child that is gone for good still ends in the background.
    drop(ending);
}

/// Each running service's CPU seconds and memory, as last measured.
type Usages = Arc<Mutex<BTreeMap<String, (Option<f64>, Option<u64>)>>>;

/// Puts what the metrics show on the board, with the services' last measured usage.
fn show(board: &Board, children: &[Running], usages: &Usages) {
    let usages = usages
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let mut shown = Vec::with_capacity(children.len());
    for c in children {
        let mut s = Shown {
            name: c.spec.name.clone(),
            contained: c.spec.container.is_some(),
            up: c.child.is_some(),
            restarts: c.restarts,
            started_ms: c.started_ms,
            ..Default::default()
        };
        if s.contained
            && s.up
            && let Some(&(cpu, memory)) = usages.get(&s.name)
        {
            (s.cpu_seconds, s.memory_bytes) = (cpu, memory);
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
        "kernable_node_container_cpu_seconds_total",
        "counter",
        "CPU time a child's container used (a service, and on Linux the kernel and the backup follower), from its cgroup.",
        rows(&|s| {
            s.cpu_seconds
                .filter(|_| s.contained)
                .map(|v| format!("{v:.3}"))
        }),
    );
    family(
        "kernable_node_container_memory_bytes",
        "gauge",
        "Memory a child's container uses now, from its cgroup.",
        rows(&|s| {
            s.memory_bytes
                .filter(|_| s.contained)
                .map(|v| v.to_string())
        }),
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

    // The same on every system: the kernel as a plain process, unless a test asks for Linux's
    // containers with `plan_on`.
    fn plan(text: &str, dir: &Path, this: &Path) -> Result<Plan, BoxError> {
        plan_on(text, dir, this, false)
    }

    fn specs(text: &str, dir: &Path, this: &Path) -> Result<Vec<Spec>, BoxError> {
        Ok(plan(text, dir, this)?.children)
    }

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
        assert_eq!(
            c.image,
            Image::Build {
                context: "/srv/shop/./worker".into(),
                file: None
            }
        );
        assert_eq!(
            worker.env["SHOP_MAIL"], "smtp://mail:25",
            "from Podman's environment"
        );
        assert_eq!(c.command, ["node", "app.ts"]);
        let args = worker.args.join(" ");
        for part in [
            "run --rm --replace --name kernable-shop-worker --network host",
            "--env KERNABLE_URL=127.0.0.1:7233",
            "--env KERNABLE_INSTANCE=node-a",
            "--volume /srv/shop/keys/worker.key:/run/kernable/key:ro,Z --env KERNABLE_KEY=/run/kernable/key",
            "--volume /srv/shop/keys/worker.grant:/run/kernable/grant:ro,Z",
            "--env SHOP_MAIL --cap-drop all --security-opt no-new-privileges --pids-limit 4096 --read-only --memory 512m",
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

    #[test]
    fn every_container_is_hardened_and_a_service_may_ask_for_a_writable_root() {
        let text = r#"
[kernel]

[[service]]
name = "web"
build = "."

[[service]]
name = "legacy"
build = "."
writable = true
"#;
        let plan = plan_on(
            text,
            Path::new("/srv/shop"),
            Path::new("/bin/kernable"),
            true,
        )
        .unwrap();
        for spec in &plan.children {
            let args = spec.args.join(" ");
            for part in [
                "--cap-drop all",
                "--security-opt no-new-privileges",
                "--pids-limit 4096",
            ] {
                assert!(
                    args.contains(part),
                    "{}: {part} missing from {args}",
                    spec.name
                );
            }
            let read_only = spec.args.iter().any(|a| a == "--read-only");
            assert_eq!(read_only, spec.name != "legacy", "{}: {args}", spec.name);
        }
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
                contained: true,
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
            "kernable_node_container_cpu_seconds_total{child=\"worker\"} 1.500",
            "kernable_node_container_memory_bytes{child=\"worker\"} 4096",
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
    fn a_pair_section_sets_the_kernel_s_pair_flags_and_both_addresses() {
        let text = r#"
[kernel]
flags = { listen = "10.0.0.1:7233", root = "root.pub" }

[pair]
witness = "10.0.0.9:7300"
node-key = "node-a.key"
peer = "10.0.0.2:7233"
kernel-keys = ["kernel-a.pub", "kernel-b.pub"]

[backup]
to = "file:backups"
key = "backup.key"
backup-key = "backup-encryption.key"

[[service]]
name = "worker"
build = "."
"#;
        let specs = specs_of(text).unwrap();
        assert_eq!(
            specs[0].args.join(" "),
            "start --listen 10.0.0.1:7233 --root root.pub --witness 10.0.0.9:7300 \
             --node-key node-a.key --advertise 10.0.0.1:7233 --primary-kernel-key kernel-a.pub \
             --primary-kernel-key kernel-b.pub --backup on"
        );
        assert_eq!(
            specs[1].args[..3],
            ["backup", "--follow", "10.0.0.1:7233,10.0.0.2:7233"]
        );
        let worker = specs[2].args.join(" ");
        assert!(
            worker.contains("--env KERNABLE_URL=10.0.0.1:7233,10.0.0.2:7233"),
            "{worker}"
        );
        let twice = text.replace("root = \"root.pub\"", "witness = \"w:1\"");
        let error = specs_of(&twice).unwrap_err().to_string();
        assert!(error.contains("witness comes from [pair]"), "{error}");
        let keyless = text.replace("[\"kernel-a.pub\", \"kernel-b.pub\"]", "[]");
        assert!(specs_of(&keyless).is_err());
    }

    #[test]
    fn a_service_reads_the_secrets_it_lists_as_files() {
        let text = r#"
[kernel]
[node]
project = "shop"

[[secret]]
name = "stripe"
file = "secrets/stripe.key"

[[secret]]
name = "smtp"
command = ["kms", "decrypt", "smtp"]

[[service]]
name = "worker"
build = "."
secrets = ["stripe"]
"#;
        let plan = plan(text, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_eq!(
            plan.secrets[0].source,
            SecretSource::File("/srv/shop/secrets/stripe.key".into())
        );
        assert_eq!(
            plan.secrets[1].source,
            SecretSource::Command(vec!["kms".into(), "decrypt".into(), "smtp".into()])
        );
        assert!(plan.secrets_dir.ends_with("kernable-shop/secrets"));
        let worker = plan.children[1].args.join(" ");
        let mount = format!(
            "--volume {}:/run/secrets/stripe:ro,Z",
            plan.secrets_dir.join("stripe").display()
        );
        assert!(worker.contains(&mount), "{worker}");
        assert!(!worker.contains("smtp"), "only what it lists: {worker}");
        assert_eq!(plan.children[1].secrets, ["stripe"]);

        let unknown = text.replace("secrets = [\"stripe\"]", "secrets = [\"paypal\"]");
        assert!(
            specs_of(&unknown)
                .unwrap_err()
                .to_string()
                .contains("paypal")
        );
        let both = text.replace(
            "file = \"secrets/stripe.key\"",
            "file = \"a\"\ncommand = [\"b\"]",
        );
        assert!(specs_of(&both).unwrap_err().to_string().contains("either"));
        let twice = text.replace("name = \"smtp\"", "name = \"stripe\"");
        assert!(specs_of(&twice).unwrap_err().to_string().contains("twice"));
    }

    #[tokio::test]
    async fn a_secret_s_new_value_restarts_only_the_services_that_read_it() {
        let dir = std::env::temp_dir().join(format!("kn-secrets-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stripe.key"), "sk_1").unwrap();
        std::fs::write(dir.join("counter"), "").unwrap();
        let text = r#"
[kernel]
[[secret]]
name = "stripe"
file = "stripe.key"
[[secret]]
name = "smtp"
command = ["sh", "-c", "echo x >> counter; echo pw-$(wc -l < counter | tr -d ' ')"]
[[service]]
name = "pay"
build = "."
secrets = ["stripe"]
[[service]]
name = "mail"
build = "."
secrets = ["smtp"]
"#;
        let mut plan = plan(text, &dir, Path::new("/bin/kernable")).unwrap();
        plan.secrets_dir = dir.join("run");
        let mut secrets = Secrets::default();
        secrets.refresh(&plan, &dir, true).await.unwrap();
        let read = |name: &str| std::fs::read_to_string(dir.join("run").join(name)).unwrap();
        assert_eq!(
            (read("stripe"), read("smtp")),
            ("sk_1".into(), "pw-1".into())
        );
        let stamped = |secrets: &Secrets| {
            let mut children = plan.children.clone();
            secrets.stamp(&mut children);
            children
        };
        let before = stamped(&secrets);

        // A look without SIGHUP: the file is read again, the command does not run.
        std::fs::write(dir.join("stripe.key"), "sk_2").unwrap();
        secrets.refresh(&plan, &dir, false).await.unwrap();
        assert_eq!(
            (read("stripe"), read("smtp")),
            ("sk_2".into(), "pw-1".into())
        );
        let after = stamped(&secrets);
        assert_ne!(before[1], after[1], "pay restarts");
        assert_eq!(before[2], after[2], "mail runs on");

        // SIGHUP: the command runs again.
        secrets.refresh(&plan, &dir, true).await.unwrap();
        assert_eq!(read("smtp"), "pw-2");
        assert_ne!(after[2], stamped(&secrets)[2], "mail restarts");

        // A file that is gone: an error, and the value stays.
        std::fs::remove_file(dir.join("stripe.key")).unwrap();
        let error = secrets.refresh(&plan, &dir, false).await.unwrap_err();
        assert!(error.contains("stripe"), "{error}");
        assert_eq!(read("stripe"), "sk_2");
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.join("run"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "nobody else enters the directory");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn on_linux_every_part_runs_in_the_instance_s_pod_within_its_limits() {
        let text = r#"
[kernel]
flags = { root = "root.pub", store = "sqlite:/srv/shop/data/k.db", listen = "0.0.0.0:7233" }
env = { KERNABLE_LOG = "info" }
cpus = "2"
memory = "1g"

[backup]
to = "file:backups"
key = "backup.key"
backup-key = "backup-encryption.key"
memory = "256m"

[node]
project = "shop"
cpus = "4"
memory = "4g"
network = "host"

[[service]]
name = "worker"
build = "."
"#;
        let (dir, this) = (Path::new("/srv/shop"), Path::new("/opt/kernable/kernable"));
        let plan = plan_on(text, dir, this, true).unwrap();
        assert_eq!(
            plan.pod,
            Some(Pod {
                name: "kernable-shop".into(),
                cpus: Some("4".into()),
                memory: Some("4g".into()),
                network: Network::Host,
            })
        );
        let kernel = &plan.children[0];
        assert_eq!(kernel.binary, Path::new("podman"));
        assert_eq!(kernel.watch, this, "a new binary restarts it");
        assert_eq!(kernel.env["KERNABLE_LOG"], "info");
        assert_eq!(
            kernel.args.join(" "),
            "run --rm --replace --name kernable-shop-kernel --pod kernable-shop \
             --volume /opt/kernable/kernable:/usr/local/bin/kernable:ro \
             --volume /srv/shop:/srv/shop --workdir /srv/shop --security-opt label=disable \
             --env KERNABLE_LOG --cap-drop all --security-opt no-new-privileges \
             --pids-limit 4096 --read-only --cpus 2 --memory 1g"
        );
        let c = kernel.container.as_ref().unwrap();
        assert_eq!(c.image, Image::Base(BASE_IMAGE.into()));
        assert_eq!(c.command[..2], ["kernable", "start"]);
        let backup = &plan.children[1];
        assert!(backup.args.join(" ").ends_with("--memory 256m"));
        assert_eq!(
            backup.container.as_ref().unwrap().command[..2],
            ["kernable", "backup"]
        );
        let worker = plan.children[2].args.join(" ");
        assert!(worker.contains("--pod kernable-shop"), "{worker}");
        assert!(!worker.contains("--network"), "the pod's network: {worker}");

        let own = text.replace(
            "[kernel]\n",
            "[kernel]\nimage = \"registry.example/kms:1\"\n",
        );
        let plan = plan_on(&own, dir, this, true).unwrap();
        let image = &plan.children[0].container.as_ref().unwrap().image;
        assert_eq!(image, &Image::Base("registry.example/kms:1".into()));

        // The container sees the project directory only.
        let outside = text.replace("sqlite:/srv/shop/data/k.db", "sqlite:/var/lib/k.db");
        let error = plan_on(&outside, dir, this, true).unwrap_err().to_string();
        assert!(
            error.contains("/var/lib/k.db") && error.contains("only directory"),
            "{error}"
        );
        let error = plan_on(&outside, dir, this, false).unwrap_err().to_string();
        assert!(
            !error.contains("only directory"),
            "a plain process sees it: {error}"
        );

        // Without containers there is nothing to limit the kernel with.
        let error = plan_on(text, dir, this, false).unwrap_err().to_string();
        assert!(error.contains("need Linux"), "{error}");
    }

    #[test]
    fn in_a_network_of_its_own_the_kernel_listens_on_a_socket_its_services_see() {
        let text = r#"
[kernel]
flags = { root = "root.pub" }

[backup]
to = "file:backups"
key = "backup.key"
backup-key = "backup-encryption.key"

[node]
project = "shop"

[[service]]
name = "worker"
build = "."
"#;
        let (dir, this) = (Path::new("/srv/shop"), Path::new("/bin/kernable"));
        let plan = plan_on(text, dir, this, true).unwrap();
        let pod = plan.pod.as_ref().unwrap();
        assert_eq!(
            pod.network,
            Network::Own {
                sockets: "/srv/shop/run".into()
            }
        );
        let kernel = &plan.children[0].container.as_ref().unwrap().command;
        assert!(
            kernel
                .join(" ")
                .contains("--listen unix:/srv/shop/run/kernel.sock"),
            "{kernel:?}"
        );
        let backup = plan.children[1]
            .container
            .as_ref()
            .unwrap()
            .command
            .join(" ");
        assert!(
            backup.contains("--follow unix:/srv/shop/run/kernel.sock"),
            "{backup}"
        );
        let worker = plan.children[2].args.join(" ");
        assert!(
            worker.contains("--volume /srv/shop/run:/run/kernable/kernel:ro"),
            "read-only, so no service puts a socket of its own in the kernel's place: {worker}"
        );
        assert!(
            worker.contains("--env KERNABLE_URL=unix:/run/kernable/kernel/kernel.sock"),
            "{worker}"
        );
        assert!(!worker.contains("--network"), "{worker}");

        // The node chooses the address; a pair needs the ingress.
        let listen = text.replace(
            "root = \"root.pub\"",
            "root = \"root.pub\", listen = \"0.0.0.0:7233\"",
        );
        let error = plan_on(&listen, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("network = \"host\""), "{error}");
        let pair = format!(
            "{text}\n[pair]\nwitness = \"10.0.0.9:7300\"\nnode-key = \"n.key\"\npeer = \"10.0.0.2:7233\"\nkernel-keys = [\"a.pub\"]\n"
        );
        let error = plan_on(&pair, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("ingress"), "{error}");
        let host = pair.replace(
            "project = \"shop\"",
            "project = \"shop\"\nnetwork = \"host\"",
        );
        assert!(plan_on(&host, dir, this, true).is_ok());
        let wrong = text.replace(
            "project = \"shop\"",
            "project = \"shop\"\nnetwork = \"bridge\"",
        );
        assert!(plan_on(&wrong, dir, this, true).is_err());

        // Without a pod nothing changes: the kernel listens where its flags say.
        let plain = plan_on(text, dir, this, false).unwrap();
        assert_eq!(plain.pod, None);
        assert!(
            !plain.children[0]
                .args
                .iter()
                .any(|a| a.starts_with("unix:"))
        );
    }

    #[test]
    fn the_node_section_names_the_metrics_address() {
        let text = "[kernel]\n[node]\nmetrics-listen = \"127.0.0.1:7240\"\n";
        let plan = plan(text, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_eq!(plan.metrics_listen.as_deref(), Some("127.0.0.1:7240"));
    }
}
