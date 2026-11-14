//! `kernable node`: one node of a project, run from its `project.toml`.
//!
//! The node runs the kernel and, with a `[backup]` section, the backup follower as child
//! processes of the same (or a named) kernable binary. It restarts a child that exits, backing
//! off up to 30 s, and reads the file again every two seconds (or on SIGHUP): a child whose
//! command changed, or whose binary was replaced, is stopped and started again, so an update is
//! a new binary and nothing else. The kernel is not restarted for a new binary but hands its
//! stores over: the new kernel starts at once and waits for them (`--take-over-ms`), the
//! old one gets SIGUSR1 once it waits, finishes its running requests and lets them go, and the
//! services run on and reconnect. SIGTERM or SIGINT stops the children in reverse order, each
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
//! check-hours = 24                           # optional: reads the backup through that often
//! ```
//!
//! The customer's services are built and run with Podman, one `[[service]]` each:
//!
//! ```toml
//! [node]                                     # optional
//! instance = "node-a"                        # KERNABLE_INSTANCE (default the host name)
//! project = "shop"                           # names the containers (default the directory)
//! podman = "podman"                          # optional
//! exclusive = false                          # optional: see below
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
//! data = "/data"                             # optional: app-data/ of the project, read-write
//! ```
//!
//! A service may name an `image` instead of `build` (`<repository>@sha256:<digest>`, or a tag
//! unless `[node] require-digest = true`): it runs as it is, nothing is built. The kernel gets
//! `--admin-socket` (`[node] admin-socket`, `admin.sock` in the project directory by default),
//! which the services' containers do not see.
//!
//! The node builds each service's image at start, when the service changes and on SIGHUP (a
//! service whose image changed restarts), then runs it with `podman run --rm` in the
//! foreground, a child like the kernel: started after the kernel, stopped before it. It runs
//! with `KERNABLE_URL` (where it reaches the kernel), `KERNABLE_INSTANCE`, and, with a key,
//! `KERNABLE_KEY` and `KERNABLE_GRANT` naming the files mounted read-only. In the instance's
//! pod (Linux) it shares the pod's network: one of its own by default, reaching the kernel
//! through its socket, or the machine's with `[node] network = "host"`; without a pod, the
//! machine's.
//!
//! On Linux every part runs in Podman, in one pod per instance (`kernable-<project>`): the
//! kernel and the backup follower in a base image with the kernable binary mounted, the project
//! directory at its own path; `cpus` and `memory` limit each part, and in `[node]` the instance.
//!
//! Secrets are named in `[[secret]]` (a `file`, or a `command` run at start and on SIGHUP)
//! and mounted read-only at `/run/secrets/<name>` into the services that list them; a new value
//! restarts those. A `[pair]` section (witness, node-key, advertise, peer, kernel-keys,
//! witness-generation, which every pair needs) sets the kernel's pair flags and gives the backup
//! follower and the services both kernels' addresses.
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
    /// An image run as it is, by reference, which Podman pulls unless it holds it: the kernel's
    /// and the backup follower's base image (the kernable binary mounted in), or a service's
    /// `image`.
    Base(String),
}

/// Whether `image` names its manifest digest: `<repository>@sha256:<64 hex digits>`.
pub fn pinned(image: &str) -> bool {
    image
        .rsplit_once("@sha256:")
        .is_some_and(|(repository, digest)| {
            !repository.is_empty()
                && digest.len() == 64
                && digest
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        })
}

/// The pod all of an instance's containers join (Linux): one network, one total limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pod {
    /// `kernable-<project>`.
    pub name: String,
    pub cpus: Option<String>,
    pub memory: Option<String>,
    pub network: Network,
    /// The seccomp profile of its containers, which the node writes as it makes the pod.
    pub seccomp: PathBuf,
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
/// The directory in the project directory that services naming `data` keep their state in.
const APP_DATA: &str = "app-data";
/// The kernel's local socket (`--admin-socket`) in the project directory, unless `[node]
/// admin-socket` names another place.
const ADMIN_SOCKET: &str = "admin.sock";

/// The kernel's and the backup follower's base image, unless `[kernel] image` names another
/// (one with your KMS's client for `--key-command`, say). Its C library is newer than any
/// supported host's, so a binary built there runs in it.
// Pinned by digest: a node runs what was tested, not what the tag points at today.
// Update it on purpose: docker buildx imagetools inspect docker.io/library/debian:trixie-slim.
const BASE_IMAGE: &str = "docker.io/library/debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2";

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
    /// The kernel's local socket (`--admin-socket`), relative to the project directory;
    /// `admin.sock` by default.
    admin_socket: Option<PathBuf>,
    /// Every image the node runs (a service's `image`, `[kernel] image`) must name its digest.
    #[serde(default)]
    require_digest: bool,
    /// A service that is new or changed starts only once every process the node is stopping
    /// has ended, not only one of the same name: an app whose state is on the instance's
    /// volume never runs two versions at once (stop, then start).
    #[serde(default)]
    exclusive: bool,
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
    /// `[node] exclusive`.
    pub exclusive: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ServiceSection {
    name: String,
    /// A directory the node builds the image from; or `image`.
    build: Option<PathBuf>,
    file: Option<String>,
    /// An image to run as it is, by reference (`<repository>@sha256:<digest>`): nothing is built.
    image: Option<String>,
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
    /// The port it serves HTTP on in the pod, for the ingress to reach it through
    /// `run/<name>.sock`.
    expose: Option<u16>,
    /// Where in its container the project's `app-data` directory is mounted, read-write: what
    /// the service keeps across restarts and new versions (an app that keeps its own state,
    /// without the kernel). Every service that names it shares the one directory.
    data: Option<PathBuf>,
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
    /// The witness's generation file (`kernable witness generation`): every pair has one.
    witness_generation: String,
}

/// The kernel flags `[pair]` sets.
const PAIR_FLAGS: [&str; 6] = [
    "witness",
    "node-key",
    "advertise",
    "primary-kernel-key",
    "standby-timeout-ms",
    "witness-generation",
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
    check_hours: Option<u64>,
    /// A backup signer's URL: the follower asks it to presign every request, with its key.
    signer: Option<String>,
    cpus: Option<String>,
    memory: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
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
        seccomp: runtime_dir()
            .join(format!("kernable-{name}"))
            .join("seccomp.json"),
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
    if node.require_digest && !pinned(&image) {
        return Err(format!(
            "[kernel] image {image}: [node] require-digest asks for <repository>@sha256:<digest>"
        )
        .into());
    }
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
        harden(&mut run, false, Some(&pod.seccomp));
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
        if flag == "admin-socket" {
            return Err(
                "[kernel] flags: admin-socket comes from [node] admin-socket; drop it".into(),
            );
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
    // The kernel's local socket for the operator's tools: in the project directory, which the
    // kernel's container sees and the services' do not.
    let admin = dir.join(
        node.admin_socket
            .as_deref()
            .unwrap_or(Path::new(ADMIN_SOCKET)),
    );
    inside("[node] admin-socket", &admin.display().to_string())?;
    if let Some(sockets) = sockets.as_ref().filter(|s| admin.starts_with(s)) {
        return Err(format!(
            "[node] admin-socket {}: not in {}, which the services see",
            admin.display(),
            sockets.display()
        )
        .into());
    }
    args.extend(["--admin-socket".into(), admin.display().to_string()]);
    // The other kernel of a pair, for the backup follower and the services: they turn to
    // whichever is the primary.
    let mut peer = None;
    // In the pod's own network the other kernel reaches this one through the machine's
    // ingress: `advertise` says where.
    if let (Some(pair), Some(_)) = (&project.pair, &sockets)
        && pair
            .advertise
            .as_deref()
            .is_none_or(|a| a.starts_with("unix:"))
    {
        return Err(
            "[pair] advertise: in the pod's own network, name where the other node reaches this \
             kernel, through this machine's ingress (host:443)"
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
        inside("[pair] witness-generation", &pair.witness_generation)?;
        args.extend([
            "--witness-generation".into(),
            pair.witness_generation.clone(),
        ]);
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
                // In the pod's own network, this kernel on its socket.
                Some((_, other)) if sockets.is_some() => format!("{listen},{other}"),
                Some((this, other)) => format!("{this},{other}"),
                None => listen.clone(),
            }),
            "--key".into(),
            b.key,
            "--to".into(),
            b.to,
            "--backup-key".into(),
            b.backup_key,
            // Beside the project, for whoever watches the node.
            "--status-file".into(),
            "backup.status".into(),
        ];
        if let Some(signer) = b.signer {
            args.extend(["--signer".into(), signer]);
        }
        for (flag, value) in [
            ("--segment-seconds", b.segment_seconds),
            ("--base-hours", b.base_hours),
            ("--keep", b.keep),
            ("--check-hours", b.check_hours),
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
    // The services' ports the ingress reaches, as `kernable expose` takes them.
    let mut exposed: Vec<String> = Vec::new();
    for svc in project.service {
        if !valid_name(&svc.name) || matches!(svc.name.as_str(), "kernel" | "backup" | "expose") {
            return Err(format!(
                "[[service]] {}: a name has 1 to 63 lowercase letters, digits and -, and is not kernel, backup or expose",
                svc.name
            )
            .into());
        }
        if specs.iter().any(|s| s.name == svc.name) {
            return Err(format!("[[service]] {} is named twice", svc.name).into());
        }
        if let Some(port) = svc.expose {
            let Some(sockets) = &sockets else {
                return Err(format!(
                    "[[service]] {}: expose needs the pod's own network, on Linux",
                    svc.name
                )
                .into());
            };
            exposed.push(format!(
                "{}={}",
                sockets.join(format!("{}.sock", svc.name)).display(),
                std::net::SocketAddr::from(([127, 0, 0, 1], port))
            ));
        }
        let image = match (svc.build, svc.image) {
            (Some(build), None) => {
                if node.require_digest {
                    return Err(format!(
                        "[[service]] {}: [node] require-digest runs images by digest only; \
                         name one with image = \"<repository>@sha256:<digest>\" instead of build",
                        svc.name
                    )
                    .into());
                }
                Image::Build {
                    context: dir.join(build),
                    file: svc.file,
                }
            }
            (None, Some(image)) => {
                if svc.file.is_some() {
                    return Err(format!(
                        "[[service]] {}: file goes with build, not image",
                        svc.name
                    )
                    .into());
                }
                if image.is_empty() || image.starts_with('-') || image.contains(char::is_whitespace)
                {
                    return Err(format!("[[service]] {}: image {image:?}", svc.name).into());
                }
                if node.require_digest && !pinned(&image) {
                    return Err(format!(
                        "[[service]] {}: image {image}: [node] require-digest asks for \
                         <repository>@sha256:<digest>",
                        svc.name
                    )
                    .into());
                }
                Image::Base(image)
            }
            _ => {
                return Err(format!(
                    "[[service]] {}: give either build (a directory) or image (a reference)",
                    svc.name
                )
                .into());
            }
        };
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
        if let Some(inside) = &svc.data {
            let inside = inside.to_str().unwrap_or_default();
            if !inside.starts_with('/') || inside.contains([':', ',']) || inside == "/" {
                return Err(format!(
                    "[[service]] {}: data {inside:?}: an absolute path in the container",
                    svc.name
                )
                .into());
            }
            // Made here if it is not there yet; one that cannot be made shows when Podman
            // starts the service.
            let outside = dir.join(APP_DATA);
            let _ = std::fs::create_dir_all(&outside);
            args.extend([
                "--volume".into(),
                format!("{}:{inside}:z", outside.display()),
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
        harden(
            &mut args,
            svc.writable,
            pod.as_ref().map(|p| p.seccomp.as_path()),
        );
        limits(&mut args, &svc.cpus, &svc.memory);
        specs.push(Spec {
            name: svc.name,
            binary: podman.clone(),
            args,
            env: svc.env,
            stop: Duration::from_secs(svc.stop_seconds.unwrap_or(30)),
            container: Some(Container {
                name: container,
                image,
                command: svc.command,
            }),
            secrets: svc.secrets,
            watch: podman.clone(),
        });
    }
    // One forwarder for the instance: each exposed service's socket in run/ to its port in the
    // pod's own network.
    if !exposed.is_empty() {
        let args = std::iter::once("expose".to_owned())
            .chain(exposed)
            .collect();
        specs.push(kernable("expose", args, BTreeMap::new(), &None, &None));
    }
    Ok(Plan {
        children: specs,
        metrics_listen,
        secrets,
        secrets_dir,
        pod,
        podman,
        exclusive: node.exclusive,
    })
}

/// Most processes a container may have at once.
const PIDS: u32 = 4096;

/// What every container gets: an init that hands signals on, no capabilities, no way to gain privileges (no
/// setuid binary works), a cap on its processes, a root filesystem it cannot change unless
/// `writable` (Podman mounts a tmpfs on `/tmp`, `/var/tmp` and `/run` of a read-only one), and,
/// in a pod, the node's tightened `seccomp` profile.
fn harden(args: &mut Vec<String>, writable: bool, seccomp: Option<&Path>) {
    args.extend([
        // A small init as the container's first process: it hands SIGTERM on and reaps. The
        // first process itself ignores a signal it set no handler for, so a program that
        // relies on the default action (most do) was killed only after its stop time.
        "--init".into(),
        "--cap-drop".into(),
        "all".into(),
        "--security-opt".into(),
        "no-new-privileges".into(),
        "--pids-limit".into(),
        PIDS.to_string(),
    ]);
    if let Some(profile) = seccomp {
        args.extend([
            "--security-opt".into(),
            format!("seccomp={}", profile.display()),
        ]);
    }
    if !writable {
        args.push("--read-only".into());
    }
}

/// Where Podman keeps its default seccomp profile: an administrator's own first.
const SECCOMP_DEFAULTS: [&str; 2] = [
    "/etc/containers/seccomp.json",
    "/usr/share/containers/seccomp.json",
];
/// `clone`'s and `unshare`'s flag for a new user namespace.
const CLONE_NEWUSER: u64 = 0x1000_0000;

/// Podman's default seccomp `profile`, tightened: no new user namespaces, whose
/// root holds capabilities that open the kernel paths most container escapes go through, and
/// no kernel keyrings, which no namespace separates. Podman allows both by default.
fn tighten(mut profile: serde_json::Value) -> Result<serde_json::Value, String> {
    const CLOSED: [&str; 6] = [
        "unshare",
        "clone",
        "clone3",
        "keyctl",
        "add_key",
        "request_key",
    ];
    let rules = profile
        .get_mut("syscalls")
        .and_then(|s| s.as_array_mut())
        .ok_or("no list of syscalls")?;
    for rule in rules.iter_mut() {
        if let Some(names) = rule.get_mut("names").and_then(|n| n.as_array_mut()) {
            names.retain(|n| !n.as_str().is_some_and(|n| CLOSED.contains(&n)));
        }
    }
    rules.retain(|r| {
        r.get("names")
            .and_then(|n| n.as_array())
            .is_none_or(|n| !n.is_empty())
    });
    let new_user = |value: u64| {
        serde_json::json!([{
            "index": 0,
            "value": CLONE_NEWUSER,
            "valueTwo": value,
            "op": "SCMP_CMP_MASKED_EQ"
        }])
    };
    rules.extend([
        serde_json::json!({"names": ["unshare", "clone"], "action": "SCMP_ACT_ALLOW", "args": new_user(0)}),
        serde_json::json!({"names": ["unshare", "clone"], "action": "SCMP_ACT_ERRNO", "errnoRet": libc::EPERM, "args": new_user(CLONE_NEWUSER)}),
        // Its flags are out of a filter's reach: "not there", and C libraries use clone instead.
        serde_json::json!({"names": ["clone3"], "action": "SCMP_ACT_ERRNO", "errnoRet": libc::ENOSYS}),
        serde_json::json!({"names": ["keyctl", "add_key", "request_key"], "action": "SCMP_ACT_ERRNO", "errnoRet": libc::EPERM}),
    ]);
    Ok(profile)
}

/// Writes Podman's default seccomp profile, [`tighten`]ed, to `to`; without one, no container
/// starts.
fn write_seccomp(to: &Path) -> Result<(), String> {
    let (from, text) = SECCOMP_DEFAULTS
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok().map(|t| (*p, t)))
        .ok_or_else(|| {
            format!(
                "no seccomp profile of Podman's in {}",
                SECCOMP_DEFAULTS.join(" or ")
            )
        })?;
    let profile = serde_json::from_str(&text)
        .map_err(|e| e.to_string())
        .and_then(tighten)
        .map_err(|e| format!("{from}: {e}"))?;
    if let Some(dir) = to.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::write(to, serde_json::to_vec(&profile).expect("JSON"))
        .map_err(|e| format!("{}: {e}", to.display()))
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
    /// The container it runs in now: a kernel that hands over runs in the other of two names.
    container_name: Option<String>,
    /// The kernel's previous process, still serving until the new one waits for the stores.
    handover: Option<Handover>,
}

/// A kernel handing its stores over to a new process.
struct Handover {
    old: Child,
    old_container: Option<String>,
    /// The new kernel said it waits for the stores.
    ready: Arc<std::sync::atomic::AtomicBool>,
    since: Instant,
}

/// What the new kernel of a handover is started with: it waits for the stores.
const TAKE_OVER: [&str; 2] = ["--take-over-ms", "60000"];
/// The line in which a new kernel says it waits for the stores (`Kernel::open`).
const TAKE_OVER_WAITING: &str = "waiting to take them over";

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
            container_name: None,
            handover: None,
        }
    }
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Passes the lines of a child's output on to the node's own, prefixed with its name.
fn forward(
    name: &str,
    out: Option<impl AsyncRead + Unpin + Send + 'static>,
    stderr: bool,
    ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
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
            if let Some(ready) = &ready
                && text.contains(TAKE_OVER_WAITING)
            {
                ready.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let _ = if stderr {
                writeln!(std::io::stderr().lock(), "{name} | {text}")
            } else {
                writeln!(std::io::stdout().lock(), "{name} | {text}")
            };
        }
    });
}

/// One build at a time: two builds of the same layers at once into one rootless storage hang
/// for good once their steps are done (seen with Podman 5.7, 3 of 3 times), and they share the
/// disk and the network anyway.
static BUILDS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
/// A build that takes longer is stopped, and tried again after a pause like a failed one.
const BUILD_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Builds a service's image; its ID. The build's own output goes on as the service's.
async fn build(spec: Spec) -> Result<String, String> {
    let _turn = BUILDS.acquire().await.expect("never closed");
    tokio::time::timeout(BUILD_TIMEOUT, build_now(&spec))
        .await
        .unwrap_or_else(|_| {
            Err(format!(
                "building {} took over {} min: stopped",
                spec.name,
                BUILD_TIMEOUT.as_secs() / 60
            ))
        })
}

async fn build_now(spec: &Spec) -> Result<String, String> {
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
    forward(&spec.name, child.stderr.take(), true, None);
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
    spawn_with(spec, image, dir, &[], None, None)
}

/// Like [`spawn`], with `extra` arguments for the child itself, in the container `name` instead
/// of its own, and `ready` set once it says it waits for the stores (a kernel's handover).
fn spawn_with(
    spec: &Spec,
    image: Option<&str>,
    dir: &Path,
    extra: &[&str],
    name: Option<&str>,
    ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Result<Child, BoxError> {
    let mut command = Command::new(&spec.binary);
    let mut args = spec.args.clone();
    if let (Some(name), Some(i)) = (name, args.iter().position(|a| a == "--name")) {
        args[i + 1] = name.to_owned();
    }
    command.args(&args);
    match (&spec.container, image) {
        (Some(c), Some(image)) => {
            command.arg(image).args(&c.command).args(extra);
        }
        _ => {
            command.args(extra);
        }
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
    forward(&spec.name, child.stdout.take(), false, ready.clone());
    forward(&spec.name, child.stderr.take(), true, ready);
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
    // A kernel handing over: the old process still serves, and stops like any kernel (it
    // drains), after the new one waiting for its stores.
    let handover = c.handover.take();
    let child = c.child.take();
    if child.is_none() && handover.is_none() {
        return;
    }
    let (name, stop, binary) = (c.spec.name.clone(), c.spec.stop, c.spec.binary.clone());
    let container = c
        .container_name
        .clone()
        .or_else(|| c.spec.container.as_ref().map(|c| c.name.clone()));
    let previous = c.stopping.take();
    c.stopping = Some(tokio::spawn(async move {
        if let Some(previous) = previous {
            let _ = previous.await;
        }
        if let Some(mut child) = child {
            stopped(&name, &mut child, stop, &binary, container.as_deref(), STOP).await;
        }
        if let Some(mut h) = handover {
            let container = h.old_container.as_deref();
            stopped(&name, &mut h.old, stop, &binary, container, STOP).await;
        }
    }));
}

/// Stops a child and waits until it ended: in order, at the node's own end.
async fn stop_now(c: &mut Running) {
    stop(c);
    if let Some(stopping) = c.stopping.take() {
        let _ = stopping.await;
    }
}

/// The signal that stops a child; a kernel of a pair hands its role to its standby first.
const STOP: i32 = libc::SIGTERM;
/// The signal that tells the old kernel to let its stores go to the new one beside it,
/// without handing its role to its standby.
const LET_GO: i32 = libc::SIGUSR1;

async fn stopped(
    name: &str,
    child: &mut Child,
    stop: Duration,
    binary: &Path,
    container: Option<&str>,
    signal: i32,
) {
    if let Some(pid) = child.id() {
        // SAFETY: kill(2) on our own child's pid; a pid that already exited is harmless.
        unsafe {
            libc::kill(pid as libc::pid_t, signal);
        }
    }
    match tokio::time::timeout(stop, child.wait()).await {
        Ok(status) => tracing::info!(child = %name, status = ?status.ok(), "stopped"),
        Err(_) => {
            tracing::warn!(child = %name, "did not stop in time: killed");
            let _ = child.kill().await;
            if let Some(container) = container {
                // Killed already: nothing to wait for (rm waits 10 s by default).
                let _ = Command::new(binary)
                    .args(["rm", "--force", "--time", "0", container])
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
    // Exclusive: when what is stopping now has ended.
    let mut ended = None;
    apply(&mut children, first.children, first.exclusive, &mut ended).await;
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
                    apply(&mut children, plan.children, plan.exclusive, &mut ended).await;
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

/// Podman's `--network` for a pod of its own: pasta without `host.containers.internal`. Older
/// pasta (Ubuntu 24.04's) does not know the option; the pod then gets plain pasta, said once in
/// the log, and the firewall keeps the machine's other addresses out (docs/how-to/node.md).
fn pasta_network() -> &'static str {
    static NETWORK: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();
    NETWORK.get_or_init(|| {
        let help = std::process::Command::new("pasta")
            .arg("--help")
            .stdin(Stdio::null())
            .output();
        let knows = help.is_ok_and(|o| {
            let text = [o.stdout, o.stderr].concat();
            String::from_utf8_lossy(&text).contains("--map-guest-addr")
        });
        if knows {
            "pasta:--map-guest-addr,none"
        } else {
            tracing::warn!("pasta does not know --map-guest-addr: pods get plain pasta");
            "pasta"
        }
    })
}

/// Makes the instance's pod, replacing one a node before left: its network (its own, or the
/// machine's with `[node] network = "host"`), and the instance's total limits.
async fn make_pod(podman: &Path, pod: &Pod) -> Result<(), String> {
    write_seccomp(&pod.seccomp)?;
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
                args.extend(["--network".into(), pasta_network().into()]);
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
    handing_over(c).await;
    // A replaced binary: start the child on the new one; the kernel hands its stores over.
    if c.child.is_some() && c.handover.is_none() && modified(&c.spec.watch) != c.binary_changed {
        if c.spec.name == "kernel" {
            hand_over(c, dir);
        } else {
            tracing::info!(child = %c.spec.name, "its binary changed: restarting it");
            stop(c);
            c.restart_at = Some(Instant::now());
        }
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
    c.container_name = c.spec.container.as_ref().map(|k| k.name.clone());
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

/// The kernel's binary changed: the new kernel starts at once, in the other container
/// name, and waits for the stores; the old one serves until it does (see [`handing_over`]).
fn hand_over(c: &mut Running, dir: &Path) {
    let Some(old) = c.child.take() else {
        return;
    };
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let name = c.spec.container.as_ref().map(|k| {
        if c.container_name.as_deref() == Some(k.name.as_str()) {
            format!("{}-b", k.name)
        } else {
            k.name.clone()
        }
    });
    tracing::info!(child = %c.spec.name, "its binary changed: the new kernel takes the stores over");
    match spawn_with(
        &c.spec,
        c.image.as_deref(),
        dir,
        &TAKE_OVER,
        name.as_deref(),
        Some(Arc::clone(&ready)),
    ) {
        Ok(new) => {
            c.handover = Some(Handover {
                old,
                old_container: c.container_name.take(),
                ready,
                since: Instant::now(),
            });
            c.child = Some(new);
            c.container_name = name;
            c.binary_changed = modified(&c.spec.watch);
            c.started = Instant::now();
            c.restarts += 1;
            c.started_ms = crate::now_ms();
        }
        Err(e) => {
            tracing::error!(child = %c.spec.name, error = %e, "cannot start the new kernel: the old one serves on");
            c.child = Some(old);
            c.binary_changed = modified(&c.spec.watch);
        }
    }
}

/// A handover under way: once the new kernel waits for the stores (or the old one ended), the
/// old one gets SIGUSR1 and lets them go; a new kernel that ends first leaves the old serving.
async fn handing_over(c: &mut Running) {
    let Some(h) = c.handover.as_mut() else {
        return;
    };
    if let Some(new) = &mut c.child
        && let Ok(Some(status)) = new.try_wait()
        && !h.ready.load(std::sync::atomic::Ordering::SeqCst)
    {
        tracing::error!(child = %c.spec.name, %status, "the new kernel ended before it took the stores over: the old one serves on");
        let h = c.handover.take().expect("checked");
        c.child = Some(h.old);
        c.container_name = h.old_container;
        return;
    }
    let old_ended = matches!(h.old.try_wait(), Ok(Some(_)));
    if !h.ready.load(std::sync::atomic::Ordering::SeqCst)
        && !old_ended
        && h.since.elapsed() < Duration::from_secs(60)
    {
        return;
    }
    let mut h = c.handover.take().expect("checked");
    tracing::info!(child = %c.spec.name, "the new kernel waits for the stores: the old one hands them over");
    let (name, stop, binary) = (c.spec.name.clone(), c.spec.stop, c.spec.binary.clone());
    tokio::spawn(async move {
        stopped(
            &name,
            &mut h.old,
            stop,
            &binary,
            h.old_container.as_deref(),
            LET_GO,
        )
        .await;
    });
}

/// Whether `new` is `old` with only another kernable binary: a kernel that hands over.
fn only_binary_changed(old: &Spec, new: &Spec) -> bool {
    if old.name != "kernel" || new.name != "kernel" || old.watch == new.watch {
        return false;
    }
    let (from, to) = (
        old.watch.display().to_string(),
        new.watch.display().to_string(),
    );
    let swap = |s: &String| s.replace(&from, &to);
    let mut moved = old.clone();
    moved.binary = PathBuf::from(swap(&old.binary.display().to_string()));
    moved.args = old.args.iter().map(swap).collect();
    moved.watch = new.watch.clone();
    if let Some(c) = &mut moved.container {
        c.command = c.command.iter().map(swap).collect();
    }
    moved == *new
}

fn signal(kind: libc::c_int) -> Result<tokio::signal::unix::Signal, BoxError> {
    Ok(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::from_raw(kind),
    )?)
}

/// Brings `children` to `specs`: stops those gone or changed (reverse order); those new or
/// changed start with the next look (in order), once the old process of the same name ended,
/// or, `exclusive`, once every process stopping here ended: also those of an earlier change
/// still ending (`ended`, which this one replaces).
async fn apply(
    children: &mut Vec<Running>,
    specs: Vec<Spec>,
    exclusive: bool,
    ended: &mut Option<tokio::sync::watch::Receiver<bool>>,
) {
    let mut keep = Vec::new();
    // Processes of changed or removed children still stopping: a child of the same name
    // starts once its old process ended.
    let mut ending: Vec<(String, JoinHandle<()>)> = Vec::new();
    for mut c in std::mem::take(children).into_iter().rev() {
        if specs.contains(&c.spec) {
            keep.push(c);
        } else if let Some(new) = specs.iter().find(|s| only_binary_changed(&c.spec, s)) {
            // The next look hands the stores over to a kernel on the new binary.
            c.spec = new.clone();
            c.binary_changed = Some(std::time::UNIX_EPOCH);
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
    // Exclusive: one signal once all of them ended, which every new child waits for.
    let earlier = ended.take().filter(|e| !*e.borrow());
    let all_ended = (exclusive && (!ending.is_empty() || earlier.is_some())).then(|| {
        let (done, all) = tokio::sync::watch::channel(false);
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut ending)
            .into_iter()
            .map(|(_, h)| h)
            .collect();
        tokio::spawn(async move {
            if let Some(mut earlier) = earlier {
                let _ = earlier.wait_for(|e| *e).await;
            }
            for h in handles {
                let _ = h.await;
            }
            let _ = done.send(true);
        });
        all
    });
    ended.clone_from(&all_ended);
    for spec in specs {
        match keep.iter().position(|c| c.spec == spec) {
            Some(i) => children.push(keep.remove(i)),
            None => {
                let mut c = Running::new(spec);
                if let Some(ended) = &all_ended {
                    let mut ended = ended.clone();
                    c.stopping = Some(tokio::spawn(async move {
                        let _ = ended.wait_for(|e| *e).await;
                    }));
                } else if let Some(i) = ending.iter().position(|(name, _)| *name == c.spec.name) {
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
        let mut stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Out of file descriptors, or a connection reset while waiting: retry after a
                // pause instead of spinning.
                tracing::warn!(error = %e, "accepting a metrics connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
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
                "--admin-socket",
                "/srv/shop/admin.sock",
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
                "--status-file",
                "backup.status",
                "--segment-seconds",
                "2"
            ]
        );
        assert_eq!(specs[0].binary, Path::new("/bin/kernable"));
        // With a signer, the follower asks it instead of holding storage credentials.
        let signed = PROJECT.replace(
            "segment-seconds = 2",
            "signer = \"https://signer.example.com/backup\"",
        );
        let signed = plan(&signed, Path::new("/srv/shop"), Path::new("/bin/kernable"))
            .unwrap()
            .children;
        assert_eq!(
            signed[1].args[11..],
            ["--signer", "https://signer.example.com/backup"]
        );
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
        assert_eq!(
            alone[0].args,
            ["start", "--admin-socket", "/srv/shop/admin.sock"]
        );
        let flagged = specs(
            "[kernel]\nflags = { admin-socket = \"a.sock\" }\n",
            dir,
            this,
        );
        let flagged = flagged.unwrap_err().to_string();
        assert!(
            flagged.contains("comes from [node] admin-socket"),
            "{flagged}"
        );
        let moved = specs(
            "[kernel]\n[node]\nadmin-socket = \"ops/k.sock\"\n",
            dir,
            this,
        );
        assert_eq!(
            moved.unwrap()[0].args,
            ["start", "--admin-socket", "/srv/shop/ops/k.sock"]
        );
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
            "--env SHOP_MAIL --init --cap-drop all --security-opt no-new-privileges --pids-limit 4096 --read-only --memory 512m",
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
                "--init",
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

    #[test]
    fn a_service_may_keep_its_state_in_the_project_s_app_data() {
        let text = r#"
[kernel]

[[service]]
name = "web"
build = "."
data = "/data"

[[service]]
name = "worker"
build = "."
"#;
        let plan = plan_on(
            text,
            Path::new("/srv/shop"),
            Path::new("/bin/kernable"),
            true,
        )
        .unwrap();
        let args = |name: &str| {
            plan.children
                .iter()
                .find(|c| c.name == name)
                .unwrap()
                .args
                .join(" ")
        };
        assert!(args("web").contains("--volume /srv/shop/app-data:/data:z"));
        assert!(!args("worker").contains("app-data"));
        for bad in ["data", "/", "/a:b", "/a,ro"] {
            let text = text.replace("\"/data\"", &format!("{bad:?}"));
            assert!(
                plan_on(
                    &text,
                    Path::new("/srv/shop"),
                    Path::new("/bin/kernable"),
                    true
                )
                .is_err(),
                "{bad}"
            );
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
witness-generation = "witness.generation"
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
            "start --listen 10.0.0.1:7233 --root root.pub --admin-socket /srv/shop/admin.sock \
             --witness 10.0.0.9:7300 \
             --node-key node-a.key --advertise 10.0.0.1:7233 --primary-kernel-key kernel-a.pub \
             --primary-kernel-key kernel-b.pub --witness-generation witness.generation --backup on"
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
        // Every pair has a witness generation: without one, it is refused.
        let ungenerated = text.replace("witness-generation = \"witness.generation\"\n", "");
        let error = specs_of(&ungenerated).unwrap_err().to_string();
        assert!(error.contains("witness-generation"), "{error}");
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

    /// Whether service `b`, put in place of `a` (which takes a second to stop), started after
    /// `a` had ended: what `b` saw when it started.
    async fn replaced(exclusive: bool) -> String {
        let dir = std::env::temp_dir().join(format!("kn-excl-{}-{exclusive}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sh = |name: &str, script: &str| Spec {
            name: name.into(),
            binary: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            env: BTreeMap::new(),
            stop: Duration::from_secs(10),
            container: None,
            secrets: vec![],
            watch: "/bin/sh".into(),
        };
        // `a` ends a second after it is asked to, as an app flushing its state.
        let a = sh(
            "web-d1",
            "trap 'sleep 1; touch a.ended; exit 0' TERM; while true; do sleep 0.05; done",
        );
        let b = sh(
            "web-d2",
            "if [ -f a.ended ]; then echo after; else echo before; fi > b.started; sleep 30",
        );
        let (mut children, mut ended) = (Vec::new(), None);
        apply(&mut children, vec![a], exclusive, &mut ended).await;
        step(&mut children[0], &dir).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        apply(&mut children, vec![b], exclusive, &mut ended).await;
        let started = dir.join("b.started");
        for _ in 0..100 {
            for c in &mut children {
                step(c, &dir).await;
            }
            if std::fs::read_to_string(&started).is_ok_and(|t| !t.is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for c in &mut children {
            stop_now(c).await;
        }
        let saw = std::fs::read_to_string(&started).unwrap_or_default();
        let _ = std::fs::remove_dir_all(&dir);
        saw.trim().to_owned()
    }

    #[tokio::test]
    async fn an_exclusive_node_starts_a_new_service_only_once_the_old_one_ended() {
        assert_eq!(replaced(true).await, "after");
        // Without: the new one starts beside the old one while it stops.
        assert_eq!(replaced(false).await, "before");
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
        let seccomp = runtime_dir().join("kernable-shop/seccomp.json");
        assert_eq!(
            plan.pod,
            Some(Pod {
                name: "kernable-shop".into(),
                cpus: Some("4".into()),
                memory: Some("4g".into()),
                network: Network::Host,
                seccomp: seccomp.clone(),
            })
        );
        let kernel = &plan.children[0];
        assert_eq!(kernel.binary, Path::new("podman"));
        assert_eq!(kernel.watch, this, "a new binary restarts it");
        assert_eq!(kernel.env["KERNABLE_LOG"], "info");
        assert_eq!(
            kernel.args.join(" "),
            format!(
                "run --rm --replace --name kernable-shop-kernel --pod kernable-shop \
                 --volume /opt/kernable/kernable:/usr/local/bin/kernable:ro \
                 --volume /srv/shop:/srv/shop --workdir /srv/shop --security-opt label=disable \
                 --env KERNABLE_LOG --init --cap-drop all --security-opt no-new-privileges \
                 --pids-limit 4096 --security-opt seccomp={} --read-only --cpus 2 --memory 1g",
                seccomp.display()
            )
        );
        let worker = plan.children[2].args.join(" ");
        assert!(
            worker.contains(&format!("--security-opt seccomp={}", seccomp.display())),
            "{worker}"
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
    fn a_service_may_run_an_image_by_digest_and_the_node_may_require_digests() {
        let digest = "a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2";
        let text = format!(
            r#"
[kernel]
[node]
project = "shop"

[[service]]
name = "web"
image = "localhost/kernable-deploy@sha256:{digest}"
command = ["node", "server.js"]
"#
        );
        let (dir, this) = (Path::new("/srv/shop"), Path::new("/bin/kernable"));
        let plan = plan_on(&text, dir, this, true).unwrap();
        let web = &plan.children[1];
        let c = web.container.as_ref().unwrap();
        let reference = format!("localhost/kernable-deploy@sha256:{digest}");
        assert_eq!(c.image, Image::Base(reference.clone()));
        assert!(!built(web), "nothing to build, also not on SIGHUP");
        assert_eq!(Running::new(web.clone()).image, Some(reference));
        // Both, or neither, is a mistake; a Containerfile goes with build only.
        let both = text.replace("[[service]]\n", "[[service]]\nbuild = \".\"\n");
        let error = plan_on(&both, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("either build"), "{error}");
        let file = text.replace("[[service]]\n", "[[service]]\nfile = \"Containerfile\"\n");
        let error = plan_on(&file, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("file goes with build"), "{error}");

        // A tag is run as it is, unless the node requires digests.
        let tagged = text.replace(&format!("@sha256:{digest}"), ":v2");
        assert!(plan_on(&tagged, dir, this, true).is_ok());
        let strict = |t: &str| t.replace("[node]\n", "[node]\nrequire-digest = true\n");
        let error = plan_on(&strict(&tagged), dir, this, true)
            .unwrap_err()
            .to_string();
        assert!(error.contains("require-digest"), "{error}");
        let short = text.replace(digest, &digest[..63]);
        assert!(plan_on(&strict(&short), dir, this, true).is_err());
        let built_one =
            "[kernel]\n[node]\nrequire-digest = true\n[[service]]\nname = \"w\"\nbuild = \".\"\n";
        let error = plan_on(built_one, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("by digest only"), "{error}");
        // The kernel's base image as well: the default one is pinned.
        assert!(plan_on(&strict(&text), dir, this, true).is_ok());
        let kernel = strict(&text).replace("[kernel]\n", "[kernel]\nimage = \"debian:13\"\n");
        let error = plan_on(&kernel, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("[kernel] image debian:13"), "{error}");
        assert!(pinned(BASE_IMAGE));
        assert!(!pinned(&format!("@sha256:{digest}")));
    }

    #[test]
    fn the_kernel_s_local_socket_lies_where_the_services_do_not_see_it() {
        let text = "[kernel]\n[node]\nproject = \"shop\"\n";
        let (dir, this) = (Path::new("/srv/shop"), Path::new("/bin/kernable"));
        let plan = plan_on(text, dir, this, true).unwrap();
        let kernel = plan.children[0]
            .container
            .as_ref()
            .unwrap()
            .command
            .join(" ");
        assert!(
            kernel.contains("--admin-socket /srv/shop/admin.sock"),
            "{kernel}"
        );
        let shared = text.replace("[node]\n", "[node]\nadmin-socket = \"run/admin.sock\"\n");
        let error = plan_on(&shared, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("which the services see"), "{error}");
        let outside = text.replace("[node]\n", "[node]\nadmin-socket = \"/tmp/k.sock\"\n");
        let error = plan_on(&outside, dir, this, true).unwrap_err().to_string();
        assert!(error.contains("only directory"), "{error}");
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
            "{text}\n[pair]\nwitness = \"10.0.0.9:7300\"\nnode-key = \"n.key\"\nwitness-generation = \"w.generation\"\npeer = \"10.0.0.2:7233\"\nkernel-keys = [\"a.pub\"]\n"
        );
        let error = plan_on(&pair, dir, this, true).unwrap_err().to_string();
        assert!(
            error.contains("advertise") && error.contains("ingress"),
            "{error}"
        );
        // Through the ingress: the kernel says where it is reached; the backup follower stays
        // on the socket here and turns to the other node through its ingress.
        let advertised = pair.replace(
            "peer = \"10.0.0.2:7233\"",
            "advertise = \"a.example.com:443\"\npeer = \"b.example.com:443\"",
        );
        let plan = plan_on(&advertised, dir, this, true).unwrap();
        let kernel = plan.children[0]
            .container
            .as_ref()
            .unwrap()
            .command
            .join(" ");
        assert!(kernel.contains("--advertise a.example.com:443"), "{kernel}");
        assert!(
            kernel.contains("--listen unix:/srv/shop/run/kernel.sock"),
            "{kernel}"
        );
        let backup = plan.children[1]
            .container
            .as_ref()
            .unwrap()
            .command
            .join(" ");
        assert!(
            backup.contains("--follow unix:/srv/shop/run/kernel.sock,b.example.com:443"),
            "{backup}"
        );
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
    fn an_exposed_service_is_reached_through_a_socket_beside_the_kernel_s() {
        let text = r#"
[kernel]

[[service]]
name = "web"
build = "web"
expose = 3000

[[service]]
name = "api"
build = "api"
expose = 8080

[[service]]
name = "worker"
build = "worker"
"#;
        let (dir, this) = (Path::new("/srv/shop"), Path::new("/bin/kernable"));
        let plan = plan_on(text, dir, this, true).unwrap();
        let forwarder = plan.children.last().unwrap();
        assert_eq!(forwarder.name, "expose");
        assert_eq!(
            forwarder.container.as_ref().unwrap().command,
            [
                "kernable",
                "expose",
                "/srv/shop/run/web.sock=127.0.0.1:3000",
                "/srv/shop/run/api.sock=127.0.0.1:8080"
            ]
        );
        assert!(forwarder.args.join(" ").contains("--pod kernable-shop"));
        assert_eq!(
            plan.children.iter().filter(|c| c.name == "expose").count(),
            1,
            "one for the instance"
        );
        let named = text.replace("name = \"worker\"", "name = \"expose\"");
        assert!(
            plan_on(&named, dir, this, true).is_err(),
            "the forwarder's name"
        );
        let error = plan_on(text, dir, this, false).unwrap_err().to_string();
        assert!(error.contains("expose needs"), "{error}");
        let none = text
            .replace("expose = 3000\n", "")
            .replace("expose = 8080\n", "");
        assert!(
            plan_on(&none, dir, this, true)
                .unwrap()
                .children
                .iter()
                .all(|c| c.name != "expose")
        );
    }

    #[test]
    fn the_seccomp_profile_closes_new_user_namespaces_and_keyrings() {
        let default = serde_json::json!({
            "defaultAction": "SCMP_ACT_ERRNO",
            "syscalls": [
                {"names": ["read", "unshare", "clone", "clone3", "keyctl"], "action": "SCMP_ACT_ALLOW"},
                {"names": ["clone"], "action": "SCMP_ACT_ALLOW", "includes": {"caps": ["CAP_SYS_ADMIN"]}},
                {"names": ["add_key"], "action": "SCMP_ACT_ALLOW"},
            ]
        });
        let tight = tighten(default).unwrap();
        let rules = tight["syscalls"].as_array().unwrap();
        // Every rule of the default that allowed them lost them; others stay as they were.
        assert_eq!(rules[0]["names"], serde_json::json!(["read"]));
        assert!(
            rules.iter().all(|r| r["action"] != "SCMP_ACT_ALLOW"
                || r["names"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|n| n != "keyctl" && n != "clone3" && n != "add_key")),
            "{tight}"
        );
        let rule = |names: &[&str], action: &str| {
            rules
                .iter()
                .find(|r| r["names"] == serde_json::json!(names) && r["action"] == action)
                .unwrap_or_else(|| panic!("{names:?} {action} missing: {tight}"))
        };
        // unshare and clone pass unless they ask for a new user namespace.
        let allow = rule(&["unshare", "clone"], "SCMP_ACT_ALLOW");
        assert_eq!(allow["args"][0]["value"], 0x1000_0000);
        assert_eq!(allow["args"][0]["valueTwo"], 0);
        assert_eq!(allow["args"][0]["op"], "SCMP_CMP_MASKED_EQ");
        let deny = rule(&["unshare", "clone"], "SCMP_ACT_ERRNO");
        assert_eq!(deny["args"][0]["valueTwo"], 0x1000_0000);
        assert_eq!(deny["errnoRet"], libc::EPERM);
        assert_eq!(
            rule(&["clone3"], "SCMP_ACT_ERRNO")["errnoRet"],
            libc::ENOSYS
        );
        rule(&["keyctl", "add_key", "request_key"], "SCMP_ACT_ERRNO");
        assert!(
            tighten(serde_json::json!({})).is_err(),
            "no list of syscalls"
        );
    }

    #[test]
    fn the_node_section_names_the_metrics_address() {
        let text = "[kernel]\n[node]\nmetrics-listen = \"127.0.0.1:7240\"\n";
        let plan = plan(text, Path::new("/srv/shop"), Path::new("/bin/kernable")).unwrap();
        assert_eq!(plan.metrics_listen.as_deref(), Some("127.0.0.1:7240"));
    }
}
