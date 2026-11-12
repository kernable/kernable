use std::{collections::HashMap, path::PathBuf};

pub use kernable_store::Durability;

/// The flags of `kernable start`: flag, environment variable, default, description. A flag
/// wins over its variable; relative paths resolve against the working directory.
pub const FLAGS: &[(&str, &str, &str, &str)] = &[
    (
        "--root",
        "KERNABLE_ROOT",
        "",
        "A root key the kernel trusts: its key ID (hex) or the path of its `.pub` file. Only a root key can sign grants and revocations. Repeat the flag for several roots, for example while you replace one; the variable takes them comma-separated. At least one is required; `--dev` uses `root.pub` if none is given.",
    ),
    (
        "--store",
        "KERNABLE_STORE",
        "sqlite:kernable.db",
        "The event store. It also holds the command queue and results, checkpoints, revocations and registrations. `sqlite:<path>` or a bare path: SQLite. `file:<path>`: an append-only log file. `memory:`: nothing survives the process, for tests.",
    ),
    (
        "--listen",
        "KERNABLE_LISTEN",
        "127.0.0.1:7233",
        "The gRPC address for every service and client: `host:port`, or `unix:<path>` on Unix, whose owner and group may connect (mode 0660). Plain TCP is accepted only on a loopback address (`127.0.0.1`, `::1`, `localhost`); any other address needs mutual TLS (`--tls-cert`, `--tls-key`, `--tls-client-ca`), or the kernel does not start.",
    ),
    (
        "--kernel-key",
        "KERNABLE_KERNEL_KEY",
        "kernel.key",
        "The kernel's own signing key, written by `kernable keys new kernel`. It signs checkpoints and stored revocations. The file must exist. On Unix, SIGHUP reads it again, so a new key signs from then on without a restart.",
    ),
    (
        "--snapshots",
        "KERNABLE_SNAPSHOTS",
        "",
        "The snapshot store, a cache of aggregate states that workers rebuild from the events. `sqlite:<path>` or a bare path: SQLite, kept across restarts. `memory:`: lost on a restart, so the first command per aggregate reads all its events again. Not `file:`. Default: SQLite next to the event store, `<its name>-snapshots.db` (`kernable-snapshots.db` for `kernable.db`), or `memory:` for a `memory:` store.",
    ),
    (
        "--data-store",
        "KERNABLE_DATA_STORE",
        "file:data",
        "Where the data service keeps files. `file:<directory>`: one file per stored file, the directory is created with the first one. `memory:`: lost on a restart.",
    ),
    (
        "--cache-store",
        "KERNABLE_CACHE_STORE",
        "memory:",
        "Where the cache service keeps values. Only `memory:` exists: values last until they expire, are deleted, or the kernel restarts.",
    ),
    (
        "--vault-key",
        "KERNABLE_VAULT_KEY",
        "",
        "The key that encrypts the vault: a file with 32 random bytes, hex encoded (`kernable keys new vault` writes one). Without it the kernel has no vault: commands and events with vault values are refused. `--dev` uses `vault.key` if none is given.",
    ),
    (
        "--vault-store",
        "KERNABLE_VAULT_STORE",
        "sqlite:vault.db",
        "Where the vault keeps personal and secret values, encrypted with `--vault-key`. `sqlite:<path>` or a bare path: SQLite. `memory:`: every value is lost on a restart, for tests. Not `file:`. Ignored without `--vault-key`.",
    ),
    (
        "--tls-cert",
        "KERNABLE_KERNEL_TLS_CERT",
        "",
        "Mutual TLS on a TCP `--listen`: the kernel's certificate (PEM). `kernable certs new` writes development certificates. Give all three TLS flags or none; TLS with a `unix:` address is an error.",
    ),
    (
        "--tls-key",
        "KERNABLE_KERNEL_TLS_KEY",
        "",
        "Mutual TLS: the private key of `--tls-cert` (PEM).",
    ),
    (
        "--tls-client-ca",
        "KERNABLE_KERNEL_TLS_CLIENT_CA",
        "",
        "Mutual TLS: the CA certificate (PEM) that must have signed every client's certificate. A client without one is refused.",
    ),
    (
        "--metrics-listen",
        "KERNABLE_METRICS_LISTEN",
        "",
        "A TCP `host:port` for plain HTTP with no login: Prometheus metrics at `GET /metrics`, `GET /health` and `GET /ready`. Bind it to a private address. Off when empty. If the address cannot be bound, the kernel stops.",
    ),
    (
        "--admin-socket",
        "KERNABLE_ADMIN_SOCKET",
        "",
        "A Unix socket for the operator's tools on this machine (mode 0600: the kernel's user, and root): `GET /metrics`, `/health`, `/ready` and `/ready?deployment=<d>` as on `--metrics-listen`, the UI's `GET /api/deployments`, `/api/deployments/warm-up` and `/api/deployments/since`, and `POST /api/deployments/drop?deployment=<d>[&force=true]` (`kernable deployments drop --socket`). Reached through the file system, so a tool need not enter the network namespace the kernel runs in. A stale socket at the path is replaced, a missing directory made (0700). Off when empty; `kernable node` sets it to `admin.sock` in the project directory. A standby serves it once it takes over. If it cannot be bound, the kernel stops.",
    ),
    (
        "--ui-listen",
        "KERNABLE_UI_LISTEN",
        "",
        "A TCP `host:port` for the kernel's UI in a browser: an overview of health and activity, the business processes and their cases, operations (services, lag, parked events, rejections, standby, store), and the domains with their commands, events and readers, the log and every aggregate's history. It has no login, so the address must be loopback, or the kernel does not start. If the address is taken, the kernel runs on without the UI and logs `the UI is off`. Off when empty; `--dev` sets `127.0.0.1:7234` unless it is given.",
    ),
    (
        "--replication",
        "KERNABLE_REPLICATION",
        "off",
        "`sync`: a standby (`--follow`) holds every write before anyone is told of it. Commits, revocations, grants and memberships wait until the standby has stored them; without a standby in sync they wait, and `/ready` answers 503. Without a witness, until a standby has caught up once (a note beside the store keeps it), the kernel serves alone and the standby catches up meanwhile; only a standby told it caught up may be promoted. `off`: no standby.",
    ),
    (
        "--backup",
        "KERNABLE_BACKUP",
        "off",
        "`on`: a backup follower (`kernable backup`, a key granted `backup`) may follow the kernel and write its records to object storage; the kernel keeps its record log for it, as for a standby. `off`: no backup follower.",
    ),
    (
        "--partition",
        "KERNABLE_PARTITION",
        "",
        "This kernel's partition, a name in the partition map: it serves the tenants the map assigns to it and refuses the others with `FAILED_PRECONDITION` `wrong_partition`, and none before it has a map. Without it, the kernel serves every tenant.",
    ),
    (
        "--control",
        "KERNABLE_CONTROL",
        "",
        "With `--partition`: the control partition's addresses, comma-separated (both kernels of a pair). This kernel takes the partition map, grants, revocations and memberships from there every second, with `--node-key` (granted `replicate`). Without it, this kernel is the control partition: maps are applied here (`kernable partitions apply`).",
    ),
    (
        "--witness",
        "KERNABLE_WITNESS",
        "",
        "Run as one kernel of a pair whose roles a witness (`kernable witness`) decides, at this address: the primary of the newest term serves with `--replication sync`, the other follows it, and takes its place on its own once the primary's lease runs out. Needs `--node-key` and `--primary-kernel-key`; not with `--follow`. The connection uses `KERNABLE_TLS_*`, as the SDKs do.",
    ),
    (
        "--node-key",
        "KERNABLE_NODE_KEY",
        "node.key",
        "With `--witness` or `--control`: this kernel's key towards the witness, the other kernel and the control partition, whose grant (`<name>.grant` next to it) allows `replicate`.",
    ),
    (
        "--advertise",
        "KERNABLE_ADVERTISE",
        "",
        "With `--witness`: the address the other kernel reaches this one at. Default: `--listen`.",
    ),
    (
        "--witness-generation",
        "KERNABLE_WITNESS_GENERATION",
        "",
        "With `--witness`, and needed then: a file of the witness's generation (`kernable witness generation`), signed by a root key; a new pair starts with generation 1. The kernel keeps the newest generation it has seen (from this file, its witness or the other kernel) in `<node-key>.generation` and ignores every witness of an older one, or one answering with another witness's generation, so a replaced witness that comes back cannot make a second primary. It reads the file again before every question to the witness: raise it on both kernels before they turn to a new witness.",
    ),
    (
        "--standby-timeout-ms",
        "KERNABLE_STANDBY_TIMEOUT_MS",
        "5000",
        "With `--witness`: how long the primary's writes wait for a standby that is away or behind before the primary tells the witness and commits alone. A standby that missed writes is never made the primary.",
    ),
    (
        "--follow",
        "KERNABLE_FOLLOW",
        "",
        "Run as the standby of the primary at this address: apply its records, serve nothing else (every request is refused with `UNAVAILABLE`, `not_primary`) until `kernable promote` makes this kernel the primary. The connection uses `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, as the SDKs do.",
    ),
    (
        "--follow-key",
        "KERNABLE_FOLLOW_KEY",
        "standby.key",
        "With `--follow`: the standby's key, whose grant (`<name>.grant` next to it) allows `replicate`.",
    ),
    (
        "--primary-kernel-key",
        "KERNABLE_PRIMARY_KERNEL_KEY",
        "",
        "With `--follow`: the primary's kernel key, as a key ID or a `.pub` file; the standby checks the primary's checkpoints with it. Repeat the flag for a key the primary had before; the variable takes them comma-separated.",
    ),
    (
        "--lease-ttl-ms",
        "KERNABLE_LEASE_TTL_MS",
        "10000",
        "How long a worker holds a command, in milliseconds. If its result has not arrived by then, the command goes to another worker and a late result is ignored. A worker that disconnects gives its commands back at once. Set it above your slowest command handler.",
    ),
    (
        "--checkpoint-interval-ms",
        "KERNABLE_CHECKPOINT_INTERVAL_MS",
        "5000",
        "How often, in milliseconds, the kernel signs a checkpoint of the hash chain with `--kernel-key`, if new events arrived. Events after the last checkpoint are not yet covered by a signature (`kernable verify` counts them).",
    ),
    (
        "--submit-timeout-ms",
        "KERNABLE_SUBMIT_TIMEOUT_MS",
        "30000",
        "How long a submit waits for its command to be committed or rejected, in milliseconds. After that the client gets `DEADLINE_EXCEEDED`, but the command stays queued and still runs: resubmit it with the same command ID to get the result.",
    ),
    (
        "--settle-timeout-ms",
        "KERNABLE_SETTLE_TIMEOUT_MS",
        "5000",
        "How long a settling submit (the default) waits after the commit for the listeners that count to process the events, in milliseconds. After that the client gets the committed outcome marked as not settled (`NotSettled` in the SDKs).",
    ),
    (
        "--deployment-retention-days",
        "KERNABLE_DEPLOYMENT_RETENTION_DAYS",
        "0",
        "Days after which the kernel drops what it keeps for a deployment (code version) beside the log: its read models' registrations with their positions, and its snapshots. Only for a deployment that no membership names, with no read model connected and none seen for that long; logged as a warning. 0: never; drop one by hand with `kernable deployments drop`. The events stay either way.",
    ),
    (
        "--deployment-switch-lag",
        "KERNABLE_DEPLOYMENT_SWITCH_LAG",
        "100",
        "How many events a deployment's read models may be behind the newest one when a membership switches a service to that deployment. Further behind, or with a listed worker or listener not connected, the switch is refused as `not_caught_up` (unless forced: `kernable apply --force`), so a new deployment warms up beside the serving one before it takes over. A deployment that wrote events before (a rollback to kept read models) takes over at once.",
    ),
    (
        "--replay",
        "KERNABLE_REPLAY",
        "off",
        "`on`: the kernel runs a check before switching on a masked copy ([Check a deployment before switching](../how-to/deployments#check-a-deployment-before-switching)): it takes only the commands `kernable replay` sends, each run as if received at its original time, and fires no scheduled command and no process step. Refused on a store that is not a masked copy. `off`: a kernel as usual.",
    ),
    (
        "--query-timeout-ms",
        "KERNABLE_QUERY_TIMEOUT_MS",
        "5000",
        "How long a query waits, in milliseconds, for a read model that answers it to catch up and answer. After that the client gets `DEADLINE_EXCEEDED`.",
    ),
    (
        "--middleware-timeout-ms",
        "KERNABLE_MIDDLEWARE_TIMEOUT_MS",
        "5000",
        "How long a request waits for each middleware in its chain to catch up and give its verdict, in milliseconds. After that the request is refused with `DEADLINE_EXCEEDED`: middleware fails closed.",
    ),
    (
        "--drain-timeout-ms",
        "KERNABLE_DRAIN_TIMEOUT_MS",
        "10000",
        "On SIGTERM or SIGINT, the kernel refuses new submits and queries and waits this long, in milliseconds, for running ones to finish before it exits. Commands still unfinished run again after a restart.",
    ),
    (
        "--take-over-ms",
        "KERNABLE_TAKE_OVER_MS",
        "0",
        "Start while another kernel still holds the stores, and take them over (a kernel update on the same machine): wait at most this long, in milliseconds, for it to let them go, then serve on the same address. The other kernel lets go when it gets SIGTERM, once its running requests finished. `kernable node` does this when its kernel's binary changes. 0: refuse stores another kernel holds.",
    ),
    (
        "--durability",
        "KERNABLE_DURABILITY",
        "full",
        "When a write to the event store counts as done. `full`: on the disk (fsync, a full flush on macOS) before a client, listener or worker hears of it, so a committed command survives a power loss or an OS crash. Commits waiting at the same time share one flush (group commit), so throughput holds up under concurrent load, while a single client waits about one flush per command. `normal`: handed to the OS, which survives a crash of the kernel but not of the machine, and is faster; `--dev` uses it unless given. The vault and the data service always write to the disk.",
    ),
    (
        "--max-connections",
        "KERNABLE_MAX_CONNECTIONS",
        "1024",
        "Connections to the gRPC address at once. A connection beyond it is closed right away and counted in `kernable_refused_total{reason=\"connections\"}`. Keep it below the process's limit of open files.",
    ),
    (
        "--max-streams-per-connection",
        "KERNABLE_MAX_STREAMS_PER_CONNECTION",
        "256",
        "Requests and streams at once on one connection (HTTP/2 concurrent streams). Further ones wait in the client until one ends.",
    ),
    (
        "--max-message-bytes",
        "KERNABLE_MAX_MESSAGE_BYTES",
        "4194304",
        "The largest gRPC message the kernel receives or sends, in bytes; the SDKs' default is the same 4 MiB. The data service takes files up to 16 MiB regardless. A command whose snapshot and the events after it do not fit one message reaches the worker in parts; only a command with its snapshot, or a single event, that does not fit one message is rejected with `too_large`. Listener batches are cut to fit.",
    ),
    (
        "--handshake-timeout-ms",
        "KERNABLE_HANDSHAKE_TIMEOUT_MS",
        "10000",
        "How long a worker or listener stream may take to send its first message and to answer the challenge, in milliseconds. A stream that is slower ends with `DEADLINE_EXCEEDED`.",
    ),
    (
        "--max-queued-commands",
        "KERNABLE_MAX_QUEUED_COMMANDS",
        "100000",
        "Commands in the queue at once, waiting or running. A new command beyond it is refused with `RESOURCE_EXHAUSTED` and not stored; resubmitting one that is already stored is always answered.",
    ),
    (
        "--max-queued-per-aggregate",
        "KERNABLE_MAX_QUEUED_PER_AGGREGATE",
        "1000",
        "Commands in the queue at once for one aggregate. A new command beyond it is refused with `RESOURCE_EXHAUSTED`, so one busy aggregate cannot fill the queue.",
    ),
    (
        "--max-queued-bytes",
        "KERNABLE_MAX_QUEUED_BYTES",
        "536870912",
        "Bytes of commands in the queue at once (the queue lives in the kernel's memory). A new command beyond it is refused with `RESOURCE_EXHAUSTED`.",
    ),
    (
        "--schedule-max-days",
        "KERNABLE_SCHEDULE_MAX_DAYS",
        "366",
        "How far ahead, in days, a scheduled command (`Command.deliver_at`) may be due. One due later is refused with `INVALID_ARGUMENT`. Scheduled commands wait in the kernel's memory until due and count towards `--max-queued-commands`, `--max-queued-per-aggregate` and `--max-queued-bytes`.",
    ),
    (
        "--max-requests-per-key",
        "KERNABLE_MAX_REQUESTS_PER_KEY",
        "1000",
        "Submits, queries, file and cache requests of one key in progress at once. One beyond it is refused with `RESOURCE_EXHAUSTED`, so one service cannot take all the kernel's capacity.",
    ),
    (
        "--max-subscriptions-per-key",
        "KERNABLE_MAX_SUBSCRIPTIONS_PER_KEY",
        "16",
        "Open event subscriptions (`EventService.Subscribe`) of one key at once. One beyond it is refused with `RESOURCE_EXHAUSTED`.",
    ),
    (
        "--max-listeners-per-key",
        "KERNABLE_MAX_LISTENERS_PER_KEY",
        "64",
        "Listeners one key may register (connected or not) or run live at once. A new name beyond it is refused with `RESOURCE_EXHAUSTED`; registered ones always reconnect. A registered listener counts for settling and readiness, so this bounds how many a key can make everyone wait for.",
    ),
    (
        "--max-cache-bytes",
        "KERNABLE_MAX_CACHE_BYTES",
        "268435456",
        "Bytes the cache service keeps (values and keys). When a value would exceed it, the oldest values are dropped first, as a cache may; a single value larger than the limit is refused with `RESOURCE_EXHAUSTED`.",
    ),
    (
        "--key-command",
        "KERNABLE_KEY_COMMAND",
        "",
        "A program and its arguments, separated by spaces, that unwraps a key: the kernel runs it with a key file's path as the last argument and reads the key from its output (64 hex characters, as in a key file), for the kernel key, the vault key and the node keys, at start and on SIGHUP. The key files then hold wrapped keys (age, a KMS, systemd-creds). Empty: the files hold the keys.",
    ),
    (
        "--max-store-bytes",
        "KERNABLE_MAX_STORE_BYTES",
        "0",
        "Bytes the event store, the vault and the files may take together, measured every 10 s (0: no limit). At the limit, new commands and file puts are refused with `RESOURCE_EXHAUSTED` `quota_storage`; queries, listeners, file deletes and the commands of `--quota-exempt` go on. At 80 % the kernel logs a warning.",
    ),
    (
        "--quota-exempt",
        "KERNABLE_QUOTA_EXEMPT",
        "",
        "Commands `--max-store-bytes` never refuses, comma-separated (`identity.ErasePerson`, `identity.*`): list the commands that erase personal data, so an erasure request is always possible.",
    ),
    (
        "--max-requests-per-second-per-key",
        "KERNABLE_MAX_REQUESTS_PER_SECOND_PER_KEY",
        "0",
        "Submits, queries, file and cache requests one key may start per second, with a burst of one second's worth (0: no limit). More are refused with `RESOURCE_EXHAUSTED` `rate`, to retry a moment later.",
    ),
    (
        "--max-data-bytes",
        "KERNABLE_MAX_DATA_BYTES",
        "16777216",
        "Bytes of one file of the data service, at most 134217728 (128 MiB). A larger put is refused with `INVALID_ARGUMENT`. A file travels whole in one message, so the kernel and the SDK hold it in memory while it is put or read.",
    ),
];

/// The largest `--max-data-bytes`: a file travels whole in one message, also to a standby.
const MAX_DATA_BYTES: usize = 128 << 20;

/// Kernel configuration, from the flags of `kernable start` (see [`FLAGS`]).
#[derive(Debug, Clone)]
pub struct Config {
    /// Root key IDs: they sign grants and revocations.
    pub roots: Vec<String>,
    pub store: String,
    pub listen: String,
    pub kernel_key: PathBuf,
    pub lease_ttl_ms: i64,
    pub checkpoint_interval_ms: u64,
    pub submit_timeout_ms: u64,
    pub settle_timeout_ms: u64,
    /// `--deployment-retention-days`: 0 keeps every deployment's read models and snapshots.
    pub deployment_retention_days: u64,
    /// `--deployment-switch-lag`: events a deployment's read models may be behind when a
    /// membership switches to it.
    pub deployment_switch_lag: u64,
    /// `--replay on`: a check before switching on a masked copy (`kernable replay`).
    pub replay: bool,
    pub query_timeout_ms: u64,
    pub middleware_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    /// `--take-over-ms`: how long to wait for another kernel to let go of the stores; 0: not.
    pub take_over_ms: u64,
    pub tls: Option<Tls>,
    pub snapshots: String,
    /// Files of the data service: `file:<directory>` or `memory:`.
    pub data: String,
    /// Values of the cache service: `memory:`.
    pub cache: String,
    pub vault: Option<VaultConfig>,
    pub metrics_listen: Option<String>,
    /// `--admin-socket`: the local socket's path.
    pub admin_socket: Option<String>,
    pub ui_listen: Option<String>,
    /// `--durability` of the event store.
    pub durability: Durability,
    pub limits: Limits,
    pub replication: Replication,
    /// `--key-command`: the program and arguments that unwrap a key file; empty: none.
    pub key_command: Vec<String>,
    /// `--partition`: one partition of several.
    pub partition: Option<PartitionConfig>,
}

/// A kernel of one partition.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionConfig {
    /// Its name in the partition map.
    pub name: String,
    /// The control partition's addresses; `None`: this kernel is the control partition.
    pub control: Option<String>,
    /// The key it takes the trust records with; its grant is `<name>.grant` next to it.
    pub node_key: PathBuf,
}

/// Replication (design/replication.md).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Replication {
    /// `--replication sync`: writes count once the standby holds them.
    pub sync: bool,
    /// `--follow`: run as a standby.
    pub follow: Option<Follow>,
    /// `--witness`: one kernel of a pair, its role decided by the witness.
    pub pair: Option<Pair>,
    /// `--backup on`: a backup follower may follow the kernel.
    pub backup: bool,
}

/// A kernel of a pair.
#[derive(Debug, Clone, PartialEq)]
pub struct Pair {
    pub witness: String,
    /// This kernel's key; its grant is `<name>.grant` next to it.
    pub node_key: PathBuf,
    /// Where the other kernel reaches this one.
    pub advertise: String,
    /// `--witness-generation`: the witness's generation, read again while it runs.
    pub witness_generation: PathBuf,
    pub standby_timeout_ms: u64,
    /// The other kernel's kernel keys (and this one's): checkpoints of either are accepted.
    pub kernel_keys: Vec<String>,
}

/// What a standby follows.
#[derive(Debug, Clone, PartialEq)]
pub struct Follow {
    /// The primary's address.
    pub primary: String,
    /// The standby's signing key; its grant is `<name>.grant` next to it.
    pub key: PathBuf,
    /// Key IDs of the primary's kernel keys.
    pub primary_kernel_keys: Vec<String>,
}

/// What the kernel takes on at most. Beyond a limit, a request is refused with
/// `RESOURCE_EXHAUSTED` (a connection is closed) and counted in `kernable_refused_total`.
#[derive(Debug, Clone, PartialEq)]
pub struct Limits {
    pub connections: usize,
    pub streams_per_connection: u32,
    pub message_bytes: usize,
    pub handshake_timeout_ms: u64,
    pub queued_commands: usize,
    pub queued_per_aggregate: usize,
    pub queued_bytes: usize,
    pub requests_per_key: usize,
    pub subscriptions_per_key: usize,
    pub listeners_per_key: usize,
    pub cache_bytes: usize,
    pub data_bytes: usize,
    /// `--max-store-bytes`; 0: no limit.
    pub store_bytes: u64,
    /// `--quota-exempt`: command patterns the storage quota never refuses.
    pub quota_exempt: Vec<String>,
    /// `--max-requests-per-second-per-key`; 0: no limit.
    pub requests_per_second_per_key: u64,
    /// `--schedule-max-days`: how far ahead a scheduled command may be due.
    pub schedule_max_days: u64,
}

/// The defaults of the `--max-*` flags and `--handshake-timeout-ms`.
impl Default for Limits {
    fn default() -> Self {
        Limits {
            connections: 1024,
            streams_per_connection: 256,
            message_bytes: 4 << 20,
            handshake_timeout_ms: 10_000,
            queued_commands: 100_000,
            queued_per_aggregate: 1000,
            queued_bytes: 512 << 20,
            requests_per_key: 1000,
            subscriptions_per_key: 16,
            listeners_per_key: 64,
            cache_bytes: 256 << 20,
            data_bytes: 16 << 20,
            store_bytes: 0,
            quota_exempt: Vec::new(),
            requests_per_second_per_key: 0,
            schedule_max_days: 366,
        }
    }
}

/// The vault: where values live and the key that encrypts them.
#[derive(Debug, Clone)]
pub struct VaultConfig {
    pub store: String,
    pub key: PathBuf,
}

/// Mutual TLS for TCP; every client needs a certificate of `client_ca`.
#[derive(Debug, Clone)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: PathBuf,
}

type Error = Box<dyn std::error::Error + Send + Sync>;

impl Config {
    /// Reads the flags of `kernable start` from `args`, falling back to the environment
    /// variables in `env` and then to the defaults.
    pub fn from_args(args: &[&str], env: impl Fn(&str) -> Option<String>) -> Result<Self, Error> {
        let mut given: HashMap<&str, Vec<String>> = HashMap::new();
        let mut rest = args.iter();
        while let Some(&arg) = rest.next() {
            let (flag, value) = match arg.split_once('=') {
                Some((flag, value)) => (flag, Some(value.to_owned())),
                None => (arg, None),
            };
            let Some((flag, ..)) = FLAGS.iter().find(|(f, ..)| *f == flag) else {
                return Err(format!("unknown flag {arg}").into());
            };
            let value = match value {
                Some(v) => v,
                None => rest
                    .next()
                    .ok_or(format!("{flag} needs a value"))?
                    .to_string(),
            };
            given.entry(flag).or_default().push(value);
        }
        // An empty value counts as not given, at every step: `--store=` or `KERNABLE_STORE=""`
        // falls back to the default, and a flag without one stays off.
        let get = |flag: &str| -> Option<String> {
            let (_, var, default, _) = FLAGS.iter().find(|(f, ..)| *f == flag).expect("known flag");
            let set = |v: &String| !v.is_empty();
            given
                .get(flag)
                .and_then(|v| v.last().cloned())
                .filter(set)
                .or_else(|| env(var).filter(set))
                .or_else(|| (!default.is_empty()).then(|| default.to_string()))
        };
        let number = |flag: &str| -> Result<u64, Error> {
            let value = get(flag).expect("has a default");
            value
                .parse()
                .map_err(|_| format!("{flag}: {value} is not a number").into())
        };
        // A limit of 0 would refuse everything: a mistake, not a setting.
        let positive = |flag: &str| -> Result<u64, Error> {
            match number(flag)? {
                0 => Err(format!("{flag} must be at least 1").into()),
                n => Ok(n),
            }
        };
        let roots = match given.get("--root") {
            Some(roots) => roots.clone(),
            None => env("KERNABLE_ROOT")
                .map(|r| {
                    r.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        };
        if roots.is_empty() {
            return Err(
                "no root key: pass --root <key ID or .pub file> (or set KERNABLE_ROOT)".into(),
            );
        }
        let roots = roots
            .iter()
            .map(|r| root_key_id(r))
            .collect::<Result<_, _>>()?;
        let kernel_keys = || -> Result<Vec<String>, Error> {
            let keys: Vec<String> = match given.get("--primary-kernel-key") {
                Some(keys) => keys.clone(),
                None => env("KERNABLE_PRIMARY_KERNEL_KEY")
                    .map(|k| k.split(',').map(|s| s.trim().to_owned()).collect())
                    .unwrap_or_default(),
            };
            let keys: Vec<String> = keys.into_iter().filter(|k| !k.is_empty()).collect();
            if keys.is_empty() {
                return Err("--follow and --witness need --primary-kernel-key: the other kernel's kernel key, to check its checkpoints".into());
            }
            keys.iter()
                .map(|k| key_id("--primary-kernel-key", k))
                .collect()
        };
        let follow = match get("--follow") {
            None => None,
            Some(primary) => Some(Follow {
                primary,
                key: get("--follow-key").expect("has a default").into(),
                primary_kernel_keys: kernel_keys()?,
            }),
        };
        let pair = match get("--witness") {
            None if get("--advertise").is_some() => {
                return Err("--advertise is for a kernel of a pair: give --witness too".into());
            }
            None if get("--witness-generation").is_some() => {
                return Err(
                    "--witness-generation is for a kernel of a pair: give --witness too".into(),
                );
            }
            None => None,
            Some(_) if follow.is_some() => {
                return Err("--witness decides the roles: drop --follow".into());
            }
            Some(_) if get("--witness-generation").is_none() => {
                return Err("a kernel of a pair needs --witness-generation: the witness's generation, signed by the root key (kernable witness generation <root.key> 1 <witness address> for a new pair)".into());
            }
            Some(witness) => Some(Pair {
                witness,
                node_key: get("--node-key").expect("has a default").into(),
                advertise: get("--advertise")
                    .unwrap_or_else(|| get("--listen").expect("has a default")),
                witness_generation: get("--witness-generation").expect("checked above").into(),
                standby_timeout_ms: positive("--standby-timeout-ms")?,
                kernel_keys: kernel_keys()?,
            }),
        };
        let replication = Replication {
            sync: match get("--replication").expect("has a default").as_str() {
                "sync" => true,
                "off" => pair.is_some(),
                other => {
                    return Err(format!("--replication: {other} is neither sync nor off").into());
                }
            },
            follow,
            pair,
            backup: match get("--backup").expect("has a default").as_str() {
                "on" => true,
                "off" => false,
                other => return Err(format!("--backup: {other} is neither on nor off").into()),
            },
        };
        let tls = match (get("--tls-cert"), get("--tls-key"), get("--tls-client-ca")) {
            (Some(cert), Some(key), Some(client_ca)) => Some(Tls {
                cert: cert.into(),
                key: key.into(),
                client_ca: client_ca.into(),
            }),
            (None, None, None) => None,
            _ => return Err("TLS needs --tls-cert, --tls-key and --tls-client-ca".into()),
        };
        Ok(Config {
            roots,
            store: get("--store").expect("has a default"),
            listen: get("--listen").expect("has a default"),
            kernel_key: get("--kernel-key").expect("has a default").into(),
            lease_ttl_ms: {
                let ms = number("--lease-ttl-ms")?;
                i64::try_from(ms).map_err(|_| format!("--lease-ttl-ms: {ms} is too large"))?
            },
            checkpoint_interval_ms: number("--checkpoint-interval-ms")?,
            submit_timeout_ms: number("--submit-timeout-ms")?,
            settle_timeout_ms: number("--settle-timeout-ms")?,
            deployment_retention_days: number("--deployment-retention-days")?,
            deployment_switch_lag: number("--deployment-switch-lag")?,
            replay: match get("--replay").expect("has a default").as_str() {
                "on" => true,
                "off" => false,
                other => return Err(format!("--replay: {other} is neither on nor off").into()),
            },
            query_timeout_ms: number("--query-timeout-ms")?,
            middleware_timeout_ms: number("--middleware-timeout-ms")?,
            drain_timeout_ms: number("--drain-timeout-ms")?,
            take_over_ms: number("--take-over-ms")?,
            tls,
            snapshots: get("--snapshots")
                .unwrap_or_else(|| snapshots_next_to(&get("--store").expect("has a default"))),
            data: get("--data-store").expect("has a default"),
            cache: get("--cache-store").expect("has a default"),
            vault: get("--vault-key").map(|key| VaultConfig {
                store: get("--vault-store").expect("has a default"),
                key: key.into(),
            }),
            metrics_listen: get("--metrics-listen"),
            admin_socket: get("--admin-socket"),
            ui_listen: get("--ui-listen"),
            durability: match get("--durability").expect("has a default").as_str() {
                "full" => Durability::Full,
                "normal" => Durability::Normal,
                other => {
                    return Err(format!("--durability: {other} is neither full nor normal").into());
                }
            },
            limits: Limits {
                connections: positive("--max-connections")? as usize,
                streams_per_connection: u32::try_from(positive("--max-streams-per-connection")?)
                    .map_err(|_| "--max-streams-per-connection is too large")?,
                message_bytes: positive("--max-message-bytes")? as usize,
                handshake_timeout_ms: positive("--handshake-timeout-ms")?,
                queued_commands: positive("--max-queued-commands")? as usize,
                queued_per_aggregate: positive("--max-queued-per-aggregate")? as usize,
                queued_bytes: positive("--max-queued-bytes")? as usize,
                requests_per_key: positive("--max-requests-per-key")? as usize,
                subscriptions_per_key: positive("--max-subscriptions-per-key")? as usize,
                listeners_per_key: positive("--max-listeners-per-key")? as usize,
                cache_bytes: positive("--max-cache-bytes")? as usize,
                data_bytes: match positive("--max-data-bytes")? {
                    n if n > MAX_DATA_BYTES as u64 => {
                        return Err(format!(
                            "--max-data-bytes: at most {MAX_DATA_BYTES} (128 MiB)"
                        )
                        .into());
                    }
                    n => n as usize,
                },
                store_bytes: number("--max-store-bytes")?,
                quota_exempt: {
                    let list: Vec<String> = get("--quota-exempt")
                        .unwrap_or_default()
                        .split(',')
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .map(str::to_owned)
                        .collect();
                    if let Some(bad) = list.iter().find(|t| !valid_pattern(t) || *t == "*") {
                        return Err(format!(
                            "--quota-exempt: {bad} is not a command type (domain.Command) or domain (domain.*)"
                        )
                        .into());
                    }
                    list
                },
                requests_per_second_per_key: number("--max-requests-per-second-per-key")?,
                schedule_max_days: positive("--schedule-max-days")?,
            },
            replication,
            key_command: get("--key-command")
                .unwrap_or_default()
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
            partition: match get("--partition") {
                None if get("--control").is_some() => {
                    return Err("--control is for a partition: give --partition too".into());
                }
                None => None,
                Some(name) if !valid_domain(&name) => {
                    return Err(format!("--partition: {name} is not a valid name").into());
                }
                Some(name) => Some(PartitionConfig {
                    name,
                    control: match get("--control") {
                        Some(c) if !c.split(',').any(|a| !a.trim().is_empty()) => {
                            return Err("--control: give the control partition's addresses, \
                                        comma-separated"
                                .into());
                        }
                        c => c,
                    },
                    node_key: get("--node-key").expect("has a default").into(),
                }),
            },
        })
    }
}

/// A root key given as a key ID or as the path of a `.pub` file.
fn root_key_id(root: &str) -> Result<String, Error> {
    key_id("--root", root)
}

/// A key given to `flag` as a key ID or as the path of a `.pub` file.
pub fn key_id(flag: &str, key: &str) -> Result<String, Error> {
    let id = if kernable_core::signing::verifying_key_from_hex(key).is_ok() {
        key.to_owned()
    } else {
        std::fs::read_to_string(key)
            .map_err(|e| format!("{flag} {key}: neither a key ID nor a readable .pub file ({e})"))?
            .trim()
            .to_owned()
    };
    kernable_core::signing::verifying_key_from_hex(&id)
        .map_err(|_| format!("{flag} {key}: not an Ed25519 key ID"))?;
    Ok(id)
}

/// Milliseconds since the epoch of a UTC time `YYYY-MM-DDTHH:MM:SSZ` (seconds optional).
pub fn utc_ms(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let num = |p: &str| p.parse::<i64>().ok();
    let d: Vec<i64> = date.split('-').map(num).collect::<Option<_>>()?;
    let t: Vec<i64> = time.split(':').map(num).collect::<Option<_>>()?;
    // `HH:MM` or `HH:MM:SS`, nothing after the seconds.
    let ([y, m, day], [h, min, sec @ ..]) = (d.as_slice(), t.as_slice()) else {
        return None;
    };
    if sec.len() > 1 {
        return None;
    }
    let sec = sec.first().copied().unwrap_or(0);
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days_in_month = match m {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    // Four-digit years only: anything else is a typo, and would overflow below.
    let valid = (0..=9999).contains(y)
        && (1..=12).contains(m)
        && (1..=days_in_month).contains(day)
        && (0..24).contains(h)
        && (0..60).contains(min)
        && (0..60).contains(&sec);
    if !valid {
        return None;
    }
    // Days from 1970-01-01 to the civil date (Howard Hinnant's algorithm).
    let y = if *m <= 2 { y - 1 } else { *y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + h) * 60 + min) * 60_000 + sec * 1000)
}

/// A UTC time `YYYY-MM-DDTHH:MM:SSZ` for milliseconds since the epoch; the inverse of [`utc_ms`].
pub fn utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rest) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // The civil date of a day count from 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let (h, min, sec) = (rest / 3600, rest % 3600 / 60, rest % 60);
    format!("{y:04}-{m:02}-{day:02}T{h:02}:{min:02}:{sec:02}Z")
}

/// Whether `key_id` may use `name` according to its granted patterns; see [`covers`].
pub fn allowed(rules: &HashMap<String, Vec<String>>, key_id: &str, name: &str) -> bool {
    rules
        .get(key_id)
        .is_some_and(|patterns| patterns.iter().any(|p| covers(p, name)))
}

/// Whether `pattern` covers `name`. Patterns are `*` (everything), `domain.*` (one domain) or a
/// qualified type `domain.Type`; `name` is a qualified type or a `domain.*` pattern itself.
pub fn covers(pattern: &str, name: &str) -> bool {
    pattern == "*"
        || pattern == name
        || pattern
            .strip_suffix(".*")
            .is_some_and(|domain| name.split_once('.').is_some_and(|(d, _)| d == domain))
}

/// The event type patterns a key may read, narrowed to `requested` (empty: all it may read).
/// `Ok(None)` means every event; `Err` that it asked for more than it may read.
pub fn readable(
    rules: &HashMap<String, Vec<String>>,
    key_id: &str,
    requested: &[String],
) -> Result<Option<Vec<String>>, &'static str> {
    let own = rules.get(key_id).ok_or("key may not read events")?;
    if requested.is_empty() {
        return Ok((!own.iter().any(|p| p == "*")).then(|| own.clone()));
    }
    if requested.iter().all(|t| allowed(rules, key_id, t)) {
        Ok(Some(requested.to_vec()))
    } else {
        Err("key may not read these event types")
    }
}

/// `domain.Type`, the name used in registrations, subscriptions and grants.
pub fn qualified(domain: &str, type_name: &str) -> String {
    format!("{domain}.{type_name}")
}

/// A domain is an identifier that starts with a letter: `orders` by convention, also `Orders`,
/// `order-mgmt`, `billing_v2`. Names are case-sensitive.
pub fn valid_domain(domain: &str) -> bool {
    domain.starts_with(|c: char| c.is_ascii_alphabetic())
        && domain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A listener or membership name: 1 to 128 ASCII letters, digits, `-`, `_` and `.`. Names
/// appear in metrics labels, logs and the UI, so nothing else.
pub fn valid_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// A type name within a domain: not empty and without dots.
pub fn valid_type(type_name: &str) -> bool {
    !type_name.is_empty() && !type_name.contains('.')
}

/// A qualified `domain.Type`.
pub fn valid_qualified(name: &str) -> bool {
    name.split_once('.')
        .is_some_and(|(d, t)| valid_domain(d) && valid_type(t))
}

/// A type pattern: `*`, a domain (`orders.*`) or a qualified type.
pub fn valid_pattern(pattern: &str) -> bool {
    pattern == "*"
        || pattern.strip_suffix(".*").is_some_and(valid_domain)
        || valid_qualified(pattern)
}

/// The default snapshot store: SQLite next to the event store `store`, or memory for a memory
/// store.
pub fn snapshots_next_to(store: &str) -> String {
    let path = match kernable_store::backend(store) {
        ("memory", _) => return "memory:".into(),
        (_, path) => std::path::Path::new(path),
    };
    let stem = path
        .file_stem()
        .map_or("kernable".into(), |s| s.to_string_lossy());
    format!(
        "sqlite:{}",
        path.with_file_name(format!("{stem}-snapshots.db"))
            .display()
    )
}

/// Keys `--key-command` unwrapped, by file: the command runs once per file until SIGHUP
/// ([`forget_unwrapped_keys`]), not on every use.
type Unwrapped = HashMap<PathBuf, zeroize::Zeroizing<[u8; 32]>>;
static UNWRAPPED: std::sync::Mutex<Option<Unwrapped>> = std::sync::Mutex::new(None);

/// How long `--key-command` may take for one key.
const KEY_COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Drops the keys `--key-command` unwrapped, so the next use runs it again (on SIGHUP).
pub fn forget_unwrapped_keys() {
    *UNWRAPPED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

impl Config {
    /// The 32 secret bytes of the key file at `path`: read from it, or unwrapped by
    /// `--key-command`, once until [`forget_unwrapped_keys`].
    pub fn read_secret(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<zeroize::Zeroizing<[u8; 32]>> {
        let Some((program, args)) = self.key_command.split_first() else {
            return kernable_core::signing::read_secret(path);
        };
        let cached = UNWRAPPED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|keys| keys.get(path).cloned());
        if let Some(key) = cached {
            return Ok(key);
        }
        let mut child = std::process::Command::new(program)
            .args(args)
            .arg(path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| std::io::Error::other(format!("--key-command {program}: {e}")))?;
        let timed_out = || {
            std::io::Error::other(format!(
                "--key-command {program} did not answer within {KEY_COMMAND_TIMEOUT:?} for {}",
                path.display()
            ))
        };
        // Its output read beside it, so a full pipe cannot block it.
        let read = |pipe: Option<Box<dyn std::io::Read + Send>>| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                if let Some(mut pipe) = pipe {
                    let _ = pipe.read_to_end(&mut bytes);
                }
                let _ = tx.send(bytes);
            });
            rx
        };
        let stdout = read(child.stdout.take().map(|p| Box::new(p) as _));
        let stderr = read(child.stderr.take().map(|p| Box::new(p) as _));
        let deadline = std::time::Instant::now() + KEY_COMMAND_TIMEOUT;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                // Through the handle: it kills only the command, never a process that took
                // its pid after it ended.
                let _ = child.kill();
                let _ = child.wait();
                return Err(timed_out());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let left = || deadline.saturating_duration_since(std::time::Instant::now());
        let out = std::process::Output {
            status,
            stdout: stdout.recv_timeout(left()).map_err(|_| timed_out())?,
            stderr: stderr.recv_timeout(left()).unwrap_or_default(),
        };
        let text = zeroize::Zeroizing::new(String::from_utf8_lossy(&out.stdout).into_owned());
        if !out.status.success() {
            let why = String::from_utf8_lossy(&out.stderr);
            return Err(std::io::Error::other(format!(
                "--key-command {program} failed for {} ({}): {}",
                path.display(),
                out.status,
                why.trim().chars().take(200).collect::<String>()
            )));
        }
        let mut key = zeroize::Zeroizing::new([0u8; 32]);
        hex::decode_to_slice(text.trim(), key.as_mut()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "--key-command {program} printed no 32-byte key in hex for {}",
                    path.display()
                ),
            )
        })?;
        UNWRAPPED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(HashMap::new)
            .insert(path.to_owned(), key.clone());
        Ok(key)
    }

    /// The signing key in the key file at `path`; see [`Config::read_secret`].
    pub fn read_signing_key(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<ed25519_dalek::SigningKey> {
        Ok(ed25519_dalek::SigningKey::from_bytes(
            &*self.read_secret(path)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_key_command_unwraps_key_files_and_its_failures_are_named() {
        let dir = std::env::temp_dir().join(format!("kn-keycmd-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("kernel.key.wrapped");
        std::fs::write(&file, format!("{}\n", "ab".repeat(32))).unwrap();
        let root = kernable_core::signing::key_id(
            &ed25519_dalek::SigningKey::from_bytes(&[1; 32]).verifying_key(),
        );
        let with = |command: &str| Config {
            key_command: command.split_whitespace().map(str::to_owned).collect(),
            ..Config::from_args(&["--root", &root], |_| None).unwrap()
        };
        // `cat` stands in for an unwrapping tool: it prints the key.
        assert_eq!(*with("cat").read_secret(&file).unwrap(), [0xab; 32]);
        assert_eq!(
            *with("").read_secret(&file).unwrap(),
            [0xab; 32],
            "no command: the file itself"
        );
        forget_unwrapped_keys();
        let failed = with("false").read_secret(&file).unwrap_err().to_string();
        assert!(failed.contains("--key-command false failed"), "{failed}");
        let garbage = with("echo not-a-key")
            .read_secret(&file)
            .unwrap_err()
            .to_string();
        assert!(garbage.contains("no 32-byte key"), "{garbage}");
        let missing = with("no-such-program-kn")
            .read_secret(&file)
            .unwrap_err()
            .to_string();
        assert!(missing.contains("no-such-program-kn"), "{missing}");
        // Unwrapped once, it is kept until SIGHUP: the command does not run on every use.
        let counted = dir.join("count.sh");
        let script = format!("#!/bin/sh\necho x >> {}/runs; cat \"$1\"\n", dir.display());
        std::fs::write(&counted, script).unwrap();
        std::fs::set_permissions(
            &counted,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();
        let counting = with(&counted.display().to_string());
        let runs = || {
            std::fs::read_to_string(dir.join("runs"))
                .unwrap()
                .lines()
                .count()
        };
        for _ in 0..3 {
            counting.read_secret(&file).unwrap();
        }
        assert_eq!(runs(), 1);
        forget_unwrapped_keys();
        counting.read_secret(&file).unwrap();
        assert_eq!(runs(), 2);
        forget_unwrapped_keys();
        let _ = std::fs::remove_dir_all(&dir);
    }

    use super::*;

    #[test]
    fn patterns_cover_types_and_domains() {
        assert!(covers("*", "orders.PlaceOrder"));
        assert!(covers("orders.*", "orders.PlaceOrder"));
        assert!(covers("orders.*", "orders.*"));
        assert!(covers("orders.PlaceOrder", "orders.PlaceOrder"));
        assert!(!covers("orders.*", "ordersx.PlaceOrder"));
        assert!(!covers("orders.PlaceOrder", "orders.*"));
        assert!(!covers("orders.PlaceOrder", "identity.PlaceOrder"));
        assert!(valid_qualified("order-mgmt.PlaceOrder"));
        assert!(!valid_qualified("PlaceOrder"));
        assert!(valid_qualified("Orders.PlaceOrder"));
        assert!(!valid_qualified("1orders.PlaceOrder") && !valid_qualified("ord ers.PlaceOrder"));
        assert!(!valid_qualified("orders.Place.Order"));
        assert!(valid_pattern("*") && valid_pattern("orders.*") && valid_pattern("orders.Get"));
        assert!(!valid_pattern("orders") && valid_pattern("Orders.*") && !valid_pattern("o.*.*"));
    }

    #[test]
    fn snapshots_default_to_sqlite_next_to_the_event_store() {
        for (store, snapshots) in [
            ("sqlite:kernable.db", "sqlite:kernable-snapshots.db"),
            ("data/k.db", "sqlite:data/k-snapshots.db"),
            (
                "file:/var/lib/kernable/k.log",
                "sqlite:/var/lib/kernable/k-snapshots.db",
            ),
            ("memory:", "memory:"),
        ] {
            assert_eq!(snapshots_next_to(store), snapshots, "{store}");
        }
        let root =
            kernable_core::signing::key_id(&kernable_core::signing::generate_key().verifying_key());
        let config =
            Config::from_args(&["--root", &root, "--store", "file:k.log"], |_| None).unwrap();
        assert_eq!(config.snapshots, "sqlite:k-snapshots.db");
        let config =
            Config::from_args(&["--root", &root, "--snapshots", "memory:"], |_| None).unwrap();
        assert_eq!(config.snapshots, "memory:");
    }

    #[test]
    fn an_empty_value_falls_back_to_the_default() {
        let root =
            kernable_core::signing::key_id(&kernable_core::signing::generate_key().verifying_key());
        let config = Config::from_args(&["--root", &root, "--store=", "--metrics-listen="], |_| {
            None
        })
        .unwrap();
        assert_eq!(config.store, "sqlite:kernable.db");
        assert_eq!(
            config.metrics_listen, None,
            "a flag without a default stays off"
        );
        let env = |var: &str| (var == "KERNABLE_LEASE_TTL_MS").then(String::new);
        let config = Config::from_args(&["--root", &root], env).unwrap();
        assert_eq!(config.lease_ttl_ms, 10_000);
    }

    #[test]
    fn a_lease_ttl_beyond_i64_is_refused_not_wrapped() {
        let root =
            kernable_core::signing::key_id(&kernable_core::signing::generate_key().verifying_key());
        let max = i64::MAX.to_string();
        let config = Config::from_args(&["--root", &root, "--lease-ttl-ms", &max], |_| None);
        assert_eq!(config.unwrap().lease_ttl_ms, i64::MAX);
        let beyond = (i64::MAX as u64 + 1).to_string();
        let refused = Config::from_args(&["--root", &root, "--lease-ttl-ms", &beyond], |_| None);
        assert!(refused.unwrap_err().to_string().contains("--lease-ttl-ms"));
    }

    #[test]
    fn limits_default_to_the_flag_defaults_and_are_positive() {
        let root =
            kernable_core::signing::key_id(&kernable_core::signing::generate_key().verifying_key());
        let config = Config::from_args(&["--root", &root], |_| None).unwrap();
        assert_eq!(config.limits, Limits::default());
        let config =
            Config::from_args(&["--root", &root, "--max-queued-commands", "5"], |_| None).unwrap();
        assert_eq!(config.limits.queued_commands, 5);
        let zero = Config::from_args(&["--root", &root, "--max-connections=0"], |_| None);
        assert!(zero.is_err());
        assert_eq!(config.durability, Durability::Full, "durable by default");
        let normal = Config::from_args(&["--root", &root, "--durability", "normal"], |_| None);
        assert_eq!(normal.unwrap().durability, Durability::Normal);
        assert!(Config::from_args(&["--root", &root, "--durability", "fast"], |_| None).is_err());
        // Flags that would do nothing are refused, not ignored.
        let refused = |args: &[&str]| {
            let mut all = vec!["--root", root.as_str()];
            all.extend(args);
            Config::from_args(&all, |_| None).unwrap_err().to_string()
        };
        assert!(refused(&["--control", "127.0.0.1:1"]).contains("give --partition too"));
        assert!(refused(&["--advertise", "127.0.0.1:1"]).contains("give --witness too"));
        assert!(refused(&["--witness-generation", "w.generation"]).contains("give --witness too"));
        assert!(
            refused(&["--witness", "127.0.0.1:7300"]).contains("needs --witness-generation"),
            "a pair without a witness generation"
        );
        assert!(refused(&["--partition", "p2", "--control", " , "]).contains("--control"));
        let tls = Config::from_args(&["--root", &root], |v| {
            (v == "KERNABLE_KERNEL_TLS_CLIENT_CA").then(|| "ca.pem".to_owned())
        });
        assert!(tls.unwrap_err().to_string().contains("TLS needs"));
    }

    #[test]
    fn names_are_plain() {
        assert!(valid_name("carts") && valid_name("shop.carts-v2_1"));
        assert!(!valid_name("") && !valid_name("a b") && !valid_name("a\"b"));
        assert!(!valid_name("x\n") && !valid_name(&"a".repeat(129)));
    }

    #[test]
    fn utc_times_parse_to_milliseconds() {
        assert_eq!(utc_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(utc_ms("2026-10-27T09:00:00Z"), Some(1_793_091_600_000));
        assert_eq!(utc_ms("2026-10-27T09:00Z"), Some(1_793_091_600_000));
        assert_eq!(utc_ms("2000-02-29T12:30:15Z"), Some(951_827_415_000));
        assert!(utc_ms("2028-02-29T00:00:00Z").is_some());
        assert!(utc_ms("2026-12-31T00:00:00Z").is_some());
        for t in [
            "1970-01-01T00:00:00Z",
            "2000-02-29T12:30:15Z",
            "2026-10-27T09:00:00Z",
            "1969-12-31T23:59:59Z",
        ] {
            assert_eq!(utc(utc_ms(t).unwrap()), t);
        }
        for bad in [
            "2026-10-27 09:00:00",
            "2026-13-01T00:00:00Z",
            "2026-02-31T00:00:00Z",
            "2026-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2026-10-27T09:00:00+01:00",
            "2026-10-27T10:00:00:99Z",
            "2026-10-27T10Z",
            "x",
        ] {
            assert_eq!(utc_ms(bad), None, "{bad}");
        }
    }

    #[test]
    fn every_day_from_1960_to_2400_is_written_as_the_calendar_has_it() {
        let leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let length = |y: i64, m: i64| match m {
            2 if leap(y) => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        let (mut y, mut m, mut d) = (1960, 1, 1);
        let first = utc_ms("1960-01-01T00:00:00Z").unwrap();
        for day in 0..160_707 {
            let ms = first + day * 86_400_000 + 45_296_000; // 12:34:56
            assert_eq!(utc(ms), format!("{y:04}-{m:02}-{d:02}T12:34:56Z"));
            assert_eq!(utc_ms(&utc(ms)), Some(ms));
            d += 1;
            if d > length(y, m) {
                (m, d) = (m + 1, 1);
            }
            if m > 12 {
                (y, m) = (y + 1, 1);
            }
        }
        assert_eq!((y, m, d), (2400, 1, 1));
    }
}
