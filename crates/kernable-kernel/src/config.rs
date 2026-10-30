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
        "The gRPC address for every service and client: `host:port`, or `unix:<path>` on Unix. Plain TCP is accepted only on a loopback address (`127.0.0.1`, `::1`, `localhost`); any other address needs mutual TLS (`--tls-cert`, `--tls-key`, `--tls-client-ca`), or the kernel does not start.",
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
        "memory:",
        "The snapshot store, a cache of aggregate states that workers rebuild from the events. `sqlite:<path>` or a bare path: SQLite, kept across restarts. `memory:`: lost on a restart, so the first command per aggregate reads all its events again. Not `file:`.",
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
        "KERNABLE_TLS_CLIENT_CA",
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
        "--ui-listen",
        "KERNABLE_UI_LISTEN",
        "",
        "A TCP `host:port` for the kernel's UI in a browser: the map of services, aggregates and listeners, the live connections, and every aggregate's history. It has no login, so the address must be loopback, or the kernel does not start. If the address is taken, the kernel runs on without the UI and logs `the UI is off`. Off when empty; `--dev` sets `127.0.0.1:7234` unless it is given.",
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
        "The largest gRPC message the kernel receives or sends, in bytes; the SDKs' default is the same 4 MiB. The data service takes files up to 16 MiB regardless. A command whose `Execute` (the command, its snapshot and the events after it) would be larger is rejected with `too_large`: snapshot such aggregates more often, or raise the limit in the kernel and every SDK. Listener batches are cut to fit.",
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
];

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
    pub query_timeout_ms: u64,
    pub middleware_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    pub tls: Option<Tls>,
    pub snapshots: String,
    /// Files of the data service: `file:<directory>` or `memory:`.
    pub data: String,
    /// Values of the cache service: `memory:`.
    pub cache: String,
    pub vault: Option<VaultConfig>,
    pub metrics_listen: Option<String>,
    pub ui_listen: Option<String>,
    /// `--durability` of the event store.
    pub durability: Durability,
    pub limits: Limits,
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
            lease_ttl_ms: number("--lease-ttl-ms")? as i64,
            checkpoint_interval_ms: number("--checkpoint-interval-ms")?,
            submit_timeout_ms: number("--submit-timeout-ms")?,
            settle_timeout_ms: number("--settle-timeout-ms")?,
            query_timeout_ms: number("--query-timeout-ms")?,
            middleware_timeout_ms: number("--middleware-timeout-ms")?,
            drain_timeout_ms: number("--drain-timeout-ms")?,
            tls,
            snapshots: get("--snapshots").expect("has a default"),
            data: get("--data-store").expect("has a default"),
            cache: get("--cache-store").expect("has a default"),
            vault: get("--vault-key").map(|key| VaultConfig {
                store: get("--vault-store").expect("has a default"),
                key: key.into(),
            }),
            metrics_listen: get("--metrics-listen"),
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
            },
        })
    }
}

/// A root key given as a key ID or as the path of a `.pub` file.
fn root_key_id(root: &str) -> Result<String, Error> {
    let id = if kernable_core::signing::verifying_key_from_hex(root).is_ok() {
        root.to_owned()
    } else {
        std::fs::read_to_string(root)
            .map_err(|e| format!("--root {root}: neither a key ID nor a readable .pub file ({e})"))?
            .trim()
            .to_owned()
    };
    kernable_core::signing::verifying_key_from_hex(&id)
        .map_err(|_| format!("--root {root}: not an Ed25519 key ID"))?;
    Ok(id)
}

/// Milliseconds since the epoch of a UTC time `YYYY-MM-DDTHH:MM:SSZ` (seconds optional).
pub fn utc_ms(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let num = |p: &str| p.parse::<i64>().ok();
    let d: Vec<i64> = date.split('-').map(num).collect::<Option<_>>()?;
    let t: Vec<i64> = time.split(':').map(num).collect::<Option<_>>()?;
    let ([y, m, day], [h, min, sec @ ..]) = (d.as_slice(), t.as_slice()) else {
        return None;
    };
    let sec = sec.first().copied().unwrap_or(0);
    let valid = (1..=12).contains(m)
        && (1..=31).contains(day)
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

#[cfg(test)]
mod tests {
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
            "2026-10-27T09:00:00+01:00",
            "x",
        ] {
            assert_eq!(utc_ms(bad), None, "{bad}");
        }
    }
}
