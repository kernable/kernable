use std::{collections::HashMap, path::PathBuf};

/// The flags of `kernable start`: flag, environment variable, default, description. A flag
/// wins over its variable; relative paths resolve against the working directory.
pub const FLAGS: &[(&str, &str, &str, &str)] = &[
    (
        "--root",
        "KERNABLE_ROOT",
        "",
        "A root key: its key ID or its `.pub` file. Grants and revocations must be signed by a root key. Repeat the flag for several roots (the variable takes them comma-separated); at least one is needed.",
    ),
    (
        "--store",
        "KERNABLE_STORE",
        "sqlite:kernable.db",
        "The event store: `sqlite:<path>` (or a bare path), `file:<path>` or `memory:`.",
    ),
    (
        "--listen",
        "KERNABLE_LISTEN",
        "127.0.0.1:7233",
        "Where the kernel listens: `host:port`, or `unix:<path>` on Unix. Plain TCP only on a loopback address; any other address needs mutual TLS (`--tls-cert`, `--tls-key`, `--tls-client-ca`).",
    ),
    (
        "--kernel-key",
        "KERNABLE_KERNEL_KEY",
        "kernel.key",
        "The kernel's own signing key (`kernable keys new kernel`): it signs checkpoints and revocations. Read again on SIGHUP (Unix).",
    ),
    (
        "--snapshots",
        "KERNABLE_SNAPSHOTS",
        "memory:",
        "Where snapshots live: `sqlite:<path>` or `memory:` (rebuilt after a restart).",
    ),
    (
        "--data-store",
        "KERNABLE_DATA_STORE",
        "file:data",
        "Where files of the data service live: `file:<directory>` or `memory:` (gone after a restart). Created on the first file.",
    ),
    (
        "--cache-store",
        "KERNABLE_CACHE_STORE",
        "memory:",
        "Where the cache service keeps values: `memory:`, emptied by a restart.",
    ),
    (
        "--vault-key",
        "KERNABLE_VAULT_KEY",
        "",
        "A file with 32 random bytes, hex encoded (`kernable keys new vault` writes one). Without it the kernel has no vault, and commands and events that refer to it are refused.",
    ),
    (
        "--vault-store",
        "KERNABLE_VAULT_STORE",
        "sqlite:vault.db",
        "Where the vault keeps personal and secret values: `sqlite:<path>` or `memory:`.",
    ),
    (
        "--tls-cert",
        "KERNABLE_KERNEL_TLS_CERT",
        "",
        "Mutual TLS, TCP only: the kernel's certificate (PEM; `kernable certs new` writes development ones). Needs `--tls-key` and `--tls-client-ca` too.",
    ),
    (
        "--tls-key",
        "KERNABLE_KERNEL_TLS_KEY",
        "",
        "The kernel certificate's private key (PEM).",
    ),
    (
        "--tls-client-ca",
        "KERNABLE_TLS_CLIENT_CA",
        "",
        "The CA that signs every client's certificate (PEM).",
    ),
    (
        "--metrics-listen",
        "KERNABLE_METRICS_LISTEN",
        "",
        "Address for Prometheus metrics (`GET /metrics`), `GET /health` and `GET /ready`; off when empty.",
    ),
    (
        "--ui-listen",
        "KERNABLE_UI_LISTEN",
        "",
        "Address of the kernel's UI in a browser: the map of services, aggregates and listeners, the live connections and every aggregate's history. Loopback only, no login: for development. Off when empty; `--dev` turns it on at 127.0.0.1:7234.",
    ),
    (
        "--lease-ttl-ms",
        "KERNABLE_LEASE_TTL_MS",
        "10000",
        "How long a worker may run a command before it goes to another worker.",
    ),
    (
        "--checkpoint-interval-ms",
        "KERNABLE_CHECKPOINT_INTERVAL_MS",
        "5000",
        "How often the kernel signs a checkpoint of the hash chain.",
    ),
    (
        "--submit-timeout-ms",
        "KERNABLE_SUBMIT_TIMEOUT_MS",
        "30000",
        "How long a submit waits for its command to complete before it answers with an error (the command still runs; resubmit it to get the result).",
    ),
    (
        "--settle-timeout-ms",
        "KERNABLE_SETTLE_TIMEOUT_MS",
        "5000",
        "How long a settling submit waits for listeners after the commit.",
    ),
    (
        "--query-timeout-ms",
        "KERNABLE_QUERY_TIMEOUT_MS",
        "5000",
        "How long a query waits for a read model to catch up and answer.",
    ),
    (
        "--middleware-timeout-ms",
        "KERNABLE_MIDDLEWARE_TIMEOUT_MS",
        "5000",
        "How long a request waits for one middleware to catch up and give its verdict.",
    ),
    (
        "--drain-timeout-ms",
        "KERNABLE_DRAIN_TIMEOUT_MS",
        "10000",
        "On SIGTERM or SIGINT: how long running submits and queries may still finish.",
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
        let get = |flag: &str| -> Option<String> {
            let (_, var, default, _) = FLAGS.iter().find(|(f, ..)| *f == flag).expect("known flag");
            given
                .get(flag)
                .and_then(|v| v.last().cloned())
                .or_else(|| env(var))
                .or_else(|| (!default.is_empty()).then(|| default.to_string()))
                .filter(|v| !v.is_empty())
        };
        let number = |flag: &str| -> Result<u64, Error> {
            let value = get(flag).expect("has a default");
            value
                .parse()
                .map_err(|_| format!("{flag}: {value} is not a number").into())
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
