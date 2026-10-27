use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// Kernel configuration, usually `kernable.toml`:
///
/// ```toml
/// store = "sqlite:kernable.db"         # or "file:kernable.log"; a bare path means SQLite
/// listen = "unix:/tmp/kernable.sock"   # or "127.0.0.1:7233"
/// kernel_key = "kernel.key"
///
/// # Allowlists name qualified types (`orders.PlaceOrder`), whole domains (`orders.*`) or `*`.
/// [clients]                            # client key ID -> commands it may submit
/// "8b84…" = ["*"]
///
/// [workers]                            # worker key ID -> commands it may handle; a worker
/// "1f0a…" = ["orders.*"]               # can only write events of its commands' domain
///
/// [subscribers]                        # subscriber key ID -> events it may read
/// "8b84…" = ["*"]
///
/// [listeners]                          # listener key ID -> events it may read
/// "1f0a…" = ["orders.*", "identity.AccountRegistered"]
///
/// [queriers]                           # client key ID -> queries it may ask
/// "8b84…" = ["identity.GetAccount"]
///
/// [query_handlers]                     # listener key ID -> queries it may answer
/// "1f0a…" = ["identity.*"]
///
/// [middlewares]                        # listener key ID -> commands and queries it may check
/// "1f0a…" = ["*"]
///
/// [privileged]                         # client key ID -> privileged request context switches
/// "8b84…" = ["bypass_authorization"]
///
/// [tls]                                # optional, TCP only: mutual TLS
/// cert = "kernel.tls.crt"             # see `kernable certs new`
/// key = "kernel.tls.key"
/// client_ca = "ca.tls.crt"             # every client must present a certificate signed by it
/// ```
///
/// `metrics_listen = "127.0.0.1:9233"` serves Prometheus metrics at `/metrics`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The event store: `sqlite:<path>` (or a bare path), `file:<path>` or `memory:`.
    pub store: String,
    /// Where the kernel listens: `unix:<path>` or `host:port` (TCP, with `[tls]` for mutual TLS).
    pub listen: String,
    /// The kernel's own signing key (`kernable keys new kernel`): it signs checkpoints.
    pub kernel_key: PathBuf,
    /// How long a worker may run a command before it goes to another worker.
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: i64,
    /// How often the kernel signs a checkpoint of the hash chain.
    #[serde(default = "default_checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u64,
    /// How long a submit waits for its command to complete before it answers with an error
    /// (the command still runs; resubmit it to get the result).
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// How long a settling submit waits for listeners after the commit.
    #[serde(default = "default_settle_timeout_ms")]
    pub settle_timeout_ms: u64,
    /// How long a query waits for a read model to catch up and answer.
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    /// How long a request waits for one middleware to catch up and give its verdict.
    #[serde(default = "default_middleware_timeout_ms")]
    pub middleware_timeout_ms: u64,
    /// On SIGTERM or SIGINT: how long running submits and queries may still finish.
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,
    /// Client key ID -> the commands it may submit.
    #[serde(default)]
    pub clients: HashMap<String, Vec<String>>,
    /// Worker key ID -> the commands it may run; it writes events of their domains only.
    #[serde(default)]
    pub workers: HashMap<String, Vec<String>>,
    /// Subscriber key ID -> the events it may read with a subscription.
    #[serde(default)]
    pub subscribers: HashMap<String, Vec<String>>,
    /// Listener key ID -> the events it may read as a listener (read model, reactor).
    #[serde(default)]
    pub listeners: HashMap<String, Vec<String>>,
    /// Client key ID -> the queries it may ask.
    #[serde(default)]
    pub queriers: HashMap<String, Vec<String>>,
    /// Listener key ID -> the queries it may answer.
    #[serde(default)]
    pub query_handlers: HashMap<String, Vec<String>>,
    /// Listener key ID -> the commands and queries it may check as a middleware.
    #[serde(default)]
    pub middlewares: HashMap<String, Vec<String>>,
    /// Client key ID -> the privileged request context switches it may set
    /// (`bypass_authorization`).
    #[serde(default)]
    pub privileged: HashMap<String, Vec<String>>,
    /// Mutual TLS for TCP; every client needs a certificate of `client_ca`.
    pub tls: Option<Tls>,
    /// Old key ID -> new key ID: a listener registered with the old key may connect with the
    /// new one and keeps its position, order and middleware registration.
    #[serde(default)]
    pub successors: HashMap<String, String>,
    /// Key ID -> since when it must not be trusted, in UTC (`2026-10-27T09:00:00Z`). The kernel
    /// stores the revocation, refuses the key from then on, also where an allowlist still names
    /// it, and `kernable verify` flags what it signed from that time on. Never undone.
    #[serde(default)]
    pub revoked: HashMap<String, String>,
    /// Where snapshots live: `sqlite:<path>` or `memory:` (default; rebuilt after a restart).
    #[serde(default = "default_snapshots")]
    pub snapshots: String,
    /// Personal and secret values; commands and events that refer to the vault are refused
    /// without it.
    pub vault: Option<VaultConfig>,
    /// Address for Prometheus metrics (`GET /metrics`), `GET /health` and `GET /ready`; off
    /// when absent.
    pub metrics_listen: Option<String>,
}

/// `[vault]`: where values live, the key that encrypts them, and who may read and erase them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// `sqlite:<path>` (or a bare path) or `memory:`.
    pub store: String,
    /// A file with 32 random bytes, hex encoded (`kernable keys new vault` writes one).
    pub key: PathBuf,
    /// Listener and middleware keys that get the values of these event and command types.
    /// Workers always get the values of the commands they run.
    #[serde(default)]
    pub readers: HashMap<String, Vec<String>>,
    /// Worker keys whose events of these types may erase a subject.
    #[serde(default)]
    pub erasers: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// The kernel's certificate (PEM; `kernable certs new` writes development ones).
    pub cert: PathBuf,
    /// The kernel certificate's private key (PEM).
    pub key: PathBuf,
    /// The CA that signs every client's certificate (PEM).
    pub client_ca: PathBuf,
}

fn default_lease_ttl_ms() -> i64 {
    10_000
}

fn default_checkpoint_interval_ms() -> u64 {
    5_000
}

fn default_submit_timeout_ms() -> u64 {
    30_000
}

fn default_settle_timeout_ms() -> u64 {
    5_000
}

fn default_query_timeout_ms() -> u64 {
    5_000
}

fn default_middleware_timeout_ms() -> u64 {
    5_000
}

fn default_snapshots() -> String {
    "memory:".into()
}

fn default_drain_timeout_ms() -> u64 {
    10_000
}

impl Config {
    /// Loads a config file; relative paths in it resolve against the file's directory.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut config: Config = toml::from_str(&fs::read_to_string(path)?)?;
        let dir = path.parent().unwrap_or(Path::new("."));
        let (backend, store) = kernable_store::backend(&config.store);
        if backend != "memory" {
            config.store = format!("{backend}:{}", dir.join(store).display());
        }
        config.kernel_key = dir.join(&config.kernel_key);
        let in_dir = |url: &str| match kernable_store::backend(url) {
            ("memory", _) => url.to_owned(),
            (backend, path) => format!("{backend}:{}", dir.join(path).display()),
        };
        config.snapshots = in_dir(&config.snapshots);
        if let Some(vault) = &mut config.vault {
            vault.store = in_dir(&vault.store);
            vault.key = dir.join(&vault.key);
        }
        if let Some(tls) = &mut config.tls {
            for path in [&mut tls.cert, &mut tls.key, &mut tls.client_ca] {
                *path = dir.join(&*path);
            }
        }
        Ok(config)
    }
}

impl Config {
    /// The revocations as key ID -> ms since epoch.
    pub fn revocations(&self) -> Result<HashMap<String, i64>, String> {
        self.revoked
            .iter()
            .map(|(key, since)| {
                let ms = utc_ms(since).ok_or_else(|| {
                    format!(
                        "revoked {key}: \"{since}\" is not a UTC time like 2026-10-27T09:00:00Z"
                    )
                })?;
                Ok((key.clone(), ms))
            })
            .collect()
    }

    /// Removes `keys` from every allowlist, so nothing grants them anything.
    pub fn without(&mut self, keys: &HashMap<String, i64>) {
        let tables = [
            &mut self.clients,
            &mut self.workers,
            &mut self.subscribers,
            &mut self.listeners,
            &mut self.queriers,
            &mut self.query_handlers,
            &mut self.middlewares,
            &mut self.privileged,
        ];
        for table in tables {
            table.retain(|key, _| !keys.contains_key(key));
        }
        if let Some(vault) = &mut self.vault {
            vault.readers.retain(|key, _| !keys.contains_key(key));
            vault.erasers.retain(|key, _| !keys.contains_key(key));
        }
    }
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

/// Whether `key_id` may use `name` according to an allowlist of patterns; see [`covers`].
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

/// `domain.Type`, the name used in registrations, subscriptions and allowlists.
pub fn qualified(domain: &str, type_name: &str) -> String {
    format!("{domain}.{type_name}")
}

/// A domain is a lowercase identifier: `orders`, `order-mgmt`, `billing_v2`.
pub fn valid_domain(domain: &str) -> bool {
    domain.starts_with(|c: char| c.is_ascii_lowercase())
        && domain
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
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
        assert!(!valid_qualified("Orders.PlaceOrder"));
        assert!(!valid_qualified("orders.Place.Order"));
        assert!(valid_pattern("*") && valid_pattern("orders.*") && valid_pattern("orders.Get"));
        assert!(!valid_pattern("orders") && !valid_pattern("Orders.*") && !valid_pattern("o.*.*"));
    }

    #[test]
    fn utc_times_parse_to_milliseconds() {
        assert_eq!(utc_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(utc_ms("2026-10-27T09:00:00Z"), Some(1_793_091_600_000));
        assert_eq!(utc_ms("2026-10-27T09:00Z"), Some(1_793_091_600_000));
        assert_eq!(utc_ms("2000-02-29T12:30:15Z"), Some(951_827_415_000));
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
