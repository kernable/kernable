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
    /// Storage backend URL, see `kernable_store::open`.
    pub store: String,
    pub listen: String,
    pub kernel_key: PathBuf,
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: i64,
    #[serde(default = "default_checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u64,
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// How long a settling submit waits for listeners after the commit.
    #[serde(default = "default_settle_timeout_ms")]
    pub settle_timeout_ms: u64,
    /// How long a query waits for a read model to catch up and answer.
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    #[serde(default)]
    pub clients: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub workers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub subscribers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub listeners: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub queriers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub query_handlers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub privileged: HashMap<String, Vec<String>>,
    pub tls: Option<Tls>,
    /// Address for Prometheus metrics (`GET /metrics`); off when absent.
    pub metrics_listen: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
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
        if let Some(tls) = &mut config.tls {
            for path in [&mut tls.cert, &mut tls.key, &mut tls.client_ca] {
                *path = dir.join(&*path);
            }
        }
        Ok(config)
    }
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
    }
}
