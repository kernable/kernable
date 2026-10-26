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
/// [clients]                            # client key ID -> command types it may submit
/// "8b84…" = ["*"]
///
/// [workers]                            # worker key ID -> command types it may handle
/// "1f0a…" = ["PlaceOrder", "AddItem"]
///
/// [subscribers]                        # subscriber key ID -> event types it may read
/// "8b84…" = ["*"]
///
/// [listeners]                          # listener key ID -> event types it may read
/// "1f0a…" = ["OrderPlaced"]
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
    #[serde(default)]
    pub clients: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub workers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub subscribers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub listeners: HashMap<String, Vec<String>>,
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

/// Whether `key_id` may use `type_name` according to an allowlist (`"*"` allows all).
pub fn allowed(rules: &HashMap<String, Vec<String>>, key_id: &str, type_name: &str) -> bool {
    rules
        .get(key_id)
        .is_some_and(|types| types.iter().any(|t| t == "*" || t == type_name))
}
