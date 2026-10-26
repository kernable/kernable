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
/// ```
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
    #[serde(default)]
    pub clients: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub workers: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub subscribers: HashMap<String, Vec<String>>,
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
        Ok(config)
    }
}

/// Whether `key_id` may use `type_name` according to an allowlist (`"*"` allows all).
pub fn allowed(rules: &HashMap<String, Vec<String>>, key_id: &str, type_name: &str) -> bool {
    rules
        .get(key_id)
        .is_some_and(|types| types.iter().any(|t| t == "*" || t == type_name))
}
