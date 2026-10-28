//! The cache service's values: under (tenant, domain, key), each until it expires, is deleted or
//! the kernel restarts. Not durable and not part of the log; for what processes may lose.

use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

/// Tenant, domain, key.
type Key = (String, String, String);

/// A value and when it expires, if it does.
type Values = HashMap<Key, (Vec<u8>, Option<Instant>)>;

/// Expired values are dropped when read, and all of them after this many sets.
const SWEEP_EVERY: u64 = 1024;

#[derive(Default)]
pub struct Cache {
    values: Mutex<Values>,
    sets: Mutex<u64>,
}

impl Cache {
    /// The cache a `--cache-store` URL names; only `memory:` so far.
    pub fn open(url: &str) -> Result<Cache, String> {
        match url {
            "memory:" => Ok(Cache::default()),
            _ => Err(format!("cache store {url}: use memory:")),
        }
    }

    fn values(&self) -> MutexGuard<'_, Values> {
        self.values.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get(&self, key: Key) -> Option<Vec<u8>> {
        let mut values = self.values();
        match values.get(&key) {
            Some((_, Some(until))) if *until <= Instant::now() => {
                values.remove(&key);
                None
            }
            other => other.map(|(value, _)| value.clone()),
        }
    }

    /// Sets the value; `ttl_ms` 0 keeps it until it is deleted.
    pub fn set(&self, key: Key, value: Vec<u8>, ttl_ms: u64) {
        let until = (ttl_ms > 0).then(|| Instant::now() + Duration::from_millis(ttl_ms));
        let mut values = self.values();
        values.insert(key, (value, until));
        let mut sets = self.sets.lock().unwrap_or_else(PoisonError::into_inner);
        *sets += 1;
        if sets.is_multiple_of(SWEEP_EVERY) {
            let now = Instant::now();
            values.retain(|_, (_, until)| until.is_none_or(|u| u > now));
        }
    }

    /// Deletes the value; whether there was a live one.
    pub fn delete(&self, key: Key) -> bool {
        let removed = self.values().remove(&key);
        removed.is_some_and(|(_, until)| until.is_none_or(|u| u > Instant::now()))
    }
}
