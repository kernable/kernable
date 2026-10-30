//! The cache service's values: under (tenant, domain, key), each until it expires, is deleted,
//! the kernel restarts, or it is the oldest when the cache is full. Not durable and not part of
//! the log; for what processes may lose.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

/// Tenant, domain, key.
type Key = (String, String, String);

/// A value, when it expires (if it does) and when it was set.
struct Entry {
    value: Vec<u8>,
    until: Option<Instant>,
    seq: u64,
}

/// Expired values are dropped when read, and all of them after this many sets.
const SWEEP_EVERY: u64 = 1024;

#[derive(Default)]
struct Values {
    entries: HashMap<Key, Entry>,
    /// Keys by when they were set, oldest first: what goes when the cache is full.
    order: BTreeMap<u64, Key>,
    /// Bytes of all keys and values.
    bytes: usize,
    sets: u64,
}

/// What a value takes up in the cache.
fn size(key: &Key, value: &[u8]) -> usize {
    key.0.len() + key.1.len() + key.2.len() + value.len()
}

impl Values {
    fn remove(&mut self, key: &Key) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        self.order.remove(&entry.seq);
        self.bytes -= size(key, &entry.value);
        Some(entry)
    }

    fn sweep(&mut self, now: Instant) {
        let expired: Vec<Key> = self
            .entries
            .iter()
            .filter(|(_, e)| e.until.is_some_and(|u| u <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            self.remove(&key);
        }
    }
}

pub struct Cache {
    values: Mutex<Values>,
    /// `--max-cache-bytes`.
    max_bytes: usize,
}

impl Cache {
    /// The cache a `--cache-store` URL names (only `memory:` so far), holding at most
    /// `max_bytes` of keys and values.
    pub fn open(url: &str, max_bytes: usize) -> Result<Cache, String> {
        match url {
            "memory:" => Ok(Cache {
                values: Mutex::default(),
                max_bytes,
            }),
            _ => Err(format!("cache store {url}: use memory:")),
        }
    }

    fn values(&self) -> MutexGuard<'_, Values> {
        self.values.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get(&self, key: Key) -> Option<Vec<u8>> {
        let mut values = self.values();
        match values.entries.get(&key) {
            Some(Entry {
                until: Some(until), ..
            }) if *until <= Instant::now() => {
                values.remove(&key);
                None
            }
            other => other.map(|e| e.value.clone()),
        }
    }

    /// Sets the value; `ttl_ms` 0 keeps it until it is deleted. The oldest values make room
    /// if needed. Returns false, keeping nothing, if the value alone exceeds the cache.
    pub fn set(&self, key: Key, value: Vec<u8>, ttl_ms: u64) -> bool {
        let needed = size(&key, &value);
        if needed > self.max_bytes {
            return false;
        }
        let until = (ttl_ms > 0).then(|| Instant::now() + Duration::from_millis(ttl_ms));
        let mut values = self.values();
        values.remove(&key);
        values.sets += 1;
        if values.sets.is_multiple_of(SWEEP_EVERY) || values.bytes + needed > self.max_bytes {
            values.sweep(Instant::now());
        }
        while values.bytes + needed > self.max_bytes {
            let Some((_, oldest)) = values.order.pop_first() else {
                break;
            };
            if let Some(entry) = values.entries.remove(&oldest) {
                values.bytes -= size(&oldest, &entry.value);
            }
        }
        let seq = values.sets;
        values.bytes += needed;
        values.order.insert(seq, key.clone());
        values.entries.insert(key, Entry { value, until, seq });
        true
    }

    /// Deletes the value; whether there was a live one.
    pub fn delete(&self, key: Key) -> bool {
        let removed = self.values().remove(&key);
        removed.is_some_and(|e| e.until.is_none_or(|u| u > Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(k: &str) -> Key {
        ("t".into(), "d".into(), k.into())
    }

    #[test]
    fn the_oldest_values_make_room() {
        // Each entry takes 2 + 1 + 4 = 7 bytes.
        let cache = Cache::open("memory:", 21).unwrap();
        for k in ["a", "b", "c"] {
            assert!(cache.set(key(k), b"1234".to_vec(), 0));
        }
        assert!(cache.set(key("d"), b"1234".to_vec(), 0));
        assert_eq!(cache.get(key("a")), None, "the oldest went");
        assert!(cache.get(key("b")).is_some() && cache.get(key("d")).is_some());

        // Setting a key again makes it the newest.
        assert!(cache.set(key("b"), b"1234".to_vec(), 0));
        assert!(cache.set(key("e"), b"1234".to_vec(), 0));
        assert_eq!(cache.get(key("c")), None);
        assert!(cache.get(key("b")).is_some());
        assert_eq!(cache.values().bytes, 21);

        assert!(
            !cache.set(key("f"), vec![0; 21], 0),
            "larger than the cache"
        );
        assert!(
            cache.get(key("b")).is_some(),
            "a refused value drops nothing"
        );
        assert!(cache.delete(key("b")));
        assert_eq!(cache.values().bytes, 14);
        assert!(
            cache.set(key("g"), vec![0; 18], 0),
            "exactly as large as the cache"
        );
        assert_eq!(cache.values().bytes, 21);
    }

    #[test]
    fn expired_values_make_room_before_live_ones() {
        let cache = Cache::open("memory:", 21).unwrap();
        assert!(cache.set(key("a"), b"1234".to_vec(), 1));
        assert!(cache.set(key("b"), b"1234".to_vec(), 0));
        assert!(cache.set(key("c"), b"1234".to_vec(), 3_600_000));
        std::thread::sleep(Duration::from_millis(5));
        assert!(cache.set(key("d"), b"1234".to_vec(), 0));
        assert_eq!(cache.get(key("a")), None);
        for k in ["b", "c", "d"] {
            assert!(cache.get(key(k)).is_some(), "{k} is live and stays");
        }
    }
}
