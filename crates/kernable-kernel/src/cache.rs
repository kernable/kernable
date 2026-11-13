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
    /// Since the kernel started: reads that found a live value, and those that did not;
    /// values dropped because they expired, and the oldest dropped to make room.
    hits: u64,
    misses: u64,
    expired: u64,
    evicted: u64,
}

/// What the cache holds and did since the kernel started, for the UI.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub entries: u64,
    pub bytes: u64,
    pub max_bytes: u64,
    pub sets: u64,
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub evicted: u64,
    /// Per tenant and domain: entries and bytes.
    pub domains: Vec<(String, String, u64, u64)>,
}

/// A cache key as the UI lists it: never its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    pub tenant: String,
    pub domain: String,
    pub key: String,
    pub bytes: u64,
    /// Milliseconds until it expires; `None`: kept until deleted.
    pub expires_in_ms: Option<u64>,
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
            if self.remove(&key).is_some() {
                self.expired += 1;
            }
        }
    }
}

/// The cache a standby keeps from its primary's values while it follows, taken by the kernel
/// it becomes (the same process): a standby that takes over starts with the cache warm.
/// By the kernel's store: the standby and the kernel it becomes have the same.
static CARRIED: Mutex<Vec<(String, Cache)>> = Mutex::new(Vec::new());

/// The cache the standby of store `owner` fills, and the kernel it becomes takes.
pub(crate) fn carried(owner: &str, max_bytes: usize, f: impl FnOnce(&Cache)) {
    let mut carried = CARRIED.lock().unwrap_or_else(PoisonError::into_inner);
    let at = match carried.iter().position(|(o, _)| o == owner) {
        Some(at) => at,
        None => {
            carried.push((
                owner.to_owned(),
                Cache {
                    values: Mutex::default(),
                    max_bytes,
                },
            ));
            carried.len() - 1
        }
    };
    f(&carried[at].1);
}

pub struct Cache {
    values: Mutex<Values>,
    /// `--max-cache-bytes`.
    max_bytes: usize,
}

impl Cache {
    /// The cache a `--cache-store` URL names (only `memory:` so far), holding at most
    /// `max_bytes` of keys and values.
    /// `owner`, the kernel's store, finds the cache its standby filled in this process.
    pub fn open(url: &str, max_bytes: usize, owner: &str) -> Result<Cache, String> {
        let taken = {
            let mut carried = CARRIED.lock().unwrap_or_else(PoisonError::into_inner);
            carried
                .iter()
                .position(|(o, _)| o == owner)
                .map(|at| carried.remove(at).1)
        };
        match url {
            // A standby promoted in this process hands over what its primary had set.
            "memory:" => Ok(match taken {
                Some(mut carried) => {
                    carried.max_bytes = max_bytes;
                    carried
                }
                None => Cache {
                    values: Mutex::default(),
                    max_bytes,
                },
            }),
            _ => Err(format!("cache store {url}: use memory:")),
        }
    }

    /// Applies a value its primary set or deleted (a standby's copy, best-effort).
    pub(crate) fn apply(&self, v: &kernable_core::proto::CacheValue) {
        let key = (v.tenant_id.clone(), v.domain.clone(), v.key.clone());
        if v.deleted {
            self.delete(key);
        } else {
            self.set(key, v.value.clone(), v.ttl_ms);
        }
    }

    fn values(&self) -> MutexGuard<'_, Values> {
        self.values.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get(&self, key: Key) -> Option<Vec<u8>> {
        let mut values = self.values();
        let found = match values.entries.get(&key) {
            Some(Entry {
                until: Some(until), ..
            }) if *until <= Instant::now() => {
                values.remove(&key);
                values.expired += 1;
                None
            }
            other => other.map(|e| e.value.clone()),
        };
        if found.is_some() {
            values.hits += 1;
        } else {
            values.misses += 1;
        }
        found
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
                values.evicted += 1;
            }
        }
        let seq = values.sets;
        values.bytes += needed;
        values.order.insert(seq, key.clone());
        values.entries.insert(key, Entry { value, until, seq });
        true
    }

    /// The counts, and per tenant and domain what is held.
    pub fn stats(&self) -> CacheStats {
        let values = self.values();
        let mut domains: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
        for (k, e) in &values.entries {
            let d = domains.entry((k.0.clone(), k.1.clone())).or_default();
            d.0 += 1;
            d.1 += size(k, &e.value) as u64;
        }
        CacheStats {
            entries: values.entries.len() as u64,
            bytes: values.bytes as u64,
            max_bytes: self.max_bytes as u64,
            sets: values.sets,
            hits: values.hits,
            misses: values.misses,
            expired: values.expired,
            evicted: values.evicted,
            domains: domains
                .into_iter()
                .map(|((t, d), (n, b))| (t, d, n, b))
                .collect(),
        }
    }

    /// Up to `limit` live keys of `tenant` and `domain` (empty: any) that contain `text`,
    /// sorted; without their values.
    pub fn keys(&self, tenant: &str, domain: &str, text: &str, limit: usize) -> Vec<CacheKey> {
        let now = Instant::now();
        let values = self.values();
        let mut keys: Vec<CacheKey> = values
            .entries
            .iter()
            .filter(|(k, e)| {
                (tenant.is_empty() || k.0 == tenant)
                    && (domain.is_empty() || k.1 == domain)
                    && (text.is_empty() || k.2.contains(text))
                    && e.until.is_none_or(|u| u > now)
            })
            .map(|(k, e)| CacheKey {
                tenant: k.0.clone(),
                domain: k.1.clone(),
                key: k.2.clone(),
                bytes: e.value.len() as u64,
                expires_in_ms: e.until.map(|u| u.duration_since(now).as_millis() as u64),
            })
            .collect();
        keys.sort_by(|a, b| (&a.tenant, &a.domain, &a.key).cmp(&(&b.tenant, &b.domain, &b.key)));
        keys.truncate(limit);
        keys
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
        let cache = Cache::open("memory:", 21, "test").unwrap();
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
        let cache = Cache::open("memory:", 21, "test").unwrap();
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

    #[test]
    fn counts_hits_misses_and_lists_keys_without_values() {
        let cache = Cache::open("memory:", 1_000, "test").unwrap();
        cache.set(
            ("t".into(), "shop".into(), "rate:c1".into()),
            b"7".to_vec(),
            0,
        );
        cache.set(
            ("t".into(), "billing".into(), "x".into()),
            b"12".to_vec(),
            60_000,
        );
        assert!(
            cache
                .get(("t".into(), "shop".into(), "rate:c1".into()))
                .is_some()
        );
        assert!(
            cache
                .get(("t".into(), "shop".into(), "gone".into()))
                .is_none()
        );
        let stats = cache.stats();
        assert_eq!(
            (stats.entries, stats.hits, stats.misses, stats.sets),
            (2, 1, 1, 2)
        );
        assert_eq!(stats.domains.len(), 2);
        let keys = cache.keys("", "shop", "", 10);
        assert_eq!(keys.len(), 1);
        assert_eq!(
            (keys[0].key.as_str(), keys[0].bytes, keys[0].expires_in_ms),
            ("rate:c1", 1, None)
        );
        assert!(
            cache.keys("t", "billing", "", 10)[0]
                .expires_in_ms
                .is_some()
        );
    }
}
