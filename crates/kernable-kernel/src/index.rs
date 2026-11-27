//! Unique keys and facts: what every aggregate holds. The store keeps it, filed by stream
//! and by key ([`kernable_store::Store::stream_index`], [`kernable_store::Store::key_holder`]);
//! the kernel reads it as commits need it and keeps the recently used part in memory.
//!
//! A worker sends with each result the full set of keys and facts its aggregate
//! holds after the events, and the facts other aggregates must hold. The kernel checks them
//! against this index while it commits, under the same lock, so two commands never take one key
//! and a requirement never sees a fact another commit is removing. Only a changed set is
//! stored, with the commit, so the store holds the latest set per stream.
//!
//! A key is filed under its identity (`kernable.v1.key`), which the kernel computes from the
//! command's tenant, domain and aggregate type. The identity of a personal key lives in the
//! vault only, as a value of its subject, filed by a hash keyed with the vault key: erasing the
//! subject frees the key, and the log keeps no trace of it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use kernable_core::{
    proto::{AggregateIndex, Command, Rejected, StoredKey, VaultValue},
    signing,
};
use kernable_store::{
    Store, StreamId,
    vault::{Owner, Vault},
};

use crate::{BoxError, config};

/// The most keys, facts and requirements one result may carry, and the longest name.
pub(crate) const MAX_KEYS: usize = 64;
pub(crate) const MAX_FACTS: usize = 64;
pub(crate) const MAX_REQUIREMENTS: usize = 64;
pub(crate) const MAX_NAME_BYTES: usize = 128;

/// A key an aggregate holds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub name: String,
    pub global: bool,
    pub id: [u8; 32],
    /// Personal: whose value it is; empty otherwise.
    pub subject: String,
}

/// What one aggregate holds.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Held {
    pub keys: BTreeSet<Key>,
    pub facts: BTreeSet<String>,
}

/// What one aggregate holds, as read for a commit: with the vault values of its personal keys.
#[derive(Debug, Clone, Default)]
pub(crate) struct Entry {
    pub held: Held,
    /// The vault value of each personal key held, by identity.
    pub refs: HashMap<[u8; 32], String>,
}

/// The index of unique keys and facts. The store holds it, and the vault the identities of
/// personal keys; the kernel keeps only what recently used aggregates hold, so startup and
/// memory do not grow with the store. Only the kernel writes the index, under the lock this
/// sits behind, so the cache is never stale.
pub(crate) struct Index {
    /// Aggregates whose index the kernel keeps in memory.
    capacity: usize,
    cache: HashMap<StreamId, (Entry, u64)>,
    /// Use order: the oldest first.
    order: BTreeMap<u64, StreamId>,
    next: u64,
}

impl Default for Index {
    fn default() -> Self {
        Index::with_capacity(100_000)
    }
}

impl Index {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Index {
            capacity: capacity.max(1),
            cache: HashMap::new(),
            order: BTreeMap::new(),
            next: 0,
        }
    }

    /// What `stream` holds now: personal keys whose vault values are gone were erased and are
    /// free.
    pub(crate) fn held(
        &mut self,
        store: &dyn Store,
        vault: Option<&Vault>,
        stream: &StreamId,
    ) -> Result<Entry, BoxError> {
        if let Some((entry, used)) = self.cache.get_mut(stream) {
            self.order.remove(used);
            *used = self.next;
            self.order.insert(self.next, stream.clone());
            self.next += 1;
            return Ok(entry.clone());
        }
        let entry = match store.stream_index(stream)? {
            Some(stored) => read(stream, stored, vault)?,
            None => Entry::default(),
        };
        self.keep(stream, entry.clone());
        Ok(entry)
    }

    /// Whether `stream` holds `fact`.
    pub(crate) fn holds(
        &mut self,
        store: &dyn Store,
        vault: Option<&Vault>,
        stream: &StreamId,
        fact: &str,
    ) -> Result<bool, BoxError> {
        Ok(self.held(store, vault, stream)?.held.facts.contains(fact))
    }

    /// The stream that holds the key `id`, if any. A personal key is held by the stream that
    /// filed it in the vault, as long as that stream still refers to the value: a value of a
    /// commit that failed, or of a key the stream dropped before a crash, holds nothing.
    pub(crate) fn holder(
        &mut self,
        store: &dyn Store,
        vault: Option<&Vault>,
        id: &[u8; 32],
    ) -> Result<Option<StreamId>, BoxError> {
        if let Some(stream) = store.key_holder(id)? {
            return Ok(Some(stream));
        }
        let Some(vault) = vault else {
            return Ok(None);
        };
        for (r, stream) in vault.key_holders(id)? {
            if self.held(store, Some(vault), &stream)?.refs.get(id) == Some(&r) {
                return Ok(Some(stream));
            }
        }
        Ok(None)
    }

    /// `stream` holds `entry` from now on (committed).
    pub(crate) fn set(&mut self, stream: &StreamId, entry: Entry) {
        self.keep(stream, entry);
    }

    /// Forgets what `streams` hold, read again when needed: an erasure freed their personal
    /// keys.
    pub(crate) fn erased(&mut self, streams: &[StreamId]) {
        for stream in streams {
            if let Some((_, used)) = self.cache.remove(stream) {
                self.order.remove(&used);
            }
        }
    }

    /// Forgets what `tenant`'s streams hold: the tenant moved away.
    pub(crate) fn forget_tenant(&mut self, tenant: &str) {
        let streams: Vec<StreamId> = self
            .cache
            .keys()
            .filter(|s| s.tenant_id == tenant)
            .cloned()
            .collect();
        self.erased(&streams);
    }

    fn keep(&mut self, stream: &StreamId, entry: Entry) {
        if let Some((_, used)) = self.cache.remove(stream) {
            self.order.remove(&used);
        }
        while self.cache.len() >= self.capacity {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.cache.remove(&oldest);
        }
        self.cache.insert(stream.clone(), (entry, self.next));
        self.order.insert(self.next, stream.clone());
        self.next += 1;
    }
}

/// What the store says `stream` holds, with the identities of its personal keys read from the
/// vault; a key whose vault value is gone was erased.
fn read(
    stream: &StreamId,
    stored: AggregateIndex,
    vault: Option<&Vault>,
) -> Result<Entry, BoxError> {
    let refs: Vec<String> = stored
        .keys
        .iter()
        .filter(|k| !k.vault_ref.is_empty())
        .map(|k| k.vault_ref.clone())
        .collect();
    let values: HashMap<String, VaultValue> = match vault {
        _ if refs.is_empty() => HashMap::new(),
        Some(vault) => vault
            .get(&refs)?
            .into_iter()
            .map(|v| (v.r#ref.clone(), v))
            .collect(),
        None => return Err(NO_VAULT.into()),
    };
    let mut entry = Entry {
        held: Held {
            facts: stored.facts.into_iter().collect(),
            ..Held::default()
        },
        ..Entry::default()
    };
    for k in stored.keys {
        let (id, subject) = if k.vault_ref.is_empty() {
            let id =
                k.id.as_slice()
                    .try_into()
                    .map_err(|_| format!("stored key {} of {stream:?} is not a key id", k.name))?;
            (id, String::new())
        } else {
            let Some(value) = values.get(&k.vault_ref) else {
                continue; // erased
            };
            let id = key_id_from(value)?;
            entry.refs.insert(id, k.vault_ref.clone());
            (id, value.subject.clone())
        };
        entry.held.keys.insert(Key {
            name: k.name,
            global: k.global,
            id,
            subject,
        });
    }
    Ok(entry)
}

/// Why a kernel without its vault cannot run on a store with personal keys.
pub(crate) const NO_VAULT: &str = "the store holds personal unique keys, which live in the vault: \
                                   start the kernel with its vault (kernable start --vault-store)";

/// The identity a personal key's vault value holds: its hex as a JSON string.
fn key_id_from(value: &VaultValue) -> Result<[u8; 32], BoxError> {
    let text: String = serde_json::from_slice(&value.value)
        .map_err(|_| format!("vault value {} is not a key id", value.r#ref))?;
    let mut id = [0u8; 32];
    hex::decode_to_slice(text, &mut id)
        .map_err(|_| format!("vault value {} is not a key id", value.r#ref))?;
    Ok(id)
}

/// A new vault value for the personal key `id` of `subject`, kept for `cmd`'s stream so that
/// erasing the subject finds the stream. Returns its reference.
pub(crate) fn put_personal(
    vault: &Vault,
    cmd: &Command,
    stream: &StreamId,
    id: &[u8; 32],
    subject: &str,
    now_ms: i64,
) -> Result<String, kernable_store::Error> {
    let mut salt = vec![0u8; 16];
    getrandom::fill(&mut salt).map_err(|_| kernable_store::Error::Vault("no randomness".into()))?;
    let value = format!("\"{}\"", hex::encode(id)).into_bytes();
    let r = signing::vault_ref(&salt, subject, false, &value);
    let v = VaultValue {
        r#ref: r.clone(),
        subject: subject.to_owned(),
        secret: false,
        salt,
        value,
    };
    let owner = Owner {
        command_id: &cmd.command_id,
        tenant: &cmd.tenant_id,
        stream: Some(stream),
    };
    vault.put_key(&v, id, &owner, now_ms)?;
    Ok(r)
}

/// `held` as the store keeps it: personal keys by their vault values in `refs`.
pub(crate) fn stored(held: &Held, refs: &HashMap<[u8; 32], String>) -> AggregateIndex {
    AggregateIndex {
        keys: held
            .keys
            .iter()
            .map(
                |k| match refs.get(&k.id).filter(|_| !k.subject.is_empty()) {
                    Some(r) => StoredKey {
                        name: k.name.clone(),
                        global: k.global,
                        id: Vec::new(),
                        vault_ref: r.clone(),
                    },
                    None => StoredKey {
                        name: k.name.clone(),
                        global: k.global,
                        id: k.id.to_vec(),
                        vault_ref: String::new(),
                    },
                },
            )
            .collect(),
        facts: held.facts.iter().cloned().collect(),
    }
}

/// Checks the limits of a result's keys, facts and requirements; a violation is a worker bug.
pub(crate) fn validate(r: &kernable_core::proto::Result) -> Result<(), Rejected> {
    let invalid = |message: String| Rejected {
        code: "invalid_index".into(),
        message,
        ..Default::default()
    };
    let name_ok = |name: &str| config::valid_type(name) && name.len() <= MAX_NAME_BYTES;
    if r.keys.len() > MAX_KEYS {
        return Err(invalid(format!("more than {MAX_KEYS} unique keys")));
    }
    if r.facts.len() > MAX_FACTS {
        return Err(invalid(format!("more than {MAX_FACTS} facts")));
    }
    if r.requires.len() > MAX_REQUIREMENTS {
        return Err(invalid(format!(
            "more than {MAX_REQUIREMENTS} requirements"
        )));
    }
    let mut keys = HashSet::new();
    for k in &r.keys {
        if !name_ok(&k.name) {
            return Err(invalid(format!(
                "a key name is non-empty, without dots and at most {MAX_NAME_BYTES} bytes"
            )));
        }
        if k.value_hash.len() != 32 {
            return Err(invalid(format!(
                "key {}: the value hash is 32 bytes (SHA-256)",
                k.name
            )));
        }
        if !keys.insert((&k.name, k.global, &k.value_hash)) {
            return Err(invalid(format!("key {} is sent twice", k.name)));
        }
    }
    let mut facts = HashSet::new();
    for f in &r.facts {
        if !name_ok(f) {
            return Err(invalid(format!(
                "a fact is non-empty, without dots and at most {MAX_NAME_BYTES} bytes"
            )));
        }
        if !facts.insert(f) {
            return Err(invalid(format!("fact {f} is sent twice")));
        }
    }
    for q in &r.requires {
        // Its aggregate by ID, or by a key and the value's 32-byte hash: one of them.
        let named = if q.key.is_empty() {
            !q.aggregate_id.is_empty() && q.value_hash.is_empty()
        } else {
            q.aggregate_id.is_empty() && name_ok(&q.key) && q.value_hash.len() == 32
        };
        let valid = config::valid_domain(&q.domain)
            && config::valid_type(&q.aggregate_type)
            && named
            && name_ok(&q.fact);
        if !valid {
            return Err(invalid(
                "a requirement names a domain, an unqualified aggregate type, an aggregate ID \
                 or a key with its 32-byte value hash, and a fact"
                    .into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use kernable_core::proto::{Command, NewEvent, Signature};
    use kernable_store::SqliteStore;

    use super::*;

    fn stream(id: &str) -> StreamId {
        StreamId {
            tenant_id: "t1".into(),
            domain: "shop".into(),
            aggregate_type: "Customer".into(),
            aggregate_id: id.into(),
        }
    }

    /// Commits `index` as the first event of `id`'s stream.
    fn commit(store: &SqliteStore, id: &str, key: u8, fact: &str) {
        let cmd = Command {
            command_id: format!("c-{id}"),
            tenant_id: "t1".into(),
            domain: "shop".into(),
            aggregate_type: "Customer".into(),
            aggregate_id: id.into(),
            command_type: "Register".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        let index = AggregateIndex {
            keys: vec![StoredKey {
                name: "email".into(),
                global: false,
                id: vec![key; 32],
                vault_ref: String::new(),
            }],
            facts: vec![fact.into()],
        };
        let event = NewEvent {
            event_type: "Registered".into(),
            payload: b"{}".to_vec(),
            schema: 1,
            ..Default::default()
        };
        store.enqueue(&cmd, 0).unwrap();
        store
            .commit(
                &cmd.command_id,
                0,
                &[event],
                &Signature::default(),
                Some(&index),
                0,
            )
            .unwrap();
    }

    #[test]
    fn an_evicted_aggregate_is_read_again_as_the_store_holds_it() {
        let store = SqliteStore::open_in_memory().unwrap();
        for (n, id) in ["a", "b", "c"].into_iter().enumerate() {
            commit(&store, id, n as u8 + 1, "active");
        }
        let mut index = Index::with_capacity(2);
        for (n, id) in ["a", "b", "c"].into_iter().enumerate() {
            let holder = index.holder(&store, None, &[n as u8 + 1; 32]).unwrap();
            assert_eq!(holder, Some(stream(id)));
            assert!(index.holds(&store, None, &stream(id), "active").unwrap());
        }
        assert_eq!(index.cache.len(), 2, "a was evicted");
        assert!(!index.cache.contains_key(&stream("a")));
        let a = index.held(&store, None, &stream("a")).unwrap();
        assert_eq!(
            a.held.keys.iter().map(|k| k.id).collect::<Vec<_>>(),
            [[1; 32]]
        );
        assert!(
            !index.cache.contains_key(&stream("b")),
            "b was the oldest then"
        );

        // What a commit sets is what the index answers, until it is evicted.
        let mut changed = a.clone();
        changed.held.facts = ["closed".to_owned()].into();
        index.set(&stream("a"), changed);
        assert!(index.holds(&store, None, &stream("a"), "closed").unwrap());
        index.erased(&[stream("a")]);
        assert!(
            index.holds(&store, None, &stream("a"), "active").unwrap(),
            "read again"
        );
        assert_eq!(index.holder(&store, None, &[9; 32]).unwrap(), None);
    }
}
