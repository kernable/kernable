//! Unique keys and facts: what every aggregate holds, in memory, rebuilt from the store
//! at startup ([`kernable_store::Store::index`]).
//!
//! A worker of protocol 2 sends with each result the full set of keys and facts its aggregate
//! holds after the events, and the facts other aggregates must hold. The kernel checks them
//! against this index while it commits, under the same lock, so two commands never take one key
//! and a requirement never sees a fact another commit is removing. Only a changed set is
//! stored, with the commit, so the store holds the latest set per stream.
//!
//! A key is filed under its identity (`kernable.v1.key`), which the kernel computes from the
//! command's tenant, domain and aggregate type. The identity of a personal key lives in the
//! vault only, as a value of its subject: erasing the subject frees the key, and the log keeps
//! no trace of it.

use std::collections::{BTreeSet, HashMap, HashSet};

use kernable_core::{
    proto::{AggregateIndex, Command, Rejected, StoredKey, VaultValue},
    signing,
};
use kernable_store::{
    StreamId,
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

impl Held {
    fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.facts.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct Index {
    /// Key identity -> the stream that holds it.
    ids: HashMap<[u8; 32], StreamId>,
    held: HashMap<StreamId, Held>,
    /// The vault value of every personal key held, by identity.
    refs: HashMap<[u8; 32], String>,
}

impl Index {
    /// The index as the store holds it, with the identities of personal keys read from the
    /// vault; a key whose vault value is gone was erased and is free.
    pub(crate) fn load(
        stored: Vec<(StreamId, AggregateIndex)>,
        vault: Option<&Vault>,
    ) -> Result<Index, BoxError> {
        let refs: Vec<String> = stored
            .iter()
            .flat_map(|(_, index)| &index.keys)
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
            None => {
                return Err(
                    "the store holds personal unique keys, which live in the vault: \
                            start the kernel with its vault (kernable start --vault-key)"
                        .into(),
                );
            }
        };
        let mut index = Index::default();
        for (stream, stored) in stored {
            let mut held = Held {
                facts: stored.facts.into_iter().collect(),
                ..Held::default()
            };
            for k in stored.keys {
                let (id, subject) = if k.vault_ref.is_empty() {
                    let id = k.id.as_slice().try_into().map_err(|_| {
                        format!("stored key {} of {stream:?} is not a key id", k.name)
                    })?;
                    (id, String::new())
                } else {
                    let Some(value) = values.get(&k.vault_ref) else {
                        continue; // erased
                    };
                    (key_id_from(value)?, value.subject.clone())
                };
                if let Some(other) = index.ids.get(&id) {
                    tracing::error!(key = %k.name, holder = ?other, stream = ?stream, "a key is held twice");
                    continue;
                }
                index.ids.insert(id, stream.clone());
                if !subject.is_empty() {
                    index.refs.insert(id, k.vault_ref.clone());
                }
                held.keys.insert(Key {
                    name: k.name,
                    global: k.global,
                    id,
                    subject,
                });
            }
            if !held.is_empty() {
                index.held.insert(stream, held);
            }
        }
        Ok(index)
    }

    /// Whether `stream` holds `fact`.
    pub(crate) fn holds(&self, stream: &StreamId, fact: &str) -> bool {
        self.held
            .get(stream)
            .is_some_and(|h| h.facts.contains(fact))
    }

    /// The stream that holds the key `id`, if any.
    pub(crate) fn holder(&self, id: &[u8; 32]) -> Option<&StreamId> {
        self.ids.get(id)
    }

    /// What `stream` holds now.
    pub(crate) fn held(&self, stream: &StreamId) -> Held {
        self.held.get(stream).cloned().unwrap_or_default()
    }

    /// The vault value of the personal key `id`.
    pub(crate) fn vault_ref(&self, id: &[u8; 32]) -> Option<&String> {
        self.refs.get(id)
    }

    /// `stream` holds `held` from now on; `refs` are the vault values of its personal keys.
    pub(crate) fn set(&mut self, stream: &StreamId, held: Held, refs: &HashMap<[u8; 32], String>) {
        for k in self.held.remove(stream).unwrap_or_default().keys {
            self.ids.remove(&k.id);
            self.refs.remove(&k.id);
        }
        for k in &held.keys {
            self.ids.insert(k.id, stream.clone());
            if let Some(r) = refs.get(&k.id) {
                self.refs.insert(k.id, r.clone());
            }
        }
        if !held.is_empty() {
            self.held.insert(stream.clone(), held);
        }
    }

    /// Frees the personal keys of `streams` whose vault values an erasure deleted.
    pub(crate) fn erased(&mut self, streams: &[StreamId], vault: &Vault) {
        for stream in streams {
            let Some(held) = self.held.get(stream) else {
                continue;
            };
            let refs: Vec<String> = held
                .keys
                .iter()
                .filter_map(|k| self.refs.get(&k.id).cloned())
                .collect();
            let kept: HashSet<String> = match vault.get(&refs) {
                Ok(values) => values.into_iter().map(|v| v.r#ref).collect(),
                Err(e) => {
                    tracing::error!(error = %e, "reading the vault failed");
                    continue;
                }
            };
            let gone: Vec<Key> = held
                .keys
                .iter()
                .filter(|k| self.refs.get(&k.id).is_some_and(|r| !kept.contains(r)))
                .cloned()
                .collect();
            if gone.is_empty() {
                continue;
            }
            let mut held = held.clone();
            for k in gone {
                held.keys.remove(&k);
                self.ids.remove(&k.id);
                self.refs.remove(&k.id);
            }
            if held.is_empty() {
                self.held.remove(stream);
            } else {
                self.held.insert(stream.clone(), held);
            }
        }
    }
}

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
    vault.put(&[v], std::slice::from_ref(&r), &owner, now_ms)?;
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
        let valid = config::valid_domain(&q.domain)
            && config::valid_type(&q.aggregate_type)
            && !q.aggregate_id.is_empty()
            && name_ok(&q.fact);
        if !valid {
            return Err(invalid(
                "a requirement names a domain, an unqualified aggregate type, an aggregate ID \
                 and a fact"
                    .into(),
            ));
        }
    }
    Ok(())
}
