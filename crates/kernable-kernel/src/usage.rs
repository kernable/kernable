//! Usage beside the store: `<store>.usage`, JSON, written every minute and at shutdown,
//! for whoever bills or watches the machine. Its counts go on across restarts: a kernel starts
//! from the file's, so a crash loses at most the last minute of them.
//!
//! ```json
//! {"at": 1793996832409, "events": 205200, "commands": 205211, "rejected": 3, "queries": 1200,
//!  "store_bytes": 241172480, "sig": "9f3c…"}
//! ```
//!
//! `events` is the store's head (it never goes back), `store_bytes` what the stores take on the
//! disk now; `commands` (committed), `rejected` and `queries` (answered) are counted since the
//! file began. `sig` is the kernel key's signature over [`Usage::signed_bytes`]: whoever reads
//! the file elsewhere can tell it was the kernel's, not written by another process on the
//! machine.

use std::{path::PathBuf, sync::atomic::Ordering::Relaxed, time::Duration};

use serde::{Deserialize, Serialize};

use crate::Kernel;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// When it was written, ms since epoch.
    pub at: i64,
    pub events: u64,
    pub commands: u64,
    pub rejected: u64,
    pub queries: u64,
    pub store_bytes: u64,
    /// With `--replication sync`: `in-sync` (a standby holds every write), `alone` (it commits
    /// without one, as the witness allows), `catching-up` (without a witness, before a standby
    /// caught up), `waiting` (writes wait for a standby that is away). Empty without.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub replication: String,
    /// The kernel key's signature over [`Usage::signed_bytes`], hex encoded.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sig: String,
}

impl Usage {
    /// What the kernel signs: every field but `sig`, one per line after a tag.
    pub fn signed_bytes(&self) -> Vec<u8> {
        format!(
            "kernable-usage-1\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            self.at,
            self.events,
            self.commands,
            self.rejected,
            self.queries,
            self.store_bytes,
            self.replication
        )
        .into_bytes()
    }

    /// Signs it with `key`.
    pub fn sign(&mut self, key: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        self.sig = hex::encode(key.sign(&self.signed_bytes()).to_bytes());
    }

    /// Whether `sig` is `key`'s over the rest.
    pub fn verify(&self, key: &ed25519_dalek::VerifyingKey) -> bool {
        let Some(sig) = hex::decode(&self.sig)
            .ok()
            .and_then(|b| <[u8; 64]>::try_from(b).ok())
        else {
            return false;
        };
        key.verify_strict(
            &self.signed_bytes(),
            &ed25519_dalek::Signature::from_bytes(&sig),
        )
        .is_ok()
    }
}

/// Where the store at `url` keeps its usage; `None` for a store in memory.
pub fn path(url: &str) -> Option<PathBuf> {
    crate::standby::marker(url, "usage")
}

/// The usage the file at `url`'s store holds, if it can be read.
pub fn read(url: &str) -> Option<Usage> {
    serde_json::from_slice(&std::fs::read(path(url)?).ok()?).ok()
}

/// The usage now: the counts the kernel started from, and its own since.
pub(crate) fn now(kernel: &Kernel) -> Usage {
    let (base, m) = (&kernel.usage_base, &kernel.metrics);
    Usage {
        at: crate::now_ms(),
        events: kernel.store.head_position().unwrap_or(0),
        commands: base.commands + m.committed.load(Relaxed),
        rejected: base.rejected + m.rejected.load(Relaxed),
        queries: base.queries + m.queries_answered.load(Relaxed),
        store_bytes: kernel.stored_bytes.load(Relaxed),
        replication: kernel
            .replication
            .as_ref()
            .map(|p| p.state_name().to_owned())
            .unwrap_or_default(),
        sig: String::new(),
    }
}

/// Writes the usage now, replacing the file in one step.
pub(crate) fn write(kernel: &Kernel) {
    let Some(path) = path(&kernel.config().store) else {
        return;
    };
    let mut usage = now(kernel);
    usage.sign(&kernel.key());
    let next = path.with_extension("usage.next");
    let written = std::fs::write(&next, serde_json::to_vec(&usage).expect("serializes"))
        .and_then(|()| std::fs::rename(&next, &path));
    if let Err(e) = written {
        tracing::warn!(path = %path.display(), error = %e, "writing the usage failed");
    }
}

/// Writes the usage every minute.
pub(crate) async fn keep(kernel: &Kernel) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.tick().await;
    loop {
        tick.tick().await;
        write(kernel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signed_usage_verifies_and_a_changed_one_does_not() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let mut usage = Usage {
            at: 1,
            events: 10,
            commands: 9,
            queries: 3,
            store_bytes: 4096,
            replication: "in-sync".into(),
            ..Usage::default()
        };
        usage.sign(&key);
        assert!(usage.verify(&key.verifying_key()));
        let read: Usage = serde_json::from_slice(&serde_json::to_vec(&usage).unwrap()).unwrap();
        assert!(read.verify(&key.verifying_key()));
        let mut more = usage.clone();
        more.commands += 1;
        assert!(!more.verify(&key.verifying_key()));
        let other = ed25519_dalek::SigningKey::from_bytes(&[8; 32]);
        assert!(!usage.verify(&other.verifying_key()));
        assert!(!Usage::default().verify(&key.verifying_key()));
    }
}
