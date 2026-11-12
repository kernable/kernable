//! Masked copies of a kernel's stores, for previews (docs/how-to/previews.md).
//!
//! A masked copy holds the source's records (commands, events, checkpoints, registrations,
//! memberships) and none of its vault values: every personal value reads as `null`, as after
//! an erasure, and nothing in the copy can decrypt the source's vault. The copy gets a vault of
//! its own under a new key, which the copy makes, so a preview still writes and reads personal
//! data of its own. Snapshots and the cache are never copied, files only when asked.
//!
//! The store carries a [`CopyRecord`]. A kernel runs such a store only alone (no standby, pair,
//! partition or backup follower) and only with the vault key the mark names; an import or a
//! restore of it, and a standby that is sent it, refuse it unless they make another masked copy.
//!
//! Three ways make one, each consistent at one position: `kernable copy` from a store on this
//! machine (read in one view, also while its kernel runs), `kernable import --mask-vault` from
//! an export, `kernable restore --mask-vault` from a continuous backup.

use std::{io::Write, path::Path};

use kernable_core::proto::{CopyRecord, ImageBatch, RecordBatch, log_record::Record};
use kernable_store::{Store, snapshots::SnapshotStore, vault::Vault};
use zeroize::Zeroizing;

use crate::{BoxError, config::Config, now_ms};

/// What a masked copy keeps besides the records.
#[derive(Debug, Clone, Copy, Default)]
pub struct Mask {
    /// Copy the source's files too (`--with-files`). Off by default: files may hold personal
    /// data that no erasure and no masking reaches.
    pub with_files: bool,
}

impl Mask {
    /// Leaves the vault values out of an image batch, and its files unless asked.
    pub fn image(&self, batch: &mut ImageBatch) {
        batch.vault.clear();
        if !self.with_files {
            batch.files.clear();
        }
    }

    /// The same for a batch of numbered records (a backup's segment).
    pub fn records(&self, batch: &mut RecordBatch) {
        batch.vault.clear();
        if !self.with_files {
            batch.files.clear();
        }
    }
}

/// Writes a new vault key to `path` (readable by its owner only; an existing file is refused)
/// and opens a new, empty vault at `store` with it: the copy's own vault.
pub fn new_vault(store: &str, path: &str) -> Result<Vault, BoxError> {
    if Path::new(path).exists() {
        return Err(format!(
            "{path} exists: a masked copy makes its own, new vault key; name a file that does not exist"
        )
        .into());
    }
    let mut key = Zeroizing::new([0u8; 32]);
    getrandom::fill(key.as_mut()).map_err(|_| "no randomness")?;
    let vault = Vault::open(store, &key)?;
    let held = vault.overview(0)?;
    if held.personal + held.secret + held.erasures + held.intents > 0 {
        return Err(format!(
            "the vault {store} holds values already: a masked copy gets a new, empty vault"
        )
        .into());
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).map_err(|e| format!("{path}: {e}"))?;
    file.write_all(Zeroizing::new(format!("{}\n", hex::encode(key.as_slice()))).as_bytes())?;
    file.sync_all()?;
    Ok(vault)
}

/// Marks `store` as a masked copy of `source` whose own vault is `vault`, checked with the
/// source's kernel keys `kernel_keys` (key IDs), which the mark carries: a kernel on the copy
/// signs with its own key, and `kernable verify` of the copy accepts the source's beside it.
pub fn mark(
    store: &dyn Store,
    vault: &Vault,
    source: &str,
    kernel_keys: &[String],
) -> Result<CopyRecord, BoxError> {
    let mut source_kernel_keys = kernel_keys.to_vec();
    source_kernel_keys.sort();
    source_kernel_keys.dedup();
    let mark = CopyRecord {
        copied_at: now_ms(),
        position: store.head_position()?,
        source: source.to_owned(),
        vault_key: vault.key_id().to_owned(),
        source_kernel_keys,
    };
    store.mark_copy(&mark)?;
    store.sync()?;
    Ok(mark)
}

/// Refuses records that hold the mark of a masked copy, where something would take them for
/// another kernel's own: a standby, or an import or restore that does not mask.
pub fn refuse_marked<'a>(
    records: impl IntoIterator<Item = &'a Record>,
    what: &str,
) -> Result<(), BoxError> {
    match records.into_iter().any(|r| matches!(r, Record::Copy(_))) {
        true => Err(format!(
            "{what} is a masked copy (a preview): it never becomes anything but a copy; make \
             another masked copy of it with --mask-vault"
        )
        .into()),
        false => Ok(()),
    }
}

/// What a kernel on a masked copy may not do, checked before it opens anything else: it runs
/// alone. Returns the mark, if the store is a copy.
pub fn check_config(config: &Config, store: &dyn Store) -> Result<Option<CopyRecord>, BoxError> {
    let Some(mark) = store.copy_mark()? else {
        return Ok(None);
    };
    let r = &config.replication;
    let role = if r.follow.is_some() {
        Some("--follow")
    } else if r.pair.is_some() {
        Some("--witness")
    } else if r.sync {
        Some("--replication sync")
    } else if r.backup {
        Some("--backup on")
    } else if config.partition.is_some() {
        Some("--partition")
    } else {
        None
    };
    if let Some(flag) = role {
        return Err(format!(
            "the store is a masked copy (a preview, from {}): it runs alone, without {flag}; \
             nothing replicates, backs up or moves it",
            describe(&mark)
        )
        .into());
    }
    Ok(Some(mark))
}

/// The vault a kernel on a masked copy opens must be the copy's own: the key the mark names.
pub fn check_vault(mark: &CopyRecord, vault: &Vault) -> Result<(), BoxError> {
    if mark.vault_key != vault.key_id() {
        return Err(format!(
            "the store is a masked copy (a preview): it runs only with the vault key its copy \
             made (key ID {}), not {}",
            mark.vault_key,
            vault.key_id()
        )
        .into());
    }
    Ok(())
}

/// A copy's snapshots must be its own: one taken before the copy was made comes from another
/// store's workers and may hold the source's personal values in clear.
pub fn check_snapshots(mark: &CopyRecord, snapshots: &dyn SnapshotStore) -> Result<(), BoxError> {
    let older = snapshots
        .infos()?
        .iter()
        .filter(|s| s.created_at > 0 && s.created_at < mark.copied_at)
        .count();
    if older > 0 {
        return Err(format!(
            "the store is a masked copy (a preview), but the snapshot store holds {older} \
             snapshots from before the copy was made: give the copy a new, empty snapshot store \
             (--snapshots)"
        )
        .into());
    }
    Ok(())
}

/// The kernel keys to check a store with: `given`, and, for a masked copy, the source's
/// kernel keys its mark carries (whose checkpoints and revocations came along), so the copy's
/// own kernel key and production's both verify. Also which keys the mark added.
pub fn verifying_keys(mark: Option<&CopyRecord>, given: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = given.to_vec();
    let mut added = Vec::new();
    for k in mark
        .map(|m| m.source_kernel_keys.as_slice())
        .unwrap_or_default()
    {
        if !keys.contains(k) {
            keys.push(k.clone());
            added.push(k.clone());
        }
    }
    (keys, added)
}

/// A copy's mark for people: where it came from, when, up to which event.
pub fn describe(mark: &CopyRecord) -> String {
    format!(
        "{}, copied {} up to event {}",
        mark.source,
        crate::config::utc(mark.copied_at),
        mark.position
    )
}
