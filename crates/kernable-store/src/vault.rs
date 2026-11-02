//! The vault: personal and secret values that commands and events keep outside the signed log.
//! The log holds only references (`vault_refs`, see `proto/SIGNING.md`), which commit to the
//! values without revealing them. The vault keeps the values encrypted (AES-256-GCM, bound to
//! their reference) in a SQLite database whose deleted rows are overwritten (`secure_delete`).
//!
//! Personal values belong to a subject (a person) and are deleted when an event erases it.
//! Secret values (passwords, one-time tokens) are deleted once their command completes.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use aes_gcm::{
    Aes256Gcm, Key, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use kernable_core::{proto::VaultValue, signing};
use rusqlite::{Connection, params, params_from_iter};
use sha2::{Digest, Sha256};

use crate::{Error, Result, StreamId};

/// The version of the vault's data format.
///
/// - 1: the first format.
/// - 2: keeps the erasures that are about to happen ([`Vault::intents`]).
pub const FORMAT: u32 = 2;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS vault (
    ref            TEXT PRIMARY KEY,
    subject        TEXT    NOT NULL, -- empty for secrets
    secret         INTEGER NOT NULL,
    command_id     TEXT    NOT NULL, -- the command that wrote it
    tenant         TEXT    NOT NULL, -- the command's tenant: an erasure stays within it
    tenant_id      TEXT,             -- the stream whose event refers to it; NULL for commands
    domain         TEXT,
    aggregate_type TEXT,
    aggregate_id   TEXT,
    nonce          BLOB    NOT NULL,
    value          BLOB    NOT NULL, -- encrypted
    created_at     INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS vault_subject ON vault (tenant, subject);
CREATE INDEX IF NOT EXISTS vault_command ON vault (command_id);
CREATE TABLE IF NOT EXISTS erasures (
    subject_hash TEXT    NOT NULL, -- SHA-256 of the subject: proves an erasure without keeping it
    tenant       TEXT    NOT NULL,
    command_id   TEXT    NOT NULL,
    erased       INTEGER NOT NULL, -- number of values
    at           INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS intents (
    tenant     TEXT NOT NULL,
    subject    TEXT NOT NULL,
    command_id TEXT NOT NULL, -- the command whose erasing event is about to be committed
    PRIMARY KEY (tenant, subject, command_id)
) STRICT;
";

/// How many values one key may encrypt. Each value gets a random 96-bit nonce; NIST SP
/// 800-38D allows 2^32 of them per key, which keeps the chance of a repeated nonce below 2^-32.
pub const MAX_SEALED: u64 = 1 << 32;

/// The number of values encrypted with the current key, created on open so that a vault of
/// format 1 counts too: it starts at the number of values held, as earlier ones were not
/// counted.
const SEALED: &str = "
CREATE TABLE IF NOT EXISTS sealed (count INTEGER NOT NULL) STRICT;
INSERT INTO sealed SELECT COUNT(*) FROM vault WHERE NOT EXISTS (SELECT 1 FROM sealed);
";

/// Checks that every value matches its reference and is referred to by `refs`.
pub fn check(values: &[VaultValue], refs: &[String]) -> Result<()> {
    for v in values {
        if !refs.contains(&v.r#ref) {
            return Err(Error::Vault(format!(
                "value {} is not referred to",
                v.r#ref
            )));
        }
        if signing::vault_ref(&v.salt, &v.subject, v.secret, &v.value) != v.r#ref {
            return Err(Error::Vault(format!(
                "value {} does not match its reference",
                v.r#ref
            )));
        }
        if v.secret != v.subject.is_empty() {
            return Err(Error::Vault(format!(
                "value {}: personal values need a subject, secrets none",
                v.r#ref
            )));
        }
    }
    Ok(())
}

/// Who wrote values: a command, and for event values the stream of the event.
pub struct Owner<'a> {
    pub command_id: &'a str,
    /// The command's tenant: an erasure deletes a subject's values within it only.
    pub tenant: &'a str,
    pub stream: Option<&'a StreamId>,
}

pub struct Vault {
    conn: Mutex<Connection>,
    cipher: Aes256Gcm,
}

impl Vault {
    /// Opens the vault at `url` (`sqlite:<path>`, a bare path, or `memory:`), encrypted with
    /// the 32-byte `key`.
    pub fn open(url: &str, key: &[u8; 32]) -> Result<Self> {
        let conn = match crate::backend(url) {
            ("memory", _) => Connection::open_in_memory()?,
            ("file", _) => return Err(Error::Vault("the vault is a SQLite database".into())),
            (_, path) => Connection::open(Path::new(path))?,
        };
        conn.busy_timeout(Duration::from_secs(5))?;
        // Deleted values are overwritten on disk, not only unlinked.
        conn.pragma_update(None, "secure_delete", "ON")?;
        // A value is on the disk when a write returns (rollback journal, synchronous FULL by
        // default; fullfsync makes that a real flush on macOS), as the log refers to it.
        conn.pragma_update(None, "fullfsync", true)?;
        let format: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let tables: u32 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        match (format, tables) {
            (0, 0) => {
                conn.execute_batch(SCHEMA)?;
                conn.pragma_update(None, "user_version", FORMAT)?;
            }
            (FORMAT, _) => {}
            (found, _) => {
                return Err(Error::Vault(format!(
                    "vault format {found}, this kernable reads {FORMAT}"
                )));
            }
        }
        conn.execute_batch(SEALED)?;
        Ok(Vault {
            conn: Mutex::new(conn),
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The number of values encrypted with the current key; see [`MAX_SEALED`].
    pub fn sealed(&self) -> Result<u64> {
        Ok(self
            .conn()
            .query_row("SELECT count FROM sealed", [], |r| r.get(0))?)
    }

    /// Checks `values` against `refs` (the signed references of one command or event) and
    /// keeps them. Every value must match its reference, and every reference must have a value
    /// unless the vault already holds it (a resubmitted command).
    pub fn put(
        &self,
        values: &[VaultValue],
        refs: &[String],
        owner: &Owner,
        now_ms: i64,
    ) -> Result<()> {
        check(values, refs)?;
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let sealed: u64 = tx.query_row("SELECT count FROM sealed", [], |r| r.get(0))?;
        let sealed = sealed + values.len() as u64;
        if sealed > MAX_SEALED {
            return Err(Error::Vault(format!(
                "the vault key has encrypted its limit of {MAX_SEALED} values; \
                 encrypt the vault with a new key (kernable vault rekey)"
            )));
        }
        tx.execute("UPDATE sealed SET count = ?1", [sealed])?;
        for v in values {
            let mut nonce = [0u8; 12];
            getrandom::fill(&mut nonce).map_err(|_| Error::Vault("no randomness".into()))?;
            let sealed = self
                .cipher
                .encrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: &v.value,
                        aad: v.r#ref.as_bytes(),
                    },
                )
                .map_err(|_| Error::Vault("encryption failed".into()))?;
            let s = owner.stream;
            tx.execute(
                "INSERT OR IGNORE INTO vault (ref, subject, secret, command_id, tenant, tenant_id,
                                              domain, aggregate_type, aggregate_id, nonce, value,
                                              created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    v.r#ref,
                    v.subject,
                    v.secret,
                    owner.command_id,
                    owner.tenant,
                    s.map(|s| &s.tenant_id),
                    s.map(|s| &s.domain),
                    s.map(|s| &s.aggregate_type),
                    s.map(|s| &s.aggregate_id),
                    nonce.to_vec(),
                    sealed,
                    now_ms
                ],
            )?;
        }
        for r in refs {
            let held: bool = tx.query_row(
                "SELECT EXISTS (SELECT 1 FROM vault WHERE ref = ?1)",
                [r],
                |row| row.get(0),
            )?;
            if !held {
                return Err(Error::Vault(format!("no value for reference {r}")));
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The values of `refs` that are still held, decrypted (without salts).
    pub fn get(&self, refs: &[String]) -> Result<Vec<VaultValue>> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn();
        let marks = vec!["?"; refs.len()].join(",");
        let mut stmt = conn.prepare(&format!(
            "SELECT ref, subject, secret, nonce, value FROM vault WHERE ref IN ({marks})"
        ))?;
        let rows = stmt.query_map(params_from_iter(refs), |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, Vec<u8>>(4)?,
            ))
        })?;
        let mut values = Vec::new();
        for row in rows {
            let (r, subject, secret, nonce, sealed) = row?;
            let value = self
                .cipher
                .decrypt(
                    stored_nonce(&r, &nonce)?,
                    Payload {
                        msg: &sealed,
                        aad: r.as_bytes(),
                    },
                )
                .map_err(|_| {
                    Error::Vault(format!("value {r} cannot be decrypted with this key"))
                })?;
            values.push(VaultValue {
                r#ref: r,
                subject,
                secret,
                salt: Vec::new(),
                value,
            });
        }
        Ok(values)
    }

    /// Which of `refs` the vault still holds, with their subject and whether they are secret,
    /// without decrypting them: what a view may show about vault fields.
    pub fn describe(&self, refs: &[String]) -> Result<Vec<(String, String, bool)>> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn();
        let marks = vec!["?"; refs.len()].join(",");
        let mut stmt = conn.prepare(&format!(
            "SELECT ref, subject, secret FROM vault WHERE ref IN ({marks})"
        ))?;
        let rows = stmt.query_map(params_from_iter(refs), |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Deletes every value of `subject` in `tenant` and returns the streams whose events
    /// referred to them, whose snapshots may hold them too. Another tenant's subject of the
    /// same name keeps its values. Logs the erasure without the subject itself.
    pub fn erase(
        &self,
        tenant: &str,
        subject: &str,
        command_id: &str,
        now_ms: i64,
    ) -> Result<Vec<StreamId>> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let streams = {
            let mut stmt = tx.prepare(
                "SELECT DISTINCT tenant_id, domain, aggregate_type, aggregate_id FROM vault
                 WHERE tenant = ?1 AND subject = ?2 AND tenant_id IS NOT NULL",
            )?;
            stmt.query_map([tenant, subject], |r| {
                Ok(StreamId {
                    tenant_id: r.get(0)?,
                    domain: r.get(1)?,
                    aggregate_type: r.get(2)?,
                    aggregate_id: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let erased = tx.execute(
            "DELETE FROM vault WHERE tenant = ?1 AND subject = ?2 AND secret = 0",
            [tenant, subject],
        )?;
        let subject_hash = hex::encode(Sha256::digest(subject.as_bytes()));
        tx.execute(
            "INSERT INTO erasures (subject_hash, tenant, command_id, erased, at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![subject_hash, tenant, command_id, erased as i64, now_ms],
        )?;
        tx.execute(
            "DELETE FROM intents WHERE tenant = ?1 AND subject = ?2 AND command_id = ?3",
            [tenant, subject, command_id],
        )?;
        tx.commit()?;
        Ok(streams)
    }

    /// Encrypts every value again with `new_key`, in one transaction; returns how many.
    pub fn rekey(&self, new_key: &[u8; 32]) -> Result<usize> {
        let next = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(new_key));
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let rows: Vec<(String, Vec<u8>, Vec<u8>)> = {
            let mut stmt = tx.prepare("SELECT ref, nonce, value FROM vault")?;
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        for (r, nonce, sealed) in &rows {
            let aad = r.as_bytes();
            let value = self
                .cipher
                .decrypt(stored_nonce(r, nonce)?, Payload { msg: sealed, aad })
                .map_err(|_| {
                    Error::Vault(format!("value {r} cannot be decrypted with the old key"))
                })?;
            let mut fresh = [0u8; 12];
            getrandom::fill(&mut fresh).map_err(|_| Error::Vault("no randomness".into()))?;
            let resealed = next
                .encrypt(Nonce::from_slice(&fresh), Payload { msg: &value, aad })
                .map_err(|_| Error::Vault("encryption failed".into()))?;
            tx.execute(
                "UPDATE vault SET nonce = ?1, value = ?2 WHERE ref = ?3",
                params![fresh.to_vec(), resealed, r],
            )?;
        }
        tx.execute("UPDATE sealed SET count = ?1", [rows.len() as u64])?;
        tx.commit()?;
        Ok(rows.len())
    }

    /// Deletes the secrets of a completed command.
    pub fn drop_secrets(&self, command_id: &str) -> Result<()> {
        self.conn().execute(
            "DELETE FROM vault WHERE command_id = ?1 AND secret = 1",
            [command_id],
        )?;
        Ok(())
    }

    /// Notes that `command_id` is about to commit an event that erases `subject`: once it is
    /// committed, the kernel erases the subject ([`Vault::erase`], which drops the intent),
    /// also after a crash ([`Vault::intents`]).
    pub fn intend_erase(&self, tenant: &str, subject: &str, command_id: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR IGNORE INTO intents (tenant, subject, command_id) VALUES (?1, ?2, ?3)",
            [tenant, subject, command_id],
        )?;
        Ok(())
    }

    /// Erasures not carried out yet: (tenant, subject, command ID).
    pub fn intents(&self) -> Result<Vec<(String, String, String)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT tenant, subject, command_id FROM intents")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    /// Drops the intents of `command_id`: its commit did not happen.
    pub fn drop_intents(&self, command_id: &str) -> Result<()> {
        self.conn()
            .execute("DELETE FROM intents WHERE command_id = ?1", [command_id])?;
        Ok(())
    }

    /// Deletes the values of `refs`: what the kernel put for a commit that did not happen.
    pub fn remove(&self, refs: &[String]) -> Result<()> {
        if refs.is_empty() {
            return Ok(());
        }
        let marks = vec!["?"; refs.len()].join(",");
        self.conn().execute(
            &format!("DELETE FROM vault WHERE ref IN ({marks})"),
            params_from_iter(refs),
        )?;
        Ok(())
    }

    /// The commands that still have secrets in the vault.
    pub fn secret_commands(&self) -> Result<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT DISTINCT command_id FROM vault WHERE secret = 1")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

/// The nonce a value was stored with. A damaged vault may hold one of another length, which
/// `Nonce::from_slice` would panic on.
fn stored_nonce<'a>(r: &str, nonce: &'a [u8]) -> Result<&'a Nonce<aes_gcm::aead::consts::U12>> {
    if nonce.len() != 12 {
        return Err(Error::Vault(format!(
            "value {r} has a nonce of {} bytes, not 12: the vault is damaged",
            nonce.len()
        )));
    }
    Ok(Nonce::from_slice(nonce))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(n: u8) -> VaultValue {
        let salt = vec![n; 16];
        VaultValue {
            r#ref: signing::vault_ref(&salt, "", true, b"\"s\""),
            salt,
            secret: true,
            value: b"\"s\"".to_vec(),
            ..Default::default()
        }
    }

    fn put(vault: &Vault, values: &[VaultValue]) -> Result<()> {
        let refs: Vec<_> = values.iter().map(|v| v.r#ref.clone()).collect();
        let owner = Owner {
            command_id: "c1",
            tenant: "t1",
            stream: None,
        };
        vault.put(values, &refs, &owner, 0)
    }

    #[test]
    fn a_key_encrypts_at_most_max_sealed_values() {
        let vault = Vault::open("memory:", &[1; 32]).unwrap();
        put(&vault, &[value(1), value(2)]).unwrap();
        assert_eq!(vault.sealed().unwrap(), 2);

        let near = MAX_SEALED - 1;
        vault
            .conn()
            .execute("UPDATE sealed SET count = ?1", [near])
            .unwrap();
        let refused = put(&vault, &[value(3), value(4)]).err().unwrap();
        assert!(
            refused.to_string().contains("kernable vault rekey"),
            "{refused}"
        );
        assert_eq!(
            vault.sealed().unwrap(),
            near,
            "a refused put counts nothing"
        );
        put(&vault, &[value(3)]).unwrap();
        assert_eq!(vault.sealed().unwrap(), MAX_SEALED);

        // A new key starts over, counting the values it encrypted again.
        vault.rekey(&[2; 32]).unwrap();
        assert_eq!(vault.sealed().unwrap(), 3);
    }

    #[test]
    fn a_damaged_nonce_is_an_error_not_a_panic() {
        // Found by the store_files fuzz target.
        let vault = Vault::open("memory:", &[1; 32]).unwrap();
        let v = value(1);
        put(&vault, std::slice::from_ref(&v)).unwrap();
        vault
            .conn()
            .execute("UPDATE vault SET nonce = ?1", [vec![0u8; 10]])
            .unwrap();
        let got = vault.get(std::slice::from_ref(&v.r#ref)).err().unwrap();
        assert!(got.to_string().contains("nonce of 10 bytes"), "{got}");
        let rekeyed = vault.rekey(&[2; 32]).err().unwrap();
        assert!(
            rekeyed.to_string().contains("nonce of 10 bytes"),
            "{rekeyed}"
        );
    }

    #[test]
    fn a_vault_of_format_1_starts_counting_at_the_values_it_holds() {
        let path =
            std::env::temp_dir().join(format!("kernable-vault-sealed-{}", std::process::id()));
        let url = format!("sqlite:{}", path.display());
        let vault = Vault::open(&url, &[1; 32]).unwrap();
        put(&vault, &[value(1), value(2), value(3)]).unwrap();
        vault.conn().execute_batch("DROP TABLE sealed").unwrap();
        drop(vault);
        assert_eq!(Vault::open(&url, &[1; 32]).unwrap().sealed().unwrap(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_vault_key_schedules_are_wiped_when_dropped() {
        fn wiped<T: zeroize::ZeroizeOnDrop>() {}
        // The AES round keys an `Aes256Gcm` holds. Its GHASH key is wiped too, except on ARM,
        // where `polyval` does not wipe it yet; it is derived from the vault key but does not
        // reveal it.
        wiped::<aes::Aes256>();
    }
}
