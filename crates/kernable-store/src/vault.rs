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
pub const FORMAT: u32 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS vault (
    ref            TEXT PRIMARY KEY,
    subject        TEXT    NOT NULL, -- empty for secrets
    secret         INTEGER NOT NULL,
    command_id     TEXT    NOT NULL, -- the command that wrote it
    tenant_id      TEXT,             -- the stream whose event refers to it; NULL for commands
    domain         TEXT,
    aggregate_type TEXT,
    aggregate_id   TEXT,
    nonce          BLOB    NOT NULL,
    value          BLOB    NOT NULL, -- encrypted
    created_at     INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS vault_subject ON vault (subject);
CREATE INDEX IF NOT EXISTS vault_command ON vault (command_id);
CREATE TABLE IF NOT EXISTS erasures (
    subject_hash TEXT    NOT NULL, -- SHA-256 of the subject: proves an erasure without keeping it
    command_id   TEXT    NOT NULL,
    erased       INTEGER NOT NULL, -- number of values
    at           INTEGER NOT NULL
) STRICT;
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
        Ok(Vault {
            conn: Mutex::new(conn),
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
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
                "INSERT OR IGNORE INTO vault (ref, subject, secret, command_id, tenant_id, domain,
                                              aggregate_type, aggregate_id, nonce, value, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    v.r#ref,
                    v.subject,
                    v.secret,
                    owner.command_id,
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
                    Nonce::from_slice(&nonce),
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

    /// Deletes every value of `subject` and returns the streams whose events referred to them,
    /// whose snapshots may hold them too. Logs the erasure without the subject itself.
    pub fn erase(&self, subject: &str, command_id: &str, now_ms: i64) -> Result<Vec<StreamId>> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let streams = {
            let mut stmt = tx.prepare(
                "SELECT DISTINCT tenant_id, domain, aggregate_type, aggregate_id FROM vault
                 WHERE subject = ?1 AND tenant_id IS NOT NULL",
            )?;
            stmt.query_map([subject], |r| {
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
            "DELETE FROM vault WHERE subject = ?1 AND secret = 0",
            [subject],
        )?;
        let subject_hash = hex::encode(Sha256::digest(subject.as_bytes()));
        tx.execute(
            "INSERT INTO erasures (subject_hash, command_id, erased, at) VALUES (?1, ?2, ?3, ?4)",
            params![subject_hash, command_id, erased as i64, now_ms],
        )?;
        tx.commit()?;
        Ok(streams)
    }

    /// Deletes the secrets of a completed command.
    pub fn drop_secrets(&self, command_id: &str) -> Result<()> {
        self.conn().execute(
            "DELETE FROM vault WHERE command_id = ?1 AND secret = 1",
            [command_id],
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
