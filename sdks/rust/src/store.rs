//! A read model's store: a key-value store with namespaces in a SQLite file (or in memory),
//! with the tables of the other SDKs' stores. The listener writes its data and its position in
//! one transaction per batch, so after a crash every event has taken effect exactly once, and a
//! restart continues where it left off instead of replaying everything.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};

use crate::Error;

/// A listener's store: JSON values under keys, grouped in namespaces (`ns`), in a SQLite file or
/// in memory; see [`crate::Listener::store`]. Cloning gives another handle to it. Handlers get
/// it as `ctx.store`: `if let Some(store) = &ctx.store { store.set("carts", &evt.aggregate_id,
/// &row)?; }`. What an event handler writes takes effect together with the listener's
/// position, once per batch. Errors are [`Error::Store`] (SQLite) or [`Error::Json`].
#[derive(Clone)]
pub struct Store {
    /// `None` once closed.
    db: Arc<Mutex<Option<Connection>>>,
    committed: Arc<AtomicU64>,
    /// Batches take turns: with partitions, several streams share the store.
    turns: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Store")
    }
}

impl Store {
    /// Opens `url` (`sqlite:<path>`, a bare path, or `memory:`). If the stored data was built
    /// at another `schema`, it is cleared and the read model rebuilds from the first event.
    pub fn open(url: &str, schema: &str) -> Result<Store, Error> {
        let db = if url == "memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(url.strip_prefix("sqlite:").unwrap_or(url))?
        };
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA secure_delete = ON;
             CREATE TABLE IF NOT EXISTS data (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (ns, key));
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )?;
        // A crash after a batch that erased a subject, before the log was emptied, left the
        // removed values' old pages in it: empty it before anything else.
        truncate(&db)?;
        let meta = |key: &str| -> Result<Option<String>, rusqlite::Error> {
            db.query_row("SELECT value FROM meta WHERE key = ?", [key], |r| r.get(0))
                .optional()
        };
        if meta("schema")?.as_deref() != Some(schema) {
            db.execute_batch(
                "BEGIN; DELETE FROM data; DELETE FROM meta WHERE key LIKE 'position:%';",
            )?;
            db.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')",
                [schema],
            )?;
            db.execute_batch("COMMIT")?;
        }
        let committed = meta("position")?.and_then(|p| p.parse().ok()).unwrap_or(0);
        Ok(Store {
            db: Arc::new(Mutex::new(Some(db))),
            committed: Arc::new(AtomicU64::new(committed)),
            turns: Arc::default(),
        })
    }

    /// The position up to which the stored data is complete.
    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::SeqCst)
    }

    /// The position up to which the data is complete for the events of partition `part`
    ///: every partition has its own positions. `""`: a kernel without partitions.
    pub fn committed_for(&self, part: &str) -> Result<u64, Error> {
        if part.is_empty() {
            return Ok(self.committed());
        }
        let value: Option<String> = self.with(|db| {
            db.query_row(
                "SELECT value FROM meta WHERE key = ?",
                [format!("position:{part}")],
                |r| r.get(0),
            )
            .optional()
        })?;
        Ok(value.and_then(|v| v.parse().ok()).unwrap_or(0))
    }

    /// Waits for the store's turn: one batch at a time, of whichever partition.
    pub(crate) async fn turn(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.turns).lock_owned().await
    }

    /// `f` on the connection; [`Error::Store`] if the store is closed.
    fn with<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, Error> {
        let db = self.db.lock().unwrap();
        let db = db.as_ref().ok_or_else(|| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("the store is closed".into()),
            )
        })?;
        Ok(f(db)?)
    }

    /// The value under `key` in namespace `ns` as a `T`; `None` if there is none.
    pub fn get<T: DeserializeOwned>(&self, ns: &str, key: &str) -> Result<Option<T>, Error> {
        let value: Option<String> = self.with(|db| {
            db.query_row(
                "SELECT value FROM data WHERE ns = ? AND key = ?",
                [ns, key],
                |r| r.get(0),
            )
            .optional()
        })?;
        Ok(value.map(|v| serde_json::from_str(&v)).transpose()?)
    }

    /// Stores `value` as JSON under `key` in namespace `ns`, replacing a value there.
    pub fn set<T: Serialize + ?Sized>(&self, ns: &str, key: &str, value: &T) -> Result<(), Error> {
        let value = serde_json::to_string(value)?;
        self.with(|db| {
            db.execute(
                "INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)",
                params![ns, key, value],
            )
        })?;
        Ok(())
    }

    /// Removes the value under `key` in namespace `ns`, if there is one.
    pub fn remove(&self, ns: &str, key: &str) -> Result<(), Error> {
        self.with(|db| db.execute("DELETE FROM data WHERE ns = ? AND key = ?", [ns, key]))?;
        Ok(())
    }

    /// Every key and value of a namespace, ordered by key.
    pub fn all<T: DeserializeOwned>(&self, ns: &str) -> Result<Vec<(String, T)>, Error> {
        let rows = self.with(|db| {
            db.prepare("SELECT key, value FROM data WHERE ns = ? ORDER BY key")?
                .query_map([ns], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()
        })?;
        rows.into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_str(&v)?)))
            .collect()
    }

    /// Clears a namespace, or all data.
    pub fn reset(&self, ns: Option<&str>) -> Result<(), Error> {
        self.with(|db| match ns {
            Some(ns) => db.execute("DELETE FROM data WHERE ns = ?", [ns]),
            None => db.execute("DELETE FROM data", []),
        })?;
        Ok(())
    }

    /// Starts a batch, dropping what is left of one that never ended (its future was dropped).
    pub(crate) fn begin(&self) -> Result<(), Error> {
        self.with(|db| {
            if !db.is_autocommit() {
                db.execute_batch("ROLLBACK")?;
            }
            db.execute_batch("BEGIN")
        })
    }

    /// Ends a batch: its data and `position` (of partition `part`, see [`Store::committed_for`]) take effect together. After a batch that
    /// `erased` a subject, the write-ahead log is emptied too: it still holds the pages from
    /// before the handlers removed the subject's data.
    pub(crate) fn commit_part(&self, position: u64, erased: bool, part: &str) -> Result<(), Error> {
        self.with(|db| {
            if part.is_empty() {
                db.execute(
                    "UPDATE meta SET value = ? WHERE key = 'position'",
                    [position.to_string()],
                )?;
            } else {
                db.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                    [format!("position:{part}"), position.to_string()],
                )?;
            }
            db.execute_batch("COMMIT")?;
            if erased {
                truncate(db)?;
            }
            Ok(())
        })?;
        if part.is_empty() {
            self.committed.store(position, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Marks where an event's writes begin within a batch, so they can be dropped alone.
    pub(crate) fn savepoint(&self) -> Result<(), Error> {
        self.with(|db| db.execute_batch("SAVEPOINT event"))
    }

    /// Keeps the event's writes (see [`Store::savepoint`]).
    pub(crate) fn release(&self) -> Result<(), Error> {
        self.with(|db| db.execute_batch("RELEASE event"))
    }

    /// Drops the event's writes, keeping the rest of the batch (see [`Store::savepoint`]).
    pub(crate) fn rollback_event(&self) -> Result<(), Error> {
        self.with(|db| db.execute_batch("ROLLBACK TO event; RELEASE event"))
    }

    /// Drops a failed batch.
    pub(crate) fn rollback(&self) {
        let _ = self.with(|db| {
            if !db.is_autocommit() {
                db.execute_batch("ROLLBACK")?;
            }
            Ok(())
        });
    }

    /// Closes the store for every handle to it: a batch still open is dropped, the
    /// write-ahead log emptied into the file (so data removed is gone from disk too), and the
    /// file released. Using it afterwards is an [`Error::Store`]. A listener closes its store
    /// when its `run()` ends; a later run opens it again.
    pub fn close(&self) {
        if let Some(db) = self.db.lock().unwrap().take() {
            if !db.is_autocommit() {
                let _ = db.execute_batch("ROLLBACK");
            }
            let _ = truncate(&db);
            let _ = db.close();
        }
    }
}

/// Moves the write-ahead log into the database file and truncates it to zero bytes.
fn truncate(db: &Connection) -> rusqlite::Result<()> {
    db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
}
