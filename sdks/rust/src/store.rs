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

/// A listener's store; see [`crate::Listener::store`]. Cloning gives another handle to it.
#[derive(Clone)]
pub struct Store {
    db: Arc<Mutex<Connection>>,
    committed: Arc<AtomicU64>,
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
        let meta = |key: &str| -> Result<Option<String>, rusqlite::Error> {
            db.query_row("SELECT value FROM meta WHERE key = ?", [key], |r| r.get(0))
                .optional()
        };
        if meta("schema")?.as_deref() != Some(schema) {
            db.execute_batch("BEGIN; DELETE FROM data;")?;
            db.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')",
                [schema],
            )?;
            db.execute_batch("COMMIT")?;
        }
        let committed = meta("position")?.and_then(|p| p.parse().ok()).unwrap_or(0);
        Ok(Store {
            db: Arc::new(Mutex::new(db)),
            committed: Arc::new(AtomicU64::new(committed)),
        })
    }

    /// The position up to which the stored data is complete.
    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::SeqCst)
    }

    pub fn get<T: DeserializeOwned>(&self, ns: &str, key: &str) -> Result<Option<T>, Error> {
        let db = self.db.lock().unwrap();
        let value: Option<String> = db
            .query_row(
                "SELECT value FROM data WHERE ns = ? AND key = ?",
                [ns, key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(value.map(|v| serde_json::from_str(&v)).transpose()?)
    }

    pub fn set<T: Serialize + ?Sized>(&self, ns: &str, key: &str, value: &T) -> Result<(), Error> {
        let value = serde_json::to_string(value)?;
        self.db.lock().unwrap().execute(
            "INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)",
            params![ns, key, value],
        )?;
        Ok(())
    }

    pub fn remove(&self, ns: &str, key: &str) -> Result<(), Error> {
        self.db
            .lock()
            .unwrap()
            .execute("DELETE FROM data WHERE ns = ? AND key = ?", [ns, key])?;
        Ok(())
    }

    /// Every key and value of a namespace, ordered by key.
    pub fn all<T: DeserializeOwned>(&self, ns: &str) -> Result<Vec<(String, T)>, Error> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare("SELECT key, value FROM data WHERE ns = ? ORDER BY key")?;
        let rows = statement
            .query_map([ns], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_str(&v)?)))
            .collect()
    }

    /// Clears a namespace, or all data.
    pub fn reset(&self, ns: Option<&str>) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        match ns {
            Some(ns) => db.execute("DELETE FROM data WHERE ns = ?", [ns])?,
            None => db.execute("DELETE FROM data", [])?,
        };
        Ok(())
    }

    /// Starts a batch.
    pub(crate) fn begin(&self) -> Result<(), Error> {
        Ok(self.db.lock().unwrap().execute_batch("BEGIN")?)
    }

    /// Ends a batch: its data and `position` take effect together.
    pub(crate) fn commit(&self, position: u64) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE meta SET value = ? WHERE key = 'position'",
            [position.to_string()],
        )?;
        db.execute_batch("COMMIT")?;
        self.committed.store(position, Ordering::SeqCst);
        Ok(())
    }

    /// Drops a failed batch.
    pub(crate) fn rollback(&self) {
        let db = self.db.lock().unwrap();
        if !db.is_autocommit() {
            let _ = db.execute_batch("ROLLBACK");
        }
    }
}
