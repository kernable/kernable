//! The snapshot store: the latest snapshot per aggregate, kept apart from the event log. Snapshots are a cache that workers rebuild from the events at any time. Unlike the
//! log, they are replaced and deleted for real, so state with personal values does not outlive
//! an erasure.
//!
//! Backends, selected by URL: `sqlite:<path>` (or a bare path) and `memory:`. A SQLite file of
//! another snapshot format is emptied: it holds nothing that cannot be rebuilt.

use std::{
    collections::HashMap,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use kernable_core::proto::{Signature, Snapshot, SnapshotRecord};
use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    Result, StreamId,
    sqlite::{bytes_at, readable},
};

/// The version of the SQLite snapshot table; a file of another version is emptied.
const FORMAT: u32 = 2;

pub trait SnapshotStore: Send + Sync {
    /// The latest snapshot of one aggregate.
    fn get(&self, stream: &StreamId) -> Result<Option<SnapshotRecord>>;

    /// Replaces the aggregate's snapshot.
    fn save(&self, record: &SnapshotRecord) -> Result<()>;

    /// Deletes the aggregate's snapshot, if any.
    fn remove(&self, stream: &StreamId) -> Result<()>;
}

/// Opens a snapshot store: `sqlite:<path>`, a bare SQLite path, or `memory:`.
pub fn open(url: &str) -> Result<Box<dyn SnapshotStore>> {
    Ok(match crate::backend(url) {
        ("memory", _) => Box::new(MemorySnapshots::default()),
        ("file", _) => {
            return Err(crate::Error::Corrupt(
                "snapshots cannot live in a file log: use sqlite: or memory:".into(),
            ));
        }
        (_, path) => Box::new(SqliteSnapshots::open(path)?),
    })
}

fn stream(record: &SnapshotRecord) -> StreamId {
    StreamId {
        tenant_id: record.tenant_id.clone(),
        domain: record.domain.clone(),
        aggregate_type: record.aggregate_type.clone(),
        aggregate_id: record.aggregate_id.clone(),
    }
}

#[derive(Default)]
pub struct MemorySnapshots(Mutex<HashMap<StreamId, SnapshotRecord>>);

impl MemorySnapshots {
    fn map(&self) -> MutexGuard<'_, HashMap<StreamId, SnapshotRecord>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SnapshotStore for MemorySnapshots {
    fn get(&self, stream: &StreamId) -> Result<Option<SnapshotRecord>> {
        Ok(self.map().get(stream).cloned())
    }

    fn save(&self, record: &SnapshotRecord) -> Result<()> {
        self.map().insert(stream(record), record.clone());
        Ok(())
    }

    fn remove(&self, stream: &StreamId) -> Result<()> {
        self.map().remove(stream);
        Ok(())
    }
}

pub struct SqliteSnapshots(Mutex<Connection>);

impl SqliteSnapshots {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        // Deleted snapshots are overwritten on disk, not only unlinked.
        conn.pragma_update(None, "secure_delete", "ON")?;
        let format: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if format != FORMAT {
            conn.execute_batch("DROP TABLE IF EXISTS snapshots")?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS snapshots (
                tenant_id      TEXT    NOT NULL,
                domain         TEXT    NOT NULL,
                aggregate_type TEXT    NOT NULL,
                aggregate_id   TEXT    NOT NULL,
                version        INTEGER NOT NULL, -- the aggregate's version it was taken at
                schema         TEXT    NOT NULL, -- format of the state (the aggregate's schema)
                state          ANY     NOT NULL, -- TEXT when UTF-8 (JSON from every SDK), else BLOB
                key_id         TEXT    NOT NULL, -- the worker key that signed it
                sig            BLOB    NOT NULL,
                event_hash     BLOB    NOT NULL, -- hash of the stream's event at `version`
                created_at     INTEGER NOT NULL, -- ms since epoch
                PRIMARY KEY (tenant_id, domain, aggregate_type, aggregate_id)
            ) STRICT",
        )?;
        conn.pragma_update(None, "user_version", FORMAT)?;
        Ok(SqliteSnapshots(Mutex::new(conn)))
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SnapshotStore for SqliteSnapshots {
    fn get(&self, s: &StreamId) -> Result<Option<SnapshotRecord>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT version, schema, state, key_id, sig, event_hash FROM snapshots
                 WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4",
                params![s.tenant_id, s.domain, s.aggregate_type, s.aggregate_id],
                |r| {
                    Ok(SnapshotRecord {
                        tenant_id: s.tenant_id.clone(),
                        domain: s.domain.clone(),
                        aggregate_type: s.aggregate_type.clone(),
                        aggregate_id: s.aggregate_id.clone(),
                        snapshot: Some(Snapshot {
                            version: r.get(0)?,
                            schema: r.get(1)?,
                            state: bytes_at(r, 2)?,
                            worker_sig: Some(Signature {
                                key_id: r.get(3)?,
                                sig: r.get(4)?,
                            }),
                        }),
                        event_hash: r.get(5)?,
                    })
                },
            )
            .optional()?)
    }

    fn save(&self, r: &SnapshotRecord) -> Result<()> {
        let snapshot = r.snapshot.clone().unwrap_or_default();
        let sig = snapshot.worker_sig.clone().unwrap_or_default();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.conn().execute(
            "INSERT OR REPLACE INTO snapshots (tenant_id, domain, aggregate_type, aggregate_id,
                 version, schema, state, key_id, sig, event_hash, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                r.tenant_id,
                r.domain,
                r.aggregate_type,
                r.aggregate_id,
                snapshot.version,
                snapshot.schema,
                readable(&snapshot.state),
                sig.key_id,
                sig.sig,
                r.event_hash,
                now
            ],
        )?;
        Ok(())
    }

    fn remove(&self, s: &StreamId) -> Result<()> {
        self.conn().execute(
            "DELETE FROM snapshots
             WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4",
            params![s.tenant_id, s.domain, s.aggregate_type, s.aggregate_id],
        )?;
        Ok(())
    }
}
