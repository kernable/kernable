//! The snapshot store: the latest snapshot per aggregate and deployment (the code version of the
//! worker that made it), kept apart from the event log.
//! Snapshots are a cache that workers rebuild from the events at any time. Unlike the
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
const FORMAT: u32 = 1;

pub trait SnapshotStore: Send + Sync {
    /// The latest snapshot of one aggregate made by `deployment` (`""`: by workers without one).
    fn get(&self, stream: &StreamId, deployment: &str) -> Result<Option<SnapshotRecord>>;

    /// Replaces the aggregate's snapshot of the record's deployment.
    fn save(&self, record: &SnapshotRecord) -> Result<()>;

    /// Deletes the aggregate's snapshots of every deployment, if any.
    fn remove(&self, stream: &StreamId) -> Result<()>;

    /// Deletes every snapshot `deployment` made; returns how many.
    fn drop_deployment(&self, deployment: &str) -> Result<usize>;

    /// Every snapshot without its state, for the UI.
    fn infos(&self) -> Result<Vec<SnapshotInfo>>;
}

/// A snapshot as the UI shows it: never its state, which may hold personal values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub stream: StreamId,
    /// The aggregate's version it was taken at.
    pub version: u64,
    pub schema: String,
    /// The state's length.
    pub bytes: u64,
    /// When it was taken (ms); 0 when the store does not keep it.
    pub created_at: i64,
    /// The deployment of the worker that made it.
    pub deployment: String,
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

type Snapshots = HashMap<(StreamId, String), SnapshotRecord>;

#[derive(Default)]
pub struct MemorySnapshots(Mutex<Snapshots>);

impl MemorySnapshots {
    fn map(&self) -> MutexGuard<'_, Snapshots> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SnapshotStore for MemorySnapshots {
    fn get(&self, stream: &StreamId, deployment: &str) -> Result<Option<SnapshotRecord>> {
        Ok(self
            .map()
            .get(&(stream.clone(), deployment.to_owned()))
            .cloned())
    }

    fn save(&self, record: &SnapshotRecord) -> Result<()> {
        self.map()
            .insert((stream(record), record.deployment.clone()), record.clone());
        Ok(())
    }

    fn remove(&self, stream: &StreamId) -> Result<()> {
        self.map().retain(|(s, _), _| s != stream);
        Ok(())
    }

    fn drop_deployment(&self, deployment: &str) -> Result<usize> {
        let mut map = self.map();
        let before = map.len();
        map.retain(|(_, d), _| d != deployment);
        Ok(before - map.len())
    }

    fn infos(&self) -> Result<Vec<SnapshotInfo>> {
        Ok(self
            .map()
            .iter()
            .filter_map(|((stream, _), r)| {
                let s = r.snapshot.as_ref()?;
                Some(SnapshotInfo {
                    stream: stream.clone(),
                    version: s.version,
                    schema: s.schema.clone(),
                    bytes: s.state.len() as u64,
                    created_at: 0,
                    deployment: r.deployment.clone(),
                })
            })
            .collect())
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
                deployment     TEXT    NOT NULL, -- the code version of the worker that made it
                PRIMARY KEY (tenant_id, domain, aggregate_type, aggregate_id, deployment)
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
    fn get(&self, s: &StreamId, deployment: &str) -> Result<Option<SnapshotRecord>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT version, schema, state, key_id, sig, event_hash FROM snapshots
                 WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4
                   AND deployment = ?5",
                params![
                    s.tenant_id,
                    s.domain,
                    s.aggregate_type,
                    s.aggregate_id,
                    deployment
                ],
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
                        deployment: deployment.to_owned(),
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
                 version, schema, state, key_id, sig, event_hash, created_at, deployment)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
                now,
                r.deployment
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

    fn drop_deployment(&self, deployment: &str) -> Result<usize> {
        Ok(self
            .conn()
            .execute("DELETE FROM snapshots WHERE deployment = ?1", [deployment])?)
    }

    fn infos(&self) -> Result<Vec<SnapshotInfo>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT tenant_id, domain, aggregate_type, aggregate_id, version, schema,
                    length(state), created_at, deployment FROM snapshots",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(SnapshotInfo {
                stream: StreamId {
                    tenant_id: r.get(0)?,
                    domain: r.get(1)?,
                    aggregate_type: r.get(2)?,
                    aggregate_id: r.get(3)?,
                },
                version: r.get::<_, i64>(4)? as u64,
                schema: r.get(5)?,
                bytes: r.get::<_, i64>(6)? as u64,
                created_at: r.get(7)?,
                deployment: r.get(8)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}
