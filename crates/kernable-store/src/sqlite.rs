//! SQLite backend (`sqlite:<path>`).
//!
//! All writes run in `BEGIN IMMEDIATE` transactions, so several stores on the same
//! file (threads or processes) stay consistent; SQLite serializes them as a single writer.
//! Tables are `STRICT`, so stored values always have the declared types.
//! Events form a hash chain anchored by kernel-signed checkpoints (see `proto/SIGNING.md`).
//! While replication is on ([`Store::keep_record_log`]), every write also files its record in
//! the table `replication`, in the same transaction (see [`crate::replication`]).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        AggregateIndex, Attribute, CheckpointRecord, Command, CommandResult, Committed,
        CommittedRecord, DataChangedRecord, EnqueuedRecord, Event, GlobalKeyRecord, Grant,
        ListenerRecord, LogRecord, LoggedEvent, Membership, MembershipRecord, Middleware, NewEvent,
        ParkedEvent, PartitionMap, Rejected, RejectedRecord, ReplicatedRecord,
        ReplicationHeadRecord, RequestContext, Requirement, RevocationRecord, Signature,
        StreamIndex, TenantImportedRecord, TenantIndexRecord, When, WorkerRecord,
        command_result::Outcome, log_record::Record,
    },
    signing::{self, GENESIS},
};
use prost::Message;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};

use crate::{
    Checkpoint, Durability, Enqueued, Error, FORMAT, Head, Pending, Result, Store, StreamId,
    StreamSummary,
    replication::{self, diverged},
    sync::Syncer,
    verify::{Report, Verifier},
};

/// The record log, created when replication is turned on and dropped when it is turned off.
/// `replication_state` holds where the log starts: 0 for a log that starts with the store, or
/// the record that started it (a `ReplicationHead` row) or that pruning kept.
const LOG_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS replication (
    seq    INTEGER PRIMARY KEY,
    hash   BLOB NOT NULL,
    record BLOB NOT NULL -- the LogRecord
) STRICT;
CREATE TABLE IF NOT EXISTS replication_state (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    base INTEGER NOT NULL
) STRICT;
";

/// Where the record log stands; `on` false while replication is off.
#[derive(Default)]
struct RecordLog {
    on: bool,
    head: Head,
    base: u64,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT    NOT NULL,
    domain          TEXT    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    aggregate_id    TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    schema          INTEGER NOT NULL,
    payload         ANY     NOT NULL, -- TEXT when UTF-8 (JSON from every SDK), else BLOB
    command_id      TEXT    NOT NULL,
    created_at      INTEGER NOT NULL,
    producer_key_id TEXT    NOT NULL,
    producer_sig    BLOB    NOT NULL,
    context         TEXT,             -- RequestContext of the command, as JSON
    vault_refs      TEXT    NOT NULL DEFAULT '', -- comma-separated
    erases          TEXT    NOT NULL DEFAULT '',
    prev_hash       BLOB    NOT NULL,
    hash            BLOB    NOT NULL,
    origin_partition TEXT   NOT NULL DEFAULT '', -- a moved event: where it was first committed
    origin_position INTEGER NOT NULL DEFAULT 0,
    UNIQUE (tenant_id, domain, aggregate_type, aggregate_id, version)
) STRICT;
CREATE TABLE IF NOT EXISTS commands (
    command_id       TEXT PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    domain           TEXT    NOT NULL,
    aggregate_type   TEXT    NOT NULL,
    aggregate_id     TEXT    NOT NULL,
    command_type     TEXT    NOT NULL,
    payload          ANY     NOT NULL, -- TEXT when UTF-8 (JSON from every SDK), else BLOB
    client_key_id    TEXT,
    client_sig       BLOB,
    context          TEXT,             -- RequestContext, as JSON
    vault_refs       TEXT    NOT NULL DEFAULT '', -- comma-separated
    status           TEXT    NOT NULL, -- pending | committed | rejected
    positions        TEXT    NOT NULL DEFAULT '', -- committed: its events' global positions, comma-separated
    rejected_code    TEXT,             -- rejected: why
    rejected_message TEXT,
    rejected_key     TEXT,             -- unique_taken: the key's name
    rejected_requirement BLOB,         -- requirement_missing: the Requirement, protobuf
    created_at       INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS commands_pending ON commands (status, created_at);
-- Every event type stored per aggregate type, with its highest schema, kept by commit: startup
-- and the overviews need not read every event.
CREATE TABLE IF NOT EXISTS event_types (
    domain         TEXT    NOT NULL,
    aggregate_type TEXT    NOT NULL,
    event_type     TEXT    NOT NULL,
    schema         INTEGER NOT NULL,
    PRIMARY KEY (domain, aggregate_type, event_type)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS checkpoints (
    global_position INTEGER PRIMARY KEY,
    chain_hash      BLOB    NOT NULL,
    kernel_key_id   TEXT    NOT NULL,
    kernel_sig      BLOB    NOT NULL,
    created_at      INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS revocations (
    key_id        TEXT PRIMARY KEY,
    since         INTEGER NOT NULL, -- from then on its signatures are suspect (ms since epoch)
    created_at    INTEGER NOT NULL,
    kernel_key_id TEXT    NOT NULL,
    kernel_sig    BLOB    NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS listeners (
    name        TEXT PRIMARY KEY,
    key_id      TEXT    NOT NULL, -- only this key (or its successor) may resume it
    ordering    INTEGER NOT NULL, -- delivery order: lower first
    event_types TEXT    NOT NULL, -- comma-separated patterns; empty: all the key may read
    position    INTEGER NOT NULL, -- last acknowledged global position
    service     TEXT    NOT NULL, -- the service it belongs to; empty: none
    last_seen   INTEGER NOT NULL  -- ms since epoch: last connected, disconnected or running at a flush
) STRICT;
CREATE TABLE IF NOT EXISTS middlewares (
    listener TEXT    NOT NULL REFERENCES listeners (name) ON DELETE CASCADE,
    phase    TEXT    NOT NULL, -- before | after
    ordering INTEGER NOT NULL, -- position in the chain: lower first
    types    TEXT    NOT NULL, -- comma-separated patterns of command and query types
    PRIMARY KEY (listener, phase)
) STRICT;
CREATE TABLE IF NOT EXISTS parked (
    listener   TEXT    NOT NULL REFERENCES listeners (name) ON DELETE CASCADE,
    position   INTEGER NOT NULL, -- global position of the event it gave up on
    event_type TEXT    NOT NULL, -- qualified
    code       TEXT    NOT NULL, -- a rejection code, or handler_error
    message    TEXT    NOT NULL, -- the rejection's message, or only the error's type
    attempts   INTEGER NOT NULL,
    parked_at  INTEGER NOT NULL, -- ms since epoch
    retry      INTEGER NOT NULL, -- 1: to be delivered again
    PRIMARY KEY (listener, position)
) STRICT;
CREATE TABLE IF NOT EXISTS workers (
    aggregate_type TEXT PRIMARY KEY, -- qualified: shop.Cart
    key_id         TEXT    NOT NULL, -- only this key (or its successor) may run it
    command_types  TEXT    NOT NULL, -- comma-separated qualified command types
    service        TEXT    NOT NULL, -- the service it belongs to; empty: none
    last_seen      INTEGER NOT NULL  -- ms since epoch: a worker for it last connected or disconnected
) STRICT;
CREATE TABLE IF NOT EXISTS memberships (
    service     TEXT PRIMARY KEY,
    workers     TEXT    NOT NULL, -- comma-separated qualified aggregate types
    listeners   TEXT    NOT NULL, -- comma-separated listener names
    middlewares TEXT    NOT NULL, -- comma-separated names of listeners that check requests
    metadata    TEXT    NOT NULL, -- JSON array of [key, value], in the signed order
    issued_at   INTEGER NOT NULL, -- ms since epoch, signed
    key_id      TEXT    NOT NULL, -- the key that applied it (granted manage)
    sig         BLOB    NOT NULL, -- its signature over kernable.v1.membership
    applied_at  INTEGER NOT NULL  -- ms since epoch
) STRICT;
CREATE TABLE IF NOT EXISTS grants (
    key_id    TEXT PRIMARY KEY, -- the grantee
    issued_at INTEGER NOT NULL, -- ms since epoch, signed
    root_id   TEXT    NOT NULL, -- the root key that signed it
    grant     BLOB    NOT NULL  -- the Grant message as presented, with its root signature
) STRICT;
CREATE TABLE IF NOT EXISTS aggregate_index (
    tenant_id      TEXT    NOT NULL,
    domain         TEXT    NOT NULL,
    aggregate_type TEXT    NOT NULL,
    aggregate_id   TEXT    NOT NULL,
    record         BLOB    NOT NULL, -- the AggregateIndex message; a stream holding nothing has no row
    personal       INTEGER NOT NULL, -- 1: it holds a personal key
    PRIMARY KEY (tenant_id, domain, aggregate_type, aggregate_id)
) STRICT;
CREATE INDEX IF NOT EXISTS aggregate_index_personal ON aggregate_index (personal) WHERE personal = 1;
-- The holder of every unique key that is not personal, by the key's identity, kept with
-- aggregate_index: a commit looks keys up here instead of keeping them all in memory.
CREATE TABLE IF NOT EXISTS unique_keys (
    id             BLOB PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    domain         TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL
) STRICT, WITHOUT ROWID;
";

const EVENT_COLUMNS: &str = "global_position, tenant_id, aggregate_type, aggregate_id, version, \
                             event_type, payload, command_id, created_at, \
                             producer_key_id, producer_sig, hash, context, domain, schema, \
                             vault_refs, erases, origin_partition, origin_position";

const COMMAND_COLUMNS: &str = "command_id, tenant_id, aggregate_type, aggregate_id, command_type, \
                               payload, client_key_id, client_sig, context, domain, vault_refs";

pub struct SqliteStore {
    conn: Mutex<Connection>,
    /// Connections that only read events, so a history read never waits for a commit nor
    /// holds one up (in WAL mode readers and the writer do not block each other). `None` for a
    /// database in memory, which has no second connection.
    readers: Option<Readers>,
    /// `<path>.lock`, held exclusively while a writer has the store open: the queue lives in
    /// one kernel's memory, so a second kernel on the same store would run commands twice.
    _lock: Option<std::fs::File>,
    /// The write-ahead log to flush in [`Store::sync`]; `None` unless [`Durability::Full`].
    wal: Option<std::path::PathBuf>,
    syncer: Syncer,
    /// Locked only while `conn` is, so writes number their records in commit order.
    log: Mutex<RecordLog>,
}

impl SqliteStore {
    /// Opens the store with [`Durability::Full`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Durability::Full)
    }

    /// Opens the store for writing, as the only writer: refused while another process holds
    /// it (see [`SqliteStore::open_unlocked`] for readers).
    pub fn open_with(path: impl AsRef<Path>, durability: Durability) -> Result<Self> {
        let path = path.as_ref();
        let lock = if path.as_os_str() == ":memory:" {
            None
        } else {
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(format!("{}.lock", path.display()))?;
            lock.try_lock().map_err(|_| {
                Error::Io(std::io::Error::other(
                    "store is in use by another process (a running kernel?)",
                ))
            })?;
            Some(lock)
        };
        let mut store = Self::configure(path, durability)?;
        let on_disk = lock.is_some();
        store._lock = lock;
        if on_disk {
            store.readers = Some(Readers::new(path));
        }
        if durability == Durability::Full && on_disk {
            store.wal = Some(std::path::PathBuf::from(format!("{}-wal", path.display())));
        }
        Ok(store)
    }

    /// A numeric setting of this connection (`synchronous`, `fullfsync`), for tests.
    pub fn pragma(&self, name: &str) -> Result<i64> {
        Ok(self.conn().pragma_query_value(None, name, |r| r.get(0))?)
    }

    /// Opens the store without taking the writer's lock, for inspection next to a running
    /// kernel (`kernable verify`). SQLite keeps the reads consistent.
    pub fn open_unlocked(path: impl AsRef<Path>) -> Result<Self> {
        Self::configure(path.as_ref(), Durability::Normal)
    }

    fn configure(path: &Path, durability: Durability) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // A commit writes the WAL without a flush; `Store::sync` flushes it once for every
        // commit waiting (group commit), and SQLite flushes before and after it copies the WAL
        // into the database. fullfsync makes those real flushes on macOS, where fsync alone
        // may leave the data in the disk's cache.
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        if durability == Durability::Full {
            conn.pragma_update(None, "fullfsync", true)?;
            conn.pragma_update(None, "checkpoint_fullfsync", true)?;
        }
        conn.busy_timeout(Duration::from_secs(5))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    /// Creates the tables of a new database; refuses one of another data format.
    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "foreign_keys", true)?;
        let format: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let tables: u32 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        match (format, tables) {
            (0, 0) => {
                // One transaction: a store killed while it is created has either no tables or
                // every table with its format, never tables without a format (a "format 0"
                // store that would never open again).
                conn.execute_batch(&format!(
                    "BEGIN IMMEDIATE; {SCHEMA}; PRAGMA user_version = {FORMAT}; COMMIT;"
                ))?;
            }
            (FORMAT, _) => {}
            (found, _) => return Err(Error::Format(found)),
        }
        let log = load_log(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            readers: None,
            _lock: None,
            wal: None,
            syncer: Syncer::default(),
            log: Mutex::new(log),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn log(&self) -> MutexGuard<'_, RecordLog> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Files `records` in the record log within `tx`, if it is on; returns the head to take
    /// once `tx` commits ([`SqliteStore::advance`]). Call with `conn` locked.
    fn file(&self, tx: &Connection, records: impl FnOnce() -> Vec<Record>) -> Result<Option<Head>> {
        let mut head = {
            let log = self.log();
            if !log.on {
                return Ok(None);
            }
            log.head.clone()
        };
        let mut insert =
            tx.prepare_cached("INSERT INTO replication (seq, hash, record) VALUES (?1, ?2, ?3)")?;
        for record in records() {
            let body = LogRecord {
                record: Some(record),
            }
            .encode_to_vec();
            let seq = head.seq + 1;
            let hash = replication::record_hash(&head.hash, seq, &replication::digest(&body));
            insert.execute(params![seq, hash.to_vec(), body])?;
            head = Head {
                seq,
                hash: hash.to_vec(),
            };
        }
        Ok(Some(head))
    }

    /// Takes the head [`SqliteStore::file`] returned, after its transaction committed.
    fn advance(&self, head: Option<Head>) {
        if let Some(head) = head {
            self.log().head = head;
        }
    }

    /// Runs `read` on a reader, or on the writer's connection without readers.
    fn read<T>(&self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        match &self.readers {
            Some(readers) => readers.with(read),
            None => read(&self.conn()),
        }
    }
}

/// Read-only connections to one database, opened as needed and kept for reuse.
struct Readers {
    path: PathBuf,
    idle: Mutex<Vec<Connection>>,
}

impl Readers {
    /// Idle connections kept; more are opened while more reads run at once, and closed after.
    const KEEP: usize = 8;

    fn new(path: &Path) -> Self {
        Readers {
            path: path.to_owned(),
            idle: Mutex::default(),
        }
    }

    fn with<T>(&self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let conn = match idle {
            Some(conn) => conn,
            None => {
                let conn = Connection::open_with_flags(
                    &self.path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                conn.busy_timeout(Duration::from_secs(5))?;
                conn
            }
        };
        let result = read(&conn);
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        if idle.len() < Self::KEEP {
            idle.push(conn);
        }
        result
    }
}

impl Store for SqliteStore {
    // ---- events ----

    fn read_stream_after(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<Event>> {
        self.read(|conn| {
            read_events(
                conn,
                "WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4
                   AND version > ?5
                 ORDER BY version",
                params![
                    tenant_id,
                    domain,
                    aggregate_type,
                    aggregate_id,
                    after_version
                ],
            )
        })
    }

    /// Up to `limit` events with a global position greater than `after`, in order.
    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        self.read(|conn| {
            read_events(
                conn,
                "WHERE global_position > ?1 ORDER BY global_position LIMIT ?2",
                params![after, limit],
            )
        })
    }

    fn head_position(&self) -> Result<u64> {
        Ok(self.conn().query_row(
            "SELECT COALESCE(MAX(global_position), 0) FROM events",
            [],
            |r| r.get(0),
        )?)
    }

    // ---- command queue ----

    /// Persists a command as pending. Idempotent on `command_id`.
    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !enqueue_tx(&tx, cmd, now_ms)? {
            let result = stored_result(&tx, &cmd.command_id)?;
            return Ok(Enqueued::Duplicate { result });
        }
        let head = self.file(&tx, || {
            vec![Record::Enqueued(EnqueuedRecord {
                command: Some(cmd.clone()),
                created_at: now_ms,
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(Enqueued::New)
    }

    /// All commands not completed yet, in submission order.
    fn pending(&self) -> Result<Vec<Pending>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {COMMAND_COLUMNS}, created_at FROM commands WHERE status = 'pending'
             ORDER BY created_at, rowid"
        ))?;
        let rows = stmt.query_map([], |r| {
            Ok(Pending {
                command: command_from_row(r)?,
                received_at: r.get(11)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Appends `events` to the pending command's stream, extends the hash chain and
    /// completes the command, atomically. `producer_sig` is the worker's result signature;
    /// verifying it is up to the caller. `index`, if set, replaces what the stream holds.
    ///
    /// On `VersionConflict` the command stays pending, so the caller can retry with fresh history.
    fn commit(
        &self,
        command_id: &str,
        expected_version: u64,
        events: &[NewEvent],
        producer_sig: &Signature,
        index: Option<&AggregateIndex>,
        now_ms: i64,
    ) -> Result<Vec<Event>> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (stored, logged) = commit_tx(
            &tx,
            command_id,
            expected_version,
            events,
            producer_sig,
            index,
            now_ms,
        )?;
        let head = self.file(&tx, || {
            vec![Record::Committed(CommittedRecord {
                command_id: command_id.to_owned(),
                events: logged,
                index: index.cloned(),
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(stored)
    }

    fn stream_index(&self, stream: &StreamId) -> Result<Option<AggregateIndex>> {
        self.read(|conn| {
            stream_index(
                conn,
                params![
                    stream.tenant_id,
                    stream.domain,
                    stream.aggregate_type,
                    stream.aggregate_id
                ],
            )
        })
    }

    fn key_holder(&self, id: &[u8]) -> Result<Option<StreamId>> {
        self.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT tenant_id, domain, aggregate_type, aggregate_id FROM unique_keys
                     WHERE id = ?1",
                    [id],
                    |r| {
                        Ok(StreamId {
                            tenant_id: r.get(0)?,
                            domain: r.get(1)?,
                            aggregate_type: r.get(2)?,
                            aggregate_id: r.get(3)?,
                        })
                    },
                )
                .optional()?)
        })
    }

    fn holds_personal_keys(&self) -> Result<bool> {
        self.read(|conn| {
            Ok(conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM aggregate_index WHERE personal = 1)",
                [],
                |r| r.get(0),
            )?)
        })
    }

    fn index(&self) -> Result<Vec<(StreamId, AggregateIndex)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT tenant_id, domain, aggregate_type, aggregate_id, record FROM aggregate_index
             ORDER BY tenant_id, domain, aggregate_type, aggregate_id",
        )?;
        let rows = stmt.query_map([], |r| {
            let stream = StreamId {
                tenant_id: r.get(0)?,
                domain: r.get(1)?,
                aggregate_type: r.get(2)?,
                aggregate_id: r.get(3)?,
            };
            Ok((stream, r.get::<_, Vec<u8>>(4)?))
        })?;
        let mut index = Vec::new();
        for row in rows {
            let (stream, record) = row?;
            index.push((stream, AggregateIndex::decode(record.as_slice())?));
        }
        Ok(index)
    }

    fn streams(&self, limit: usize) -> Result<Vec<StreamSummary>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT tenant_id, domain, aggregate_type, aggregate_id, MAX(version), MAX(created_at)
             FROM events GROUP BY tenant_id, domain, aggregate_type, aggregate_id
             ORDER BY MAX(global_position) DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |r| {
            Ok(StreamSummary {
                stream: StreamId {
                    tenant_id: r.get(0)?,
                    domain: r.get(1)?,
                    aggregate_type: r.get(2)?,
                    aggregate_id: r.get(3)?,
                },
                version: r.get(4)?,
                last_at: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn event_sources(&self) -> Result<Vec<(String, String, String)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT domain, aggregate_type, event_type FROM event_types ORDER BY 1, 2, 3",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn sync(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        // Every row changed so far counts as a write the flush must cover.
        let changes = self.conn().total_changes();
        self.syncer.wrote_up_to(changes);
        self.syncer.sync(|| match std::fs::File::open(wal) {
            Ok(file) => file.sync_data(),
            // No WAL: nothing written since SQLite copied it into the synced database.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        })
    }

    fn command_ids(&self) -> Result<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT command_id FROM commands ORDER BY rowid")?;
        let ids = stmt
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(ids)
    }

    fn rejections(&self, limit: usize) -> Result<Vec<(Pending, Rejected)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {COMMAND_COLUMNS}, created_at, rejected_code, rejected_message, rejected_key,
                    rejected_requirement
             FROM commands WHERE status = 'rejected'
             ORDER BY created_at DESC, rowid DESC LIMIT ?1"
        ))?;
        let rows = stmt.query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |r| {
            let pending = Pending {
                command: command_from_row(r)?,
                received_at: r.get(11)?,
            };
            let code: Option<String> = r.get(12)?;
            let message: Option<String> = r.get(13)?;
            let key: Option<String> = r.get(14)?;
            let requirement: Option<Vec<u8>> = r.get(15)?;
            Ok((pending, code, message, key, requirement))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (pending, code, message, key, requirement) = row?;
            let requirement = requirement
                .map(|bytes| Requirement::decode(bytes.as_slice()).map(Box::new))
                .transpose()
                .map_err(|e| Error::Corrupt(format!("a stored requirement: {e}")))?;
            let reason = Rejected {
                code: code.unwrap_or_default(),
                message: message.unwrap_or_default(),
                key: key.unwrap_or_default(),
                requirement,
            };
            out.push((pending, reason));
        }
        Ok(out)
    }

    fn command(&self, command_id: &str) -> Result<Option<Pending>> {
        Ok(self
            .conn()
            .query_row(
                &format!(
                    "SELECT {COMMAND_COLUMNS}, created_at FROM commands WHERE command_id = ?1"
                ),
                [command_id],
                |r| {
                    Ok(Pending {
                        command: command_from_row(r)?,
                        received_at: r.get(11)?,
                    })
                },
            )
            .optional()?)
    }

    fn event_schemas(&self) -> Result<Vec<(String, u32)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT domain || '.' || event_type, MAX(schema) FROM event_types
             GROUP BY domain, event_type ORDER BY 1",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn import(&self, copies: &[crate::Copy]) -> Result<()> {
        use crate::Copy;
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut records = Vec::with_capacity(copies.len());
        for copy in copies {
            match copy {
                Copy::Enqueue {
                    command,
                    created_at,
                } => {
                    if enqueue_tx(&tx, command, *created_at)? {
                        records.push(Record::Enqueued(EnqueuedRecord {
                            command: Some(command.clone()),
                            created_at: *created_at,
                        }));
                    } else {
                        let tenant: String = tx.query_row(
                            "SELECT tenant_id FROM commands WHERE command_id = ?1",
                            [&command.command_id],
                            |r| r.get(0),
                        )?;
                        if tenant != command.tenant_id {
                            return Err(Error::Corrupt(format!(
                                "command {} has the ID of a command of another tenant here",
                                command.command_id
                            )));
                        }
                    }
                }
                Copy::Commit {
                    command_id,
                    expected_version,
                    events,
                    producer_sig,
                    index,
                    at,
                } => {
                    if stored_result(&tx, command_id)?.is_some() {
                        continue;
                    }
                    let commit = |version| {
                        commit_tx(
                            &tx,
                            command_id,
                            version,
                            events,
                            producer_sig,
                            index.as_ref(),
                            *at,
                        )
                    };
                    let (_, logged) = match commit(*expected_version) {
                        Err(Error::VersionConflict { actual, .. }) if events.is_empty() => {
                            commit(actual)?
                        }
                        other => other?,
                    };
                    records.push(Record::Committed(CommittedRecord {
                        command_id: command_id.clone(),
                        events: logged,
                        index: index.clone(),
                    }));
                }
                Copy::Reject { command_id, reason } => {
                    if stored_result(&tx, command_id)?.is_none() {
                        reject_tx(&tx, command_id, reason)?;
                        records.push(Record::Rejected(RejectedRecord {
                            command_id: command_id.clone(),
                            reason: Some(reason.clone()),
                        }));
                    }
                }
            }
        }
        let head = self.file(&tx, || records)?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        reject_tx(&tx, command_id, &reason)?;
        let head = self.file(&tx, || {
            vec![Record::Rejected(RejectedRecord {
                command_id: command_id.to_owned(),
                reason: Some(reason.clone()),
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    /// The final result of a command, once it is committed or rejected.
    fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        stored_result(&self.conn(), command_id)
    }

    // ---- listeners ----

    fn listeners(&self) -> Result<Vec<ListenerRecord>> {
        read_listeners(&self.conn())
    }

    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for l in listeners {
            save_listener_tx(&tx, l)?;
        }
        let head = self.file(&tx, || {
            listeners.iter().cloned().map(Record::Listener).collect()
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    // ---- workers ----

    fn workers(&self) -> Result<Vec<WorkerRecord>> {
        read_workers(&self.conn())
    }

    fn save_workers(&self, workers: &[WorkerRecord]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for w in workers {
            save_worker_tx(&tx, w)?;
        }
        let head = self.file(&tx, || {
            workers.iter().cloned().map(Record::Worker).collect()
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    // ---- memberships ----

    fn memberships(&self) -> Result<Vec<MembershipRecord>> {
        read_memberships(&self.conn())
    }

    fn save_membership(&self, record: &MembershipRecord) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_membership_tx(&tx, record)?;
        let head = self.file(&tx, || vec![Record::Membership(record.clone())])?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    // ---- integrity ----

    /// Signs the current head of the hash chain. Returns `None` while there are no events.
    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = tx
            .query_row(
                "SELECT global_position, hash FROM events ORDER BY global_position DESC LIMIT 1",
                [],
                |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        let Some((global_position, chain_hash)) = head else {
            return Ok(None);
        };
        if let Some(existing) = read_checkpoints(&tx, Some(global_position))?.pop() {
            return Ok(Some(existing));
        }
        let msg = signing::checkpoint_message(global_position, &chain_hash, now_ms);
        let record = CheckpointRecord {
            global_position,
            chain_hash,
            kernel_sig: Some(signing::sign(kernel, &msg)),
            created_at: now_ms,
        };
        insert_checkpoint(&tx, &record)?;
        let filed = self.file(&tx, || vec![Record::Checkpoint(record.clone())])?;
        tx.commit()?;
        self.advance(filed);
        Ok(Some(Checkpoint {
            global_position: record.global_position,
            chain_hash: record.chain_hash,
            kernel_sig: record.kernel_sig.unwrap_or_default(),
            created_at: record.created_at,
        }))
    }

    fn revoke(&self, revocation: &RevocationRecord) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !revoke_tx(&tx, revocation)? {
            return Ok(());
        }
        let head = self.file(&tx, || vec![Record::Revocation(revocation.clone())])?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    fn revocations(&self) -> Result<Vec<RevocationRecord>> {
        read_revocations(&self.conn())
    }

    fn grants(&self) -> Result<Vec<Grant>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT grant FROM grants ORDER BY key_id")?;
        let blobs = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        let mut grants = Vec::new();
        for blob in blobs {
            grants.push(Grant::decode(blob?.as_slice())?);
        }
        Ok(grants)
    }

    fn save_grant(&self, grant: &Grant) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !save_grant_tx(&tx, grant)? {
            return Ok(());
        }
        let head = self.file(&tx, || vec![Record::Grant(grant.clone())])?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    /// Recomputes the hash chain and checks all signatures; see [`Report`].
    fn verify(&self, kernel_key_ids: &[String]) -> Result<Report> {
        verify_tables(&self.conn(), kernel_key_ids)
    }

    fn partition_map(&self) -> Result<Option<PartitionMap>> {
        read_partition_map(&self.conn())
    }

    fn global_key(&self, id: &[u8]) -> Result<Option<String>> {
        read_global_key(&self.conn(), id)
    }

    fn tenant_imported(&self, tenant: &str) -> Result<Option<TenantImportedRecord>> {
        Ok(read_imported(&self.conn(), Some(tenant))?.pop())
    }

    fn set_tenant_index(&self, tenant: &str, streams: &[StreamIndex]) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let held = set_tenant_index_tx(&tx, tenant, streams)?;
        if held == 0 && streams.is_empty() {
            return Ok(0);
        }
        let head = self.file(&tx, || {
            vec![Record::TenantIndex(TenantIndexRecord {
                tenant: tenant.to_owned(),
                streams: streams.to_vec(),
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(held)
    }

    fn save_tenant_imported(&self, record: &TenantImportedRecord) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_imported_tx(&tx, record)?;
        let head = self.file(&tx, || vec![Record::TenantImported(record.clone())])?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    fn save_global_key(&self, id: &[u8], partition: &str) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if read_global_key(&tx, id)?.is_some_and(|p| p == partition) {
            return Ok(());
        }
        save_global_key_tx(&tx, id, partition)?;
        let head = self.file(&tx, || {
            vec![Record::GlobalKey(GlobalKeyRecord {
                id: id.to_vec(),
                partition: partition.to_owned(),
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    fn save_partition_map(&self, map: &PartitionMap) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !save_partition_map_tx(&tx, map)? {
            return Ok(());
        }
        let head = self.file(&tx, || vec![Record::PartitionMap(map.clone())])?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }

    // ---- the record log ----

    fn record_head(&self) -> Result<Head> {
        let log = self.log();
        if !log.on {
            return Err(Error::NoRecordLog);
        }
        Ok(log.head.clone())
    }

    fn record_hash_at(&self, seq: u64) -> Result<Option<Vec<u8>>> {
        {
            let log = self.log();
            if !log.on {
                return Err(Error::NoRecordLog);
            }
            if seq < log.base {
                return Ok(None);
            }
            if seq == log.head.seq {
                return Ok(Some(log.head.hash.clone()));
            }
            if seq == 0 {
                return Ok(Some(replication::ZERO.to_vec()));
            }
        }
        // Also beyond the head in memory: a record may be committed a moment before it moves.
        self.read(|conn| {
            Ok(conn
                .query_row("SELECT hash FROM replication WHERE seq = ?1", [seq], |r| {
                    r.get(0)
                })
                .optional()?)
        })
    }

    fn records_after(&self, seq: u64, limit: usize) -> Result<Option<Vec<ReplicatedRecord>>> {
        // The rows decide, not the head in memory: a reader may see a record committed a moment
        // before the head moves past it, and ask for the records after it.
        let head = {
            let log = self.log();
            if !log.on {
                return Err(Error::NoRecordLog);
            }
            if seq < log.base {
                return Ok(None);
            }
            log.head.seq
        };
        let rows = self.read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT seq, hash, record FROM replication WHERE seq > ?1 ORDER BY seq LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![seq, limit.max(1) as u64], |r| {
                Ok((r.get::<_, u64>(0)?, r.get(1)?, r.get::<_, Vec<u8>>(2)?))
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        match rows.first() {
            None if seq > head && seq > 0 => {
                // Beyond everything written: not this log's record.
                let held: bool = self.read(|conn| {
                    Ok(conn.query_row(
                        "SELECT EXISTS (SELECT 1 FROM replication WHERE seq = ?1)",
                        [seq],
                        |r| r.get(0),
                    )?)
                })?;
                if !held {
                    return Ok(None);
                }
            }
            // Pruned meanwhile: the record after `seq` is gone.
            Some((first, _, _)) if *first != seq + 1 => return Ok(None),
            _ => {}
        }
        let mut out = Vec::with_capacity(rows.len());
        for (seq, hash, body) in rows {
            out.push(ReplicatedRecord {
                seq,
                hash,
                record: Some(LogRecord::decode(body.as_slice())?),
            });
        }
        Ok(Some(out))
    }

    fn keep_record_log(&self, on: bool) -> Result<()> {
        let mut conn = self.conn();
        let mut log = self.log();
        if log.on == on {
            return Ok(());
        }
        if !on {
            conn.execute_batch(
                "DROP TABLE IF EXISTS replication; DROP TABLE IF EXISTS replication_state;",
            )?;
            *log = RecordLog::default();
            return Ok(());
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(LOG_SCHEMA)?;
        let empty: bool = tx.query_row(
            "SELECT NOT EXISTS (SELECT 1 FROM commands) AND NOT EXISTS (SELECT 1 FROM listeners)
                AND NOT EXISTS (SELECT 1 FROM workers) AND NOT EXISTS (SELECT 1 FROM memberships)
                AND NOT EXISTS (SELECT 1 FROM grants) AND NOT EXISTS (SELECT 1 FROM revocations)",
            [],
            |r| r.get(0),
        )?;
        // An empty store's log starts with the store, so an empty standby continues it. One
        // with data starts a log whose first hash nobody else has: every standby needs an
        // image first.
        let head = if empty {
            Head::default()
        } else {
            let mut hash = [0u8; 32];
            getrandom::fill(&mut hash).map_err(|_| Error::Corrupt("no randomness".into()))?;
            let head = Head {
                seq: 1,
                hash: hash.to_vec(),
            };
            insert_head(&tx, &head)?;
            head
        };
        tx.execute(
            "INSERT OR REPLACE INTO replication_state (id, base) VALUES (1, ?1)",
            [head.seq],
        )?;
        tx.commit()?;
        *log = RecordLog {
            on: true,
            base: head.seq,
            head,
        };
        Ok(())
    }

    fn prune_records(&self, seq: u64) -> Result<()> {
        let mut conn = self.conn();
        let mut log = self.log();
        if !log.on {
            return Ok(());
        }
        let keep = seq.min(log.head.seq);
        if keep <= log.base {
            return Ok(());
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM replication WHERE seq < ?1", [keep])?;
        tx.execute("UPDATE replication_state SET base = ?1", [keep])?;
        tx.commit()?;
        log.base = keep;
        Ok(())
    }

    fn apply(&self, records: &[ReplicatedRecord]) -> Result<()> {
        let mut conn = self.conn();
        let from = {
            let log = self.log();
            if !log.on {
                return Err(Error::NoRecordLog);
            }
            log.head.clone()
        };
        let (inner, head) = replication::chained(&from, records)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO replication (seq, hash, record) VALUES (?1, ?2, ?3)",
            )?;
            for (r, record) in records.iter().zip(inner) {
                apply_record(&tx, record)?;
                insert.execute(params![r.seq, r.hash, replication::body(record)])?;
            }
        }
        tx.commit()?;
        self.log().head = head;
        Ok(())
    }

    fn tenant_records(
        &self,
        tenant: &str,
        after: Option<u64>,
        f: &mut dyn FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        self.read(|conn| {
            let tx = conn.unchecked_transaction()?;
            records_of(&tx, Some(tenant), after, f)
        })
    }

    fn tenant_index(&self, tenant: &str) -> Result<Vec<(StreamId, AggregateIndex)>> {
        self.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT tenant_id, domain, aggregate_type, aggregate_id, record
                 FROM aggregate_index WHERE tenant_id = ?1",
            )?;
            let rows = stmt.query_map([tenant], |r| {
                Ok((
                    StreamId {
                        tenant_id: r.get(0)?,
                        domain: r.get(1)?,
                        aggregate_type: r.get(2)?,
                        aggregate_id: r.get(3)?,
                    },
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })?;
            rows.map(|row| {
                let (stream, record) = row?;
                Ok((stream, AggregateIndex::decode(record.as_slice())?))
            })
            .collect()
        })
    }

    fn image(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<Head> {
        if !self.log().on {
            return Err(Error::NoRecordLog);
        }
        self.read(|conn| {
            // One read transaction: every read below sees the store as of its first read.
            let tx = conn.unchecked_transaction()?;
            let head = tx
                .query_row(
                    "SELECT seq, hash FROM replication ORDER BY seq DESC LIMIT 1",
                    [],
                    |r| {
                        Ok(Head {
                            seq: r.get(0)?,
                            hash: r.get(1)?,
                        })
                    },
                )
                .optional()?
                .unwrap_or_default();
            image_records(&tx, f)?;
            Ok(head)
        })
    }

    fn apply_image(&self, records: &[Record]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for record in records {
            match record {
                Record::ReplicationHead(_) | Record::DataChanged(_) => {}
                record => apply_record(&tx, record)?,
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn start_record_log(&self, head: &Head) -> Result<()> {
        let mut conn = self.conn();
        let mut log = self.log();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(LOG_SCHEMA)?;
        tx.execute("DELETE FROM replication", [])?;
        insert_head(&tx, head)?;
        tx.execute(
            "INSERT OR REPLACE INTO replication_state (id, base) VALUES (1, ?1)",
            [head.seq],
        )?;
        tx.commit()?;
        *log = RecordLog {
            on: true,
            head: head.clone(),
            base: head.seq,
        };
        Ok(())
    }

    fn data_changed(&self, tenant_id: &str, domain: &str, key: &str) -> Result<()> {
        let mut conn = self.conn();
        if !self.log().on {
            return Ok(());
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = self.file(&tx, || {
            vec![Record::DataChanged(DataChangedRecord {
                tenant_id: tenant_id.to_owned(),
                domain: domain.to_owned(),
                key: key.to_owned(),
            })]
        })?;
        tx.commit()?;
        self.advance(head);
        Ok(())
    }
}

fn read_checkpoints(conn: &Connection, at: Option<u64>) -> Result<Vec<Checkpoint>> {
    let mut stmt = conn.prepare(
        "SELECT global_position, chain_hash, kernel_key_id, kernel_sig, created_at FROM checkpoints
         WHERE ?1 IS NULL OR global_position = ?1 ORDER BY global_position",
    )?;
    let rows = stmt.query_map([at], |r| {
        Ok(Checkpoint {
            global_position: r.get(0)?,
            chain_hash: r.get(1)?,
            kernel_sig: Signature {
                key_id: r.get(2)?,
                sig: r.get(3)?,
            },
            created_at: r.get(4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn read_revocations(conn: &Connection) -> Result<Vec<RevocationRecord>> {
    let mut stmt = conn.prepare(
        "SELECT key_id, since, created_at, kernel_key_id, kernel_sig FROM revocations
         ORDER BY rowid",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(RevocationRecord {
            key_id: r.get(0)?,
            since: r.get(1)?,
            created_at: r.get(2)?,
            kernel_sig: Some(Signature {
                key_id: r.get(3)?,
                sig: r.get(4)?,
            }),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn verify_tables(conn: &Connection, kernel_key_ids: &[String]) -> Result<Report> {
    let mut v = Verifier::new(
        kernel_key_ids,
        read_checkpoints(conn, None)?,
        read_revocations(conn)?,
    );

    let mut stmt = conn.prepare(&format!(
        "SELECT {EVENT_COLUMNS}, prev_hash FROM events ORDER BY global_position"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let prev_hash: Vec<u8> = r.get(19)?;
        v.event(event_from_row(r)?, &prev_hash);
    }

    let mut stmt = conn.prepare(&format!(
        "SELECT {COMMAND_COLUMNS}, created_at FROM commands ORDER BY created_at, rowid"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        v.command(&command_from_row(r)?, r.get(11)?);
    }
    Ok(v.finish())
}

/// Bytes as stored in an `ANY` column: TEXT when they are UTF-8, so JSON is readable in any
/// SQLite browser, else BLOB. Read back with [`bytes_at`], they are the same bytes.
pub(crate) fn readable(bytes: &[u8]) -> rusqlite::types::ToSqlOutput<'_> {
    use rusqlite::types::{ToSqlOutput, ValueRef};
    ToSqlOutput::Borrowed(match std::str::from_utf8(bytes) {
        Ok(text) => ValueRef::Text(text.as_bytes()),
        Err(_) => ValueRef::Blob(bytes),
    })
}

/// The bytes of an `ANY` column written by [`readable`].
pub(crate) fn bytes_at(r: &rusqlite::Row<'_>, i: usize) -> rusqlite::Result<Vec<u8>> {
    match r.get_ref(i)? {
        rusqlite::types::ValueRef::Text(b) | rusqlite::types::ValueRef::Blob(b) => Ok(b.to_vec()),
        other => Err(rusqlite::Error::InvalidColumnType(
            i,
            "payload".into(),
            other.data_type(),
        )),
    }
}

fn event_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Event> {
    Ok(Event {
        global_position: r.get(0)?,
        tenant_id: r.get(1)?,
        aggregate_type: r.get(2)?,
        aggregate_id: r.get(3)?,
        version: r.get(4)?,
        event_type: r.get(5)?,
        payload: bytes_at(r, 6)?,
        command_id: r.get(7)?,
        created_at: r.get(8)?,
        producer_sig: join_sig(r.get(9)?, r.get(10)?),
        hash: r.get(11)?,
        context: context_from(r.get(12)?)?,
        domain: r.get(13)?,
        schema: r.get(14)?,
        vault_refs: split_list(r.get(15)?),
        erases: r.get(16)?,
        origin_partition: r.get(17)?,
        origin_position: r.get(18)?,
    })
}

fn command_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Command> {
    Ok(Command {
        command_id: r.get(0)?,
        tenant_id: r.get(1)?,
        aggregate_type: r.get(2)?,
        aggregate_id: r.get(3)?,
        command_type: r.get(4)?,
        payload: bytes_at(r, 5)?,
        client_sig: join_sig(r.get(6)?, r.get(7)?),
        context: context_from(r.get(8)?)?,
        domain: r.get(9)?,
        vault_refs: split_list(r.get(10)?),
    })
}

fn split_list(joined: String) -> Vec<String> {
    if joined.is_empty() {
        return Vec::new();
    }
    joined.split(',').map(str::to_owned).collect()
}

/// A request context as JSON with the field names of `kernable.proto`, so the database can be
/// read without it (`json_extract(context, '$.sender_identity')`). Fields left at their
/// defaults are omitted. Hashes and signatures cover the fields, not this text.
fn context_json(c: &RequestContext) -> String {
    use serde_json::Value;
    let text = |v: &str| (!v.is_empty()).then(|| Value::from(v));
    let attributes = c
        .attributes
        .iter()
        .map(|a| serde_json::json!({ "key": a.key, "value": a.value }))
        .collect::<Vec<_>>();
    let fields = [
        ("sender_tenant", text(&c.sender_tenant)),
        ("sender_identity", text(&c.sender_identity)),
        ("sender_account", text(&c.sender_account)),
        ("sender_session", text(&c.sender_session)),
        ("on_behalf_of", text(&c.on_behalf_of)),
        ("expected_version", c.expected_version.map(Value::from)),
        ("correlation_id", text(&c.correlation_id)),
        ("causation_id", text(&c.causation_id)),
        ("trace_id", text(&c.trace_id)),
        (
            "bypass_authorization",
            c.bypass_authorization.then_some(Value::Bool(true)),
        ),
        (
            "attributes",
            (!attributes.is_empty()).then_some(Value::Array(attributes)),
        ),
    ];
    let m: serde_json::Map<String, Value> = fields
        .into_iter()
        .filter_map(|(name, v)| Some((name.to_owned(), v?)))
        .collect();
    serde_json::Value::Object(m).to_string()
}

fn context_from(json: Option<String>) -> rusqlite::Result<Option<RequestContext>> {
    json.map(|j| parse_context(&j)).transpose().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, e.into())
    })
}

fn parse_context(json: &str) -> std::result::Result<RequestContext, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let field = |name: &str| v.get(name).filter(|f| !f.is_null());
    let str = |name: &str| match field(name) {
        None => Ok(String::new()),
        Some(f) => f
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("context {name}: not a string")),
    };
    let attributes = match field("attributes") {
        None => Vec::new(),
        Some(a) => a
            .as_array()
            .ok_or("context attributes: not an array")?
            .iter()
            .map(|a| match (a["key"].as_str(), a["value"].as_str()) {
                (Some(key), Some(value)) => Ok(Attribute {
                    key: key.into(),
                    value: value.into(),
                }),
                _ => Err("context attribute: needs a key and a value".to_owned()),
            })
            .collect::<std::result::Result<_, _>>()?,
    };
    Ok(RequestContext {
        sender_tenant: str("sender_tenant")?,
        sender_identity: str("sender_identity")?,
        sender_account: str("sender_account")?,
        sender_session: str("sender_session")?,
        on_behalf_of: str("on_behalf_of")?,
        expected_version: field("expected_version")
            .map(|f| f.as_u64().ok_or("context expected_version: not a number"))
            .transpose()?,
        correlation_id: str("correlation_id")?,
        causation_id: str("causation_id")?,
        trace_id: str("trace_id")?,
        bypass_authorization: field("bypass_authorization")
            .map(|f| {
                f.as_bool()
                    .ok_or("context bypass_authorization: not a boolean")
            })
            .transpose()?
            .unwrap_or(false),
        attributes,
    })
}

/// What the stream of `stream` (tenant, domain, type, ID) holds; `None` if nothing.
fn stream_index(
    conn: &Connection,
    stream: impl rusqlite::Params,
) -> Result<Option<AggregateIndex>> {
    let record: Option<Vec<u8>> = conn
        .query_row(
            "SELECT record FROM aggregate_index
             WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4",
            stream,
            |r| r.get(0),
        )
        .optional()?;
    Ok(record
        .map(|r| AggregateIndex::decode(r.as_slice()))
        .transpose()?)
}

fn read_events(
    conn: &Connection,
    filter: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events {filter}"))?;
    let rows = stmt.query_map(params, event_from_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn split_sig(sig: Option<&Signature>) -> (Option<&str>, Option<&[u8]>) {
    match sig {
        Some(s) => (Some(&s.key_id), Some(&s.sig)),
        None => (None, None),
    }
}

fn join_sig(key_id: Option<String>, sig: Option<Vec<u8>>) -> Option<Signature> {
    Some(Signature {
        key_id: key_id?,
        sig: sig?,
    })
}

/// A pending command.
fn pending_command(tx: &Transaction<'_>, command_id: &str) -> Result<Command> {
    tx.query_row(
        &format!(
            "SELECT {COMMAND_COLUMNS} FROM commands WHERE command_id = ?1 AND status = 'pending'"
        ),
        [command_id],
        command_from_row,
    )
    .optional()?
    .ok_or(Error::NotPending)
}

/// The events at `positions` (comma-separated), which `command_id` produced.
fn events_at(conn: &Connection, command_id: &str, positions: &str) -> Result<Vec<Event>> {
    let positions = split_list(positions.to_owned())
        .iter()
        .map(|p| p.parse::<u64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Corrupt(format!("command {command_id}: positions {positions}")))?;
    let mut events = Vec::with_capacity(positions.len());
    for position in positions {
        let mut found = read_events(conn, "WHERE global_position = ?1", [position])?;
        match found.pop() {
            Some(event) if event.command_id == command_id => events.push(event),
            _ => {
                return Err(Error::Corrupt(format!(
                    "command {command_id}: no event of it at position {position}"
                )));
            }
        }
    }
    Ok(events)
}

/// The result of a completed command: its events are read back from the log.
fn stored_result(conn: &Connection, command_id: &str) -> Result<Option<CommandResult>> {
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<Vec<u8>>,
    );
    let row: Option<Row> = conn
        .query_row(
            "SELECT status, positions, rejected_code, rejected_message, rejected_key,
                    rejected_requirement
             FROM commands WHERE command_id = ?1",
            [command_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .optional()?;
    let outcome = match row {
        None => return Ok(None),
        Some((status, positions, code, message, key, requirement)) => match status.as_str() {
            "committed" => Outcome::Committed(Committed {
                events: events_at(conn, command_id, &positions)?,
                settled: false,
            }),
            "rejected" => Outcome::Rejected(Rejected {
                code: code.unwrap_or_default(),
                message: message.unwrap_or_default(),
                key: key.unwrap_or_default(),
                requirement: requirement
                    .map(|bytes| Requirement::decode(bytes.as_slice()).map(Box::new))
                    .transpose()
                    .map_err(|e| Error::Corrupt(format!("a stored requirement: {e}")))?,
            }),
            _ => return Ok(None),
        },
    };
    Ok(Some(CommandResult {
        command_id: command_id.to_owned(),
        outcome: Some(outcome),
    }))
}

/// Inserts `cmd` as pending; false if a command with its ID is stored already.
fn enqueue_tx(tx: &Connection, cmd: &Command, now_ms: i64) -> Result<bool> {
    let (key_id, sig) = split_sig(cmd.client_sig.as_ref());
    let inserted = tx.execute(
        "INSERT INTO commands (command_id, tenant_id, aggregate_type, aggregate_id, command_type,
                               payload, client_key_id, client_sig, context, domain, status,
                               created_at, vault_refs)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', ?11, ?12)
         ON CONFLICT (command_id) DO NOTHING",
        params![
            cmd.command_id,
            cmd.tenant_id,
            cmd.aggregate_type,
            cmd.aggregate_id,
            cmd.command_type,
            readable(&cmd.payload),
            key_id,
            sig,
            cmd.context.as_ref().map(context_json),
            cmd.domain,
            now_ms,
            cmd.vault_refs.join(",")
        ],
    )?;
    Ok(inserted == 1)
}

/// Appends `events` to the pending command's stream, extends the hash chain and
/// completes the command, within `tx`; returns the events, and them with their predecessors'
/// hashes for the record log. `producer_sig` is the worker's result signature;
/// verifying it is up to the caller. `index`, if set, replaces what the stream holds.
///
/// On `VersionConflict` the command stays pending, so the caller can retry with fresh history.
fn commit_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    expected_version: u64,
    events: &[NewEvent],
    producer_sig: &Signature,
    index: Option<&AggregateIndex>,
    now_ms: i64,
) -> Result<(Vec<Event>, Vec<LoggedEvent>)> {
    let cmd = pending_command(tx, command_id)?;

    let actual: u64 = tx.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM events
         WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4",
        params![
            cmd.tenant_id,
            cmd.domain,
            cmd.aggregate_type,
            cmd.aggregate_id
        ],
        |r| r.get(0),
    )?;
    if actual != expected_version {
        return Err(Error::VersionConflict {
            expected: expected_version,
            actual,
        });
    }

    let (mut position, mut prev) = tx
        .query_row(
            "SELECT global_position, hash FROM events ORDER BY global_position DESC LIMIT 1",
            [],
            |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?
        .unwrap_or((0, GENESIS.to_vec()));

    let mut stored = Vec::with_capacity(events.len());
    let mut logged = Vec::with_capacity(events.len());
    for (version, e) in (expected_version + 1..).zip(events) {
        position += 1;
        let mut event = Event {
            global_position: position,
            tenant_id: cmd.tenant_id.clone(),
            domain: cmd.domain.clone(),
            aggregate_type: cmd.aggregate_type.clone(),
            aggregate_id: cmd.aggregate_id.clone(),
            version,
            event_type: e.event_type.clone(),
            schema: e.schema,
            vault_refs: e.vault_refs.clone(),
            erases: e.erases.clone(),
            payload: e.payload.clone(),
            command_id: cmd.command_id.clone(),
            created_at: now_ms,
            hash: vec![],
            producer_sig: Some(producer_sig.clone()),
            context: cmd.context.clone(),
            origin_partition: e.origin_partition.clone(),
            origin_position: e.origin_position,
        };
        event.hash = signing::event_hash(&prev, &event).to_vec();
        tx.execute(
            "INSERT INTO events (global_position, tenant_id, aggregate_type, aggregate_id, version,
                                 event_type, payload, command_id, created_at,
                                 producer_key_id, producer_sig, prev_hash, hash, context, domain,
                                 schema, vault_refs, erases, origin_partition, origin_position)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                     ?18, ?19, ?20)",
            params![
                event.global_position,
                event.tenant_id,
                event.aggregate_type,
                event.aggregate_id,
                event.version,
                event.event_type,
                readable(&event.payload),
                event.command_id,
                event.created_at,
                producer_sig.key_id,
                producer_sig.sig,
                prev,
                event.hash,
                event.context.as_ref().map(context_json),
                event.domain,
                event.schema,
                event.vault_refs.join(","),
                event.erases,
                event.origin_partition,
                event.origin_position
            ],
        )?;
        tx.execute(
            "INSERT INTO event_types (domain, aggregate_type, event_type, schema)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (domain, aggregate_type, event_type) DO UPDATE SET schema = excluded.schema
             WHERE excluded.schema > schema",
            params![
                event.domain,
                event.aggregate_type,
                event.event_type,
                event.schema
            ],
        )?;
        logged.push(LoggedEvent {
            event: Some(event.clone()),
            prev_hash: std::mem::replace(&mut prev, event.hash.clone()),
        });
        stored.push(event);
    }

    if let Some(index) = index {
        // The kernel checks the keys first, so a key another stream holds is a bug.
        let stream_id = StreamId {
            tenant_id: cmd.tenant_id.clone(),
            domain: cmd.domain.clone(),
            aggregate_type: cmd.aggregate_type.clone(),
            aggregate_id: cmd.aggregate_id.clone(),
        };
        set_stream_index_tx(tx, &stream_id, index)?;
    }

    tx.execute(
        "UPDATE commands SET status = 'committed', positions = ?2 WHERE command_id = ?1",
        params![
            cmd.command_id,
            stored
                .iter()
                .map(|e| e.global_position.to_string())
                .collect::<Vec<_>>()
                .join(","),
        ],
    )?;
    Ok((stored, logged))
}

/// Where the record log stands, from its tables; off without them.
fn load_log(conn: &Connection) -> Result<RecordLog> {
    let on: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'replication_state')",
        [],
        |r| r.get(0),
    )?;
    if !on {
        return Ok(RecordLog::default());
    }
    let base: u64 = conn
        .query_row("SELECT base FROM replication_state", [], |r| r.get(0))
        .optional()?
        .unwrap_or(0);
    let head = conn
        .query_row(
            "SELECT seq, hash FROM replication ORDER BY seq DESC LIMIT 1",
            [],
            |r| {
                Ok(Head {
                    seq: r.get(0)?,
                    hash: r.get(1)?,
                })
            },
        )
        .optional()?
        .unwrap_or_default();
    Ok(RecordLog {
        on: true,
        head,
        base,
    })
}

/// Completes the pending command as rejected.
fn reject_tx(tx: &Transaction<'_>, command_id: &str, reason: &Rejected) -> Result<()> {
    let cmd = pending_command(tx, command_id)?;
    tx.execute(
        "UPDATE commands SET status = 'rejected', rejected_code = ?2, rejected_message = ?3,
                rejected_key = ?4, rejected_requirement = ?5
         WHERE command_id = ?1",
        params![
            cmd.command_id,
            reason.code,
            reason.message,
            Some(&reason.key).filter(|k| !k.is_empty()),
            reason.requirement.as_ref().map(Message::encode_to_vec),
        ],
    )?;
    Ok(())
}

/// Stores a listener's registration and position, or removes it.
fn save_listener_tx(tx: &Connection, l: &ListenerRecord) -> Result<()> {
    // Removing a listener removes its middlewares too (ON DELETE CASCADE).
    tx.execute("DELETE FROM middlewares WHERE listener = ?1", [&l.name])?;
    if l.removed {
        tx.execute("DELETE FROM listeners WHERE name = ?1", [&l.name])?;
        return Ok(());
    }
    tx.execute(
        "INSERT INTO listeners
             (name, key_id, ordering, event_types, position, service, last_seen)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT (name) DO UPDATE SET key_id = excluded.key_id,
             ordering = excluded.ordering, event_types = excluded.event_types,
             position = excluded.position, service = excluded.service,
             last_seen = excluded.last_seen",
        params![
            l.name,
            l.key_id,
            l.order,
            l.event_types.join(","),
            l.position,
            l.service,
            l.last_seen
        ],
    )?;
    for m in &l.middlewares {
        let phase = if m.when() == When::After {
            "after"
        } else {
            "before"
        };
        tx.execute(
            "INSERT INTO middlewares (listener, phase, ordering, types)
             VALUES (?1, ?2, ?3, ?4)",
            params![l.name, phase, m.order, m.types.join(",")],
        )?;
    }
    if l.parked_changed {
        tx.execute("DELETE FROM parked WHERE listener = ?1", [&l.name])?;
        for p in &l.parked {
            tx.execute(
                "INSERT INTO parked
                     (listener, position, event_type, code, message, attempts, parked_at, retry)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    l.name,
                    p.position,
                    p.event_type,
                    p.code,
                    p.message,
                    p.attempts,
                    p.parked_at,
                    p.retry
                ],
            )?;
        }
    }
    Ok(())
}

/// Stores an aggregate's registration, or removes it.
fn save_worker_tx(tx: &Connection, w: &WorkerRecord) -> Result<()> {
    if w.removed {
        tx.execute(
            "DELETE FROM workers WHERE aggregate_type = ?1",
            [&w.aggregate_type],
        )?;
        return Ok(());
    }
    tx.execute(
        "INSERT INTO workers (aggregate_type, key_id, command_types, service, last_seen)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (aggregate_type) DO UPDATE SET key_id = excluded.key_id,
             command_types = excluded.command_types, service = excluded.service,
             last_seen = excluded.last_seen",
        params![
            w.aggregate_type,
            w.key_id,
            w.command_types.join(","),
            w.service,
            w.last_seen
        ],
    )?;
    Ok(())
}

/// Replaces the membership of the record's service.
fn save_membership_tx(tx: &Connection, record: &MembershipRecord) -> Result<()> {
    let m = record.membership.clone().unwrap_or_default();
    let sig = record.sig.clone().unwrap_or_default();
    let metadata: Vec<(&str, &str)> = m
        .metadata
        .iter()
        .map(|a| (a.key.as_str(), a.value.as_str()))
        .collect();
    tx.execute(
        "INSERT OR REPLACE INTO memberships
             (service, workers, listeners, middlewares, metadata, issued_at, key_id, sig,
              applied_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            m.service,
            m.workers.join(","),
            m.listeners.join(","),
            m.middlewares.join(","),
            serde_json::to_string(&metadata).expect("strings serialize"),
            m.issued_at,
            sig.key_id,
            sig.sig,
            record.applied_at
        ],
    )?;
    Ok(())
}

/// Keeps a signed checkpoint; false if one at its position is kept already.
fn insert_checkpoint(tx: &Connection, cp: &CheckpointRecord) -> Result<bool> {
    let sig = cp.kernel_sig.clone().unwrap_or_default();
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO checkpoints
             (global_position, chain_hash, kernel_key_id, kernel_sig, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            cp.global_position,
            cp.chain_hash,
            sig.key_id,
            sig.sig,
            cp.created_at
        ],
    )?;
    Ok(inserted == 1)
}

/// Keeps a revocation; false if its key is revoked already.
fn revoke_tx(tx: &Connection, revocation: &RevocationRecord) -> Result<bool> {
    let sig = revocation.kernel_sig.as_ref();
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO revocations (key_id, since, created_at, kernel_key_id, kernel_sig)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            revocation.key_id,
            revocation.since,
            revocation.created_at,
            sig.map(|s| &s.key_id),
            sig.map(|s| &s.sig),
        ],
    )?;
    Ok(inserted == 1)
}

/// Keeps `grant` unless a grant of its key issued at the same time or later is kept; whether
/// it was kept.
fn save_grant_tx(tx: &Connection, grant: &Grant) -> Result<bool> {
    let changed = tx.execute(
        "INSERT INTO grants (key_id, issued_at, root_id, grant) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (key_id) DO UPDATE
         SET issued_at = excluded.issued_at, root_id = excluded.root_id, grant = excluded.grant
         WHERE excluded.issued_at > grants.issued_at",
        params![
            grant.key_id,
            grant.issued_at,
            grant
                .root_sig
                .as_ref()
                .map(|s| s.key_id.as_str())
                .unwrap_or_default(),
            grant.encode_to_vec(),
        ],
    )?;
    Ok(changed == 1)
}

fn read_listeners(conn: &Connection) -> Result<Vec<ListenerRecord>> {
    let mut stmt = conn.prepare(
        "SELECT name, key_id, ordering, event_types, position, service, last_seen
         FROM listeners
         ORDER BY name",
    )?;
    let mut listeners = stmt
        .query_map([], |r| {
            Ok(ListenerRecord {
                name: r.get(0)?,
                key_id: r.get(1)?,
                order: r.get(2)?,
                event_types: split_list(r.get(3)?),
                position: r.get(4)?,
                removed: false,
                middlewares: Vec::new(),
                service: r.get(5)?,
                last_seen: r.get(6)?,
                parked: Vec::new(),
                parked_changed: false,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut parked = conn.prepare(
        "SELECT position, event_type, code, message, attempts, parked_at, retry FROM parked
         WHERE listener = ?1 ORDER BY position",
    )?;
    for l in &mut listeners {
        l.parked = parked
            .query_map([&l.name], |r| {
                Ok(ParkedEvent {
                    position: r.get(0)?,
                    event_type: r.get(1)?,
                    code: r.get(2)?,
                    message: r.get(3)?,
                    attempts: r.get(4)?,
                    parked_at: r.get(5)?,
                    retry: r.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    let mut stmt = conn.prepare(
        "SELECT phase, ordering, types FROM middlewares WHERE listener = ?1 ORDER BY phase DESC",
    )?;
    for l in &mut listeners {
        l.middlewares = stmt
            .query_map([&l.name], |r| {
                let phase: String = r.get(0)?;
                let when = if phase == "after" {
                    When::After
                } else {
                    When::Before
                };
                Ok(Middleware {
                    order: r.get(1)?,
                    types: split_list(r.get(2)?),
                    when: when.into(),
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(listeners)
}

fn read_workers(conn: &Connection) -> Result<Vec<WorkerRecord>> {
    let mut stmt = conn.prepare(
        "SELECT aggregate_type, key_id, command_types, service, last_seen FROM workers
         ORDER BY aggregate_type",
    )?;
    let workers = stmt
        .query_map([], |r| {
            Ok(WorkerRecord {
                aggregate_type: r.get(0)?,
                key_id: r.get(1)?,
                command_types: split_list(r.get(2)?),
                service: r.get(3)?,
                last_seen: r.get(4)?,
                removed: false,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(workers)
}

fn read_memberships(conn: &Connection) -> Result<Vec<MembershipRecord>> {
    let mut stmt = conn.prepare(
        "SELECT service, workers, listeners, middlewares, metadata, issued_at, key_id, sig,
                applied_at
         FROM memberships ORDER BY service",
    )?;
    let memberships = stmt
        .query_map([], |r| {
            let metadata: String = r.get(4)?;
            let metadata: Vec<(String, String)> =
                serde_json::from_str(&metadata).unwrap_or_default();
            Ok(MembershipRecord {
                membership: Some(Membership {
                    service: r.get(0)?,
                    workers: split_list(r.get(1)?),
                    listeners: split_list(r.get(2)?),
                    middlewares: split_list(r.get(3)?),
                    metadata: metadata
                        .into_iter()
                        .map(|(key, value)| Attribute { key, value })
                        .collect(),
                    issued_at: r.get(5)?,
                }),
                sig: Some(Signature {
                    key_id: r.get(6)?,
                    sig: r.get(7)?,
                }),
                applied_at: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(memberships)
}

/// Files the record that starts a log at `head`: an image's head, or a new log's random start.
fn insert_head(tx: &Connection, head: &Head) -> Result<()> {
    if head.seq == 0 {
        return Ok(());
    }
    let record = Record::ReplicationHead(ReplicationHeadRecord {
        seq: head.seq,
        hash: head.hash.clone(),
    });
    tx.execute(
        "INSERT INTO replication (seq, hash, record) VALUES (?1, ?2, ?3)",
        params![head.seq, head.hash, replication::body(&record)],
    )?;
    Ok(())
}

/// Applies one record of another store's log within `tx`, with the checks [`Store::apply`]
/// names.
fn apply_record(tx: &Transaction<'_>, record: &Record) -> Result<()> {
    let not_pending = |id: &str| {
        let id = id.to_owned();
        move |e: Error| match e {
            Error::NotPending => diverged(format!("command {id} is not pending")),
            e => e,
        }
    };
    match record {
        Record::Enqueued(e) => {
            let cmd = e
                .command
                .as_ref()
                .ok_or_else(|| diverged("an enqueued record without its command".into()))?;
            if !enqueue_tx(tx, cmd, e.created_at)? {
                return Err(diverged(format!(
                    "command {} is enqueued already",
                    cmd.command_id
                )));
            }
        }
        Record::Committed(c) => apply_commit(tx, c)?,
        Record::Rejected(r) => {
            let reason = r.reason.clone().unwrap_or_default();
            reject_tx(tx, &r.command_id, &reason).map_err(not_pending(&r.command_id))?;
        }
        Record::Checkpoint(cp) => {
            let hash: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT hash FROM events WHERE global_position = ?1",
                    [cp.global_position],
                    |r| r.get(0),
                )
                .optional()?;
            if hash.as_ref() != Some(&cp.chain_hash) || !insert_checkpoint(tx, cp)? {
                return Err(diverged(format!(
                    "a checkpoint of event {} this store does not hold",
                    cp.global_position
                )));
            }
        }
        Record::Listener(l) => save_listener_tx(tx, l)?,
        Record::Worker(w) => save_worker_tx(tx, w)?,
        Record::Membership(m) => save_membership_tx(tx, m)?,
        Record::Revocation(r) => {
            revoke_tx(tx, r)?;
        }
        Record::Grant(g) => {
            save_grant_tx(tx, g)?;
        }
        Record::PartitionMap(m) => {
            save_partition_map_tx(tx, m)?;
        }
        Record::GlobalKey(g) => save_global_key_tx(tx, &g.id, &g.partition)?,
        Record::TenantImported(t) => save_imported_tx(tx, t)?,
        Record::TenantIndex(t) => {
            set_tenant_index_tx(tx, &t.tenant, &t.streams)?;
        }
        Record::DataChanged(_) => {}
        Record::Format(_) | Record::ReplicationHead(_) => {
            return Err(diverged("a record that starts a log".into()));
        }
    }
    Ok(())
}

/// Where SQLite keeps the newest partition map; created with the first one.
const MAP_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS partition_map (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    epoch INTEGER NOT NULL,
    map   BLOB    NOT NULL -- the PartitionMap message, with its signature
) STRICT;
";

/// On the target of a move, created with the first tenant copied here (S4).
const IMPORTED_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS tenants_imported (
    tenant TEXT PRIMARY KEY,
    record BLOB NOT NULL -- the TenantImportedRecord
) STRICT, WITHOUT ROWID;
";

fn read_imported(conn: &Connection, tenant: Option<&str>) -> Result<Vec<TenantImportedRecord>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'tenants_imported')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(Vec::new());
    }
    let blobs: Vec<Vec<u8>> = conn
        .prepare(
            "SELECT record FROM tenants_imported WHERE ?1 IS NULL OR tenant = ?1 ORDER BY tenant",
        )?
        .query_map([tenant], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    blobs
        .iter()
        .map(|b| Ok(TenantImportedRecord::decode(b.as_slice())?))
        .collect()
}

/// Forgets the keys and facts `tenant`'s streams hold; how many streams held some.
/// Files what `stream_id` holds now: the keys it held before are free, then its new ones are
/// filed; a key another stream holds is `KeyTaken`.
fn set_stream_index_tx(
    tx: &Connection,
    stream_id: &StreamId,
    index: &AggregateIndex,
) -> Result<()> {
    let stream = params![
        stream_id.tenant_id,
        stream_id.domain,
        stream_id.aggregate_type,
        stream_id.aggregate_id
    ];
    // The keys it held before are free, then its new ones are filed: a key another
    // stream holds fails the commit (the kernel checks first, so that is a bug).
    if let Some(old) = stream_index(tx, stream)? {
        for k in old.keys.iter().filter(|k| !k.id.is_empty()) {
            tx.execute("DELETE FROM unique_keys WHERE id = ?1", [&k.id])?;
        }
    }
    if index.keys.is_empty() && index.facts.is_empty() {
        tx.execute(
            "DELETE FROM aggregate_index
             WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3
               AND aggregate_id = ?4",
            stream,
        )?;
    } else {
        let personal = index.keys.iter().any(|k| !k.vault_ref.is_empty());
        tx.execute(
            "INSERT INTO aggregate_index
                 (tenant_id, domain, aggregate_type, aggregate_id, record, personal)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (tenant_id, domain, aggregate_type, aggregate_id)
             DO UPDATE SET record = excluded.record, personal = excluded.personal",
            params![
                stream_id.tenant_id,
                stream_id.domain,
                stream_id.aggregate_type,
                stream_id.aggregate_id,
                index.encode_to_vec(),
                personal
            ],
        )?;
    }
    for k in index.keys.iter().filter(|k| !k.id.is_empty()) {
        tx.execute(
            "INSERT INTO unique_keys (id, tenant_id, domain, aggregate_type, aggregate_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                k.id,
                stream_id.tenant_id,
                stream_id.domain,
                stream_id.aggregate_type,
                stream_id.aggregate_id
            ],
        )
        .map_err(|e| match e {
            rusqlite::Error::SqliteFailure(f, _)
                if f.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Error::KeyTaken
            }
            e => e.into(),
        })?;
    }
    Ok(())
}

/// Replaces what `tenant`'s streams hold with `streams`; how many held something before.
fn set_tenant_index_tx(tx: &Connection, tenant: &str, streams: &[StreamIndex]) -> Result<usize> {
    let held = drop_index_tx(tx, tenant)?;
    for s in streams {
        let stream = StreamId {
            tenant_id: tenant.to_owned(),
            domain: s.domain.clone(),
            aggregate_type: s.aggregate_type.clone(),
            aggregate_id: s.aggregate_id.clone(),
        };
        set_stream_index_tx(tx, &stream, &s.index.clone().unwrap_or_default())?;
    }
    Ok(held)
}

fn drop_index_tx(tx: &Connection, tenant: &str) -> Result<usize> {
    tx.execute("DELETE FROM unique_keys WHERE tenant_id = ?1", [tenant])?;
    Ok(tx.execute("DELETE FROM aggregate_index WHERE tenant_id = ?1", [tenant])?)
}

fn save_imported_tx(tx: &Connection, record: &TenantImportedRecord) -> Result<()> {
    tx.execute_batch(IMPORTED_SCHEMA)?;
    tx.execute(
        "INSERT OR REPLACE INTO tenants_imported (tenant, record) VALUES (?1, ?2)",
        params![record.tenant, record.encode_to_vec()],
    )?;
    Ok(())
}

/// On the key owner, created with the first global key it keeps (S3).
const GLOBAL_KEYS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS global_keys (
    id        BLOB PRIMARY KEY,
    partition TEXT NOT NULL -- the partition that may hold the key
) STRICT, WITHOUT ROWID;
";

fn read_global_key(conn: &Connection, id: &[u8]) -> Result<Option<String>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'global_keys')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT partition FROM global_keys WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .optional()?)
}

fn save_global_key_tx(tx: &Connection, id: &[u8], partition: &str) -> Result<()> {
    tx.execute_batch(GLOBAL_KEYS_SCHEMA)?;
    tx.execute(
        "INSERT INTO global_keys (id, partition) VALUES (?1, ?2)
         ON CONFLICT (id) DO UPDATE SET partition = excluded.partition",
        params![id, partition],
    )?;
    Ok(())
}

/// The newest partition map, if one was ever kept.
fn read_partition_map(conn: &Connection) -> Result<Option<PartitionMap>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'partition_map')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(None);
    }
    let blob: Option<Vec<u8>> = conn
        .query_row("SELECT map FROM partition_map WHERE id = 1", [], |r| {
            r.get(0)
        })
        .optional()?;
    Ok(blob
        .map(|b| PartitionMap::decode(b.as_slice()))
        .transpose()?)
}

/// Keeps `map` unless one of the same or a higher epoch is kept; whether it was kept.
fn save_partition_map_tx(tx: &Connection, map: &PartitionMap) -> Result<bool> {
    tx.execute_batch(MAP_SCHEMA)?;
    let changed = tx.execute(
        "INSERT INTO partition_map (id, epoch, map) VALUES (1, ?1, ?2)
         ON CONFLICT (id) DO UPDATE SET epoch = excluded.epoch, map = excluded.map
         WHERE excluded.epoch > partition_map.epoch",
        params![map.epoch, map.encode_to_vec()],
    )?;
    Ok(changed == 1)
}

/// Commits another store's commit here, through the same path as a worker's result: the
/// events must continue the chain, and committing them must give exactly them.
fn apply_commit(tx: &Transaction<'_>, c: &CommittedRecord) -> Result<()> {
    let (position, prev) = tx
        .query_row(
            "SELECT global_position, hash FROM events ORDER BY global_position DESC LIMIT 1",
            [],
            |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?
        .unwrap_or((0, GENESIS.to_vec()));
    replication::check_events(prev, position, &c.events)?;
    let theirs: Vec<&Event> = c.events.iter().filter_map(|l| l.event.as_ref()).collect();
    let cmd = pending_command(tx, &c.command_id).map_err(|e| match e {
        Error::NotPending => diverged(format!("command {} is not pending", c.command_id)),
        e => e,
    })?;
    let expected = match theirs.first() {
        Some(e) => e
            .version
            .checked_sub(1)
            .ok_or_else(|| diverged(format!("event {} has version 0", e.global_position)))?,
        None => tx.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM events
             WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3 AND aggregate_id = ?4",
            params![
                cmd.tenant_id,
                cmd.domain,
                cmd.aggregate_type,
                cmd.aggregate_id
            ],
            |r| r.get(0),
        )?,
    };
    let events: Vec<NewEvent> = theirs
        .iter()
        .map(|e| NewEvent {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
            schema: e.schema,
            vault_refs: e.vault_refs.clone(),
            erases: e.erases.clone(),
            origin_partition: e.origin_partition.clone(),
            origin_position: e.origin_position,
        })
        .collect();
    let first = theirs.first();
    let sig = first
        .and_then(|e| e.producer_sig.clone())
        .unwrap_or_default();
    let now_ms = first.map_or(0, |e| e.created_at);
    let (stored, _) = commit_tx(
        tx,
        &c.command_id,
        expected,
        &events,
        &sig,
        c.index.as_ref(),
        now_ms,
    )
    .map_err(|e| match e {
        Error::VersionConflict { .. } | Error::KeyTaken | Error::NotPending => diverged(format!(
            "command {} does not commit here: {e}",
            c.command_id
        )),
        e => e,
    })?;
    if !stored.iter().eq(theirs.iter().copied()) {
        return Err(diverged(format!(
            "command {} commits other events here",
            c.command_id
        )));
    }
    Ok(())
}

/// Calls `f` with the whole store as records, in an order that applies: what is registered,
/// every command as enqueued, every commit in log order (each stream's newest keys and facts
/// with the commit of its last event), commits without events, rejections, listeners with
/// their parked events, checkpoints.
/// The commands as enqueued, the commits in log order (each stream's index with its last
/// commit), then the rejections: of every tenant, or of `tenant` only.
/// The records of `tenant` (every tenant: `None`); with `after`, see [`Store::tenant_records`].
fn records_of(
    conn: &Connection,
    tenant: Option<&str>,
    after: Option<u64>,
    f: &mut dyn FnMut(Record) -> Result<()>,
) -> Result<()> {
    // Events (and the commits they make) after this position; 0: all.
    let since = after.unwrap_or(0);
    {
        // With `after`: commands with an outcome that is not sent already.
        let delta = if after.is_some() {
            " AND status != 'pending' AND (status = 'rejected' OR positions = ''
               OR command_id IN (SELECT command_id FROM events
                                 WHERE tenant_id = ?1 AND global_position > ?2))"
        } else {
            ""
        };
        let mut stmt = conn.prepare(&format!(
            "SELECT {COMMAND_COLUMNS}, created_at FROM commands
             WHERE (?1 IS NULL OR tenant_id = ?1) AND ?2 >= 0{delta} ORDER BY rowid"
        ))?;
        let mut rows = stmt.query(params![tenant, since])?;
        while let Some(r) = rows.next()? {
            f(Record::Enqueued(EnqueuedRecord {
                command: Some(command_from_row(r)?),
                created_at: r.get(11)?,
            }))?;
        }
    }
    let mut indexes: HashMap<u64, AggregateIndex> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT a.record, (SELECT MAX(e.global_position) FROM events e
                               WHERE e.tenant_id = a.tenant_id AND e.domain = a.domain
                                 AND e.aggregate_type = a.aggregate_type
                                 AND e.aggregate_id = a.aggregate_id)
             FROM aggregate_index a WHERE ?1 IS NULL OR a.tenant_id = ?1",
        )?;
        let rows = stmt.query_map([tenant], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<u64>>(1)?))
        })?;
        for row in rows {
            let (record, last) = row?;
            let last = last.ok_or_else(|| {
                Error::Corrupt("a stream holds keys or facts but has no events".into())
            })?;
            indexes.insert(last, AggregateIndex::decode(record.as_slice())?);
        }
    }
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT {EVENT_COLUMNS}, prev_hash FROM events
             WHERE (?1 IS NULL OR tenant_id = ?1) AND global_position > ?2
             ORDER BY global_position"
        ))?;
        let mut rows = stmt.query(params![tenant, since])?;
        let mut commit: Option<CommittedRecord> = None;
        while let Some(r) = rows.next()? {
            let event = event_from_row(r)?;
            let prev_hash: Vec<u8> = r.get(19)?;
            if let Some(done) = commit.take_if(|c| c.command_id != event.command_id) {
                f(Record::Committed(done))?;
            }
            let c = commit.get_or_insert_with(|| CommittedRecord {
                command_id: event.command_id.clone(),
                events: Vec::new(),
                index: None,
            });
            if let Some(index) = indexes.remove(&event.global_position) {
                c.index = Some(index);
            }
            c.events.push(LoggedEvent {
                event: Some(event),
                prev_hash,
            });
        }
        if let Some(done) = commit {
            f(Record::Committed(done))?;
        }
    }
    let ids = |status: &str| -> Result<Vec<String>> {
        let positions = if status == "committed" {
            " AND positions = ''"
        } else {
            ""
        };
        Ok(conn
            .prepare(&format!(
                "SELECT command_id FROM commands
                 WHERE status = ?1{positions} AND (?2 IS NULL OR tenant_id = ?2) ORDER BY rowid"
            ))?
            .query_map(params![status, tenant], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    };
    for command_id in ids("committed")? {
        f(Record::Committed(CommittedRecord {
            command_id,
            events: Vec::new(),
            index: None,
        }))?;
    }
    for command_id in ids("rejected")? {
        let reason = match stored_result(conn, &command_id)?.and_then(|r| r.outcome) {
            Some(Outcome::Rejected(reason)) => reason,
            _ => return Err(Error::Corrupt(format!("rejected command {command_id}"))),
        };
        f(Record::Rejected(RejectedRecord {
            command_id,
            reason: Some(reason),
        }))?;
    }
    Ok(())
}

fn image_records(conn: &Connection, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
    let grants: Vec<Vec<u8>> = conn
        .prepare("SELECT grant FROM grants ORDER BY key_id")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for g in grants {
        f(Record::Grant(Grant::decode(g.as_slice())?))?;
    }
    for r in read_revocations(conn)? {
        f(Record::Revocation(r))?;
    }
    for m in read_memberships(conn)? {
        f(Record::Membership(m))?;
    }
    for w in read_workers(conn)? {
        f(Record::Worker(w))?;
    }
    records_of(conn, None, None, f)?;
    for mut l in read_listeners(conn)? {
        l.parked_changed = true;
        f(Record::Listener(l))?;
    }
    if let Some(map) = read_partition_map(conn)? {
        f(Record::PartitionMap(map))?;
    }
    for record in read_imported(conn, None)? {
        f(Record::TenantImported(record))?;
    }
    let held: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'global_keys')",
        [],
        |r| r.get(0),
    )?;
    if held {
        let keys: Vec<(Vec<u8>, String)> = conn
            .prepare("SELECT id, partition FROM global_keys ORDER BY id")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (id, partition) in keys {
            f(Record::GlobalKey(GlobalKeyRecord { id, partition }))?;
        }
    }
    for cp in read_checkpoints(conn, None)? {
        f(Record::Checkpoint(CheckpointRecord {
            global_position: cp.global_position,
            chain_hash: cp.chain_hash,
            kernel_sig: Some(cp.kernel_sig),
            created_at: cp.created_at,
        }))?;
    }
    Ok(())
}
