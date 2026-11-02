//! SQLite backend (`sqlite:<path>`).
//!
//! All writes run in `BEGIN IMMEDIATE` transactions, so several stores on the same
//! file (threads or processes) stay consistent; SQLite serializes them as a single writer.
//! Tables are `STRICT`, so stored values always have the declared types.
//! Events form a hash chain anchored by kernel-signed checkpoints (see `proto/SIGNING.md`).

use std::{
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        AggregateIndex, Attribute, Command, CommandResult, Committed, Event, Grant, ListenerRecord,
        Membership, MembershipRecord, Middleware, NewEvent, Rejected, RequestContext, Requirement,
        RevocationRecord, Signature, When, WorkerRecord, command_result::Outcome,
    },
    signing::{self, GENESIS},
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    Checkpoint, Durability, Enqueued, Error, FORMAT, Pending, Result, Store, StreamId,
    StreamSummary,
    sync::Syncer,
    verify::{Report, Verifier},
};

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
    tenant_id      TEXT NOT NULL,
    domain         TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    record         BLOB NOT NULL, -- the AggregateIndex message; a stream holding nothing has no row
    PRIMARY KEY (tenant_id, domain, aggregate_type, aggregate_id)
) STRICT;
";

const EVENT_COLUMNS: &str = "global_position, tenant_id, aggregate_type, aggregate_id, version, \
                             event_type, payload, command_id, created_at, \
                             producer_key_id, producer_sig, hash, context, domain, schema, \
                             vault_refs, erases";

const COMMAND_COLUMNS: &str = "command_id, tenant_id, aggregate_type, aggregate_id, command_type, \
                               payload, client_key_id, client_sig, context, domain, vault_refs";

pub struct SqliteStore {
    conn: Mutex<Connection>,
    /// `<path>.lock`, held exclusively while a writer has the store open: the queue lives in
    /// one kernel's memory, so a second kernel on the same store would run commands twice.
    _lock: Option<std::fs::File>,
    /// The write-ahead log to flush in [`Store::sync`]; `None` unless [`Durability::Full`].
    wal: Option<std::path::PathBuf>,
    syncer: Syncer,
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
                conn.execute_batch(SCHEMA)?;
                conn.pragma_update(None, "user_version", FORMAT)?;
            }
            (FORMAT, _) => {}
            (found, _) => return Err(Error::Format(found)),
        }
        Ok(Self {
            conn: Mutex::new(conn),
            _lock: None,
            wal: None,
            syncer: Syncer::default(),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
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
        read_events(
            &self.conn(),
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
    }

    /// Up to `limit` events with a global position greater than `after`, in order.
    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        read_events(
            &self.conn(),
            "WHERE global_position > ?1 ORDER BY global_position LIMIT ?2",
            params![after, limit],
        )
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
        let conn = self.conn();
        let (key_id, sig) = split_sig(cmd.client_sig.as_ref());
        let inserted = conn.execute(
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
        if inserted == 1 {
            return Ok(Enqueued::New);
        }
        let result = stored_result(&conn, &cmd.command_id)?;
        Ok(Enqueued::Duplicate { result })
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
        let cmd = pending_command(&tx, command_id)?;

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
            };
            event.hash = signing::event_hash(&prev, &event).to_vec();
            tx.execute(
                "INSERT INTO events (global_position, tenant_id, aggregate_type, aggregate_id, version,
                                     event_type, payload, command_id, created_at,
                                     producer_key_id, producer_sig, prev_hash, hash, context, domain,
                                     schema, vault_refs, erases)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                         ?18)",
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
                    event.erases
                ],
            )?;
            prev = event.hash.clone();
            stored.push(event);
        }

        let stream = params![
            cmd.tenant_id,
            cmd.domain,
            cmd.aggregate_type,
            cmd.aggregate_id
        ];
        match index {
            None => {}
            Some(index) if index.keys.is_empty() && index.facts.is_empty() => {
                tx.execute(
                    "DELETE FROM aggregate_index
                     WHERE tenant_id = ?1 AND domain = ?2 AND aggregate_type = ?3
                       AND aggregate_id = ?4",
                    stream,
                )?;
            }
            Some(index) => {
                tx.execute(
                    "INSERT INTO aggregate_index
                         (tenant_id, domain, aggregate_type, aggregate_id, record)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT (tenant_id, domain, aggregate_type, aggregate_id)
                     DO UPDATE SET record = excluded.record",
                    params![
                        cmd.tenant_id,
                        cmd.domain,
                        cmd.aggregate_type,
                        cmd.aggregate_id,
                        index.encode_to_vec()
                    ],
                )?;
            }
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
        tx.commit()?;
        Ok(stored)
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
            "SELECT DISTINCT domain, aggregate_type, event_type FROM events ORDER BY 1, 2, 3",
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
            "SELECT domain || '.' || event_type, MAX(schema) FROM events GROUP BY domain, event_type",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cmd = pending_command(&tx, command_id)?;
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
        tx.commit()?;
        Ok(())
    }

    /// The final result of a command, once it is committed or rejected.
    fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        stored_result(&self.conn(), command_id)
    }

    // ---- listeners ----

    fn listeners(&self) -> Result<Vec<ListenerRecord>> {
        let conn = self.conn();
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
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
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

    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for l in listeners {
            // Removing a listener removes its middlewares too (ON DELETE CASCADE).
            tx.execute("DELETE FROM middlewares WHERE listener = ?1", [&l.name])?;
            if l.removed {
                tx.execute("DELETE FROM listeners WHERE name = ?1", [&l.name])?;
                continue;
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
        }
        tx.commit()?;
        Ok(())
    }

    // ---- workers ----

    fn workers(&self) -> Result<Vec<WorkerRecord>> {
        let conn = self.conn();
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

    fn save_workers(&self, workers: &[WorkerRecord]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for w in workers {
            if w.removed {
                tx.execute(
                    "DELETE FROM workers WHERE aggregate_type = ?1",
                    [&w.aggregate_type],
                )?;
                continue;
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
        }
        tx.commit()?;
        Ok(())
    }

    // ---- memberships ----

    fn memberships(&self) -> Result<Vec<MembershipRecord>> {
        let conn = self.conn();
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

    fn save_membership(&self, record: &MembershipRecord) -> Result<()> {
        let m = record.membership.clone().unwrap_or_default();
        let sig = record.sig.clone().unwrap_or_default();
        let metadata: Vec<(&str, &str)> = m
            .metadata
            .iter()
            .map(|a| (a.key.as_str(), a.value.as_str()))
            .collect();
        self.conn().execute(
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

    // ---- integrity ----

    /// Signs the current head of the hash chain. Returns `None` while there are no events.
    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>> {
        let conn = self.conn();
        let head = conn
            .query_row(
                "SELECT global_position, hash FROM events ORDER BY global_position DESC LIMIT 1",
                [],
                |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        let Some((global_position, chain_hash)) = head else {
            return Ok(None);
        };
        if let Some(existing) = read_checkpoints(&conn, Some(global_position))?.pop() {
            return Ok(Some(existing));
        }
        let msg = signing::checkpoint_message(global_position, &chain_hash, now_ms);
        let kernel_sig = signing::sign(kernel, &msg);
        conn.execute(
            "INSERT OR IGNORE INTO checkpoints (global_position, chain_hash, kernel_key_id, kernel_sig, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                global_position,
                chain_hash,
                kernel_sig.key_id,
                kernel_sig.sig,
                now_ms
            ],
        )?;
        Ok(Some(Checkpoint {
            global_position,
            chain_hash,
            kernel_sig,
            created_at: now_ms,
        }))
    }

    fn revoke(&self, revocation: &RevocationRecord) -> Result<()> {
        let sig = revocation.kernel_sig.as_ref();
        self.conn().execute(
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
        self.conn().execute(
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
        Ok(())
    }

    /// Recomputes the hash chain and checks all signatures; see [`Report`].
    fn verify(&self, kernel_key_ids: &[String]) -> Result<Report> {
        verify_tables(&self.conn(), kernel_key_ids)
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
        let prev_hash: Vec<u8> = r.get(17)?;
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
