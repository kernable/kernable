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
        Command, CommandResult, Committed, Event, ListenerRecord, NewEvent, Rejected, Signature,
        SnapshotRecord, command_result::Outcome,
    },
    signing::{self, GENESIS},
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    Checkpoint, Enqueued, Error, Result, Store,
    verify::{Report, Verifier},
};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    aggregate_id    TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    payload         BLOB    NOT NULL,
    command_id      TEXT    NOT NULL,
    created_at      INTEGER NOT NULL,
    producer_key_id TEXT    NOT NULL,
    producer_sig    BLOB    NOT NULL,
    prev_hash       BLOB    NOT NULL,
    hash            BLOB    NOT NULL,
    UNIQUE (tenant_id, aggregate_type, aggregate_id, version)
) STRICT;
CREATE TABLE IF NOT EXISTS commands (
    command_id       TEXT PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    aggregate_type   TEXT    NOT NULL,
    aggregate_id     TEXT    NOT NULL,
    command_type     TEXT    NOT NULL,
    payload          BLOB    NOT NULL,
    client_key_id    TEXT,
    client_sig       BLOB,
    status           TEXT    NOT NULL, -- pending | committed | rejected
    result           BLOB,             -- encoded CommandResult once completed
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
CREATE TABLE IF NOT EXISTS snapshots (
    tenant_id      TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    record         BLOB NOT NULL, -- encoded SnapshotRecord
    PRIMARY KEY (tenant_id, aggregate_type, aggregate_id)
) STRICT;
CREATE TABLE IF NOT EXISTS listeners (
    name   TEXT PRIMARY KEY,
    record BLOB NOT NULL -- encoded ListenerRecord
) STRICT;
";

const EVENT_COLUMNS: &str = "global_position, tenant_id, aggregate_type, aggregate_id, version, \
                             event_type, payload, command_id, created_at, \
                             producer_key_id, producer_sig, hash";

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
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
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<Event>> {
        read_events(
            &self.conn(),
            "WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3 AND version > ?4
             ORDER BY version",
            params![tenant_id, aggregate_type, aggregate_id, after_version],
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

    // ---- command queue ----

    /// Persists a command as pending. Idempotent on `command_id`.
    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued> {
        let conn = self.conn();
        let (key_id, sig) = split_sig(cmd.client_sig.as_ref());
        let inserted = conn.execute(
            "INSERT INTO commands (command_id, tenant_id, aggregate_type, aggregate_id,
                                   command_type, payload, client_key_id, client_sig, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9)
             ON CONFLICT (command_id) DO NOTHING",
            params![
                cmd.command_id,
                cmd.tenant_id,
                cmd.aggregate_type,
                cmd.aggregate_id,
                cmd.command_type,
                cmd.payload,
                key_id,
                sig,
                now_ms
            ],
        )?;
        if inserted == 1 {
            return Ok(Enqueued::New);
        }
        let result = stored_result(&conn, &cmd.command_id)?;
        Ok(Enqueued::Duplicate { result })
    }

    /// All commands not completed yet, in submission order.
    fn pending(&self) -> Result<Vec<Command>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT command_id, tenant_id, aggregate_type, aggregate_id, command_type, payload,
                    client_key_id, client_sig
             FROM commands WHERE status = 'pending' ORDER BY created_at, rowid",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Command {
                command_id: r.get(0)?,
                tenant_id: r.get(1)?,
                aggregate_type: r.get(2)?,
                aggregate_id: r.get(3)?,
                command_type: r.get(4)?,
                payload: r.get(5)?,
                client_sig: join_sig(r.get(6)?, r.get(7)?),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Appends `events` to the pending command's stream, extends the hash chain and
    /// completes the command, atomically. `producer_sig` is the worker's result signature;
    /// verifying it is up to the caller.
    ///
    /// On `VersionConflict` the command stays pending, so the caller can retry with fresh history.
    fn commit(
        &self,
        command_id: &str,
        expected_version: u64,
        events: &[NewEvent],
        producer_sig: &Signature,
        now_ms: i64,
    ) -> Result<Vec<Event>> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cmd = pending_command(&tx, command_id)?;

        let actual: u64 = tx.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM events
             WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3",
            params![cmd.tenant_id, cmd.aggregate_type, cmd.aggregate_id],
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
                aggregate_type: cmd.aggregate_type.clone(),
                aggregate_id: cmd.aggregate_id.clone(),
                version,
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                command_id: cmd.command_id.clone(),
                created_at: now_ms,
                hash: vec![],
                producer_sig: Some(producer_sig.clone()),
            };
            event.hash = signing::event_hash(&prev, &event).to_vec();
            tx.execute(
                "INSERT INTO events (global_position, tenant_id, aggregate_type, aggregate_id, version,
                                     event_type, payload, command_id, created_at,
                                     producer_key_id, producer_sig, prev_hash, hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    event.global_position,
                    event.tenant_id,
                    event.aggregate_type,
                    event.aggregate_id,
                    event.version,
                    event.event_type,
                    event.payload,
                    event.command_id,
                    event.created_at,
                    producer_sig.key_id,
                    producer_sig.sig,
                    prev,
                    event.hash
                ],
            )?;
            prev = event.hash.clone();
            stored.push(event);
        }

        let outcome = Outcome::Committed(Committed {
            events: stored.clone(),
            settled: false,
        });
        complete(&tx, &cmd.command_id, "committed", outcome)?;
        tx.commit()?;
        Ok(stored)
    }

    /// Completes the pending command as rejected by the business logic.
    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cmd = pending_command(&tx, command_id)?;
        complete(&tx, &cmd.command_id, "rejected", Outcome::Rejected(reason))?;
        tx.commit()?;
        Ok(())
    }

    /// The final result of a command, once it is committed or rejected.
    fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        stored_result(&self.conn(), command_id)
    }

    // ---- snapshots ----

    fn snapshot(
        &self,
        tenant_id: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<SnapshotRecord>> {
        let bytes: Option<Vec<u8>> = self
            .conn()
            .query_row(
                "SELECT record FROM snapshots
                 WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3",
                params![tenant_id, aggregate_type, aggregate_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(bytes
            .map(|b| SnapshotRecord::decode(b.as_slice()))
            .transpose()?)
    }

    fn save_snapshot(&self, record: &SnapshotRecord) -> Result<()> {
        let r = record;
        if r.removed {
            self.conn().execute(
                "DELETE FROM snapshots
                 WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3",
                params![r.tenant_id, r.aggregate_type, r.aggregate_id],
            )?;
        } else {
            self.conn().execute(
                "INSERT INTO snapshots (tenant_id, aggregate_type, aggregate_id, record)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (tenant_id, aggregate_type, aggregate_id)
                 DO UPDATE SET record = excluded.record",
                params![
                    r.tenant_id,
                    r.aggregate_type,
                    r.aggregate_id,
                    r.encode_to_vec()
                ],
            )?;
        }
        Ok(())
    }

    // ---- listeners ----

    fn listeners(&self) -> Result<Vec<ListenerRecord>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT record FROM listeners ORDER BY name")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|bytes| Ok(ListenerRecord::decode(bytes?.as_slice())?))
            .collect()
    }

    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for l in listeners {
            if l.removed {
                tx.execute("DELETE FROM listeners WHERE name = ?1", [&l.name])?;
            } else {
                tx.execute(
                    "INSERT INTO listeners (name, record) VALUES (?1, ?2)
                     ON CONFLICT (name) DO UPDATE SET record = excluded.record",
                    params![l.name, l.encode_to_vec()],
                )?;
            }
        }
        tx.commit()?;
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

    /// Recomputes the hash chain and checks all signatures; see [`Report`].
    fn verify(&self, kernel_key_id: &str) -> Result<Report> {
        verify_tables(&self.conn(), kernel_key_id)
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

fn verify_tables(conn: &Connection, kernel_key_id: &str) -> Result<Report> {
    let mut v = Verifier::new(kernel_key_id, read_checkpoints(conn, None)?);

    let mut stmt = conn.prepare(&format!(
        "SELECT {EVENT_COLUMNS}, prev_hash FROM events ORDER BY global_position"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let prev_hash: Vec<u8> = r.get(12)?;
        v.event(event_from_row(r)?, &prev_hash);
    }

    let mut stmt = conn.prepare("SELECT record FROM snapshots")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let record = SnapshotRecord::decode(r.get::<_, Vec<u8>>(0)?.as_slice())?;
        let version = record.snapshot.as_ref().map_or(0, |s| s.version);
        let hash: Option<Vec<u8>> = conn
            .query_row(
                "SELECT hash FROM events
                 WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3 AND version = ?4",
                params![record.tenant_id, record.aggregate_type, record.aggregate_id, version],
                |r| r.get(0),
            )
            .optional()?;
        v.snapshot(&record, hash.as_deref());
    }

    let mut stmt = conn.prepare(
        "SELECT command_id, tenant_id, aggregate_type, aggregate_id, command_type, payload,
                client_key_id, client_sig
         FROM commands ORDER BY created_at, rowid",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        v.command(&Command {
            command_id: r.get(0)?,
            tenant_id: r.get(1)?,
            aggregate_type: r.get(2)?,
            aggregate_id: r.get(3)?,
            command_type: r.get(4)?,
            payload: r.get(5)?,
            client_sig: join_sig(r.get(6)?, r.get(7)?),
        });
    }
    Ok(v.finish())
}

fn event_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Event> {
    Ok(Event {
        global_position: r.get(0)?,
        tenant_id: r.get(1)?,
        aggregate_type: r.get(2)?,
        aggregate_id: r.get(3)?,
        version: r.get(4)?,
        event_type: r.get(5)?,
        payload: r.get(6)?,
        command_id: r.get(7)?,
        created_at: r.get(8)?,
        producer_sig: join_sig(r.get(9)?, r.get(10)?),
        hash: r.get(11)?,
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

/// The stream identity of a pending command.
fn pending_command(tx: &Transaction<'_>, command_id: &str) -> Result<Command> {
    tx.query_row(
        "SELECT command_id, tenant_id, aggregate_type, aggregate_id FROM commands
         WHERE command_id = ?1 AND status = 'pending'",
        [command_id],
        |r| {
            Ok(Command {
                command_id: r.get(0)?,
                tenant_id: r.get(1)?,
                aggregate_type: r.get(2)?,
                aggregate_id: r.get(3)?,
                ..Default::default()
            })
        },
    )
    .optional()?
    .ok_or(Error::NotPending)
}

fn complete(tx: &Transaction<'_>, command_id: &str, status: &str, outcome: Outcome) -> Result<()> {
    let result = CommandResult {
        command_id: command_id.to_owned(),
        outcome: Some(outcome),
    };
    tx.execute(
        "UPDATE commands SET status = ?2, result = ?3 WHERE command_id = ?1",
        params![command_id, status, result.encode_to_vec()],
    )?;
    Ok(())
}

fn stored_result(conn: &Connection, command_id: &str) -> Result<Option<CommandResult>> {
    let bytes: Option<Vec<u8>> = conn
        .query_row(
            "SELECT result FROM commands WHERE command_id = ?1",
            [command_id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    Ok(bytes
        .map(|b| CommandResult::decode(b.as_slice()))
        .transpose()?)
}
