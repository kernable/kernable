//! SQLite event store and persistent command queue.
//!
//! All writes run in `BEGIN IMMEDIATE` transactions, so several stores on the same
//! file (threads or processes) stay consistent; SQLite serializes them as a single writer.

use std::{
    fmt,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use kernable_core::proto::{
    Command, CommandResult, Committed, Event, NewEvent, Rejected, command_result::Outcome,
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, ToSql, Transaction, TransactionBehavior, params};

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
    UNIQUE (tenant_id, aggregate_type, aggregate_id, version)
);
CREATE TABLE IF NOT EXISTS commands (
    command_id       TEXT PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    aggregate_type   TEXT    NOT NULL,
    aggregate_id     TEXT    NOT NULL,
    command_type     TEXT    NOT NULL,
    payload          BLOB    NOT NULL,
    status           TEXT    NOT NULL, -- pending | leased | committed | rejected
    attempts         INTEGER NOT NULL DEFAULT 0,
    lease_id         TEXT,
    lease_expires_at INTEGER,
    result           BLOB,             -- encoded CommandResult once completed
    created_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS commands_ready ON commands (status, created_at);
CREATE INDEX IF NOT EXISTS commands_lease ON commands (lease_id);
";

const EVENT_COLUMNS: &str = "global_position, tenant_id, aggregate_type, aggregate_id, version, \
                             event_type, payload, command_id, created_at";

#[derive(Debug)]
pub enum Error {
    /// The stream is no longer at the version the events were based on.
    VersionConflict {
        expected: u64,
        actual: u64,
    },
    /// The lease is not held anymore: it expired and was re-leased, or the command is done.
    LeaseLost,
    Sqlite(rusqlite::Error),
    Decode(prost::DecodeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::VersionConflict { expected, actual } => {
                write!(
                    f,
                    "version conflict: expected {expected}, stream is at {actual}"
                )
            }
            Error::LeaseLost => write!(f, "lease lost"),
            Error::Sqlite(e) => write!(f, "sqlite: {e}"),
            Error::Decode(e) => write!(f, "decode: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sqlite(e)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::Decode(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Outcome of submitting a command.
#[derive(Debug, PartialEq)]
pub enum Enqueued {
    /// The command is new and waits for a worker.
    New,
    /// A command with this ID was submitted before; `result` is set once it is completed.
    Duplicate { result: Option<CommandResult> },
}

/// A command handed to a worker for a limited time.
#[derive(Debug, Clone)]
pub struct Lease {
    pub lease_id: String,
    pub command: Command,
}

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

    // ---- events ----

    /// All events of one aggregate, ordered by version.
    pub fn read_stream(
        &self,
        tenant_id: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<Event>> {
        read_events(
            &self.conn(),
            "WHERE tenant_id = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3 ORDER BY version",
            params![tenant_id, aggregate_type, aggregate_id],
        )
    }

    /// Up to `limit` events with a global position greater than `after`, in order.
    pub fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        read_events(
            &self.conn(),
            "WHERE global_position > ?1 ORDER BY global_position LIMIT ?2",
            params![after, limit],
        )
    }

    // ---- command queue ----

    /// Persists a command as pending. Idempotent on `command_id`.
    pub fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued> {
        let conn = self.conn();
        let inserted = conn.execute(
            "INSERT INTO commands (command_id, tenant_id, aggregate_type, aggregate_id,
                                   command_type, payload, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)
             ON CONFLICT (command_id) DO NOTHING",
            params![
                cmd.command_id,
                cmd.tenant_id,
                cmd.aggregate_type,
                cmd.aggregate_id,
                cmd.command_type,
                cmd.payload,
                now_ms
            ],
        )?;
        if inserted == 1 {
            return Ok(Enqueued::New);
        }
        let result = stored_result(&conn, &cmd.command_id)?;
        Ok(Enqueued::Duplicate { result })
    }

    /// Leases the oldest pending (or expired) command of one of `command_types` for `ttl_ms`.
    pub fn lease(&self, command_types: &[&str], now_ms: i64, ttl_ms: i64) -> Result<Option<Lease>> {
        if command_types.is_empty() {
            return Ok(None);
        }
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let placeholders = vec!["?"; command_types.len()].join(", ");
        let sql = format!(
            "SELECT command_id, tenant_id, aggregate_type, aggregate_id, command_type, payload, attempts
             FROM commands
             WHERE (status = 'pending' OR (status = 'leased' AND lease_expires_at <= ?1))
               AND command_type IN ({placeholders})
             ORDER BY created_at, rowid
             LIMIT 1"
        );
        let mut args: Vec<&dyn ToSql> = vec![&now_ms];
        args.extend(command_types.iter().map(|t| t as &dyn ToSql));
        let row = tx
            .query_row(&sql, args.as_slice(), |r| {
                let command = Command {
                    command_id: r.get(0)?,
                    tenant_id: r.get(1)?,
                    aggregate_type: r.get(2)?,
                    aggregate_id: r.get(3)?,
                    command_type: r.get(4)?,
                    payload: r.get(5)?,
                    client_sig: None,
                };
                Ok((command, r.get::<_, i64>(6)?))
            })
            .optional()?;
        let Some((command, attempts)) = row else {
            return Ok(None);
        };

        let attempts = attempts + 1;
        let lease_id = format!("{}#{attempts}", command.command_id);
        tx.execute(
            "UPDATE commands SET status = 'leased', attempts = ?2, lease_id = ?3, lease_expires_at = ?4
             WHERE command_id = ?1",
            params![command.command_id, attempts, lease_id, now_ms + ttl_ms],
        )?;
        tx.commit()?;
        Ok(Some(Lease { lease_id, command }))
    }

    /// Appends `events` to the leased command's stream and completes the command, atomically.
    ///
    /// On `VersionConflict` the lease stays held, so the caller can retry with fresh history.
    pub fn commit(
        &self,
        lease_id: &str,
        expected_version: u64,
        events: &[NewEvent],
        now_ms: i64,
    ) -> Result<Vec<Event>> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cmd = held_command(&tx, lease_id)?;

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

        let mut stored = Vec::with_capacity(events.len());
        for (version, e) in (expected_version + 1..).zip(events) {
            tx.execute(
                "INSERT INTO events (tenant_id, aggregate_type, aggregate_id, version,
                                     event_type, payload, command_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    cmd.tenant_id,
                    cmd.aggregate_type,
                    cmd.aggregate_id,
                    version,
                    e.event_type,
                    e.payload,
                    cmd.command_id,
                    now_ms
                ],
            )?;
            stored.push(Event {
                global_position: tx.last_insert_rowid() as u64,
                tenant_id: cmd.tenant_id.clone(),
                aggregate_type: cmd.aggregate_type.clone(),
                aggregate_id: cmd.aggregate_id.clone(),
                version,
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                command_id: cmd.command_id.clone(),
                created_at: now_ms,
                ..Default::default()
            });
        }

        let outcome = Outcome::Committed(Committed {
            events: stored.clone(),
        });
        complete(&tx, &cmd.command_id, "committed", outcome)?;
        tx.commit()?;
        Ok(stored)
    }

    /// Completes the leased command as rejected by the business logic.
    pub fn reject(&self, lease_id: &str, reason: Rejected) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cmd = held_command(&tx, lease_id)?;
        complete(&tx, &cmd.command_id, "rejected", Outcome::Rejected(reason))?;
        tx.commit()?;
        Ok(())
    }

    /// The final result of a command, once it is committed or rejected.
    pub fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        stored_result(&self.conn(), command_id)
    }
}

fn read_events(
    conn: &Connection,
    filter: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events {filter}"))?;
    let rows = stmt.query_map(params, |r| {
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
            ..Default::default()
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The stream identity of the command currently held under `lease_id`.
fn held_command(tx: &Transaction<'_>, lease_id: &str) -> Result<Command> {
    tx.query_row(
        "SELECT command_id, tenant_id, aggregate_type, aggregate_id FROM commands
         WHERE lease_id = ?1 AND status = 'leased'",
        [lease_id],
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
    .ok_or(Error::LeaseLost)
}

fn complete(tx: &Transaction<'_>, command_id: &str, status: &str, outcome: Outcome) -> Result<()> {
    let result = CommandResult {
        command_id: command_id.to_owned(),
        outcome: Some(outcome),
    };
    tx.execute(
        "UPDATE commands SET status = ?2, result = ?3, lease_id = NULL, lease_expires_at = NULL
         WHERE command_id = ?1",
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
