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
        Attribute, Command, CommandResult, Committed, Event, ListenerRecord, NewEvent, Rejected,
        RequestContext, RevocationRecord, Signature, command_result::Outcome,
    },
    signing::{self, GENESIS},
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    Checkpoint, Enqueued, Error, FORMAT, Pending, Result, Store,
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
    payload         BLOB    NOT NULL,
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
    payload          BLOB    NOT NULL,
    client_key_id    TEXT,
    client_sig       BLOB,
    context          TEXT,             -- RequestContext, as JSON
    vault_refs       TEXT    NOT NULL DEFAULT '', -- comma-separated
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
CREATE TABLE IF NOT EXISTS revocations (
    key_id TEXT PRIMARY KEY,
    record BLOB NOT NULL -- encoded RevocationRecord
) STRICT;
CREATE TABLE IF NOT EXISTS listeners (
    name   TEXT PRIMARY KEY,
    record BLOB NOT NULL -- encoded ListenerRecord
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

    /// Creates the tables of a new database; refuses one of another data format.
    fn init(conn: Connection) -> Result<Self> {
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
                cmd.payload,
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
                    event.payload,
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

        let outcome = Outcome::Committed(Committed {
            events: stored.clone(),
            settled: false,
        });
        complete(&tx, &cmd.command_id, "committed", outcome)?;
        tx.commit()?;
        Ok(stored)
    }

    /// Completes the pending command as rejected by the business logic.
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
        complete(&tx, &cmd.command_id, "rejected", Outcome::Rejected(reason))?;
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

    fn revoke(&self, revocation: &RevocationRecord) -> Result<()> {
        self.conn().execute(
            "INSERT OR IGNORE INTO revocations (key_id, record) VALUES (?1, ?2)",
            params![revocation.key_id, revocation.encode_to_vec()],
        )?;
        Ok(())
    }

    fn revocations(&self) -> Result<Vec<RevocationRecord>> {
        read_revocations(&self.conn())
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
    let mut stmt = conn.prepare("SELECT record FROM revocations ORDER BY rowid")?;
    let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
    let mut out = Vec::new();
    for bytes in rows {
        out.push(RevocationRecord::decode(bytes?.as_slice())?);
    }
    Ok(out)
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
        context: context_from(r.get(12)?)?,
        domain: r.get(13)?,
        schema: r.get(14)?,
        vault_refs: split_refs(r.get(15)?),
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
        payload: r.get(5)?,
        client_sig: join_sig(r.get(6)?, r.get(7)?),
        context: context_from(r.get(8)?)?,
        domain: r.get(9)?,
        vault_refs: split_refs(r.get(10)?),
    })
}

fn split_refs(joined: String) -> Vec<String> {
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

/// Upgrades a SQLite store to [`FORMAT`]; returns the format it had.
pub(crate) fn migrate(path: &Path) -> Result<u32> {
    let conn = Connection::open(path)?;
    match conn.pragma_query_value(None, "user_version", |r| r.get(0))? {
        FORMAT => Ok(FORMAT),
        found => Err(Error::Format(found)),
    }
}
