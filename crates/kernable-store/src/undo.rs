//! Undoing the newest records of a SQLite store's record log (design/rewind.md, option 2): a
//! kernel that was cut off from its standby comes back with a few records the new primary never
//! had (commits under way, a listener's position). Instead of setting its stores aside and
//! copying everything again, it takes back exactly those and follows on.
//!
//! Every write that files records also files, in the same transaction, how to take it back: a
//! few steps written by hand for the writes that come often (an enqueued command, a commit, a
//! rejection, a checkpoint, a listener's position, a worker), `Irreversible` for the rest (a
//! grant, an erasure, a data file, …). The steps are kept until the standby holds the records.
//! A rewind applies them newest first and drops the records; it never goes past an irreversible
//! write: the store starts over instead, as before.

use prost::Message;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use kernable_core::proto::{AggregateIndex, Command, ListenerRecord, NewEvent, WorkerRecord};

use crate::{Error, Result, StreamId};

/// One step that takes part of a write back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Step {
    /// An enqueued command: it goes, with what the vault holds for it.
    DeleteCommand(String),
    /// A committed or rejected command: pending again.
    Pending(String),
    /// The vault values the command brought: a rewind needs them still held (its secrets are
    /// dropped once its completion is held by the standby, or after a crash).
    Values(Vec<String>),
    /// A commit's events, from this position on.
    DeleteEventsFrom(u64),
    /// An event type's schema as it was; `None`: the type was new.
    Schema {
        domain: String,
        aggregate_type: String,
        event_type: String,
        before: Option<u32>,
    },
    /// A stream's index (keys and facts) as it was, encoded; `None`: it held none.
    Index {
        tenant_id: String,
        domain: String,
        aggregate_type: String,
        aggregate_id: String,
        before: Option<Vec<u8>>,
    },
    /// A checkpoint at this position.
    DeleteCheckpoint(u64),
    /// A listener as it was, encoded with its middlewares and parked events; `None`: new.
    Listener {
        name: String,
        before: Option<Vec<u8>>,
    },
    /// A worker's registration as it was, encoded; `None`: new.
    Worker {
        aggregate_type: String,
        before: Option<Vec<u8>>,
    },
    /// The process engine's position as it was.
    ProcessPosition(u64),
    /// A write that is not taken back: a rewind stops before it.
    Irreversible(String),
}

pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS undo (
    first INTEGER PRIMARY KEY, -- the write's first record
    last  INTEGER NOT NULL,    -- and its last
    steps TEXT NOT NULL        -- JSON: how to take it back
) STRICT;
";

/// Files `steps` for the records `first..=last`.
pub(crate) fn file(tx: &Connection, first: u64, last: u64, steps: &[Step]) -> Result<()> {
    let text = serde_json::to_string(steps).map_err(|e| Error::Corrupt(e.to_string()))?;
    tx.execute(
        "INSERT OR REPLACE INTO undo (first, last, steps) VALUES (?1, ?2, ?3)",
        params![first, last, text],
    )?;
    // Without a standby nothing is acknowledged: the newest 100,000 records are undoable.
    if first.is_multiple_of(4096) {
        tx.execute("DELETE FROM undo WHERE last + 100000 < ?1", [first])?;
    }
    Ok(())
}

/// Drops the steps of records up to `seq`: the standby holds them. The commands among them
/// that completed with vault values: their secrets may go now.
pub(crate) fn forget(conn: &Connection, seq: u64) -> Result<Vec<String>> {
    let completed = completions(conn, Some(seq))?;
    conn.execute("DELETE FROM undo WHERE last <= ?1", [seq])?;
    Ok(completed)
}

/// The commands that completed with vault values in the writes still to be confirmed (up to
/// `seq`, or all): their secrets stay until then, so that a rewind finds them.
pub(crate) fn completions(conn: &Connection, seq: Option<u64>) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT steps FROM undo WHERE last <= ?1")?;
    let rows = stmt
        .query_map([seq.unwrap_or(u64::MAX >> 1)], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut ids = Vec::new();
    for steps in rows {
        let steps: Vec<Step> =
            serde_json::from_str(&steps).map_err(|e| Error::Corrupt(e.to_string()))?;
        if steps.iter().any(|s| matches!(s, Step::Values(_))) {
            ids.extend(steps.iter().filter_map(|s| match s {
                Step::Pending(id) => Some(id.clone()),
                _ => None,
            }));
        }
    }
    Ok(ids)
}

/// The records a rewind may stop at, newest first, down to the oldest it can reach: `head`,
/// then the record before each write it can take back, as long as the writes join up and none
/// is irreversible.
pub(crate) fn points(conn: &Connection, head: u64) -> Result<Vec<u64>> {
    let mut stmt = conn.prepare("SELECT first, last, steps FROM undo ORDER BY first DESC")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut points = vec![head];
    let mut next = head;
    for (first, last, steps) in rows {
        if last != next {
            break;
        }
        let steps: Vec<Step> =
            serde_json::from_str(&steps).map_err(|e| Error::Corrupt(e.to_string()))?;
        if steps.iter().any(|s| matches!(s, Step::Irreversible(_))) {
            break;
        }
        points.push(first - 1);
        next = first - 1;
    }
    Ok(points)
}

/// What taking back every write after record `to` needs and does, before it changes anything.
pub(crate) fn plan(conn: &Connection, to: u64) -> Result<crate::RewindPlan> {
    let mut stmt = conn.prepare("SELECT steps FROM undo WHERE last > ?1 ORDER BY first DESC")?;
    let rows = stmt
        .query_map([to], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut plan = crate::RewindPlan::default();
    let mut needed: Vec<(String, Vec<String>)> = Vec::new();
    for steps in rows {
        let steps: Vec<Step> =
            serde_json::from_str(&steps).map_err(|e| Error::Corrupt(e.to_string()))?;
        let pending = steps.iter().find_map(|s| match s {
            Step::Pending(id) => Some(id.clone()),
            _ => None,
        });
        for step in steps.iter().rev() {
            match step {
                Step::DeleteCommand(id) => plan.deletes.push(id.clone()),
                Step::Values(refs) => {
                    needed.push((pending.clone().unwrap_or_default(), refs.clone()));
                }
                _ => {}
            }
        }
    }
    // A command whose enqueue goes too needs nothing: it goes with its values.
    plan.needs = needed
        .into_iter()
        .filter(|(id, _)| !plan.deletes.contains(id))
        .flat_map(|(_, refs)| refs)
        .collect();
    Ok(plan)
}

/// Takes back every write after record `to`, newest first; the commands it deleted (their vault
/// values go too). Refused if one of them cannot be taken back.
pub(crate) fn rewind(tx: &Transaction<'_>, to: u64) -> Result<Vec<String>> {
    let rows = {
        let mut stmt =
            tx.prepare("SELECT first, last, steps FROM undo WHERE last > ?1 ORDER BY first DESC")?;
        stmt.query_map([to], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut deleted = Vec::new();
    for (first, _, steps) in &rows {
        if *first <= to {
            return Err(Error::Diverged(format!(
                "record {to} is inside a write ({first}…): it cannot be rewound to"
            )));
        }
        let steps: Vec<Step> =
            serde_json::from_str(steps).map_err(|e| Error::Corrupt(e.to_string()))?;
        for step in steps.iter().rev() {
            if let Step::DeleteCommand(id) = step {
                deleted.push(id.clone());
            }
            apply(tx, step)?;
        }
    }
    tx.execute("DELETE FROM undo WHERE last > ?1", [to])?;
    Ok(deleted)
}

fn apply(tx: &Transaction<'_>, step: &Step) -> Result<()> {
    match step {
        Step::DeleteCommand(id) => {
            tx.execute("DELETE FROM commands WHERE command_id = ?1", [id])?;
        }
        Step::Pending(id) => {
            tx.execute(
                "UPDATE commands SET status = 'pending', positions = '', rejected_code = NULL,
                        rejected_message = NULL, rejected_key = NULL, rejected_requirement = NULL
                 WHERE command_id = ?1",
                [id],
            )?;
        }
        Step::DeleteEventsFrom(position) => {
            tx.execute("DELETE FROM events WHERE global_position >= ?1", [position])?;
        }
        Step::Schema {
            domain,
            aggregate_type,
            event_type,
            before,
        } => match before {
            Some(schema) => {
                tx.execute(
                    "UPDATE event_types SET schema = ?4
                     WHERE domain = ?1 AND aggregate_type = ?2 AND event_type = ?3",
                    params![domain, aggregate_type, event_type, schema],
                )?;
            }
            None => {
                tx.execute(
                    "DELETE FROM event_types
                     WHERE domain = ?1 AND aggregate_type = ?2 AND event_type = ?3",
                    params![domain, aggregate_type, event_type],
                )?;
            }
        },
        Step::Index {
            tenant_id,
            domain,
            aggregate_type,
            aggregate_id,
            before,
        } => {
            let index = match before {
                Some(bytes) => AggregateIndex::decode(bytes.as_slice())
                    .map_err(|e| Error::Corrupt(e.to_string()))?,
                None => AggregateIndex::default(),
            };
            let stream = StreamId {
                tenant_id: tenant_id.clone(),
                domain: domain.clone(),
                aggregate_type: aggregate_type.clone(),
                aggregate_id: aggregate_id.clone(),
            };
            crate::sqlite::set_stream_index_tx(tx, &stream, &index)?;
        }
        Step::DeleteCheckpoint(position) => {
            tx.execute(
                "DELETE FROM checkpoints WHERE global_position = ?1",
                [position],
            )?;
        }
        Step::Listener { name, before } => match before {
            Some(bytes) => {
                let mut old = ListenerRecord::decode(bytes.as_slice())
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
                old.parked_changed = true;
                crate::sqlite::save_listener_tx(tx, &old)?;
            }
            None => {
                tx.execute("DELETE FROM parked WHERE listener = ?1", [name])?;
                tx.execute("DELETE FROM middlewares WHERE listener = ?1", [name])?;
                tx.execute("DELETE FROM listeners WHERE name = ?1", [name])?;
            }
        },
        Step::Worker {
            aggregate_type,
            before,
        } => match before {
            Some(bytes) => {
                let old = WorkerRecord::decode(bytes.as_slice())
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
                crate::sqlite::save_worker_tx(tx, &old)?;
            }
            None => {
                tx.execute(
                    "DELETE FROM workers WHERE aggregate_type = ?1",
                    [aggregate_type],
                )?;
            }
        },
        Step::ProcessPosition(position) => {
            crate::sqlite::save_process_position_tx(tx, *position)?;
        }
        Step::Values(_) => {}
        Step::Irreversible(what) => {
            return Err(Error::Diverged(format!("{what} cannot be taken back")));
        }
    }
    Ok(())
}

// ---- what each write keeps, read before it changes anything

/// For a commit of `command` with `events`: its command pending again, each event type's schema
/// and the stream's index as they were. The events themselves are added once their positions
/// are known ([`Step::DeleteEventsFrom`]). Irreversible if the command carried vault values (its
/// secrets are dropped once it completes) or an event erases a subject.
pub(crate) fn for_commit(
    tx: &Transaction<'_>,
    command: &Command,
    events: &[NewEvent],
    index: bool,
) -> Result<Vec<Step>> {
    if events.iter().any(|e| !e.erases.is_empty()) {
        return Ok(vec![Step::Irreversible("an erasure".into())]);
    }
    let mut steps = vec![Step::Pending(command.command_id.clone())];
    if !command.vault_refs.is_empty() {
        steps.push(Step::Values(command.vault_refs.clone()));
    }
    for e in events {
        let before: Option<u32> = tx
            .query_row(
                "SELECT schema FROM event_types
                 WHERE domain = ?1 AND aggregate_type = ?2 AND event_type = ?3",
                params![command.domain, command.aggregate_type, e.event_type],
                |r| r.get(0),
            )
            .optional()?;
        let step = Step::Schema {
            domain: command.domain.clone(),
            aggregate_type: command.aggregate_type.clone(),
            event_type: e.event_type.clone(),
            before,
        };
        if !steps.contains(&step) {
            steps.push(step);
        }
    }
    if index {
        let before = crate::sqlite::stream_index(
            tx,
            params![
                command.tenant_id,
                command.domain,
                command.aggregate_type,
                command.aggregate_id
            ],
        )?
        .map(|i| i.encode_to_vec());
        steps.push(Step::Index {
            tenant_id: command.tenant_id.clone(),
            domain: command.domain.clone(),
            aggregate_type: command.aggregate_type.clone(),
            aggregate_id: command.aggregate_id.clone(),
            before,
        });
    }
    Ok(steps)
}

/// For a rejection of `command`: pending again, with its vault values if it brought any.
pub(crate) fn for_reject(command: &Command) -> Vec<Step> {
    let mut steps = vec![Step::Pending(command.command_id.clone())];
    if !command.vault_refs.is_empty() {
        steps.push(Step::Values(command.vault_refs.clone()));
    }
    steps
}

/// For saving `listeners`: each as it was.
pub(crate) fn for_listeners(conn: &Connection, listeners: &[ListenerRecord]) -> Result<Vec<Step>> {
    let held = crate::sqlite::read_listeners(conn)?;
    Ok(listeners
        .iter()
        .map(|l| Step::Listener {
            name: l.name.clone(),
            before: held
                .iter()
                .find(|h| h.name == l.name)
                .map(Message::encode_to_vec),
        })
        .collect())
}

/// For saving `workers`: each registration as it was.
pub(crate) fn for_workers(conn: &Connection, workers: &[WorkerRecord]) -> Result<Vec<Step>> {
    let held = crate::sqlite::read_workers(conn)?;
    Ok(workers
        .iter()
        .map(|w| Step::Worker {
            aggregate_type: w.aggregate_type.clone(),
            before: held
                .iter()
                .find(|h| h.aggregate_type == w.aggregate_type)
                .map(Message::encode_to_vec),
        })
        .collect())
}

/// What a write that nothing takes back was, for the reason a rewind stops.
pub(crate) fn record_kind(record: &kernable_core::proto::log_record::Record) -> &'static str {
    use kernable_core::proto::log_record::Record;
    match record {
        Record::Enqueued(_) => "an enqueued command",
        Record::Committed(_) => "a commit",
        Record::Rejected(_) => "a rejection",
        Record::Checkpoint(_) => "a checkpoint",
        Record::Listener(_) => "a listener",
        Record::Format(_) => "the format",
        Record::Revocation(_) => "a revocation",
        Record::Worker(_) => "a worker",
        Record::Membership(_) => "a membership",
        Record::Grant(_) => "a grant",
        Record::DataChanged(_) => "a data file",
        Record::ReplicationHead(_) => "the start of the record log",
        Record::PartitionMap(_) => "a partition map",
        Record::GlobalKey(_) => "a global key",
        Record::TenantImported(_) => "an imported tenant",
        Record::TenantIndex(_) => "a tenant's index",
        Record::Process(_) => "a process",
        Record::ProcessPosition(_) => "the process engine's position",
        Record::Copy(_) => "the mark of a masked copy",
    }
}
