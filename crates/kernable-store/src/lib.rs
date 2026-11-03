//! Pluggable storage for the kernel: the event log, the command queue and integrity anchors.
//!
//! Backends, selected by URL:
//! - `sqlite:<path>` (or a bare path): SQLite in WAL mode; several processes may open it.
//! - `file:<path>`: one append-only log file, state rebuilt in memory on open; one writer.
//! - `memory:`: the file backend's logic without a file; nothing survives the process.
//!
//! Every backend gives the same guarantees: commands are persisted before they run, a command
//! completes at most once, a commit appends all events of a command atomically and only on the
//! expected stream version, and events form a hash chain (see `proto/SIGNING.md`). Which
//! command runs when (one per aggregate, in submission order) is up to the kernel's queue.
//! They also keep the registered event listeners with their acknowledged positions, the
//! aggregates workers registered, and the unique keys and facts each aggregate holds
//! ([`Store::index`]), written with the commit that changed them.
//!
//! Every write is also a record of the [`replication`] record log, which a primary sends its
//! standby: the file log is one by itself, SQLite keeps one in a table while replication is on.
//!
//! Two more stores live apart from the log, because what they hold must be deletable:
//! [`snapshots`] (the latest snapshot per aggregate, a cache) and the [`vault`] (personal and
//! secret values that commands and events refer to).

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod data;
#[cfg(feature = "faults")]
pub mod faults;
mod file;
pub mod invariants;
pub mod replication;
pub mod snapshots;
mod sqlite;
mod sync;
pub mod vault;
mod verify;

use std::fmt;

use ed25519_dalek::SigningKey;
use kernable_core::proto::{
    AggregateIndex, Command, CommandResult, Event, Grant, ListenerRecord, MembershipRecord,
    NewEvent, PartitionMap, Rejected, ReplicatedRecord, RevocationRecord, Signature,
    TenantImportedRecord, WorkerRecord, log_record::Record,
};

/// The version of the stored data format this build reads and writes; a store of another
/// format is refused. Until the first release it stays 1 and changes in place: a store of an
/// earlier development build may not open, or may not read correctly.
pub const FORMAT: u32 = 1;

pub use file::FileStore;
pub use replication::Head;
pub use sqlite::SqliteStore;
pub use verify::Report;

#[derive(Debug)]
pub enum Error {
    /// The stream is no longer at the version the events were based on.
    VersionConflict {
        expected: u64,
        actual: u64,
    },
    /// The command is not pending: it is unknown or already completed.
    NotPending,
    /// The commit files a unique key another stream holds; nothing was written. The kernel
    /// checks keys before it commits, so this is a bug.
    KeyTaken,
    /// A write on a store opened read-only.
    ReadOnly,
    /// The stored data cannot be read back (other than a torn last record).
    Corrupt(String),
    /// The store has a data format other than [`FORMAT`].
    Format(u32),
    /// Vault values that do not match their references, or a vault that cannot be used.
    Vault(String),
    /// Records to apply that do not continue this store's record log, or that say something
    /// the store does not hold (see [`replication`]); nothing of them was written.
    Diverged(String),
    /// The store keeps no record log: SQLite with replication off, or `memory:`.
    NoRecordLog,
    Io(std::io::Error),
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
            Error::NotPending => write!(f, "command is not pending"),
            Error::KeyTaken => write!(f, "a unique key of the commit is held by another stream"),
            Error::ReadOnly => write!(f, "store is open read-only"),
            Error::Corrupt(what) => write!(f, "corrupt store: {what}"),
            Error::Format(found) if *found > FORMAT => write!(
                f,
                "store format {found} was written by a newer kernable; this one reads format {FORMAT}"
            ),
            Error::Format(found) => write!(
                f,
                "store format {found}, this kernable reads only format {FORMAT}: the store comes from an earlier development build"
            ),
            Error::Vault(what) => write!(f, "vault: {what}"),
            Error::Diverged(what) => write!(f, "the record log diverged: {what}"),
            Error::NoRecordLog => write!(f, "the store keeps no record log"),
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Sqlite(e) => write!(f, "sqlite: {e}"),
            Error::Decode(e) => write!(f, "decode: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

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

/// A write of a moving tenant's copy on the target (S4), for [`Store::import`].
#[derive(Debug, Clone)]
pub enum Copy {
    /// A command, as the source stored it.
    Enqueue { command: Command, created_at: i64 },
    /// Its commit: the events with their origin, at the stream's version before them.
    Commit {
        command_id: String,
        expected_version: u64,
        events: Vec<NewEvent>,
        producer_sig: Signature,
        index: Option<AggregateIndex>,
        at: i64,
    },
    /// Its rejection.
    Reject {
        command_id: String,
        reason: Rejected,
    },
}

/// Outcome of submitting a command.
#[derive(Debug, PartialEq)]
pub enum Enqueued {
    /// The command is new and waits for a worker.
    New,
    /// A command with this ID was submitted before; `result` is set once it is completed.
    Duplicate { result: Option<CommandResult> },
}

/// One aggregate's stream of events.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId {
    pub tenant_id: String,
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
}

/// An aggregate with events: its stream, version and newest event.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamSummary {
    pub stream: StreamId,
    pub version: u64,
    /// When its newest event was stored (ms since epoch).
    pub last_at: i64,
}

/// A command not completed yet.
#[derive(Debug, Clone, PartialEq)]
pub struct Pending {
    pub command: Command,
    /// When the kernel accepted it (ms since epoch).
    pub received_at: i64,
}

/// A kernel-signed anchor of the hash chain.
#[derive(Debug, Clone, PartialEq)]
pub struct Checkpoint {
    pub global_position: u64,
    pub chain_hash: Vec<u8>,
    pub kernel_sig: Signature,
    pub created_at: i64,
}

pub trait Store: Send + Sync {
    /// All events of one aggregate, ordered by version.
    fn read_stream(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<Event>> {
        self.read_stream_after(tenant_id, domain, aggregate_type, aggregate_id, 0)
    }

    /// The events of one aggregate with a version greater than `after_version`, in order.
    fn read_stream_after(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<Event>>;

    /// Up to `limit` events with a global position greater than `after`, in order.
    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>>;

    /// Global position of the newest event; 0 while there are none.
    fn head_position(&self) -> Result<u64>;

    /// Persists a command as pending. Idempotent on `command_id`.
    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued>;

    /// All commands not completed yet, in submission order.
    fn pending(&self) -> Result<Vec<Pending>>;

    /// Appends `events` to the pending command's stream, extends the hash chain and
    /// completes the command, atomically. `producer_sig` is the worker's result signature;
    /// verifying it is up to the caller. `index`, if set, replaces the keys and facts the
    /// stream holds, in the same write (also for a commit without events); checking it is up
    /// to the caller too.
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
    ) -> Result<Vec<Event>>;

    /// The unique keys and facts every stream holds now; streams that hold nothing are left out.
    fn index(&self) -> Result<Vec<(StreamId, AggregateIndex)>>;

    /// What `stream` holds now; `None` if it holds nothing.
    fn stream_index(&self, stream: &StreamId) -> Result<Option<AggregateIndex>>;

    /// The stream that holds the unique key with identity `id`, if any. Personal keys are not
    /// filed here: their identities live in the vault.
    fn key_holder(&self, id: &[u8]) -> Result<Option<StreamId>>;

    /// Whether any stream holds a personal key, which only the vault can resolve.
    fn holds_personal_keys(&self) -> Result<bool>;

    /// Every aggregate with events, the most recently changed first, at most `limit`.
    fn streams(&self, limit: usize) -> Result<Vec<StreamSummary>> {
        let mut latest = std::collections::HashMap::<StreamId, StreamSummary>::new();
        self.each_event(&mut |e| {
            let stream = StreamId {
                tenant_id: e.tenant_id.clone(),
                domain: e.domain.clone(),
                aggregate_type: e.aggregate_type.clone(),
                aggregate_id: e.aggregate_id.clone(),
            };
            latest.insert(
                stream.clone(),
                StreamSummary {
                    stream,
                    version: e.version,
                    last_at: e.created_at,
                },
            );
        })?;
        let mut all: Vec<StreamSummary> = latest.into_values().collect();
        all.sort_by(|a, b| b.last_at.cmp(&a.last_at));
        all.truncate(limit);
        Ok(all)
    }

    /// Which aggregate types wrote which event types: `(domain, aggregate_type, event_type)`,
    /// sorted.
    fn event_sources(&self) -> Result<Vec<(String, String, String)>> {
        let mut sources = std::collections::BTreeSet::new();
        self.each_event(&mut |e| {
            sources.insert((
                e.domain.clone(),
                e.aggregate_type.clone(),
                e.event_type.clone(),
            ));
        })?;
        Ok(sources.into_iter().collect())
    }

    /// Calls `f` with every event, in order.
    fn each_event(&self, f: &mut dyn FnMut(&Event)) -> Result<()> {
        let mut after = 0;
        loop {
            let events = self.read_all(after, 1000)?;
            let Some(last) = events.last() else {
                return Ok(());
            };
            after = last.global_position;
            events.iter().for_each(&mut *f);
        }
    }

    /// A stored command, pending or completed, with when the kernel received it.
    fn command(&self, command_id: &str) -> Result<Option<Pending>>;

    /// The IDs of every stored command, pending or completed, in the order they were stored.
    fn command_ids(&self) -> Result<Vec<String>>;

    /// The `limit` most recently received rejected commands, newest first, each with why.
    fn rejections(&self, limit: usize) -> Result<Vec<(Pending, Rejected)>>;

    /// Puts every write that returned before this call on the disk, if the store was opened
    /// with [`Durability::Full`]; concurrent calls share one flush (group commit). Call it
    /// before telling anyone about a write. An error means the store refuses every further
    /// write: stop and restart.
    fn sync(&self) -> Result<()> {
        Ok(())
    }

    /// The highest payload version (`schema`) stored per qualified event type (`domain.Type`).
    fn event_schemas(&self) -> Result<Vec<(String, u32)>> {
        let mut max = std::collections::BTreeMap::<String, u32>::new();
        let mut after = 0;
        loop {
            let events = self.read_all(after, 1000)?;
            let Some(last) = events.last() else {
                return Ok(max.into_iter().collect());
            };
            after = last.global_position;
            for e in &events {
                let v = max
                    .entry(format!("{}.{}", e.domain, e.event_type))
                    .or_default();
                *v = (*v).max(e.schema);
            }
        }
    }

    /// Completes the pending command as rejected by the business logic.
    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()>;

    /// The final result of a command, once it is committed or rejected.
    fn result(&self, command_id: &str) -> Result<Option<CommandResult>>;

    /// Registered listeners with their acknowledged positions, ordered by name.
    fn listeners(&self) -> Result<Vec<ListenerRecord>>;

    /// Stores listener registrations and positions by name; a record with `removed` set
    /// deletes the listener (with its parked events). Parked events are replaced only by a
    /// record with `parked_changed`; [`Store::listeners`] returns them without the flag.
    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()>;

    /// Registered aggregates (see `Register.aggregates`), ordered by aggregate type.
    fn workers(&self) -> Result<Vec<WorkerRecord>>;

    /// Stores aggregate registrations by aggregate type; a record with `removed` set deletes it.
    fn save_workers(&self, workers: &[WorkerRecord]) -> Result<()>;

    /// The applied memberships, one per service, ordered by service.
    fn memberships(&self) -> Result<Vec<MembershipRecord>>;

    /// Replaces the membership of the record's service.
    fn save_membership(&self, membership: &MembershipRecord) -> Result<()>;

    /// Signs the current head of the hash chain. Returns `None` while there are no events.
    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>>;

    /// Keeps a kernel-signed revocation; a key revoked before stays revoked as it was.
    fn revoke(&self, revocation: &RevocationRecord) -> Result<()>;

    /// Every revocation, in the order they were stored.
    fn revocations(&self) -> Result<Vec<RevocationRecord>>;

    /// The newest grant the kernel took into effect per key, ordered by key ID.
    fn grants(&self) -> Result<Vec<Grant>>;

    /// Keeps `grant` unless the store holds a grant of its key issued at the same time or later.
    fn save_grant(&self, grant: &Grant) -> Result<()>;

    /// Recomputes the hash chain and checks all signatures against the kernel's keys (the
    /// current one and any it had before); see [`Report`].
    fn verify(&self, kernel_key_ids: &[String]) -> Result<Report>;

    /// The newest partition map kept, if any.
    fn partition_map(&self) -> Result<Option<PartitionMap>> {
        Ok(None)
    }

    /// On the target of a move (S4): writes `copies` in order, as few writes as the backend
    /// allows; a command stored already is left as it is (one of another tenant under the same
    /// ID is refused, `Corrupt`), a commit or rejection of a command
    /// with a result is skipped, and a commit without events stands at its stream's version.
    fn import(&self, copies: &[Copy]) -> Result<()> {
        for copy in copies {
            match copy {
                Copy::Enqueue {
                    command,
                    created_at,
                } => {
                    if let Enqueued::Duplicate { .. } = self.enqueue(command, *created_at)? {
                        let held = self.command(&command.command_id)?;
                        if held.is_some_and(|h| h.command.tenant_id != command.tenant_id) {
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
                    if self.result(command_id)?.is_some() {
                        continue;
                    }
                    let commit = |version| {
                        self.commit(
                            command_id,
                            version,
                            events,
                            producer_sig,
                            index.as_ref(),
                            *at,
                        )
                    };
                    match commit(*expected_version) {
                        Err(Error::VersionConflict { actual, .. }) if events.is_empty() => {
                            commit(actual)?;
                        }
                        other => {
                            other?;
                        }
                    }
                }
                Copy::Reject { command_id, reason } => {
                    if self.result(command_id)?.is_none() {
                        self.reject(command_id, reason.clone())?;
                    }
                }
            }
        }
        Ok(())
    }

    /// On the target of a move (S4): that `tenant` was copied here, if it was.
    fn tenant_imported(&self, tenant: &str) -> Result<Option<TenantImportedRecord>> {
        let _ = tenant;
        Ok(None)
    }

    /// On the source of a move (S4): `tenant`'s streams hold no unique keys or facts here any
    /// more; returns how many streams held some.
    fn drop_tenant_index(&self, tenant: &str) -> Result<usize> {
        let _ = tenant;
        Err(Error::ReadOnly)
    }

    /// Notes that a tenant was copied here.
    fn save_tenant_imported(&self, record: &TenantImportedRecord) -> Result<()> {
        let _ = record;
        Err(Error::ReadOnly)
    }

    /// On the key owner (S3): the partition that may hold global key `id`, if any.
    fn global_key(&self, id: &[u8]) -> Result<Option<String>> {
        let _ = id;
        Ok(None)
    }

    /// On the key owner: global key `id` may be held by `partition` from now on.
    fn save_global_key(&self, id: &[u8], partition: &str) -> Result<()> {
        let _ = (id, partition);
        Err(Error::ReadOnly)
    }

    /// Keeps `map` unless a map of the same or a higher epoch is kept.
    fn save_partition_map(&self, map: &PartitionMap) -> Result<()> {
        let _ = map;
        Err(Error::ReadOnly)
    }

    // ---- the record log (see [`replication`]) ----

    /// Where the record log stands.
    fn record_head(&self) -> Result<Head> {
        Err(Error::NoRecordLog)
    }

    /// The hash of record `seq`, if the log holds it: from where the log starts (0 for a log
    /// that starts with the store) to its head.
    fn record_hash_at(&self, seq: u64) -> Result<Option<Vec<u8>>> {
        let _ = seq;
        Err(Error::NoRecordLog)
    }

    /// Up to `limit` records after record `seq`, in order; `None` unless the log holds record
    /// `seq` and the ones after it (it was pruned, started later, or is behind `seq`).
    fn records_after(&self, seq: u64, limit: usize) -> Result<Option<Vec<ReplicatedRecord>>> {
        let _ = (seq, limit);
        Err(Error::NoRecordLog)
    }

    /// Turns SQLite's record log on or off; the file log always is one. Turned on in a store
    /// that already holds data, a new log starts that no standby continues: each needs an
    /// image first. Turned off, the log is dropped.
    fn keep_record_log(&self, on: bool) -> Result<()> {
        let _ = on;
        Ok(())
    }

    /// Drops the records before `seq` from a log kept in a table (SQLite); record `seq` and
    /// the newest record stay. The file log keeps every record.
    fn prune_records(&self, seq: u64) -> Result<()> {
        let _ = seq;
        Ok(())
    }

    /// Applies records of another store's log that follow this store's head, in order. Each
    /// must chain to the one before and say something this store can take: a command
    /// enqueued once, events that continue the hash chain on their command's stream, a
    /// completion of a pending command, a checkpoint of an event it holds. The first that
    /// does not is [`Error::Diverged`] and is not written; records before it may stay applied
    /// (SQLite applies all or none), each a valid continuation.
    fn apply(&self, records: &[ReplicatedRecord]) -> Result<()> {
        let _ = records;
        Err(Error::NoRecordLog)
    }

    /// Calls `f` with the whole state as records, read in one consistent view, and returns
    /// the head of the record log at that view: a full image for a standby.
    fn image(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<Head> {
        let _ = f;
        Err(Error::NoRecordLog)
    }

    /// Calls `f` with the records of `tenant` (S4, moving a tenant): its commands as enqueued,
    /// then its commits in log order (the index with each stream's last), then its rejections,
    /// in one consistent view.
    fn tenant_records(&self, tenant: &str, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
        let mut commands = std::collections::HashSet::new();
        self.image(&mut |record| {
            let ours = match &record {
                Record::Enqueued(e) => {
                    let ours = e.command.as_ref().is_some_and(|c| c.tenant_id == tenant);
                    if ours {
                        commands.extend(e.command.iter().map(|c| c.command_id.clone()));
                    }
                    ours
                }
                Record::Committed(c) => commands.contains(&c.command_id),
                Record::Rejected(r) => commands.contains(&r.command_id),
                _ => false,
            };
            if ours { f(record) } else { Ok(()) }
        })?;
        Ok(())
    }

    /// Applies records of an image ([`Store::image`]) to an empty store, unnumbered, with the
    /// checks of [`Store::apply`]. Then [`Store::start_record_log`] at the image's head.
    fn apply_image(&self, records: &[Record]) -> Result<()> {
        let _ = records;
        Err(Error::NoRecordLog)
    }

    /// Starts the record log at `head`: where an applied image stands.
    fn start_record_log(&self, head: &Head) -> Result<()> {
        let _ = head;
        Err(Error::NoRecordLog)
    }

    /// Notes in the record log that a file of the data store was put or deleted.
    fn data_changed(&self, tenant_id: &str, domain: &str, key: &str) -> Result<()> {
        let _ = (tenant_id, domain, key);
        Ok(())
    }
}

/// How far a write has come when a store call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// On the disk (fsync, `F_FULLFSYNC` on macOS): survives a power loss or an OS crash.
    #[default]
    Full,
    /// Handed to the OS: survives a crash of the process, not of the machine. Faster; for
    /// development, or where the disk itself is replicated.
    Normal,
}

/// Opens a store for reading and writing: `sqlite:<path>`, `file:<path>`, `memory:` or a bare
/// SQLite path, with [`Durability::Full`].
pub fn open(url: &str) -> Result<Box<dyn Store>> {
    open_with(url, Durability::Full)
}

/// Like [`open`], with the given durability.
pub fn open_with(url: &str, durability: Durability) -> Result<Box<dyn Store>> {
    Ok(match backend(url) {
        ("memory", _) => Box::new(FileStore::in_memory()),
        ("file", path) => Box::new(FileStore::open_with(path, durability)?),
        (_, path) => Box::new(SqliteStore::open_with(path, durability)?),
    })
}

/// Opens a store for inspection, e.g. `kernable verify` while a kernel is running.
pub fn open_read_only(url: &str) -> Result<Box<dyn Store>> {
    Ok(match backend(url) {
        ("file", path) => Box::new(FileStore::open_read_only(path)?),
        ("memory", path) => Box::new(SqliteStore::open_unlocked(path)?),
        // A store that is not there is an error, never a new, empty one: a mistyped path must
        // not make `kernable verify` report an intact log.
        (_, path) if !std::path::Path::new(path).exists() => {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no store at {path}"),
            )));
        }
        (_, path) => Box::new(SqliteStore::open_unlocked(path)?),
    })
}

/// Splits a store URL into backend and path; a bare path means SQLite.
pub fn backend(url: &str) -> (&str, &str) {
    match url.split_once(':') {
        Some((scheme @ ("sqlite" | "file" | "memory"), path)) => (scheme, path),
        _ => ("sqlite", url),
    }
}
