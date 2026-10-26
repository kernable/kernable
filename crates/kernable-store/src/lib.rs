//! Pluggable storage for the kernel: the event log, the command queue and integrity anchors.
//!
//! Backends, selected by URL:
//! - `sqlite:<path>` (or a bare path): SQLite in WAL mode; several processes may open it.
//! - `file:<path>`: one append-only log file, state rebuilt in memory on open; one writer.
//! - `memory:`: the file backend's logic without a file; nothing survives the process.
//!
//! Every backend gives the same guarantees: commands are persisted before they run, at most
//! one command per aggregate is leased at a time and in submission order, a commit appends all
//! events of a command atomically, and events form a hash chain (see `proto/SIGNING.md`).

mod file;
mod sqlite;
mod verify;

use std::fmt;

use ed25519_dalek::SigningKey;
use kernable_core::proto::{Command, CommandResult, Event, NewEvent, Rejected, Signature};

pub use file::FileStore;
pub use sqlite::SqliteStore;
pub use verify::Report;

#[derive(Debug)]
pub enum Error {
    /// The stream is no longer at the version the events were based on.
    VersionConflict {
        expected: u64,
        actual: u64,
    },
    /// The lease is not held anymore: it expired and was re-leased, or the command is done.
    LeaseLost,
    /// A write on a store opened read-only.
    ReadOnly,
    /// The stored data cannot be read back (other than a torn last record).
    Corrupt(String),
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
            Error::LeaseLost => write!(f, "lease lost"),
            Error::ReadOnly => write!(f, "store is open read-only"),
            Error::Corrupt(what) => write!(f, "corrupt store: {what}"),
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
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<Event>>;

    /// Up to `limit` events with a global position greater than `after`, in order.
    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>>;

    /// Persists a command as pending. Idempotent on `command_id`.
    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued>;

    /// Leases the oldest pending (or expired) command of one of `command_types` for `ttl_ms`.
    ///
    /// Per aggregate, at most one command is leased at a time and commands are leased in
    /// submission order; commands of a busy aggregate wait in the queue.
    fn lease(&self, command_types: &[&str], now_ms: i64, ttl_ms: i64) -> Result<Option<Lease>>;

    /// Appends `events` to the leased command's stream, extends the hash chain and
    /// completes the command, atomically. `producer_sig` is the worker's result signature;
    /// verifying it is up to the caller.
    ///
    /// On `VersionConflict` the lease stays held, so the caller can retry with fresh history.
    fn commit(
        &self,
        lease_id: &str,
        expected_version: u64,
        events: &[NewEvent],
        producer_sig: &Signature,
        now_ms: i64,
    ) -> Result<Vec<Event>>;

    /// Completes the leased command as rejected by the business logic.
    fn reject(&self, lease_id: &str, reason: Rejected) -> Result<()>;

    /// Hands a leased command back to the queue before its lease expires.
    /// Returns false if the lease was not held anymore.
    fn release(&self, lease_id: &str) -> Result<bool>;

    /// Hands every leased command back to the queue, e.g. on shutdown.
    fn release_all(&self) -> Result<usize>;

    /// The final result of a command, once it is committed or rejected.
    fn result(&self, command_id: &str) -> Result<Option<CommandResult>>;

    /// Signs the current head of the hash chain. Returns `None` while there are no events.
    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>>;

    /// Recomputes the hash chain and checks all signatures; see [`Report`].
    fn verify(&self, kernel_key_id: &str) -> Result<Report>;
}

/// Opens a store for reading and writing: `sqlite:<path>`, `file:<path>`, `memory:` or a bare
/// SQLite path.
pub fn open(url: &str) -> Result<Box<dyn Store>> {
    Ok(match backend(url) {
        ("memory", _) => Box::new(FileStore::in_memory()),
        ("file", path) => Box::new(FileStore::open(path)?),
        (_, path) => Box::new(SqliteStore::open(path)?),
    })
}

/// Opens a store for inspection, e.g. `kernable verify` while a kernel is running.
pub fn open_read_only(url: &str) -> Result<Box<dyn Store>> {
    Ok(match backend(url) {
        ("file", path) => Box::new(FileStore::open_read_only(path)?),
        (_, path) => Box::new(SqliteStore::open(path)?),
    })
}

/// Splits a store URL into backend and path; a bare path means SQLite.
pub fn backend(url: &str) -> (&str, &str) {
    match url.split_once(':') {
        Some((scheme @ ("sqlite" | "file" | "memory"), path)) => (scheme, path),
        _ => ("sqlite", url),
    }
}
