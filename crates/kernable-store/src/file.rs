//! Append-only file backend (`file:<path>`), and the same logic without a file (`memory:`).
//!
//! The whole store is one log of records (`proto/kernable/v1/storage.proto`). The first holds
//! the data format (see [`crate::FORMAT`]); then come enqueued
//! commands, commits (all events of one command in a single record, hence atomic),
//! rejections, checkpoints and listener positions. Each record is framed as `u32 BE length || first 4 bytes of
//! SHA-256(body) || body`. Opening replays the log into memory. A torn last record from a
//! crash is moved to `<path>.torn-<offset>` and cut off; damage anywhere else refuses to open.
//!
//! One process writes (exclusive file lock); others may open the log read-only.
//! Writes reach the OS before a call returns but are not fsynced, which survives a process
//! crash but not a power loss (like SQLite with `synchronous=NORMAL`).
//!
//! `memory:` keeps the state only in memory: nothing survives the process. For tests and
//! benchmarks.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, PoisonError},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        CheckpointRecord, Command, CommandResult, Committed, CommittedRecord, EnqueuedRecord,
        Event, FormatRecord, ListenerRecord, LogRecord, LoggedEvent, NewEvent, Rejected,
        RejectedRecord, RevocationRecord, Signature, command_result::Outcome, log_record::Record,
    },
    signing::{self, GENESIS},
};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::{
    Checkpoint, Enqueued, Error, FORMAT, Pending, Result, Store,
    verify::{Report, Verifier},
};

const HEADER: usize = 8;

/// Tenant, domain, aggregate type, aggregate ID.
type StreamKey = (String, String, String, String);

fn stream_key(c: &Command) -> StreamKey {
    (
        c.tenant_id.clone(),
        c.domain.clone(),
        c.aggregate_type.clone(),
        c.aggregate_id.clone(),
    )
}

struct Cmd {
    command: Command,
    received_at: i64,
    /// Submission order.
    seq: u64,
    result: Option<CommandResult>,
}

/// Everything the log says, indexed for the queries the kernel makes.
#[derive(Default)]
struct State {
    /// Index `i` holds global position `i + 1`.
    events: Vec<Event>,
    prev_hashes: Vec<Vec<u8>>,
    streams: HashMap<StreamKey, Vec<usize>>,
    commands: HashMap<String, Cmd>,
    /// Commands not yet completed, in submission order: seq -> command ID.
    open: BTreeMap<u64, String>,
    next_seq: u64,
    checkpoints: Vec<Checkpoint>,
    listeners: BTreeMap<String, ListenerRecord>,
    revocations: Vec<RevocationRecord>,
}

impl State {
    fn apply(&mut self, record: Record) -> Result<()> {
        match record {
            // Checked by `replay`, which only accepts it first.
            Record::Format(_) => {}
            Record::Enqueued(EnqueuedRecord {
                command,
                created_at,
            }) => {
                let command = command.ok_or_else(|| corrupt("enqueued record without command"))?;
                let id = command.command_id.clone();
                if self.commands.contains_key(&id) {
                    return Err(corrupt(&format!("command {id} enqueued twice")));
                }
                let seq = self.next_seq;
                self.next_seq += 1;
                self.open.insert(seq, id.clone());
                let cmd = Cmd {
                    command,
                    received_at: created_at,
                    seq,
                    result: None,
                };
                self.commands.insert(id, cmd);
            }
            Record::Committed(CommittedRecord { command_id, events }) => {
                let mut stored = Vec::with_capacity(events.len());
                for logged in events {
                    let event = logged
                        .event
                        .ok_or_else(|| corrupt("commit without event"))?;
                    if event.global_position != self.events.len() as u64 + 1 {
                        return Err(corrupt(&format!(
                            "event {} out of order",
                            event.global_position
                        )));
                    }
                    let key = (
                        event.tenant_id.clone(),
                        event.domain.clone(),
                        event.aggregate_type.clone(),
                        event.aggregate_id.clone(),
                    );
                    self.streams.entry(key).or_default().push(self.events.len());
                    self.prev_hashes.push(logged.prev_hash);
                    self.events.push(event.clone());
                    stored.push(event);
                }
                self.complete(
                    &command_id,
                    Outcome::Committed(Committed {
                        events: stored,
                        settled: false,
                    }),
                )?;
            }
            Record::Rejected(RejectedRecord { command_id, reason }) => {
                self.complete(&command_id, Outcome::Rejected(reason.unwrap_or_default()))?;
            }
            Record::Checkpoint(cp) => self.checkpoints.push(Checkpoint {
                global_position: cp.global_position,
                chain_hash: cp.chain_hash,
                kernel_sig: cp.kernel_sig.unwrap_or_default(),
                created_at: cp.created_at,
            }),
            Record::Revocation(r) => {
                if !self.revocations.iter().any(|k| k.key_id == r.key_id) {
                    self.revocations.push(r);
                }
            }
            Record::Listener(l) if l.removed => {
                self.listeners.remove(&l.name);
            }
            Record::Listener(l) => {
                self.listeners.insert(l.name.clone(), l);
            }
        }
        Ok(())
    }

    fn complete(&mut self, command_id: &str, outcome: Outcome) -> Result<()> {
        let cmd = self
            .commands
            .get_mut(command_id)
            .filter(|c| c.result.is_none())
            .ok_or_else(|| {
                corrupt(&format!(
                    "completion of unknown or done command {command_id}"
                ))
            })?;
        self.open.remove(&cmd.seq);
        cmd.result = Some(CommandResult {
            command_id: command_id.to_owned(),
            outcome: Some(outcome),
        });
        Ok(())
    }

    fn pending(&self, command_id: &str) -> Result<&Cmd> {
        self.commands
            .get(command_id)
            .filter(|c| c.result.is_none())
            .ok_or(Error::NotPending)
    }
}

/// Where records go besides the in-memory state.
enum Sink {
    File(File),
    Memory,
    ReadOnly,
}

struct Inner {
    state: State,
    sink: Sink,
    /// Length of the log; a failed write is cut back to it.
    len: u64,
}

pub struct FileStore {
    inner: Mutex<Inner>,
}

impl FileStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        file.try_lock()
            .map_err(|_| Error::Io(std::io::Error::other("store is in use by another process")))?;
        let bytes = fs::read(path)?;
        let (state, valid) = replay(&bytes)?;
        if valid == 0 {
            // A new log (or one torn before its first record ended) starts with its format.
            file.set_len(0)?;
            let store = Self::new(state, Sink::File(file), 0);
            append(
                &mut store.inner(),
                Record::Format(FormatRecord { version: FORMAT }),
            )?;
            return Ok(store);
        }
        if valid < bytes.len() {
            let aside = PathBuf::from(format!("{}.torn-{valid}", path.display()));
            fs::write(&aside, &bytes[valid..])?;
            file.set_len(valid as u64)?;
            eprintln!(
                "file store: cut off a torn record at byte {valid}, saved to {}",
                aside.display()
            );
        }
        Ok(Self::new(state, Sink::File(file), valid as u64))
    }

    /// Replays the log without locking or repairing it; all writes fail with `ReadOnly`.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = fs::read(path)?;
        let (state, valid) = replay(&bytes)?;
        Ok(Self::new(state, Sink::ReadOnly, valid as u64))
    }

    /// Replays a log held in memory, read-only: for tools, property tests and fuzzing.
    pub fn open_bytes(bytes: &[u8]) -> Result<Self> {
        let (state, valid) = replay(bytes)?;
        Ok(Self::new(state, Sink::ReadOnly, valid as u64))
    }

    /// A store that lives only in this process.
    pub fn in_memory() -> Self {
        Self::new(State::default(), Sink::Memory, 0)
    }

    fn new(state: State, sink: Sink, len: u64) -> Self {
        FileStore {
            inner: Mutex::new(Inner { state, sink, len }),
        }
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes one record, then applies it to the in-memory state.
fn append(inner: &mut Inner, record: Record) -> Result<()> {
    let file = match &mut inner.sink {
        Sink::File(file) => file,
        Sink::Memory => return inner.state.apply(record),
        Sink::ReadOnly => return Err(Error::ReadOnly),
    };
    let body = LogRecord {
        record: Some(record),
    }
    .encode_to_vec();
    let mut buf = Vec::with_capacity(HEADER + body.len());
    buf.extend((body.len() as u32).to_be_bytes());
    buf.extend(checksum(&body));
    buf.extend(&body);
    if let Err(e) = file.write_all(&buf) {
        // Never leave a partial record in front of the next one.
        let _ = file.set_len(inner.len);
        return Err(e.into());
    }
    inner.len += buf.len() as u64;
    let record = LogRecord::decode(body.as_slice())?.record;
    inner.state.apply(record.expect("just encoded"))
}

/// Rebuilds the state; returns it with the length of the intact prefix.
fn replay(bytes: &[u8]) -> Result<(State, usize)> {
    let mut state = State::default();
    let mut at = 0;
    while at < bytes.len() {
        let Some(header) = bytes.get(at..at + HEADER) else {
            break; // torn header
        };
        let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let Some(body) = bytes.get(at + HEADER..at + HEADER + len) else {
            break; // torn body
        };
        if checksum(body) != header[4..] {
            if at + HEADER + len == bytes.len() {
                break; // torn last record
            }
            return Err(corrupt(&format!("record at byte {at}: checksum mismatch")));
        }
        let record = LogRecord::decode(body)
            .map_err(|e| corrupt(&format!("record at byte {at}: {e}")))?
            .record
            .ok_or_else(|| corrupt(&format!("record at byte {at} is empty")))?;
        match (&record, at) {
            (Record::Format(f), 0) if f.version == FORMAT => {}
            (Record::Format(f), 0) => return Err(Error::Format(f.version)),
            (_, 0) => return Err(Error::Format(0)), // written before formats were recorded
            (Record::Format(_), _) => return Err(corrupt(&format!("format record at byte {at}"))),
            _ => {}
        }
        state.apply(record)?;
        at += HEADER + len;
    }
    Ok((state, at))
}

fn checksum(body: &[u8]) -> [u8; 4] {
    Sha256::digest(body)[..4].try_into().unwrap()
}

fn corrupt(what: &str) -> Error {
    Error::Corrupt(what.to_owned())
}

impl Store for FileStore {
    fn read_stream_after(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<Event>> {
        let inner = self.inner();
        let key = (
            tenant_id.to_owned(),
            domain.to_owned(),
            aggregate_type.to_owned(),
            aggregate_id.to_owned(),
        );
        let indices = inner.state.streams.get(&key).map_or(&[][..], Vec::as_slice);
        // Versions are 1, 2, 3, ...: version v is at index v - 1.
        let after = (after_version as usize).min(indices.len());
        Ok(indices[after..]
            .iter()
            .map(|&i| inner.state.events[i].clone())
            .collect())
    }

    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        let inner = self.inner();
        let events = inner
            .state
            .events
            .iter()
            .skip(after as usize)
            .take(limit as usize);
        Ok(events.cloned().collect())
    }

    fn head_position(&self) -> Result<u64> {
        Ok(self.inner().state.events.len() as u64)
    }

    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued> {
        let mut inner = self.inner();
        if let Some(existing) = inner.state.commands.get(&cmd.command_id) {
            return Ok(Enqueued::Duplicate {
                result: existing.result.clone(),
            });
        }
        let record = EnqueuedRecord {
            command: Some(cmd.clone()),
            created_at: now_ms,
        };
        append(&mut inner, Record::Enqueued(record))?;
        Ok(Enqueued::New)
    }

    fn pending(&self) -> Result<Vec<Pending>> {
        let inner = self.inner();
        let state = &inner.state;
        Ok(state
            .open
            .values()
            .map(|id| Pending {
                command: state.commands[id].command.clone(),
                received_at: state.commands[id].received_at,
            })
            .collect())
    }

    fn commit(
        &self,
        command_id: &str,
        expected_version: u64,
        events: &[NewEvent],
        producer_sig: &Signature,
        now_ms: i64,
    ) -> Result<Vec<Event>> {
        let mut inner = self.inner();
        let cmd = inner.state.pending(command_id)?.command.clone();
        let stream = inner.state.streams.get(&stream_key(&cmd));
        let actual = stream.map_or(0, |s| s.len() as u64);
        if actual != expected_version {
            return Err(Error::VersionConflict {
                expected: expected_version,
                actual,
            });
        }

        let mut prev = inner
            .state
            .events
            .last()
            .map_or(GENESIS.to_vec(), |e| e.hash.clone());
        let mut position = inner.state.events.len() as u64;
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
            };
            event.hash = signing::event_hash(&prev, &event).to_vec();
            let prev_hash = std::mem::replace(&mut prev, event.hash.clone());
            logged.push(LoggedEvent {
                event: Some(event),
                prev_hash,
            });
        }
        let stored = logged.iter().filter_map(|l| l.event.clone()).collect();
        let record = CommittedRecord {
            command_id: cmd.command_id,
            events: logged,
        };
        append(&mut inner, Record::Committed(record))?;
        Ok(stored)
    }

    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()> {
        let mut inner = self.inner();
        let command_id = inner.state.pending(command_id)?.command.command_id.clone();
        let record = RejectedRecord {
            command_id,
            reason: Some(reason),
        };
        append(&mut inner, Record::Rejected(record))
    }

    fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        let inner = self.inner();
        Ok(inner
            .state
            .commands
            .get(command_id)
            .and_then(|c| c.result.clone()))
    }

    fn listeners(&self) -> Result<Vec<ListenerRecord>> {
        Ok(self.inner().state.listeners.values().cloned().collect())
    }

    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()> {
        let mut inner = self.inner();
        for l in listeners {
            append(&mut inner, Record::Listener(l.clone()))?;
        }
        Ok(())
    }

    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>> {
        let mut inner = self.inner();
        let Some(head) = inner.state.events.last() else {
            return Ok(None);
        };
        let (global_position, chain_hash) = (head.global_position, head.hash.clone());
        if let Some(last) = inner.state.checkpoints.last()
            && last.global_position == global_position
        {
            return Ok(Some(last.clone()));
        }
        let msg = signing::checkpoint_message(global_position, &chain_hash, now_ms);
        let record = CheckpointRecord {
            global_position,
            chain_hash,
            kernel_sig: Some(signing::sign(kernel, &msg)),
            created_at: now_ms,
        };
        append(&mut inner, Record::Checkpoint(record))?;
        Ok(inner.state.checkpoints.last().cloned())
    }

    fn revoke(&self, revocation: &RevocationRecord) -> Result<()> {
        let mut inner = self.inner();
        if inner
            .state
            .revocations
            .iter()
            .any(|r| r.key_id == revocation.key_id)
        {
            return Ok(());
        }
        append(&mut inner, Record::Revocation(revocation.clone()))
    }

    fn revocations(&self) -> Result<Vec<RevocationRecord>> {
        Ok(self.inner().state.revocations.clone())
    }

    fn verify(&self, kernel_key_ids: &[String]) -> Result<Report> {
        let inner = self.inner();
        let state = &inner.state;
        let mut v = Verifier::new(
            kernel_key_ids,
            state.checkpoints.iter().cloned(),
            state.revocations.iter().cloned(),
        );
        for (event, prev_hash) in state.events.iter().zip(&state.prev_hashes) {
            v.event(event.clone(), prev_hash);
        }
        let mut commands: Vec<&Cmd> = state.commands.values().collect();
        commands.sort_by_key(|c| c.seq);
        for c in commands {
            v.command(&c.command, c.received_at);
        }
        Ok(v.finish())
    }
}

/// Upgrades a file log to [`FORMAT`]; returns the format it had.
pub(crate) fn migrate(path: &Path) -> Result<u32> {
    let bytes = fs::read(path)?;
    let Some(header) = bytes.get(..HEADER) else {
        return Ok(FORMAT); // empty: a new log
    };
    let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    let body = bytes
        .get(HEADER..HEADER + len)
        .filter(|body| checksum(body) == header[4..])
        .ok_or_else(|| corrupt("first record: open the store once to repair it"))?;
    match LogRecord::decode(body)?.record {
        Some(Record::Format(f)) if f.version == FORMAT => Ok(FORMAT),
        Some(Record::Format(f)) => Err(Error::Format(f.version)),
        _ => Err(Error::Format(0)),
    }
}
