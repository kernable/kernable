//! Append-only file backend (`file:<path>`), and the same logic without a file (`memory:`).
//!
//! The whole store is one log of records (`proto/kernable/v1/storage.proto`). The first holds
//! the data format (see [`crate::FORMAT`]); then come enqueued
//! commands, commits (all events of one command in a single record, hence atomic, with the
//! keys and facts the command's stream holds from then on if they changed),
//! rejections, checkpoints and listener positions. Each record is framed as
//! `u32 BE length || first 4 bytes of SHA-256(length) || first 4 bytes of SHA-256(body) || body`;
//! the check of the length tells a damaged length apart from a record that was cut short.
//! Opening replays the log into memory. A torn last record from a crash is moved to
//! `<path>.torn-<offset>` and cut off; damage anywhere else refuses to open.
//!
//! One process writes (exclusive file lock); others may open the log read-only.
//! A write returns once its record has reached the OS, which survives a crash of the process.
//! With [`Durability::Full`], [`Store::sync`] then puts every record written so far on the
//! disk with one `sync_data` shared by all callers waiting (group commit), which survives a
//! power loss; with [`Durability::Normal`] it does nothing. A failed sync makes the store refuse
//! every further write: what the OS still buffers can no longer be trusted, and only a restart,
//! which replays the log from the disk, knows what was kept.
//!
//! The log is also the record log a primary sends its standby (see [`crate::replication`]):
//! record `n` is the `n`-th record after the format record, or after the newest
//! `ReplicationHead` record where an image started the log. Every [`STRIDE`]-th record's offset
//! and hash are kept in memory, so records are found without reading the log from its start.
//!
//! `memory:` keeps the state only in memory: nothing survives the process. For tests and
//! benchmarks; it keeps no record log.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::{
    Checkpoint, Durability, Enqueued, Error, FORMAT, Head, Pending, Result, Store, StreamId,
    replication::{self, diverged},
    sync::Syncer,
    verify::{Report, Verifier},
};
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        AggregateIndex, CheckpointRecord, Command, CommandResult, Committed, CommittedRecord,
        CopyRecord, DataChangedRecord, EnqueuedRecord, Event, FormatRecord, GlobalKeyRecord, Grant,
        ListenerRecord, LogRecord, LoggedEvent, MembershipRecord, NewEvent, PartitionMap,
        ProcessPositionRecord, ProcessRecord, Rejected, RejectedRecord, ReplicatedRecord,
        ReplicationHeadRecord, RevocationRecord, Signature, StreamIndex, TenantImportedRecord,
        TenantIndexRecord, WorkerRecord, command_result::Outcome, log_record::Record,
    },
    signing::{self, GENESIS},
};
use prost::Message;

/// The bytes in front of each record body: its length, a check of the length, a check of the
/// body.
const HEADER: usize = 12;

/// The check of a record's length field.
fn length_check(len: &[u8]) -> [u8; 4] {
    let hash = replication::digest(len);
    hash[..4].try_into().expect("4 bytes")
}

/// One record as the log holds it: its header, then `body`, whose SHA-256 is `digest`.
fn frame(body: &[u8], digest: &[u8]) -> Vec<u8> {
    let len = (body.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(HEADER + body.len());
    buf.extend(len);
    buf.extend(length_check(&len));
    buf.extend(&digest[..4]);
    buf.extend(body);
    buf
}

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
    workers: BTreeMap<String, WorkerRecord>,
    memberships: BTreeMap<String, MembershipRecord>,
    /// Every version of every process, by name and version.
    process_versions: BTreeMap<(String, u32), ProcessRecord>,
    /// The newest change of state per process.
    process_states: BTreeMap<String, ProcessRecord>,
    /// How far the process engine has handled the log.
    process_position: u64,
    /// The newest mark of a masked copy, if the store is one.
    copy: Option<CopyRecord>,
    revocations: Vec<RevocationRecord>,
    grants: BTreeMap<String, Grant>,
    /// What each stream holds, from its latest commit with an index; none if it holds nothing.
    index: BTreeMap<StreamKey, AggregateIndex>,
    /// The highest schema per qualified event type.
    schemas: BTreeMap<String, u32>,
    /// The holder of every unique key that is not personal, by identity.
    holders: HashMap<Vec<u8>, StreamKey>,
    /// The newest partition map.
    partition_map: Option<PartitionMap>,
    /// On the key owner: which partition may hold each global key.
    global_keys: HashMap<Vec<u8>, String>,
    /// Tenants copied here from another partition.
    imported: HashMap<String, TenantImportedRecord>,
}

impl State {
    /// Files what `stream` holds now; a key another stream holds is an error.
    fn set_index(&mut self, stream: StreamKey, index: AggregateIndex) -> Result<()> {
        if let Some(old) = self.index.remove(&stream) {
            for k in old.keys.iter().filter(|k| !k.id.is_empty()) {
                self.holders.remove(&k.id);
            }
        }
        for k in index.keys.iter().filter(|k| !k.id.is_empty()) {
            if self.holders.insert(k.id.clone(), stream.clone()).is_some() {
                return Err(corrupt("a unique key is held twice"));
            }
        }
        if !index.keys.is_empty() || !index.facts.is_empty() {
            self.index.insert(stream, index);
        }
        Ok(())
    }

    /// Forgets the keys and facts `tenant`'s streams hold.
    fn drop_index(&mut self, tenant: &str) {
        let gone: Vec<StreamKey> = self
            .index
            .keys()
            .filter(|s| s.0 == tenant)
            .cloned()
            .collect();
        for stream in gone {
            if let Some(old) = self.index.remove(&stream) {
                for k in old.keys.iter().filter(|k| !k.id.is_empty()) {
                    self.holders.remove(&k.id);
                }
            }
        }
    }

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
            Record::Committed(CommittedRecord {
                command_id,
                events,
                index,
            }) => {
                if let Some(index) = index {
                    let cmd = self.pending(&command_id).map_err(|_| {
                        corrupt(&format!(
                            "completion of unknown or done command {command_id}"
                        ))
                    })?;
                    let stream = stream_key(&cmd.command);
                    self.set_index(stream, index).map_err(|_| {
                        corrupt(&format!("a unique key of {command_id} is held twice"))
                    })?;
                }
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
                    let schema = self
                        .schemas
                        .entry(format!("{}.{}", event.domain, event.event_type))
                        .or_default();
                    *schema = (*schema).max(event.schema);
                    self.prev_hashes.push(logged.prev_hash);
                    self.events.push(event.clone());
                    stored.push(event);
                }
                self.complete(
                    &command_id,
                    Outcome::Committed(Committed {
                        position: stored.last().map_or(0, |e| e.global_position),
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
            Record::Listener(mut l) => {
                if !l.parked_changed
                    && let Some(old) = self.listeners.get_mut(&l.name)
                {
                    l.parked = std::mem::take(&mut old.parked);
                }
                l.parked_changed = false;
                self.listeners.insert(l.name.clone(), l);
            }
            Record::Worker(w) if w.removed => {
                self.workers.remove(&w.aggregate_type);
            }
            Record::Worker(w) => {
                self.workers.insert(w.aggregate_type.clone(), w);
            }
            Record::Membership(m) => {
                let service = m.membership.as_ref().map(|m| m.service.clone());
                self.memberships.insert(service.unwrap_or_default(), m);
            }
            Record::ProcessPosition(p) => self.process_position = p.position,
            Record::Copy(c) => self.copy = Some(c),
            Record::Process(p) if p.version == 0 => {
                self.process_states.insert(p.name.clone(), p);
            }
            Record::Process(p) => {
                self.process_versions
                    .entry((p.name.clone(), p.version))
                    .or_insert(p);
            }
            Record::Grant(g) => {
                if self
                    .grants
                    .get(&g.key_id)
                    .is_none_or(|held| held.issued_at < g.issued_at)
                {
                    self.grants.insert(g.key_id.clone(), g);
                }
            }
            Record::GlobalKey(g) => {
                self.global_keys.insert(g.id, g.partition);
            }
            Record::TenantImported(t) => {
                self.imported.insert(t.tenant.clone(), t);
            }
            Record::TenantIndex(t) => {
                self.drop_index(&t.tenant);
                for s in t.streams {
                    let stream = (t.tenant.clone(), s.domain, s.aggregate_type, s.aggregate_id);
                    self.set_index(stream, s.index.unwrap_or_default())?;
                }
            }
            Record::PartitionMap(m) => {
                if self
                    .partition_map
                    .as_ref()
                    .is_none_or(|held| held.epoch < m.epoch)
                {
                    self.partition_map = Some(m);
                }
            }
            // Only the record log counts them.
            Record::DataChanged(_) | Record::ReplicationHead(_) => {}
        }
        Ok(())
    }

    /// Whether `record`, from another store's record log, continues this state: what
    /// [`Store::apply`] checks before it writes. A record that passes applies.
    fn admissible(&self, record: &Record) -> Result<()> {
        match record {
            Record::Enqueued(e) => {
                let c = e
                    .command
                    .as_ref()
                    .ok_or_else(|| diverged("an enqueued record without its command".into()))?;
                if self.commands.contains_key(&c.command_id) {
                    return Err(diverged(format!(
                        "command {} is enqueued already",
                        c.command_id
                    )));
                }
            }
            Record::Committed(c) => {
                let cmd = &self
                    .pending(&c.command_id)
                    .map_err(|_| diverged(format!("command {} is not pending", c.command_id)))?
                    .command;
                let own = stream_key(cmd);
                let prev = self
                    .events
                    .last()
                    .map_or(GENESIS.to_vec(), |e| e.hash.clone());
                replication::check_events(prev, self.events.len() as u64, &c.events)?;
                let version = self.streams.get(&own).map_or(0, |s| s.len() as u64);
                for (v, logged) in (version + 1..).zip(&c.events) {
                    let e = logged.event.as_ref().expect("checked by check_events");
                    let stream = (
                        e.tenant_id.clone(),
                        e.domain.clone(),
                        e.aggregate_type.clone(),
                        e.aggregate_id.clone(),
                    );
                    if e.command_id != c.command_id || stream != own || e.version != v {
                        return Err(diverged(format!(
                            "event {} is not version {v} of command {}'s stream",
                            e.global_position, c.command_id
                        )));
                    }
                }
                let taken = c.index.iter().flat_map(|i| &i.keys).any(|k| {
                    !k.id.is_empty() && self.holders.get(&k.id).is_some_and(|h| *h != own)
                });
                if taken {
                    return Err(diverged(format!(
                        "a unique key of command {} is held by another stream",
                        c.command_id
                    )));
                }
            }
            Record::Rejected(r) => {
                self.pending(&r.command_id)
                    .map_err(|_| diverged(format!("command {} is not pending", r.command_id)))?;
            }
            Record::Checkpoint(cp) => {
                let held = (cp.global_position as usize)
                    .checked_sub(1)
                    .and_then(|i| self.events.get(i))
                    .is_some_and(|e| e.hash == cp.chain_hash);
                if !held {
                    return Err(diverged(format!(
                        "a checkpoint of event {} this store does not hold",
                        cp.global_position
                    )));
                }
            }
            Record::Format(_) | Record::ReplicationHead(_) => {
                return Err(diverged("a record that starts a log".into()));
            }
            Record::Listener(_)
            | Record::Revocation(_)
            | Record::Worker(_)
            | Record::Membership(_)
            | Record::Process(_)
            | Record::ProcessPosition(_)
            | Record::Copy(_)
            | Record::Grant(_)
            | Record::PartitionMap(_)
            | Record::GlobalKey(_)
            | Record::TenantImported(_)
            | Record::TenantIndex(_)
            | Record::DataChanged(_) => {}
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

/// Every this many records, the record log notes where one starts and the hash before it.
const STRIDE: u64 = 1024;

/// Where the record log stands, and how to find its records in the file.
#[derive(Default)]
struct RecordLog {
    head: Head,
    /// The log holds the records after this one: 0, or where a `ReplicationHead` started it.
    base: u64,
    /// `(seq, offset, hash)`: record `seq + 1` starts at byte `offset` and record `seq` has
    /// `hash`; one every [`STRIDE`] records from `base`.
    marks: Vec<(u64, u64, Vec<u8>)>,
    /// The same for each of the newest records: a standby close behind the head is found
    /// without reading forward from a mark.
    recent: std::collections::VecDeque<(u64, u64, Vec<u8>)>,
}

impl RecordLog {
    /// Counts `record`, which starts at byte `offset` and whose body has SHA-256 `digest`.
    fn advance(&mut self, record: &Record, offset: u64, digest: &[u8]) {
        match record {
            Record::Format(_) => {}
            Record::ReplicationHead(h) => {
                self.head = Head {
                    seq: h.seq,
                    hash: h.hash.clone(),
                };
                self.base = h.seq;
                self.marks.clear();
                self.recent.clear();
            }
            _ => {
                let mark = (self.head.seq, offset, self.head.hash.clone());
                if (self.head.seq - self.base).is_multiple_of(STRIDE) {
                    self.marks.push(mark.clone());
                }
                if self.recent.len() == STRIDE as usize {
                    self.recent.pop_front();
                }
                self.recent.push_back(mark);
                let seq = self.head.seq + 1;
                let hash = replication::record_hash(&self.head.hash, seq, digest);
                self.head = Head {
                    seq,
                    hash: hash.to_vec(),
                };
            }
        }
    }

    /// Whether the log holds record `seq` (`base` counts: its hash is known).
    fn holds(&self, seq: u64) -> bool {
        (self.base..=self.head.seq).contains(&seq)
    }

    /// The newest mark at or before record `seq`, for `base <= seq < head`.
    fn mark(&self, seq: u64) -> (u64, u64, Vec<u8>) {
        if let Some(first) = self.recent.front()
            && first.0 <= seq
        {
            return self.recent[(seq - first.0) as usize].clone();
        }
        let i = self.marks.partition_point(|m| m.0 <= seq);
        self.marks[i.checked_sub(1).expect("a mark at base")].clone()
    }
}

struct Inner {
    state: State,
    sink: Sink,
    /// Length of the log; a failed write is cut back to it.
    len: u64,
    log: RecordLog,
    syncer: Arc<Syncer>,
    /// No more records: see [`Store::seal_records`].
    sealed: bool,
}

pub struct FileStore {
    inner: Mutex<Inner>,
    syncer: Arc<Syncer>,
    /// The log's file, to flush it without holding the store's lock; `None` unless
    /// [`Durability::Full`].
    flush: Option<File>,
    /// Where the log is, to read records back; `None` without a file.
    path: Option<PathBuf>,
}

impl FileStore {
    /// One record body framed as the log holds it: for tools, tests and fuzzing that write
    /// logs by hand.
    pub fn frame(body: &[u8]) -> Vec<u8> {
        frame(body, &replication::digest(body))
    }

    /// Opens the log with [`Durability::Full`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Durability::Full)
    }

    pub fn open_with(path: impl AsRef<Path>, durability: Durability) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        file.try_lock()
            .map_err(|_| Error::Io(std::io::Error::other("store is in use by another process")))?;
        let bytes = fs::read(path)?;
        let (state, log, valid) = replay(&bytes)?;
        if valid < bytes.len() {
            // Kept aside, on the disk, before the log lets go of them.
            let aside = PathBuf::from(format!("{}.torn-{valid}", path.display()));
            let mut kept = File::create(&aside)?;
            kept.write_all(&bytes[valid..])?;
            kept.sync_all()?;
            sync_directory(&aside)?;
            file.set_len(valid as u64)?;
            if durability == Durability::Full {
                file.sync_data()?;
            }
            eprintln!(
                "file store: cut off a torn record at byte {valid}, saved to {}",
                aside.display()
            );
        }
        if valid == 0 {
            // A new log (or one torn before its first record ended) starts with its format.
            let flush = flush_handle(&file, durability)?;
            let mut store = Self::new(state, log, Sink::File(file), 0);
            store.flush = flush;
            store.path = Some(path.to_owned());
            append(
                &mut store.inner(),
                Record::Format(FormatRecord { version: FORMAT }),
            )?;
            if durability == Durability::Full {
                // The new file's name must survive a power loss, too.
                store.sync()?;
                sync_directory(path)?;
            }
            return Ok(store);
        }
        let flush = flush_handle(&file, durability)?;
        let mut store = Self::new(state, log, Sink::File(file), valid as u64);
        store.flush = flush;
        store.path = Some(path.to_owned());
        Ok(store)
    }

    /// Replays the log without locking or repairing it; all writes fail with `ReadOnly`.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = fs::read(path.as_ref())?;
        let (state, log, valid) = replay(&bytes)?;
        let mut store = Self::new(state, log, Sink::ReadOnly, valid as u64);
        store.path = Some(path.as_ref().to_owned());
        Ok(store)
    }

    /// Replays a log held in memory, read-only: for tools, property tests and fuzzing.
    pub fn open_bytes(bytes: &[u8]) -> Result<Self> {
        let (state, log, valid) = replay(bytes)?;
        Ok(Self::new(state, log, Sink::ReadOnly, valid as u64))
    }

    /// A store that lives only in this process.
    pub fn in_memory() -> Self {
        Self::new(State::default(), RecordLog::default(), Sink::Memory, 0)
    }

    fn new(state: State, log: RecordLog, sink: Sink, len: u64) -> Self {
        let syncer = Arc::new(Syncer::default());
        FileStore {
            inner: Mutex::new(Inner {
                state,
                sink,
                len,
                log,
                syncer: Arc::clone(&syncer),
                sealed: false,
            }),
            syncer,
            flush: None,
            path: None,
        }
    }

    /// The vault values the command `id` brought.
    fn vault_refs_of(&self, id: &str) -> Result<Vec<String>> {
        Ok(self
            .command(id)?
            .map(|c| c.command.vault_refs)
            .unwrap_or_default())
    }

    /// The log's path and length, for reading records back; refused without a file.
    fn readable(&self, inner: &Inner) -> Result<(PathBuf, u64)> {
        match (&inner.sink, &self.path) {
            (Sink::Memory, _) | (_, None) => Err(Error::NoRecordLog),
            (_, Some(path)) => Ok((path.clone(), inner.len)),
        }
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes one record, then applies it to the in-memory state.
fn append(inner: &mut Inner, record: Record) -> Result<()> {
    inner.syncer.check()?;
    if inner.sealed && !matches!(inner.sink, Sink::Memory) {
        return Err(Error::Sealed);
    }
    let file = match &mut inner.sink {
        Sink::File(file) => file,
        Sink::Memory => return inner.state.apply(record),
        Sink::ReadOnly => return Err(Error::ReadOnly),
    };
    let body = LogRecord {
        record: Some(record),
    }
    .encode_to_vec();
    let digest = replication::digest(&body);
    let buf = frame(&body, &digest);
    if let Err(e) = file.write_all(&buf) {
        // Never leave a partial record in front of the next one.
        let _ = file.set_len(inner.len);
        return Err(e.into());
    }
    inner.syncer.wrote();
    let offset = inner.len;
    inner.len += buf.len() as u64;
    let record = LogRecord::decode(body.as_slice())?
        .record
        .expect("just encoded");
    inner.log.advance(&record, offset, &digest);
    inner.state.apply(record)
}

/// Calls `f` with the offset and body of each record from byte `from` up to byte `to` of the
/// log at `path`, until it returns false. The bytes up to `to` were checked when the log was
/// opened or written.
fn scan(
    path: &Path,
    from: u64,
    to: u64,
    f: &mut dyn FnMut(u64, &[u8]) -> Result<bool>,
) -> Result<()> {
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(from))?;
    let (mut at, mut header, mut body) = (from, [0u8; HEADER], Vec::new());
    while at < to {
        file.read_exact(&mut header)?;
        let len = u32::from_be_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        body.resize(len, 0);
        file.read_exact(&mut body)?;
        if !f(at, &body)? {
            break;
        }
        at += (HEADER + len) as u64;
    }
    Ok(())
}

fn decode(body: &[u8]) -> Result<Record> {
    LogRecord::decode(body)?
        .record
        .ok_or_else(|| corrupt("an empty record"))
}

/// Rebuilds the state and the record log; returns them with the length of the intact prefix.
fn replay(bytes: &[u8]) -> Result<(State, RecordLog, usize)> {
    let mut state = State::default();
    let mut log = RecordLog::default();
    let mut at = 0;
    while at < bytes.len() {
        let Some(header) = bytes.get(at..at + HEADER) else {
            break; // torn header
        };
        if length_check(&header[..4]) != header[4..8] {
            // A crash can leave zeros where a write did not reach the disk; anything else
            // after a damaged length is a record that must not be lost.
            if bytes[at + HEADER..].iter().all(|&b| b == 0) {
                break; // torn header
            }
            return Err(corrupt(&format!("record at byte {at}: damaged length")));
        }
        let len = u32::from_be_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let Some(body) = bytes.get(at + HEADER..at + HEADER + len) else {
            break; // torn body
        };
        let digest = replication::digest(body);
        if digest[..4] != header[8..] {
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
        log.advance(&record, at as u64, &digest);
        state.apply(record)?;
        at += HEADER + len;
    }
    Ok((state, log, at))
}

/// A second handle on the log to flush it with, for [`Durability::Full`].
fn flush_handle(file: &File, durability: Durability) -> Result<Option<File>> {
    match durability {
        Durability::Full => Ok(Some(file.try_clone()?)),
        Durability::Normal => Ok(None),
    }
}

/// Syncs the directory holding `path`, so a newly created file is found after a power loss.
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
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
        index: Option<&AggregateIndex>,
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
        // Refused before it is logged: a log that files one key twice would not replay.
        let own = stream_key(&cmd);
        let taken = index.iter().flat_map(|i| &i.keys).any(|k| {
            !k.id.is_empty()
                && inner
                    .state
                    .holders
                    .get(&k.id)
                    .is_some_and(|holder| *holder != own)
        });
        if taken {
            return Err(Error::KeyTaken);
        }

        let mut prev = inner
            .state
            .events
            .last()
            .map_or(GENESIS.to_vec(), |e| e.hash.clone());
        let first = inner.state.events.len() as u64 + 1;
        let mut logged = Vec::with_capacity(events.len());
        for (position, (version, e)) in (first..).zip((expected_version + 1..).zip(events)) {
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
                deployment: e.deployment.clone(),
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
            index: index.cloned(),
        };
        append(&mut inner, Record::Committed(record))?;
        Ok(stored)
    }

    fn event_schemas(&self) -> Result<Vec<(String, u32)>> {
        Ok(self
            .inner()
            .state
            .schemas
            .iter()
            .map(|(t, v)| (t.clone(), *v))
            .collect())
    }

    fn stream_index(&self, stream: &StreamId) -> Result<Option<AggregateIndex>> {
        let key = (
            stream.tenant_id.clone(),
            stream.domain.clone(),
            stream.aggregate_type.clone(),
            stream.aggregate_id.clone(),
        );
        Ok(self.inner().state.index.get(&key).cloned())
    }

    fn key_holder(&self, id: &[u8]) -> Result<Option<StreamId>> {
        Ok(self.inner().state.holders.get(id).map(
            |(tenant_id, domain, aggregate_type, aggregate_id)| StreamId {
                tenant_id: tenant_id.clone(),
                domain: domain.clone(),
                aggregate_type: aggregate_type.clone(),
                aggregate_id: aggregate_id.clone(),
            },
        ))
    }

    fn holds_personal_keys(&self) -> Result<bool> {
        Ok(self
            .inner()
            .state
            .index
            .values()
            .any(|i| i.keys.iter().any(|k| !k.vault_ref.is_empty())))
    }

    fn index(&self) -> Result<Vec<(StreamId, AggregateIndex)>> {
        let inner = self.inner();
        Ok(inner
            .state
            .index
            .iter()
            .map(
                |((tenant_id, domain, aggregate_type, aggregate_id), index)| {
                    let stream = StreamId {
                        tenant_id: tenant_id.clone(),
                        domain: domain.clone(),
                        aggregate_type: aggregate_type.clone(),
                        aggregate_id: aggregate_id.clone(),
                    };
                    (stream, index.clone())
                },
            )
            .collect())
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

    fn sync(&self) -> Result<()> {
        match &self.flush {
            Some(file) => self.syncer.sync(|| file.sync_data()),
            None => self.syncer.check(),
        }
    }

    fn command_ids(&self) -> Result<Vec<String>> {
        let inner = self.inner();
        let mut commands: Vec<(&u64, &String)> = inner
            .state
            .commands
            .iter()
            .map(|(id, c)| (&c.seq, id))
            .collect();
        commands.sort();
        Ok(commands.into_iter().map(|(_, id)| id.clone()).collect())
    }

    fn rejections(&self, limit: usize) -> Result<Vec<(Pending, Rejected)>> {
        let inner = self.inner();
        let mut rejected: Vec<(&Cmd, &Rejected)> = inner
            .state
            .commands
            .values()
            .filter_map(|c| match c.result.as_ref()?.outcome.as_ref()? {
                Outcome::Rejected(r) => Some((c, r)),
                _ => None,
            })
            .collect();
        rejected.sort_by_key(|(c, _)| std::cmp::Reverse((c.received_at, c.seq)));
        Ok(rejected
            .into_iter()
            .take(limit)
            .map(|(c, r)| {
                let pending = Pending {
                    command: c.command.clone(),
                    received_at: c.received_at,
                };
                (pending, r.clone())
            })
            .collect())
    }

    fn command(&self, command_id: &str) -> Result<Option<Pending>> {
        Ok(self
            .inner()
            .state
            .commands
            .get(command_id)
            .map(|c| Pending {
                command: c.command.clone(),
                received_at: c.received_at,
            }))
    }

    fn workers(&self) -> Result<Vec<WorkerRecord>> {
        Ok(self.inner().state.workers.values().cloned().collect())
    }

    fn save_workers(&self, workers: &[WorkerRecord]) -> Result<()> {
        let mut inner = self.inner();
        for w in workers {
            append(&mut inner, Record::Worker(w.clone()))?;
        }
        Ok(())
    }

    fn memberships(&self) -> Result<Vec<MembershipRecord>> {
        Ok(self.inner().state.memberships.values().cloned().collect())
    }

    fn save_membership(&self, membership: &MembershipRecord) -> Result<()> {
        append(&mut self.inner(), Record::Membership(membership.clone()))
    }

    fn processes(&self) -> Result<Vec<ProcessRecord>> {
        let inner = self.inner();
        let state = &inner.state;
        Ok(state
            .process_versions
            .values()
            .chain(state.process_states.values())
            .cloned()
            .collect())
    }

    fn process_position(&self) -> Result<u64> {
        Ok(self.inner().state.process_position)
    }

    fn save_process_position(&self, position: u64) -> Result<()> {
        append(
            &mut self.inner(),
            Record::ProcessPosition(ProcessPositionRecord { position }),
        )
    }

    fn copy_mark(&self) -> Result<Option<CopyRecord>> {
        Ok(self.inner().state.copy.clone())
    }

    fn mark_copy(&self, mark: &CopyRecord) -> Result<()> {
        append(&mut self.inner(), Record::Copy(mark.clone()))
    }

    fn save_process(&self, process: &ProcessRecord) -> Result<()> {
        let mut inner = self.inner();
        if process.version > 0
            && inner
                .state
                .process_versions
                .contains_key(&(process.name.clone(), process.version))
        {
            return Ok(());
        }
        append(&mut inner, Record::Process(process.clone()))
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

    fn grants(&self) -> Result<Vec<Grant>> {
        Ok(self.inner().state.grants.values().cloned().collect())
    }

    fn save_grant(&self, grant: &Grant) -> Result<()> {
        let mut inner = self.inner();
        if inner
            .state
            .grants
            .get(&grant.key_id)
            .is_some_and(|held| held.issued_at >= grant.issued_at)
        {
            return Ok(());
        }
        append(&mut inner, Record::Grant(grant.clone()))
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

    fn partition_map(&self) -> Result<Option<PartitionMap>> {
        Ok(self.inner().state.partition_map.clone())
    }

    fn global_key(&self, id: &[u8]) -> Result<Option<String>> {
        Ok(self.inner().state.global_keys.get(id).cloned())
    }

    fn tenant_imported(&self, tenant: &str) -> Result<Option<TenantImportedRecord>> {
        Ok(self.inner().state.imported.get(tenant).cloned())
    }

    fn set_tenant_index(&self, tenant: &str, streams: &[StreamIndex]) -> Result<usize> {
        let mut inner = self.inner();
        let held = inner.state.index.keys().filter(|s| s.0 == tenant).count();
        if held > 0 || !streams.is_empty() {
            let record = Record::TenantIndex(TenantIndexRecord {
                tenant: tenant.to_owned(),
                streams: streams.to_vec(),
            });
            append(&mut inner, record)?;
        }
        Ok(held)
    }

    fn save_tenant_imported(&self, record: &TenantImportedRecord) -> Result<()> {
        append(&mut self.inner(), Record::TenantImported(record.clone()))
    }

    fn save_global_key(&self, id: &[u8], partition: &str) -> Result<()> {
        let mut inner = self.inner();
        if inner
            .state
            .global_keys
            .get(id)
            .is_some_and(|p| p == partition)
        {
            return Ok(());
        }
        let record = Record::GlobalKey(GlobalKeyRecord {
            id: id.to_vec(),
            partition: partition.to_owned(),
        });
        append(&mut inner, record)
    }

    fn save_partition_map(&self, map: &PartitionMap) -> Result<()> {
        let mut inner = self.inner();
        if inner
            .state
            .partition_map
            .as_ref()
            .is_some_and(|held| held.epoch >= map.epoch)
        {
            return Ok(());
        }
        append(&mut inner, Record::PartitionMap(map.clone()))
    }

    fn seal_records(&self, sealed: bool) -> Result<Head> {
        self.inner().sealed = sealed;
        self.record_head()
    }

    fn record_head(&self) -> Result<Head> {
        let inner = self.inner();
        match inner.sink {
            Sink::Memory => Err(Error::NoRecordLog),
            _ => Ok(inner.log.head.clone()),
        }
    }

    /// The file log takes back any record whose effect is in the log alone: the state is
    /// replayed from it. Not past one with effects outside it (design/rewind.md): a data file,
    /// an erasure, a command completed with vault values (its secrets dropped), the log's
    /// start.
    fn rewind_points(&self) -> Result<Vec<Head>> {
        let (head, base) = {
            let inner = self.inner();
            if matches!(inner.sink, Sink::Memory) {
                return Err(Error::NoRecordLog);
            }
            (inner.log.head.clone(), inner.log.base)
        };
        let from = head.seq.saturating_sub(1000).max(base);
        let Some(records) = self.records_after(from, 1000)? else {
            return Ok(vec![head]);
        };
        let Some(from_hash) = self.record_hash_at(from)? else {
            return Ok(vec![head]);
        };
        let mut points = vec![head];
        for (i, r) in records.iter().enumerate().rev() {
            let record = r.record.as_ref().and_then(|l| l.record.as_ref());
            let keeps_outside = match record {
                Some(Record::DataChanged(_) | Record::ReplicationHead(_)) | None => true,
                Some(Record::Committed(c)) => c
                    .events
                    .iter()
                    .any(|e| e.event.as_ref().is_some_and(|e| !e.erases.is_empty())),
                Some(_) => false,
            };
            if keeps_outside {
                break;
            }
            let hash = match i {
                0 => from_hash.clone(),
                i => records[i - 1].hash.clone(),
            };
            points.push(Head {
                seq: r.seq - 1,
                hash,
            });
        }
        Ok(points)
    }

    fn rewind_plan(&self, to: u64) -> Result<crate::RewindPlan> {
        let mut plan = crate::RewindPlan::default();
        let mut completed = Vec::new();
        let records = self.records_after(to, usize::MAX)?.unwrap_or_default();
        for r in records.iter().rev() {
            match r.record.as_ref().and_then(|l| l.record.as_ref()) {
                Some(Record::Enqueued(e)) => {
                    if let Some(c) = &e.command {
                        plan.deletes.push(c.command_id.clone());
                    }
                }
                Some(Record::Committed(c)) => completed.push(c.command_id.clone()),
                Some(Record::Rejected(rj)) => completed.push(rj.command_id.clone()),
                _ => {}
            }
        }
        // A command whose enqueue goes too needs nothing: it goes with its values.
        for id in completed.iter().filter(|id| !plan.deletes.contains(id)) {
            plan.needs.extend(self.vault_refs_of(id)?);
        }
        Ok(plan)
    }

    fn rewind(&self, to: u64) -> Result<Vec<String>> {
        let points = self.rewind_points()?;
        if !points.iter().any(|p| p.seq == to) {
            return Err(diverged(format!(
                "record {to} is not one the log can go back to"
            )));
        }
        let mut inner = self.inner();
        if to == inner.log.head.seq {
            return Ok(Vec::new());
        }
        let (path, end) = self.readable(&inner)?;
        // Where record `to + 1` starts, and the commands enqueued from there on.
        let (mut at, offset, _) = inner.log.mark(to);
        let (mut cut, mut deleted) = (None, Vec::new());
        scan(&path, offset, end, &mut |start, body| {
            at += 1;
            if at == to + 1 {
                cut = Some(start);
            }
            if at > to
                && let Record::Enqueued(e) = decode(body)?
                && let Some(command) = e.command
            {
                deleted.push(command.command_id);
            }
            Ok(true)
        })?;
        let cut = cut.ok_or_else(|| corrupt(&format!("record {} not found", to + 1)))?;
        let Sink::File(file) = &inner.sink else {
            return Err(Error::ReadOnly);
        };
        file.set_len(cut)?;
        file.sync_data()?;
        let bytes = fs::read(&path)?;
        let (state, log, valid) = replay(&bytes)?;
        (inner.state, inner.log, inner.len) = (state, log, valid as u64);
        deleted.reverse();
        Ok(deleted)
    }

    fn record_hash_at(&self, seq: u64) -> Result<Option<Vec<u8>>> {
        let (path, to, (mut at, from, mut hash)) = {
            let inner = self.inner();
            let (path, to) = self.readable(&inner)?;
            if !inner.log.holds(seq) {
                return Ok(None);
            }
            if seq == inner.log.head.seq {
                return Ok(Some(inner.log.head.hash.clone()));
            }
            (path, to, inner.log.mark(seq))
        };
        scan(&path, from, to, &mut |_, body| {
            if at == seq {
                return Ok(false);
            }
            at += 1;
            hash = replication::record_hash(&hash, at, &replication::digest(body)).to_vec();
            Ok(at < seq)
        })?;
        Ok(Some(hash))
    }

    fn records_after(&self, seq: u64, limit: usize) -> Result<Option<Vec<ReplicatedRecord>>> {
        let (path, to, (mut at, from, mut hash)) = {
            let inner = self.inner();
            let (path, to) = self.readable(&inner)?;
            if !inner.log.holds(seq) {
                return Ok(None);
            }
            if seq == inner.log.head.seq {
                return Ok(Some(Vec::new()));
            }
            (path, to, inner.log.mark(seq))
        };
        let mut out = Vec::new();
        scan(&path, from, to, &mut |_, body| {
            at += 1;
            hash = replication::record_hash(&hash, at, &replication::digest(body)).to_vec();
            if at > seq {
                out.push(ReplicatedRecord {
                    sealed: Vec::new(),
                    seq: at,
                    hash: hash.clone(),
                    record: Some(LogRecord {
                        record: Some(decode(body)?),
                    }),
                });
            }
            Ok(out.len() < limit.max(1))
        })?;
        Ok(Some(out))
    }

    fn apply(&self, records: &[ReplicatedRecord]) -> Result<()> {
        let mut inner = self.inner();
        if matches!(inner.sink, Sink::Memory) {
            return Err(Error::NoRecordLog);
        }
        let (records, head) = replication::chained(&inner.log.head, records)?;
        // A seal keeps the kernel's own writes out (a reader's); the records it follows apply.
        let sealed = std::mem::replace(&mut inner.sealed, false);
        let applied = records.into_iter().try_for_each(|record| {
            inner.state.admissible(record)?;
            append(&mut inner, record.clone())
        });
        inner.sealed = sealed;
        applied?;
        if inner.log.head != head {
            return Err(diverged(format!(
                "record {} hashes differently here",
                inner.log.head.seq
            )));
        }
        Ok(())
    }

    fn image(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<Head> {
        let (path, to, head) = {
            let inner = self.inner();
            let (path, to) = self.readable(&inner)?;
            (path, to, inner.log.head.clone())
        };
        // The log only grows, so its first `to` bytes are the state at `head`.
        scan(&path, 0, to, &mut |_, body| {
            match decode(body)? {
                Record::Format(_) | Record::ReplicationHead(_) | Record::DataChanged(_) => {}
                record => f(record)?,
            }
            Ok(true)
        })?;
        Ok(head)
    }

    fn apply_image(&self, records: &[Record]) -> Result<()> {
        let mut inner = self.inner();
        if matches!(inner.sink, Sink::Memory) {
            return Err(Error::NoRecordLog);
        }
        for record in records {
            match record {
                Record::ReplicationHead(_) | Record::DataChanged(_) => {}
                record => {
                    inner.state.admissible(record)?;
                    append(&mut inner, record.clone())?;
                }
            }
        }
        Ok(())
    }

    fn start_record_log(&self, head: &Head) -> Result<()> {
        let mut inner = self.inner();
        if matches!(inner.sink, Sink::Memory) {
            return Err(Error::NoRecordLog);
        }
        let record = Record::ReplicationHead(ReplicationHeadRecord {
            seq: head.seq,
            hash: head.hash.clone(),
        });
        append(&mut inner, record)
    }

    fn data_changed(&self, tenant_id: &str, domain: &str, key: &str) -> Result<()> {
        let mut inner = self.inner();
        if matches!(inner.sink, Sink::Memory) {
            return Ok(());
        }
        let record = Record::DataChanged(DataChangedRecord {
            tenant_id: tenant_id.to_owned(),
            domain: domain.to_owned(),
            key: key.to_owned(),
        });
        append(&mut inner, record)
    }
}
