//! A store that fails on purpose, for robustness tests (feature `faults`, never in a release).
//!
//! [`FaultyStore`] wraps any store and, while its [`Faults`] are armed, fails a seeded share of
//! the calls with an I/O error (`StorageFull`). A write fails either **before** it reaches the
//! store (nothing happened) or **after** (it happened, but the caller is told it failed), as a
//! full disk, a failed fsync or a lost connection would. The kernel must then fail without
//! corrupting anything: once the faults stop, every invariant holds and every command still
//! gets exactly one outcome.

use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
};

use ed25519_dalek::SigningKey;
use kernable_core::proto::{
    AggregateIndex, Command, CommandResult, Event, Grant, ListenerRecord, MembershipRecord,
    NewEvent, PartitionMap, Rejected, ReplicatedRecord, RevocationRecord, Signature,
    TenantImportedRecord, WorkerRecord, log_record::Record,
};

use crate::{
    Checkpoint, Enqueued, Error, Head, Pending, Report, Result, Store, StreamId, StreamSummary,
};

/// What fails, and how often.
pub struct Faults {
    armed: AtomicBool,
    /// One write in this many fails; 0 never.
    write_one_in: AtomicU64,
    /// One read in this many fails; 0 never.
    read_one_in: AtomicU64,
    /// xorshift state: the same seed fails the same calls.
    rng: Mutex<u64>,
    /// Failures so far: before a write, after a write, of a read.
    pub failed_before: AtomicU64,
    pub failed_after: AtomicU64,
    pub failed_reads: AtomicU64,
    /// Commits still to fail with a version conflict, as if another writer got there first.
    conflicts: AtomicU64,
    /// While set, every commit fails before it writes, as on a full disk.
    full: AtomicBool,
    /// Commits failed because of `full`.
    pub failed_full: AtomicU64,
}

impl Faults {
    /// Faults that fail one write in `write_one_in` and one read in `read_one_in` (0: none),
    /// armed from the start.
    pub fn new(seed: u64, write_one_in: u64, read_one_in: u64) -> Arc<Faults> {
        Arc::new(Faults {
            armed: AtomicBool::new(true),
            write_one_in: AtomicU64::new(write_one_in),
            read_one_in: AtomicU64::new(read_one_in),
            rng: Mutex::new(seed.max(1)),
            failed_before: AtomicU64::default(),
            failed_after: AtomicU64::default(),
            failed_reads: AtomicU64::default(),
            conflicts: AtomicU64::default(),
            full: AtomicBool::default(),
            failed_full: AtomicU64::default(),
        })
    }

    /// Stops or resumes failing.
    pub fn arm(&self, armed: bool) {
        self.armed.store(armed, SeqCst);
    }

    /// Fails the next `n` commits with a version conflict, armed or not, writing nothing.
    pub fn conflict(&self, n: u64) {
        self.conflicts.store(n, SeqCst);
    }

    /// Fails every commit before it writes, armed or not, until called with `false`: a full
    /// disk.
    pub fn full(&self, full: bool) {
        self.full.store(full, SeqCst);
    }

    fn next(&self) -> u64 {
        let mut x = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    fn hit(&self, one_in: &AtomicU64) -> bool {
        let n = one_in.load(SeqCst);
        self.armed.load(SeqCst) && n > 0 && self.next().is_multiple_of(n)
    }

    /// Whether this read fails.
    fn read(&self) -> Result<()> {
        if self.hit(&self.read_one_in) {
            self.failed_reads.fetch_add(1, SeqCst);
            return Err(injected("read"));
        }
        Ok(())
    }

    /// Runs `write`, failing before it, or after it with its effect kept.
    fn write<T>(&self, write: impl FnOnce() -> Result<T>) -> Result<T> {
        if !self.hit(&self.write_one_in) {
            return write();
        }
        if self.next().is_multiple_of(2) {
            self.failed_before.fetch_add(1, SeqCst);
            return Err(injected("write, not done"));
        }
        let done = write();
        self.failed_after.fetch_add(1, SeqCst);
        done.and(Err(injected("write, done")))
    }
}

fn injected(what: &str) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        format!("injected fault: {what}"),
    ))
}

/// A store whose calls fail as its [`Faults`] say.
pub struct FaultyStore {
    inner: Box<dyn Store>,
    faults: Arc<Faults>,
}

impl FaultyStore {
    pub fn new(inner: Box<dyn Store>, faults: Arc<Faults>) -> Self {
        FaultyStore { inner, faults }
    }
}

impl Store for FaultyStore {
    fn read_stream(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<Event>> {
        self.faults.read()?;
        self.inner
            .read_stream(tenant_id, domain, aggregate_type, aggregate_id)
    }

    fn read_stream_after(
        &self,
        tenant_id: &str,
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<Event>> {
        self.faults.read()?;
        self.inner.read_stream_after(
            tenant_id,
            domain,
            aggregate_type,
            aggregate_id,
            after_version,
        )
    }

    fn read_all(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        self.faults.read()?;
        self.inner.read_all(after, limit)
    }

    fn head_position(&self) -> Result<u64> {
        self.faults.read()?;
        self.inner.head_position()
    }

    fn enqueue(&self, cmd: &Command, now_ms: i64) -> Result<Enqueued> {
        self.faults.write(|| self.inner.enqueue(cmd, now_ms))
    }

    fn pending(&self) -> Result<Vec<Pending>> {
        self.faults.read()?;
        self.inner.pending()
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
        let conflicts = &self.faults.conflicts;
        if conflicts
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::VersionConflict {
                expected: expected_version,
                actual: expected_version + 1,
            });
        }
        if self.faults.full.load(SeqCst) {
            self.faults.failed_full.fetch_add(1, SeqCst);
            return Err(injected("disk full"));
        }
        self.faults.write(|| {
            self.inner.commit(
                command_id,
                expected_version,
                events,
                producer_sig,
                index,
                now_ms,
            )
        })
    }

    fn streams(&self, limit: usize) -> Result<Vec<StreamSummary>> {
        self.faults.read()?;
        self.inner.streams(limit)
    }

    fn event_sources(&self) -> Result<Vec<(String, String, String)>> {
        self.faults.read()?;
        self.inner.event_sources()
    }

    fn each_event(&self, f: &mut dyn FnMut(&Event)) -> Result<()> {
        self.faults.read()?;
        self.inner.each_event(f)
    }

    fn command(&self, command_id: &str) -> Result<Option<Pending>> {
        self.faults.read()?;
        self.inner.command(command_id)
    }

    fn command_ids(&self) -> Result<Vec<String>> {
        self.faults.read()?;
        self.inner.command_ids()
    }

    fn rejections(&self, limit: usize) -> Result<Vec<(Pending, Rejected)>> {
        self.faults.read()?;
        self.inner.rejections(limit)
    }

    fn event_schemas(&self) -> Result<Vec<(String, u32)>> {
        self.faults.read()?;
        self.inner.event_schemas()
    }

    fn stream_index(&self, stream: &StreamId) -> Result<Option<AggregateIndex>> {
        self.faults.read()?;
        self.inner.stream_index(stream)
    }

    fn key_holder(&self, id: &[u8]) -> Result<Option<StreamId>> {
        self.faults.read()?;
        self.inner.key_holder(id)
    }

    fn holds_personal_keys(&self) -> Result<bool> {
        self.faults.read()?;
        self.inner.holds_personal_keys()
    }

    fn reject(&self, command_id: &str, reason: Rejected) -> Result<()> {
        self.faults.write(|| self.inner.reject(command_id, reason))
    }

    fn result(&self, command_id: &str) -> Result<Option<CommandResult>> {
        self.faults.read()?;
        self.inner.result(command_id)
    }

    fn listeners(&self) -> Result<Vec<ListenerRecord>> {
        self.faults.read()?;
        self.inner.listeners()
    }

    fn save_listeners(&self, listeners: &[ListenerRecord]) -> Result<()> {
        self.faults.write(|| self.inner.save_listeners(listeners))
    }

    fn workers(&self) -> Result<Vec<WorkerRecord>> {
        self.faults.read()?;
        self.inner.workers()
    }

    fn save_workers(&self, workers: &[WorkerRecord]) -> Result<()> {
        self.faults.write(|| self.inner.save_workers(workers))
    }

    fn memberships(&self) -> Result<Vec<MembershipRecord>> {
        self.faults.read()?;
        self.inner.memberships()
    }

    fn save_membership(&self, membership: &MembershipRecord) -> Result<()> {
        self.faults.write(|| self.inner.save_membership(membership))
    }

    fn checkpoint(&self, kernel: &SigningKey, now_ms: i64) -> Result<Option<Checkpoint>> {
        self.faults.write(|| self.inner.checkpoint(kernel, now_ms))
    }

    fn revoke(&self, revocation: &RevocationRecord) -> Result<()> {
        self.faults.write(|| self.inner.revoke(revocation))
    }

    fn revocations(&self) -> Result<Vec<RevocationRecord>> {
        self.faults.read()?;
        self.inner.revocations()
    }

    fn index(&self) -> Result<Vec<(StreamId, AggregateIndex)>> {
        self.faults.read()?;
        self.inner.index()
    }

    fn grants(&self) -> Result<Vec<Grant>> {
        self.faults.read()?;
        self.inner.grants()
    }

    fn save_grant(&self, grant: &Grant) -> Result<()> {
        self.faults.write(|| self.inner.save_grant(grant))
    }

    /// Never fails on purpose: a failed flush stops the kernel, which an in-process test cannot
    /// survive. The file log's refusal after a failed flush has its own test.
    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn verify(&self, kernel_key_ids: &[String]) -> Result<Report> {
        self.inner.verify(kernel_key_ids)
    }

    fn tenant_imported(&self, tenant: &str) -> Result<Option<TenantImportedRecord>> {
        self.faults.read()?;
        self.inner.tenant_imported(tenant)
    }

    fn tenant_records(
        &self,
        tenant: &str,
        after: Option<u64>,
        f: &mut dyn FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        self.faults.read()?;
        self.inner.tenant_records(tenant, after, f)
    }

    fn set_tenant_index(
        &self,
        tenant: &str,
        streams: &[kernable_core::proto::StreamIndex],
    ) -> Result<usize> {
        self.faults
            .write(|| self.inner.set_tenant_index(tenant, streams))
    }

    fn tenant_index(&self, tenant: &str) -> Result<Vec<(StreamId, AggregateIndex)>> {
        self.faults.read()?;
        self.inner.tenant_index(tenant)
    }

    fn import(&self, copies: &[crate::Copy]) -> Result<()> {
        self.faults.write(|| self.inner.import(copies))
    }

    fn save_tenant_imported(&self, record: &TenantImportedRecord) -> Result<()> {
        self.faults
            .write(|| self.inner.save_tenant_imported(record))
    }

    fn global_key(&self, id: &[u8]) -> Result<Option<String>> {
        self.faults.read()?;
        self.inner.global_key(id)
    }

    fn save_global_key(&self, id: &[u8], partition: &str) -> Result<()> {
        self.faults
            .write(|| self.inner.save_global_key(id, partition))
    }

    fn partition_map(&self) -> Result<Option<PartitionMap>> {
        self.faults.read()?;
        self.inner.partition_map()
    }

    fn save_partition_map(&self, map: &PartitionMap) -> Result<()> {
        self.faults.write(|| self.inner.save_partition_map(map))
    }

    fn record_head(&self) -> Result<Head> {
        self.faults.read()?;
        self.inner.record_head()
    }

    fn record_hash_at(&self, seq: u64) -> Result<Option<Vec<u8>>> {
        self.faults.read()?;
        self.inner.record_hash_at(seq)
    }

    fn records_after(&self, seq: u64, limit: usize) -> Result<Option<Vec<ReplicatedRecord>>> {
        self.faults.read()?;
        self.inner.records_after(seq, limit)
    }

    fn keep_record_log(&self, on: bool) -> Result<()> {
        self.inner.keep_record_log(on)
    }

    fn rewind_points(&self) -> Result<Vec<Head>> {
        self.faults.read()?;
        self.inner.rewind_points()
    }

    fn rewind(&self, to: u64) -> Result<Vec<String>> {
        self.faults.write(|| self.inner.rewind(to))
    }

    fn forget_undo(&self, seq: u64) -> Result<()> {
        self.inner.forget_undo(seq)
    }

    fn seal_records(&self, sealed: bool) -> Result<Head> {
        self.inner.seal_records(sealed)
    }

    fn prune_records(&self, seq: u64) -> Result<()> {
        self.faults.write(|| self.inner.prune_records(seq))
    }

    fn apply(&self, records: &[ReplicatedRecord]) -> Result<()> {
        self.faults.write(|| self.inner.apply(records))
    }

    fn image(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<Head> {
        self.faults.read()?;
        self.inner.image(f)
    }

    fn apply_image(&self, records: &[Record]) -> Result<()> {
        self.faults.write(|| self.inner.apply_image(records))
    }

    fn start_record_log(&self, head: &Head) -> Result<()> {
        self.faults.write(|| self.inner.start_record_log(head))
    }

    fn data_changed(&self, tenant_id: &str, domain: &str, key: &str) -> Result<()> {
        self.faults
            .write(|| self.inner.data_changed(tenant_id, domain, key))
    }
}
