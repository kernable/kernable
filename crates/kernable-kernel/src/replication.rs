//! Replication, the primary's side (design/replication.md): the standby's stream, and the
//! wait that keeps every write from being told before the standby holds it ("When a write
//! counts").
//!
//! With `--replication sync`, [`crate::Kernel::sync`] waits in [`Primary::wait`] until the
//! standby has acknowledged every record written so far, before the `durable` position moves
//! and before anyone hears of the write. Without a witness, a primary whose standby never caught
//! up commits alone until it has: it then waits for it from that write on, notes so beside
//! its store, and tells the standby, which only then may be promoted. The standby connects with `Follow`: it says where its
//! record log stands, and gets the records after it, or first a full image if it is empty or
//! too far behind, or `DIVERGED` if it holds records the primary does not.

use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable_core::{
    proto::{
        Challenge, DataFile, FollowAck, FollowMode, FollowRequest, FollowResponse, FollowStart,
        ImageBatch, ImageEnd, LogRecord, PromoteRequest, PromoteResult, RecordBatch,
        ReplicatedRecord, Signature, StandbyInSync, SwitchoverRequest, SwitchoverResult, TakeOver,
        TookOver, VaultRow, follow_request, follow_response, log_record::Record,
        replication_service_server::ReplicationService,
    },
    signing,
};
use kernable_store::{Error as StoreError, Head, data::DataKey};
use prost::Message;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataMap};

use crate::{Api, BoxError, Kernel, config, protocol};

/// Records in one batch at most; a batch also ends after [`BATCH_BYTES`].
pub(crate) const BATCH: usize = 512;
/// Bytes of records after which a batch ends (its vault values and files come on top).
pub(crate) const BATCH_BYTES: usize = 1 << 20;
/// Records sent ahead of the standby's acknowledgements at most.
const AHEAD: u64 = 8_192;
/// Records SQLite keeps beyond what the standby acknowledged, for a standby that reconnects.
const RETAIN: u64 = 100_000;
/// How often the stream looks for records no `sync` announced (listener positions).
const TICK: Duration = Duration::from_millis(50);
/// The largest message on a replication stream: a batch with files of the data service.
pub const MAX_MESSAGE_BYTES: usize = 256 << 20;

/// Runs `f`, which blocks until the standby acknowledges, or until a request that waits for it
/// is done (a lock held across [`Primary::wait`]), with the worker's tasks handed to another
/// thread. A worker blocked in place may be the one that last polled the network for the
/// runtime: the others asleep, nobody would read the acknowledgement, and the kernel would stop
/// answering for good.
pub(crate) fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// The standby, as the primary sees it.
pub(crate) struct Primary {
    state: Mutex<State>,
    acked: Condvar,
    /// Wakes the standby's stream: records were written.
    wrote: Notify,
    /// The primary handed its role over: the standby's stream ends, so it claims at once.
    handed_over: std::sync::atomic::AtomicBool,
    /// Without a witness; a pair's primary has one.
    fixed: bool,
    /// Without a witness: where the primary notes that its standby caught up (`<store>.standby-
    /// caught-up`); `None` for a pair, or a store in memory.
    caught_up_marker: Option<PathBuf>,
    /// A switchover without a witness: the standby answered TookOver.
    took_over: Notify,
    /// The standby said it leaves.
    left: Notify,
    /// A pair's primary: the newest witness generation seen, which the standby learns.
    generations: std::sync::OnceLock<Arc<crate::witness::Generations>>,
}

#[derive(Default)]
struct State {
    /// Every record up to this one is on the standby's disk.
    acked: u64,
    /// The key of the standby that is connected.
    standby: Option<String>,
    /// The connected standby acknowledged the head at least once.
    in_sync: bool,
    /// The witness recorded that the primary commits alone (`--witness`).
    alone: bool,
    /// Until when the primary's lease at the witness holds, measured from when it asked and a
    /// margin short of the witness's own count.
    lease_until: Option<Instant>,
    /// Without a witness: no standby has caught up yet, so writes are told without one.
    catching_up: bool,
    /// The newest record told without the standby holding it: the standby has caught up only
    /// once it holds this one.
    told_alone: u64,
    /// The standby caught up just now: the stream notes it on the disk and tells the standby.
    caught_up_now: bool,
    /// Without a witness: whether the standby may be told it is in sync (the note is on the
    /// disk, or there is no disk).
    in_sync_noted: bool,
    /// A switchover without a witness: the record the standby takes over at.
    take_over: Option<u64>,
    /// The record the standby took over at.
    took_over: Option<u64>,
    /// The standby said it leaves; a new one connecting clears it.
    left: bool,
    /// Since when a write waits for the standby's acknowledgment, while one does.
    stalled_since: Option<Instant>,
}

impl State {
    /// Whether a write may be told without the standby: alone, under a lease.
    fn alone_now(&self) -> bool {
        self.alone && self.lease_until.is_some_and(|until| Instant::now() < until)
    }
}

impl Primary {
    /// The primary of a pair: the witness decides when it commits alone.
    pub(crate) fn new() -> Self {
        Primary {
            state: Mutex::default(),
            acked: Condvar::new(),
            wrote: Notify::new(),
            handed_over: std::sync::atomic::AtomicBool::new(false),
            fixed: false,
            caught_up_marker: None,
            took_over: Notify::new(),
            left: Notify::new(),
            generations: std::sync::OnceLock::new(),
        }
    }

    /// A pair's primary: the witness generations its standby learns from it.
    pub(crate) fn set_generations(&self, generations: Arc<crate::witness::Generations>) {
        let _ = self.generations.set(generations);
    }

    /// A primary without a witness, its store at `store`: until a standby has caught up
    /// once, writes are told without one; after that, as long as the note beside the store
    /// says so, never again.
    pub(crate) fn fixed(store: &str) -> Self {
        let marker = crate::standby::marker(store, "standby-caught-up");
        let caught_up = marker.as_ref().is_some_and(|m| m.exists());
        let primary = Primary {
            fixed: true,
            caught_up_marker: marker,
            ..Primary::new()
        };
        {
            let mut state = primary.state();
            state.catching_up = !caught_up;
            state.in_sync_noted = caught_up;
        }
        primary
    }

    /// A primary without a witness, whose switchover the operator asks for.
    pub(crate) fn is_fixed(&self) -> bool {
        self.fixed
    }

    /// Whether writes are told without a standby until one catches up.
    pub(crate) fn catching_up(&self) -> bool {
        self.state().catching_up
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns once the standby holds record `seq` (design/replication.md, "When a write
    /// counts"). Waits as long as it takes: without a
    /// standby, nothing written may be told (with a witness, the witness decides otherwise). Blocks the
    /// thread, [`blocking`]: the other tasks move on meanwhile.
    pub(crate) fn wait(&self, seq: u64) {
        self.wrote.notify_one();
        let wait = || {
            let (started, mut warned) = (Instant::now(), Instant::now());
            let mut state = self.state();
            if state.acked < seq && !state.alone_now() && !state.catching_up {
                state.stalled_since.get_or_insert_with(Instant::now);
            }
            while state.acked < seq && !state.alone_now() && !state.catching_up {
                state = self
                    .acked
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
                if state.acked < seq && warned.elapsed() >= Duration::from_secs(10) {
                    warned = Instant::now();
                    tracing::warn!(
                        record = seq,
                        waited_s = started.elapsed().as_secs(),
                        standby = state.standby.is_some(),
                        acknowledged = state.acked,
                        "a write waits for the standby (--replication sync)"
                    );
                }
            }
            if state.acked < seq {
                state.told_alone = state.told_alone.max(seq);
            }
        };
        blocking(wait);
    }

    /// A standby is connected and has acknowledged the head at least once, or the primary
    /// commits alone (under a lease, or until its first standby caught up): writes are told.
    pub(crate) fn in_sync(&self) -> bool {
        let state = self.state();
        (state.standby.is_some() && state.in_sync) || state.alone_now() || state.catching_up
    }

    /// A standby is connected, has caught up, and the primary waits for it.
    pub(crate) fn standby_in_sync(&self) -> bool {
        let state = self.state();
        state.standby.is_some() && state.in_sync && !state.catching_up && !state.alone
    }

    /// A pair's primary whose lease at the witness ran out while no standby in sync holds its
    /// writes: it may have been replaced, so nothing it writes now may be told.
    pub(crate) fn fenced(&self) -> bool {
        if self.fixed {
            return false;
        }
        let state = self.state();
        // A standby that holds every write keeps the primary safe without a lease (the witness
        // down, say); one that has not acknowledged a write for a second is gone, even if its
        // connection still looks open (a partition drops packets silently).
        let standby_holds = state.standby.is_some()
            && state.in_sync
            && !state.alone
            && state
                .stalled_since
                .is_none_or(|since| since.elapsed() < STALLED);
        state
            .lease_until
            .is_some_and(|until| Instant::now() > until)
            && !standby_holds
    }

    /// Where replication stands, for the usage file.
    pub(crate) fn state_name(&self) -> &'static str {
        if self.standby_in_sync() {
            "in-sync"
        } else if self.state().alone_now() {
            "alone"
        } else if self.catching_up() {
            "catching-up"
        } else {
            "waiting"
        }
    }

    /// The key of the connected standby.
    pub(crate) fn standby(&self) -> Option<String> {
        self.state().standby.clone()
    }

    /// Whether the standby said it leaves, and none connected since.
    pub(crate) fn standby_left(&self) -> bool {
        self.state().left
    }

    /// Until the standby says it leaves.
    pub(crate) async fn standby_leaves(&self) {
        self.left.notified().await
    }

    /// Whether the primary commits alone.
    pub(crate) fn alone(&self) -> bool {
        self.state().alone
    }

    /// Commits alone (once the witness recorded it), or waits for the standby again.
    pub(crate) fn set_alone(&self, alone: bool) {
        self.state().alone = alone;
        self.acked.notify_all();
    }

    /// The lease at the witness holds until `until`.
    pub(crate) fn set_lease(&self, until: Instant) {
        self.state().lease_until = Some(until);
        self.acked.notify_all();
    }

    /// Ends the standby's stream for good: the primary handed its role over.
    pub(crate) fn hand_over(&self) {
        self.handed_over
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.wrote.notify_one();
    }

    /// Whether the primary handed its role over.
    pub(crate) fn handed_over(&self) -> bool {
        self.handed_over.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The record the standby acknowledged last.
    pub(crate) fn acked_seq(&self) -> u64 {
        self.state().acked
    }

    fn ack(&self, seq: u64, head: u64) {
        let mut state = self.state();
        let progressed = seq > state.acked;
        state.acked = state.acked.max(seq);
        if seq >= head {
            state.in_sync = true;
            state.stalled_since = None;
        } else if progressed && state.stalled_since.is_some() {
            // Writes still wait, but the standby moves on: the clock starts again.
            state.stalled_since = Some(Instant::now());
        }
        // Caught up: it holds every write told without it. Writes from here wait for it, so
        // nothing is told it lacks. A write that passed `wait` alone after `head` was read
        // raised `told_alone` above `seq`, and keeps the primary catching up a little longer.
        if state.catching_up && state.standby.is_some() && seq >= head && seq >= state.told_alone {
            state.catching_up = false;
            state.caught_up_now = true;
            tracing::info!(
                record = seq,
                "the standby caught up: writes wait for it from now on"
            );
        }
        self.acked.notify_all();
    }

    /// Notes beside the store that a standby caught up, once it did: started again, the primary
    /// waits for its standby at once. Whether the standby may be told it is in sync.
    fn note_caught_up(&self) -> bool {
        let mut state = self.state();
        if std::mem::take(&mut state.caught_up_now) {
            crate::crash::point("replication.caught_up");
            match &self.caught_up_marker {
                Some(marker) => {
                    match write_synced(marker, b"a standby caught up; writes wait for it\n") {
                        Ok(()) => state.in_sync_noted = true,
                        Err(e) => {
                            tracing::error!(error = %e, marker = %marker.display(), "noting that the standby caught up failed; it is not told it is in sync")
                        }
                    }
                }
                // A store in memory keeps nothing.
                None => state.in_sync_noted = true,
            }
        }
        state.in_sync_noted && !state.catching_up && state.in_sync
    }

    /// A switchover without a witness: the standby takes over at `seq`, which it holds.
    pub(crate) fn take_over(&self, seq: u64) {
        self.state().take_over = Some(seq);
        self.wrote.notify_one();
    }

    /// Resolves once the standby took over; the record it took over at.
    pub(crate) async fn took_over(&self) -> u64 {
        loop {
            let notified = self.took_over.notified();
            if let Some(seq) = self.state().took_over {
                return seq;
            }
            notified.await;
        }
    }

    /// Admits `key_id` as the standby, unless another one is connected.
    fn connect(self: &Arc<Self>, key_id: &str) -> Result<Connected, Status> {
        let mut state = self.state();
        if let Some(other) = &state.standby {
            return Err(Status::already_exists(format!(
                "a standby is connected already (key {other}); the primary has one standby"
            )));
        }
        state.standby = Some(key_id.to_owned());
        state.in_sync = false;
        state.left = false;
        Ok(Connected(Arc::clone(self)))
    }
}

/// How long a write may wait for the standby's acknowledgment before a primary whose lease ran
/// out counts the standby as gone: a healthy one answers within milliseconds.
const STALLED: Duration = Duration::from_secs(1);

/// How long a primary without a witness waits for its standby to confirm that it took over.
const TAKE_OVER_TIMEOUT: Duration = Duration::from_secs(10);

/// A primary without a witness answers `kernable switchover`: the future ends, with why,
/// once it handed its role to its standby, and never otherwise. `None` for a kernel without
/// `--replication sync`, or one of a pair (whose lease keeper answers instead).
pub fn switchover_keeper(
    kernel: &Arc<Kernel>,
) -> Option<std::pin::Pin<Box<dyn Future<Output = BoxError> + Send>>> {
    let primary = kernel.replication.clone().filter(|p| p.fixed)?;
    let (tx, asked) = mpsc::channel(1);
    *kernel
        .switchover
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(tx);
    Some(Box::pin(hand_over_when_asked(
        Arc::clone(kernel),
        primary,
        asked,
    )))
}

async fn hand_over_when_asked(
    kernel: Arc<Kernel>,
    primary: Arc<Primary>,
    mut asked: mpsc::Receiver<crate::node::SwitchoverAsk>,
) -> BoxError {
    use std::sync::atomic::Ordering::SeqCst;
    loop {
        let Some(reply) = asked.recv().await else {
            return std::future::pending().await;
        };
        if !primary.standby_in_sync() {
            let _ = reply.send(Err(
                "no standby is in sync: there is nobody to hand over to".into(),
            ));
            continue;
        }
        let standby = primary.standby().unwrap_or_default();
        kernel.switching.store(true, SeqCst);
        let head = match crate::node::get_ready(Arc::clone(&kernel)).await {
            Ok(head) => head,
            Err(why) => {
                kernel.serve_on();
                tracing::warn!(%why, "switchover abandoned: serving on");
                let _ = reply.send(Err(why));
                continue;
            }
        };
        // From here this kernel tells nothing more, whatever the standby does; noted beside the
        // store, so that it does not serve as the primary again by mistake.
        if let Some(marker) = crate::standby::marker(&kernel.config().store, "handed-over") {
            let note =
                format!("handed the primary role over to standby {standby} at record {head}\n");
            if let Err(e) = write_synced(&marker, note.as_bytes()) {
                kernel.serve_on();
                let why = format!("noting the switchover in {} failed: {e}", marker.display());
                tracing::warn!(%why, "switchover abandoned: serving on");
                let _ = reply.send(Err(why));
                continue;
            }
        }
        primary.take_over(head);
        match tokio::time::timeout(TAKE_OVER_TIMEOUT, primary.took_over()).await {
            Ok(seq) => {
                primary.hand_over();
                tracing::warn!(%standby, record = seq, "handed the primary role over");
                let _ = reply.send(Ok(SwitchoverResult {
                    seq,
                    standby: standby.clone(),
                }));
                return format!(
                    "handed over: standby {standby} is the primary now; start this kernel with --follow to follow it"
                )
                .into();
            }
            Err(_) => {
                let why = format!(
                    "the standby did not confirm within {TAKE_OVER_TIMEOUT:?}; this kernel serves no more. \
                     The standby holds every record up to {head}: promote it by hand (kernable promote)"
                );
                let _ = reply.send(Err(why.clone()));
                return why.into();
            }
        }
    }
}

/// Refuses to start a kernel as the primary on stores that handed the role over: the
/// standby serves now. Started with `--follow`, it follows it instead.
pub fn refuse_if_handed_over(config: &crate::config::Config) -> Result<(), BoxError> {
    if config.replication.follow.is_some() || config.replication.pair.is_some() {
        return Ok(());
    }
    match crate::standby::marker(&config.store, "handed-over") {
        Some(marker) if marker.exists() => Err(format!(
            "{}: {}this kernel's standby serves as the primary now. Start this kernel with --follow \
             to follow it, or delete the file if that standby is gone for good and this kernel must serve again",
            marker.display(),
            std::fs::read_to_string(&marker).unwrap_or_default()
        )
        .into()),
        _ => Ok(()),
    }
}

/// Writes `bytes` to `path` and syncs it.
pub(crate) fn write_synced(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// The backup follower, as the kernel sees it: one at a time, and how far it got.
#[derive(Default)]
pub(crate) struct Backup {
    state: Mutex<BackupState>,
}

#[derive(Default)]
struct BackupState {
    /// The key of the backup follower that is connected.
    connected: Option<String>,
    /// The record it acknowledged last, connected or not.
    acked: u64,
}

impl Backup {
    fn state(&self) -> MutexGuard<'_, BackupState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn connect(&self, key_id: &str) -> Result<BackupConnected<'_>, Status> {
        let mut state = self.state();
        if let Some(other) = &state.connected {
            return Err(Status::already_exists(format!(
                "a backup follower is connected already (key {other})"
            )));
        }
        state.connected = Some(key_id.to_owned());
        Ok(BackupConnected(self))
    }

    fn ack(&self, seq: u64) {
        let mut state = self.state();
        state.acked = state.acked.max(seq);
    }

    /// The record it acknowledged last.
    pub(crate) fn acked(&self) -> u64 {
        self.state().acked
    }

    /// The key of the backup follower connected now.
    pub(crate) fn connected(&self) -> Option<String> {
        self.state().connected.clone()
    }
}

struct BackupConnected<'a>(&'a Backup);

impl Drop for BackupConnected<'_> {
    fn drop(&mut self) {
        self.0.state().connected = None;
        tracing::warn!("the backup follower disconnected");
    }
}

/// Records SQLite keeps for a backup follower that is away, beyond what the standby needs: once
/// it is further behind, it takes a full image again (a new generation of the backup).
const BACKUP_RETAIN: u64 = 1_000_000;

/// The record up to which SQLite may drop its record log, given the standby's and the backup
/// follower's acknowledgements.
fn prune_point(kernel: &Kernel) -> Option<u64> {
    let head = kernel.store.record_head().ok()?.seq;
    let mut keep = head;
    if let Some(primary) = &kernel.replication {
        keep = keep.min(primary.acked_seq());
    }
    if kernel.config().replication.backup {
        keep = keep.min(
            kernel
                .backup
                .acked()
                .max(head.saturating_sub(BACKUP_RETAIN)),
        );
    }
    keep.checked_sub(RETAIN).filter(|&p| p > 0)
}

/// The connected standby; disconnects it when dropped.
struct Connected(Arc<Primary>);

impl Drop for Connected {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.standby = None;
        state.in_sync = false;
        tracing::warn!("the standby disconnected");
    }
}

#[tonic::async_trait]
impl ReplicationService for Api {
    type FollowStream = ReceiverStream<Result<FollowResponse, Status>>;

    async fn follow(
        &self,
        request: Request<Streaming<FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let (out, rx) = mpsc::channel(4);
        let kernel = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = run(kernel, &metadata, &mut inbound, &out).await {
                tracing::warn!(error = %status.message(), "the standby's stream ended");
                let _ = out.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn promote(
        &self,
        _request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResult>, Status> {
        Err(Status::failed_precondition(
            "this kernel is a primary already",
        ))
    }

    async fn switchover(
        &self,
        request: Request<SwitchoverRequest>,
    ) -> Result<Response<SwitchoverResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("the switchover request is not signed"))?;
        signing::verify(&sig, &signing::switchover_message(req.issued_at))
            .map_err(|_| Status::unauthenticated("invalid switchover request signature"))?;
        if (crate::now_ms() - req.issued_at).abs() > crate::MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "switchover request expired; check the clock",
            ));
        }
        kernel.present(&metadata, &sig.key_id)?;
        if !config::allowed(&kernel.permissions().manage, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "the key may not switch over: its grant needs manage for *",
            ));
        }
        let asks = kernel
            .switchover
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "this kernel runs without --replication sync: it has no standby to hand over to",
                )
            })?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        tracing::warn!(key = %sig.key_id, "switchover asked by an operator");
        asks.send(tx)
            .await
            .map_err(|_| Status::failed_precondition("this kernel is no longer the primary"))?;
        match rx.await {
            Ok(Ok(result)) => Ok(Response::new(result)),
            Ok(Err(why)) => Err(Status::failed_precondition(why)),
            Err(_) => Err(Status::failed_precondition(
                "this kernel is no longer the primary",
            )),
        }
    }
}

type Outbox = mpsc::Sender<Result<FollowResponse, Status>>;

async fn next(
    inbound: &mut Streaming<FollowRequest>,
) -> Result<Option<follow_request::Msg>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

async fn send(out: &Outbox, msg: follow_response::Msg) -> Result<(), Status> {
    out.send(Ok(FollowResponse { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("the standby left"))
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

async fn run(
    kernel: Arc<Kernel>,
    metadata: &MetadataMap,
    inbound: &mut Streaming<FollowRequest>,
    out: &Outbox,
) -> Result<(), Status> {
    let Some(follow_request::Msg::Hello(hello)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected FollowHello"));
    };
    protocol(hello.protocol)?;
    let backup = hello.backup;
    let primary = match (backup, kernel.replication.clone()) {
        (true, _) if !kernel.config().replication.backup => {
            return Err(Status::failed_precondition(
                "this kernel runs without --backup on: it keeps no backup follower",
            ));
        }
        (true, _) => None,
        (false, Some(primary)) => Some(primary),
        (false, None) => {
            return Err(Status::failed_precondition(
                "this kernel runs without --replication sync: it keeps no standby",
            ));
        }
    };
    kernel.present_standby(metadata, &hello.key_id)?;
    let allowed = |kernel: &Kernel| {
        let permissions = kernel.permissions();
        let rules = if backup {
            &permissions.backup
        } else {
            &permissions.replicate
        };
        config::allowed(rules, &hello.key_id, "*")
    };
    if !allowed(&kernel) {
        return Err(Status::permission_denied(if backup {
            "the key may not back up this kernel: its grant needs backup"
        } else {
            "the key may not follow this kernel: its grant needs replicate"
        }));
    }
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    send(
        out,
        follow_response::Msg::Challenge(Challenge {
            nonce: nonce.to_vec(),
        }),
    )
    .await?;
    let Some(follow_request::Msg::ChallengeResponse(resp)) =
        kernel.handshake(next(inbound)).await?
    else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: hello.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;
    // The standby's newest witness generation: a primary that had not seen it learns it.
    if let (Some(g), Some(generations)) = (
        &hello.witness_generation,
        primary.as_ref().and_then(|p| p.generations.get()),
    ) && let Err(e) = generations.learn(g)
    {
        tracing::warn!(error = %e, "the standby's witness generation is not taken");
    }
    let _connected = primary
        .as_ref()
        .map(|p| p.connect(&hello.key_id))
        .transpose()?;
    let _backup = backup
        .then(|| kernel.backup.connect(&hello.key_id))
        .transpose()?;
    let acked = |kernel: &Kernel| match &primary {
        Some(p) => p.acked_seq(),
        None => kernel.backup.acked(),
    };

    // Where the standby stands against this log.
    let head = kernel.store.record_head().map_err(internal)?;
    let (mut mode, mut message) = match kernel.store.record_hash_at(hello.seq).map_err(internal)? {
        Some(hash) if hash == hello.hash => (FollowMode::Follow, String::new()),
        Some(_) => (
            FollowMode::Diverged,
            format!(
                "the standby's record {} differs from the primary's",
                hello.seq
            ),
        ),
        None if hello.seq > head.seq => (
            FollowMode::Diverged,
            format!(
                "the standby is at record {}, the primary at {}",
                hello.seq, head.seq
            ),
        ),
        None => (FollowMode::Image, String::new()),
    };
    // Diverged, it may take its newest writes back to a record both hold (design/rewind.md).
    let mut rewind_to = 0;
    if mode == FollowMode::Diverged {
        for point in &hello.rewind_points {
            if point.seq <= head.seq
                && kernel
                    .store
                    .record_hash_at(point.seq)
                    .map_err(internal)?
                    .as_ref()
                    == Some(&point.hash)
            {
                (mode, rewind_to) = (FollowMode::Rewind, point.seq);
                message = format!(
                    "{message}: it takes its records after {} back and follows on",
                    point.seq
                );
                break;
            }
        }
    }
    if backup {
        tracing::info!(key = %hello.key_id, from = hello.seq, mode = ?mode, "a backup follower follows");
    } else {
        tracing::info!(standby = %hello.key_id, from = hello.seq, mode = ?mode, "a standby follows");
    }
    send(
        out,
        follow_response::Msg::Start(FollowStart {
            mode: mode.into(),
            message,
            rewind_to,
            vault_key: kernel
                .vault
                .as_ref()
                .map_or(String::new(), |v| v.key_id().to_owned()),
        }),
    )
    .await?;
    let mut sent = match mode {
        FollowMode::Follow => {
            match &primary {
                Some(p) => p.ack(hello.seq, head.seq),
                None => kernel.backup.ack(hello.seq),
            }
            hello.seq
        }
        FollowMode::Image => send_image(&kernel, out, backup).await?,
        _ => return Ok(()),
    };

    let mut tick = tokio::time::interval(TICK);
    let mut pruned = 0;
    let mut undone = 0;
    let handed_over = || {
        primary
            .as_ref()
            .is_some_and(|p| p.handed_over.load(std::sync::atomic::Ordering::SeqCst))
    };
    let mut changes = kernel.changes();
    let mut told_in_sync = false;
    let mut told_take_over = false;
    let mut told_generation = 0;
    loop {
        if let Some(p) = &primary {
            // A pair: the standby learns the newest witness generation, and again when it rises.
            if let Some(newest) = p.generations.get().and_then(|g| g.newest())
                && newest.generation > told_generation
            {
                told_generation = newest.generation;
                send(out, follow_response::Msg::Generation(newest)).await?;
            }
            // Without a witness: the standby learns that it is in sync, once per stream.
            if p.fixed {
                if !told_in_sync && p.note_caught_up() {
                    crate::crash::point("replication.noted");
                    send(out, follow_response::Msg::InSync(StandbyInSync {})).await?;
                    told_in_sync = true;
                }
                let take_over = p.state().take_over;
                if let Some(seq) = take_over
                    && !told_take_over
                    && sent >= seq
                    && p.acked_seq() >= seq
                {
                    send(out, follow_response::Msg::TakeOver(TakeOver { seq })).await?;
                    told_take_over = true;
                    crate::crash::point("replication.take_over");
                    tracing::warn!(record = seq, "told the standby to take over");
                }
            }
        }
        if handed_over() {
            return Err(Status::unavailable(
                "the primary handed its role over: ask the witness who serves now",
            ));
        }
        if sent < acked(&kernel) + AHEAD {
            let records = kernel
                .store
                .records_after(sent, BATCH)
                .map_err(internal)?
                .ok_or_else(|| {
                    Status::unavailable(
                        "the primary no longer keeps the records the standby needs; it takes an image",
                    )
                })?;
            if !records.is_empty() {
                let records = within_bytes(records);
                sent = records.last().map_or(sent, |r| r.seq);
                let inner: Vec<&Record> = records.iter().filter_map(inner).collect();
                let (vault, files) = attachments(&kernel, inner.into_iter(), backup)?;
                let head = kernel.store.record_head().map_err(internal)?.seq;
                send(
                    out,
                    follow_response::Msg::Records(RecordBatch {
                        records,
                        vault,
                        files,
                        head,
                    }),
                )
                .await?;
                crate::crash::point("replication.sent");
                continue;
            }
        }
        tokio::select! {
            message = next(inbound) => match message? {
                Some(follow_request::Msg::TookOver(TookOver { seq })) if told_take_over => {
                    if let Some(p) = &primary {
                        p.state().took_over = Some(seq);
                        p.took_over.notify_waiters();
                    }
                    return Ok(());
                }
                Some(follow_request::Msg::Leaving(_)) => {
                    if let Some(p) = &primary {
                        tracing::info!("the standby leaves: the primary need not wait for it");
                        p.state().left = true;
                        p.left.notify_one();
                    }
                    return Ok(());
                }
                Some(follow_request::Msg::Ack(FollowAck { seq })) => {
                    if seq > sent {
                        return Err(Status::invalid_argument(format!(
                            "acknowledged record {seq}, but only {sent} were sent"
                        )));
                    }
                    let head = kernel.store.record_head().map_err(internal)?.seq;
                    match &primary {
                        Some(p) => p.ack(seq, head),
                        None => kernel.backup.ack(seq),
                    }
                    // What the standby holds is never to be taken back (design/rewind.md):
                    // its undo goes, a thousand records at a time.
                    if primary.is_some() && seq >= undone + 1000 {
                        undone = seq;
                        let kernel = Arc::clone(&kernel);
                        tokio::task::spawn_blocking(move || {
                            // Confirmed now: the secrets of commands completed among them go.
                            match kernel.store.forget_undo(seq) {
                                Ok(completed) => {
                                    for id in completed {
                                        kernel.drop_secrets(&id);
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "dropping undo the standby holds failed")
                                }
                            }
                        });
                    }
                    // SQLite keeps what a reconnecting standby or backup follower may need;
                    // the file log keeps all.
                    if seq >= pruned + RETAIN / 10
                        && let Some(point) = prune_point(&kernel)
                    {
                        pruned = seq;
                        let kernel = Arc::clone(&kernel);
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = kernel.store.prune_records(point) {
                                tracing::error!(error = %e, "pruning the record log failed");
                            }
                        });
                    }
                }
                None => return Ok(()),
                Some(_) => return Err(Status::invalid_argument("unexpected message")),
            },
            () = wrote(primary.as_deref()) => {}
            Ok(()) = changes.changed() => {
                // A revoked key, or a grant without replicate (backup): it follows no longer.
                if !allowed(&kernel) {
                    return Err(Status::permission_denied(
                        "the follower's key may follow no longer: revoked, or its grant changed",
                    ));
                }
            }
            _ = tick.tick() => {}
        }
    }
}

/// Resolves when the primary wrote records for its standby; never for a backup follower, which
/// looks every tick.
async fn wrote(primary: Option<&Primary>) {
    match primary {
        Some(p) => p.wrote.notified().await,
        None => std::future::pending().await,
    }
}

fn inner(r: &ReplicatedRecord) -> Option<&Record> {
    r.record.as_ref().and_then(|l| l.record.as_ref())
}

/// The first records of `records` within [`BATCH_BYTES`], at least one.
fn within_bytes(mut records: Vec<ReplicatedRecord>) -> Vec<ReplicatedRecord> {
    let mut bytes = 0;
    let keep = records
        .iter()
        .position(|r| {
            bytes += r.encoded_len();
            bytes > BATCH_BYTES
        })
        .map_or(records.len(), |i| i.max(1));
    records.truncate(keep);
    records
}

/// The vault rows and files `records` refer to, as they are now: a value erased or a file
/// deleted meanwhile travels as absent, and the records after converge the standby. `sealed`:
/// the vault values as stored, encrypted with the kernel's vault key (for a backup).
pub(crate) fn attachments<'a>(
    kernel: &Kernel,
    records: impl Iterator<Item = &'a Record> + Clone,
    sealed: bool,
) -> Result<(Vec<VaultRow>, Vec<DataFile>), Status> {
    let refs = vault_refs(records.clone());
    let mut files = Vec::new();
    for record in records {
        if let Record::DataChanged(d) = record {
            let key = DataKey {
                tenant_id: d.tenant_id.clone(),
                domain: d.domain.clone(),
                key: d.key.clone(),
            };
            let file = kernel.data.get(&key).map_err(internal)?;
            files.push(DataFile {
                tenant_id: d.tenant_id.clone(),
                domain: d.domain.clone(),
                key: d.key.clone(),
                absent: file.is_none(),
                content_type: file
                    .as_ref()
                    .map(|f| f.content_type.clone())
                    .unwrap_or_default(),
                data: file.map(|f| f.data).unwrap_or_default(),
                path: String::new(),
            });
        }
    }
    let vault = match &kernel.vault {
        Some(vault) if !refs.is_empty() && sealed => {
            vault.export_sealed(&refs).map_err(internal)?
        }
        Some(vault) if !refs.is_empty() => vault.export(&refs).map_err(internal)?,
        _ => Vec::new(),
    };
    Ok((vault, files))
}

/// The vault values `records` refer to: those of commands, events and personal unique keys.
pub(crate) fn vault_refs<'a>(records: impl Iterator<Item = &'a Record>) -> Vec<String> {
    let mut refs = Vec::new();
    for record in records {
        match record {
            Record::Enqueued(e) => {
                refs.extend(e.command.iter().flat_map(|c| c.vault_refs.iter().cloned()));
            }
            Record::Committed(c) => {
                for logged in &c.events {
                    refs.extend(
                        logged
                            .event
                            .iter()
                            .flat_map(|e| e.vault_refs.iter().cloned()),
                    );
                }
                refs.extend(
                    c.index
                        .iter()
                        .flat_map(|i| &i.keys)
                        .filter(|k| !k.vault_ref.is_empty())
                        .map(|k| k.vault_ref.clone()),
                );
            }
            _ => {}
        }
    }
    refs
}

/// Sends a full image: the store's records, with the vault rows they refer to, then every file
/// of the data store, then where the image stands. Returns that record's sequence number.
async fn send_image(kernel: &Arc<Kernel>, out: &Outbox, sealed: bool) -> Result<u64, Status> {
    let (tx, mut rx) = mpsc::channel::<ImageBatch>(4);
    let k = Arc::clone(kernel);
    let reading = tokio::task::spawn_blocking(move || -> Result<Head, Status> {
        let left = || StoreError::Io(std::io::Error::other("the standby left"));
        let flush = |batch: &mut Vec<Record>| -> Result<(), StoreError> {
            let (vault, files) = attachments(&k, batch.iter(), sealed)
                .map_err(|s| StoreError::Io(std::io::Error::other(s.message().to_owned())))?;
            let records = batch
                .drain(..)
                .map(|r| LogRecord { record: Some(r) })
                .collect();
            tx.blocking_send(ImageBatch {
                records,
                vault,
                files,
            })
            .map_err(|_| left())
        };
        let (mut batch, mut bytes) = (Vec::new(), 0);
        let head = k
            .store
            .image(&mut |record| {
                bytes += record_len(&record);
                batch.push(record);
                if batch.len() >= BATCH || bytes >= BATCH_BYTES {
                    bytes = 0;
                    flush(&mut batch)?;
                }
                Ok(())
            })
            .map_err(internal)?;
        flush(&mut batch).map_err(internal)?;
        let (mut files, mut bytes) = (Vec::new(), 0);
        k.data
            .dump(&mut |place, stored| {
                bytes += stored.len();
                files.push(DataFile {
                    path: place.to_owned(),
                    data: stored.to_vec(),
                    ..Default::default()
                });
                if bytes >= BATCH_BYTES {
                    bytes = 0;
                    let files = std::mem::take(&mut files);
                    tx.blocking_send(ImageBatch {
                        files,
                        ..Default::default()
                    })
                    .map_err(|_| left())?;
                }
                Ok(())
            })
            .map_err(internal)?;
        if !files.is_empty() {
            tx.blocking_send(ImageBatch {
                files,
                ..Default::default()
            })
            .map_err(|_| internal("the standby left"))?;
        }
        Ok(head)
    });
    while let Some(batch) = rx.recv().await {
        send(out, follow_response::Msg::Image(batch)).await?;
    }
    let head = reading.await.map_err(internal)??;
    tracing::info!(record = head.seq, "sent the standby a full image");
    send(
        out,
        follow_response::Msg::ImageEnd(ImageEnd {
            seq: head.seq,
            hash: head.hash.clone(),
        }),
    )
    .await?;
    Ok(head.seq)
}

fn record_len(record: &Record) -> usize {
    LogRecord {
        record: Some(record.clone()),
    }
    .encoded_len()
}
