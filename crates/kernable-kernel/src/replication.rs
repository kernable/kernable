//! Replication, the primary's side (design/replication.md): the standby's stream, and the
//! wait that keeps every write from being told before the standby holds it (P1).
//!
//! With `--replication sync`, [`crate::Kernel::sync`] waits in [`Primary::wait`] until the
//! standby has acknowledged every record written so far, before the `durable` position moves
//! and before anyone hears of the write. The standby connects with `Follow`: it says where its
//! record log stands, and gets the records after it, or first a full image if it is empty or
//! too far behind, or `DIVERGED` if it holds records the primary does not.

use std::{
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable_core::{
    proto::{
        Challenge, DataFile, FollowAck, FollowMode, FollowRequest, FollowResponse, FollowStart,
        ImageBatch, ImageEnd, LogRecord, PromoteRequest, PromoteResult, RecordBatch,
        ReplicatedRecord, Signature, VaultRow, follow_request, follow_response, log_record::Record,
        replication_service_server::ReplicationService,
    },
    signing,
};
use kernable_store::{Error as StoreError, Head, data::DataKey};
use prost::Message;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataMap};

use crate::{Api, Kernel, config, protocol};

/// Records in one batch at most; a batch also ends after [`BATCH_BYTES`].
const BATCH: usize = 512;
/// Bytes of records after which a batch ends (its vault values and files come on top).
const BATCH_BYTES: usize = 1 << 20;
/// Records sent ahead of the standby's acknowledgements at most.
const AHEAD: u64 = 8_192;
/// Records SQLite keeps beyond what the standby acknowledged, for a standby that reconnects.
const RETAIN: u64 = 100_000;
/// How often the stream looks for records no `sync` announced (listener positions).
const TICK: Duration = Duration::from_millis(50);
/// The largest message on a replication stream: a batch with files of the data service.
pub(crate) const MAX_MESSAGE_BYTES: usize = 256 << 20;

/// The standby, as the primary sees it.
pub(crate) struct Primary {
    state: Mutex<State>,
    acked: Condvar,
    /// Wakes the standby's stream: records were written.
    wrote: Notify,
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
}

impl State {
    /// Whether a write may be told without the standby: alone, under a lease.
    fn alone_now(&self) -> bool {
        self.alone && self.lease_until.is_some_and(|until| Instant::now() < until)
    }
}

impl Primary {
    pub(crate) fn new() -> Self {
        Primary {
            state: Mutex::default(),
            acked: Condvar::new(),
            wrote: Notify::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns once the standby holds record `seq` (P1). Waits as long as it takes: without a
    /// standby, nothing written may be told (S1b lets the witness decide otherwise). Blocks the
    /// thread; on a multi-threaded runtime the other tasks move on meanwhile.
    pub(crate) fn wait(&self, seq: u64) {
        self.wrote.notify_one();
        let wait = || {
            let (started, mut warned) = (Instant::now(), Instant::now());
            let mut state = self.state();
            while state.acked < seq && !state.alone_now() {
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
                        "a write waits for the standby (--replication sync)"
                    );
                }
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(wait)
            }
            _ => wait(),
        }
    }

    /// A standby is connected and has acknowledged the head at least once, or the primary
    /// commits alone under a lease: writes are told.
    pub(crate) fn in_sync(&self) -> bool {
        let state = self.state();
        (state.standby.is_some() && state.in_sync) || state.alone_now()
    }

    /// The key of the connected standby.
    pub(crate) fn standby(&self) -> Option<String> {
        self.state().standby.clone()
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

    /// The record the standby acknowledged last.
    pub(crate) fn acked_seq(&self) -> u64 {
        self.state().acked
    }

    fn ack(&self, seq: u64, head: u64) {
        let mut state = self.state();
        state.acked = state.acked.max(seq);
        if seq >= head {
            state.in_sync = true;
        }
        self.acked.notify_all();
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
        Ok(Connected(Arc::clone(self)))
    }
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
    let Some(primary) = kernel.replication.clone() else {
        return Err(Status::failed_precondition(
            "this kernel runs without --replication sync: it keeps no standby",
        ));
    };
    let Some(follow_request::Msg::Hello(hello)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected FollowHello"));
    };
    protocol(hello.protocol)?;
    kernel.present_standby(metadata, &hello.key_id)?;
    if !config::allowed(&kernel.permissions().replicate, &hello.key_id, "*") {
        return Err(Status::permission_denied(
            "the key may not follow this kernel: its grant needs replicate",
        ));
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
    let _connected = primary.connect(&hello.key_id)?;

    // Where the standby stands against this log.
    let head = kernel.store.record_head().map_err(internal)?;
    let (mode, message) = match kernel.store.record_hash_at(hello.seq).map_err(internal)? {
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
    tracing::info!(standby = %hello.key_id, from = hello.seq, mode = ?mode, "a standby follows");
    send(
        out,
        follow_response::Msg::Start(FollowStart {
            mode: mode.into(),
            message,
        }),
    )
    .await?;
    let mut sent = match mode {
        FollowMode::Follow => {
            primary.ack(hello.seq, head.seq);
            hello.seq
        }
        FollowMode::Image => send_image(&kernel, out).await?,
        _ => return Ok(()),
    };

    let mut tick = tokio::time::interval(TICK);
    let mut pruned = 0;
    loop {
        if sent < primary.acked_seq() + AHEAD {
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
                let (vault, files) = attachments(&kernel, inner.into_iter())?;
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
                continue;
            }
        }
        tokio::select! {
            message = next(inbound) => match message? {
                Some(follow_request::Msg::Ack(FollowAck { seq })) => {
                    if seq > sent {
                        return Err(Status::invalid_argument(format!(
                            "acknowledged record {seq}, but only {sent} were sent"
                        )));
                    }
                    let head = kernel.store.record_head().map_err(internal)?.seq;
                    primary.ack(seq, head);
                    // SQLite keeps what a reconnecting standby may need; the file log keeps all.
                    if seq >= pruned + RETAIN / 10 && seq > RETAIN {
                        pruned = seq;
                        let kernel = Arc::clone(&kernel);
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = kernel.store.prune_records(seq - RETAIN) {
                                tracing::error!(error = %e, "pruning the record log failed");
                            }
                        });
                    }
                }
                None => return Ok(()),
                Some(_) => return Err(Status::invalid_argument("unexpected message")),
            },
            () = primary.wrote.notified() => {}
            _ = tick.tick() => {}
        }
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
/// deleted meanwhile travels as absent, and the records after converge the standby.
fn attachments<'a>(
    kernel: &Kernel,
    records: impl Iterator<Item = &'a Record>,
) -> Result<(Vec<VaultRow>, Vec<DataFile>), Status> {
    let mut refs = Vec::new();
    let mut files = Vec::new();
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
            Record::DataChanged(d) => {
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
            _ => {}
        }
    }
    let vault = match &kernel.vault {
        Some(vault) if !refs.is_empty() => vault.export(&refs).map_err(internal)?,
        _ => Vec::new(),
    };
    Ok((vault, files))
}

/// Sends a full image: the store's records, with the vault rows they refer to, then every file
/// of the data store, then where the image stands. Returns that record's sequence number.
async fn send_image(kernel: &Arc<Kernel>, out: &Outbox) -> Result<u64, Status> {
    let (tx, mut rx) = mpsc::channel::<ImageBatch>(4);
    let k = Arc::clone(kernel);
    let reading = tokio::task::spawn_blocking(move || -> Result<Head, Status> {
        let left = || StoreError::Io(std::io::Error::other("the standby left"));
        let flush = |batch: &mut Vec<Record>| -> Result<(), StoreError> {
            let (vault, files) = attachments(&k, batch.iter())
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
