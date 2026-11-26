//! A reader: a read-only copy of the writer (design/replication.md, "Readers"; `--reader`).
//!
//! It catches up as a standby does first (`standby.rs`, from a backup if it is new and one is
//! given), then opens its stores as a kernel with a sealed record log, so it writes nothing of
//! its own, and follows on ([`follow_writer`]): every record of the writer, checked and applied
//! as on a standby, only those the writer may tell (with a standby, what the standby holds).
//! Nothing on the writer waits for it, and it is never promoted.
//!
//! What it serves from its own copy: queries, subscriptions and listeners (read models,
//! middlewares and live listeners connected to it), with the grants, revocations, memberships
//! and vault values it got from the writer. What it forwards to the writer: every command
//! ([`submit`], answered once its own copy holds the command's events, so a query on this
//! reader after it sees them) and every other write, as is ([`Forward`]). What waits on it:
//! reactors (`Waiting.reader_of`) and workers (`Registered.reader_of`); timers, scheduled
//! commands and business processes run on the writer only.
//!
//! It follows whichever of its addresses is the primary: a standby answers `not_primary` and it
//! turns to the next. If its copy no longer continues the writer's log (the writer pruned what
//! it needs, or its log differs), it stops with an error; started again, it catches up anew.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Challenge, CommandResult, FollowAck, FollowHello, FollowMode, FollowRequest, Registered,
        Signature, SubmitRequest, Wait, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, follow_request, follow_response,
        kernel_message::Msg as Out, log_record::Record, worker_message::Msg as In,
    },
    signing,
};
use tonic::{
    Request, Status, Streaming,
    codegen::{Bytes, StdError, http},
    metadata::MetadataMap,
    transport::Channel,
};

use crate::{BoxError, GRANT_HEADER, Kernel, config, listeners, now_ms, standby};

/// How long a reader may go without word from the writer (records or the beat it sends every
/// second) before it counts as away: `/ready` answers 503.
pub(crate) const STALE: Duration = Duration::from_secs(5);

/// The longest pause between tries to reach the writer.
const MAX_DELAY: Duration = Duration::from_secs(5);

/// The addresses of `--reader`, comma-separated.
pub(crate) fn addresses(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The reader, as its kernel holds it.
pub(crate) struct Reader {
    addresses: Vec<String>,
    /// The address it follows now (the primary, as far as it knows).
    current: Mutex<String>,
    key: SigningKey,
    grant: String,
    /// The writer's kernel keys: its checkpoints are signed with one of them.
    kernel_keys: Vec<String>,
    /// `--reader-copy`: the addresses are relays, and it follows this copy there.
    copy: String,
    /// `--reader-copy-key`: what it gets from a relay is opened with it.
    copy_key: Option<Arc<crate::seal::CopyKey>>,
    /// The newest record applied.
    applied: AtomicU64,
    /// How far the writer said it may follow, last.
    head: AtomicU64,
    /// When it last held everything the writer may tell (ms since epoch).
    current_at: AtomicI64,
    /// `--reader-max-lag-ms`; 0: no limit.
    max_lag_ms: u64,
    /// Its stream to the writer is open.
    connected: AtomicBool,
    /// When the writer last sent anything (ms since epoch).
    heard_at: AtomicI64,
    /// A channel to the writer for what it forwards, per address.
    channels: Mutex<HashMap<String, Channel>>,
    /// Reactors connected here, waiting (they act on the writer only), by name: how many
    /// connections. They count as connected for `/ready`.
    reactors: Mutex<HashMap<String, usize>>,
    /// Why the writer (or the relay) refuses it, while it does.
    refused: Mutex<Option<String>>,
}

impl Reader {
    /// The reader of `config` (`--reader`), its stores at record `applied`.
    pub(crate) fn open(config: &config::Config, applied: u64) -> Result<Self, BoxError> {
        let follow = config
            .replication
            .follow
            .as_ref()
            .ok_or("a reader needs --reader <writer address>")?;
        let addresses = addresses(&follow.primary);
        let first = addresses
            .first()
            .cloned()
            .ok_or("--reader needs the writer's address")?;
        let key = config
            .read_signing_key(&follow.key)
            .map_err(|e| format!("--follow-key {}: {e}", follow.key.display()))?;
        let grant_path = follow.key.with_extension("grant");
        let grant = std::fs::read_to_string(&grant_path)
            .map_err(|e| format!("the reader's grant {}: {e}", grant_path.display()))?
            .trim()
            .to_owned();
        Ok(Reader {
            addresses,
            current: Mutex::new(first),
            key,
            grant,
            kernel_keys: follow.primary_kernel_keys.clone(),
            copy: follow.copy.clone(),
            copy_key: standby::copy_key(follow, true)?,
            applied: AtomicU64::new(applied),
            head: AtomicU64::new(applied),
            // It just caught up (`kernable start` catches up first).
            current_at: AtomicI64::new(now_ms()),
            max_lag_ms: config.replication.reader_max_lag_ms,
            connected: AtomicBool::new(false),
            heard_at: AtomicI64::new(0),
            channels: Mutex::default(),
            reactors: Mutex::default(),
            refused: Mutex::default(),
        })
    }

    /// The writer's address it follows now.
    pub(crate) fn writer(&self) -> String {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn next_address(&self) {
        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let at = self
            .addresses
            .iter()
            .position(|a| *a == *current)
            .unwrap_or(0);
        *current = self.addresses[(at + 1) % self.addresses.len()].clone();
    }

    /// The newest record it holds.
    pub(crate) fn applied(&self) -> u64 {
        self.applied.load(SeqCst)
    }

    /// Records it is behind what the writer may tell, as last heard.
    pub(crate) fn lag(&self) -> u64 {
        self.head.load(SeqCst).saturating_sub(self.applied())
    }

    /// How far the writer said it may follow, last.
    pub(crate) fn head(&self) -> u64 {
        self.head.load(SeqCst)
    }

    /// Milliseconds since the writer last sent anything; `None` if it never did.
    pub(crate) fn silent_ms(&self) -> Option<i64> {
        match self.heard_at.load(SeqCst) {
            0 => None,
            at => Some((now_ms() - at).max(0)),
        }
    }

    /// Whether it follows the writer now: connected, and heard from it within [`STALE`].
    pub(crate) fn following(&self) -> bool {
        self.connected.load(SeqCst)
            && self
                .silent_ms()
                .is_some_and(|ms| ms < STALE.as_millis() as i64)
    }

    fn heard(&self) {
        self.heard_at.store(now_ms(), SeqCst);
    }

    /// Notes that it holds everything the writer may tell, if it does.
    fn current_if_caught_up(&self) {
        if self.applied() >= self.head() {
            self.current_at.store(now_ms(), SeqCst);
        }
    }

    /// Milliseconds since it last held everything the writer may tell, when that is longer
    /// than `--reader-max-lag-ms`.
    pub(crate) fn too_far_behind(&self) -> Option<i64> {
        let behind = now_ms() - self.current_at.load(SeqCst);
        (self.max_lag_ms > 0 && behind > self.max_lag_ms as i64).then_some(behind)
    }

    /// Why it is not ready, if it is not: it lost the writer, or fell too far behind.
    pub(crate) fn unready(&self) -> Option<String> {
        if self.following() {
            return self.too_far_behind().map(|ms| {
                format!(
                    "a reader of {}: behind its writer for {} s (more than --reader-max-lag-ms), {} records",
                    self.writer(),
                    ms / 1000,
                    self.lag()
                )
            });
        }
        if let Some(why) = self
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Some(format!("a reader of {}: refused: {why}", self.writer()));
        }
        Some(match self.silent_ms() {
            Some(ms) => format!(
                "a reader of {}: no word from the writer for {} s ({} records behind as last heard)",
                self.writer(),
                ms / 1000,
                self.lag()
            ),
            None => format!(
                "a reader of {}: not following the writer yet",
                self.writer()
            ),
        })
    }

    /// Whether a reactor named `name` (its base name, without a deployment) waits here.
    pub(crate) fn reactor_waits(&self, name: &str) -> bool {
        self.reactors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .is_some_and(|n| *n > 0)
    }

    fn reactor_count(&self, name: &str, connected: bool) {
        let mut reactors = self.reactors.lock().unwrap_or_else(PoisonError::into_inner);
        let n = reactors.entry(name.to_owned()).or_default();
        if connected {
            *n += 1;
        } else {
            *n = n.saturating_sub(1);
            if *n == 0 {
                reactors.remove(name);
            }
        }
    }

    /// A channel to the writer it follows now, for what it forwards.
    async fn channel(&self) -> Result<(String, Route), Status> {
        let (writer, channel) = self.connect().await?;
        let route = match &self.copy_key {
            None => Route::Direct(channel),
            // Through the relay, sealed, to the copy's pusher, which makes the call on the writer.
            Some(key) => Route::Relay(Arc::new(RelayRoute {
                channel,
                copy: self.copy.clone(),
                key: Arc::clone(key),
                signer: self.key.clone(),
                grant: self.grant.clone(),
            })),
        };
        Ok((writer, route))
    }

    async fn connect(&self) -> Result<(String, Channel), Status> {
        let writer = self.writer();
        let cached = self
            .channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&writer)
            .cloned();
        if let Some(channel) = cached {
            return Ok((writer, channel));
        }
        let channel = standby::connect(&writer)
            .await
            .map_err(|e| away(&writer, &*e))?;
        self.channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(writer.clone(), channel.clone());
        Ok((writer, channel))
    }

    /// Drops the channel to `writer`: the next request connects anew.
    fn forget(&self, writer: &str) {
        self.channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(writer);
    }
}

/// UNAVAILABLE: the writer cannot be reached; the SDKs try again (another address, if they have
/// one). Commands are idempotent by ID, so a resubmit is safe.
fn away(writer: &str, e: &(dyn std::error::Error + 'static)) -> Status {
    Status::unavailable(format!(
        "writer_away: this reader cannot reach its writer {writer} ({e}); try again"
    ))
}

/// What this kernel is and how it stands, for `GET /api/role` on the local socket.
pub(crate) fn role(kernel: &Kernel) -> serde_json::Value {
    let position = kernel.durable.load(SeqCst);
    match &kernel.reader {
        Some(r) => serde_json::json!({
            "role": "reader",
            "state": if r.following() { "following" } else { "away" },
            "writer": r.writer(),
            "applied": r.applied(),
            "head": r.head(),
            "lag": r.lag(),
            "position": position,
            "silent_ms": r.silent_ms(),
        }),
        None => {
            let head = kernel.store.record_head().map_or(0, |h| h.seq);
            let readers: Vec<serde_json::Value> = kernel
                .readers
                .connected()
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "key": r.key,
                        "acknowledged": r.acked,
                        "lag": head.saturating_sub(r.acked),
                        "since": r.since,
                    })
                })
                .collect();
            serde_json::json!({
                "role": "writer",
                "standby": kernel.replication.as_ref().map_or("none", |p| p.state_name()),
                "head": head,
                "position": position,
                "readers": readers,
                // Listeners stuck at the cap of parked events (they get no further).
                "parked_cap": kernel.listeners.capped(),
            })
        }
    }
}

/// Why a stream to the writer ended.
enum Ended {
    /// Connect again (to the next address).
    Retry(BoxError),
    /// The copy no longer continues the writer's log: the kernel stops, and catches up anew
    /// when started again.
    Restart(String),
}

impl<E: Into<BoxError>> From<E> for Ended {
    fn from(e: E) -> Self {
        Ended::Retry(e.into())
    }
}

/// Follows the writer for as long as the kernel runs; returns only with why the reader must
/// stop (and be started again to catch up). Pending on a kernel that is no reader.
pub(crate) async fn follow_writer(kernel: &Arc<Kernel>) -> BoxError {
    let Some(reader) = kernel.reader.clone() else {
        return std::future::pending().await;
    };
    let mut delay = Duration::from_millis(100);
    let mut last_refusal = None;
    loop {
        let mut max = MAX_DELAY;
        match follow_once(kernel, &reader).await {
            Ok(()) => {
                tracing::warn!(writer = %reader.writer(), "the writer ended the stream; connecting again");
                delay = Duration::from_millis(100);
            }
            Err(Ended::Restart(why)) => {
                reader.connected.store(false, SeqCst);
                return format!(
                    "{why}: this reader's copy no longer continues its writer's log; start it again to catch up anew"
                )
                .into();
            }
            Err(Ended::Retry(e)) => {
                let why = standby::refusal(&*e);
                *reader
                    .refused
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = why.clone();
                match why {
                    Some(why) => {
                        standby::refused_once(&mut last_refusal, why, &reader.writer());
                        max = standby::REFUSED_MAX_DELAY;
                    }
                    None => {
                        last_refusal = None;
                        tracing::warn!(writer = %reader.writer(), error = %e, "following the writer failed; trying the next address in {delay:?}");
                    }
                }
                reader.next_address();
            }
        }
        reader.connected.store(false, SeqCst);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(max);
    }
}

async fn follow_once(kernel: &Arc<Kernel>, reader: &Arc<Reader>) -> Result<(), Ended> {
    let head = kernel.store.record_head()?;
    let hello = FollowHello {
        key_id: signing::key_id(&reader.key.verifying_key()),
        seq: head.seq,
        hash: head.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        backup: false,
        rewind_points: Vec::new(),
        witness_generation: None,
        reader: true,
        copy: reader.copy.clone(),
    };
    let writer = reader.writer();
    let standby::Opened {
        tx,
        mut inbound,
        start,
    } = standby::open_stream(
        &writer,
        &reader.key,
        &reader.grant,
        hello,
        STALE / 3,
        reader.copy_key.clone(),
    )
    .await?;
    if start.mode() != FollowMode::Follow {
        return Err(Ended::Restart(format!(
            "the writer {writer} answered {:?} at record {} ({})",
            start.mode(),
            head.seq,
            start.message
        )));
    }
    reader.connected.store(true, SeqCst);
    *reader
        .refused
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
    reader.heard();
    tracing::info!(%writer, from = head.seq, "the reader follows the writer");
    loop {
        let message = inbound.message().await?.and_then(|m| m.msg);
        match message {
            Some(follow_response::Msg::Records(batch)) => {
                reader.heard();
                reader.head.store(batch.head, SeqCst);
                reader.current_if_caught_up();
                if batch.records.is_empty() {
                    continue;
                }
                crate::copy::refuse_marked(
                    batch
                        .records
                        .iter()
                        .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref())),
                    "the writer",
                )
                .map_err(|e| Ended::Restart(e.to_string()))?;
                // What changes the kernel's own view, besides the events.
                let changes: Vec<Record> = batch
                    .records
                    .iter()
                    .filter_map(|r| r.record.as_ref().and_then(|l| l.record.clone()))
                    .filter(|r| {
                        matches!(
                            r,
                            Record::Grant(_)
                                | Record::Revocation(_)
                                | Record::Membership(_)
                                | Record::Listener(_)
                                | Record::PartitionMap(_)
                                | Record::Process(_)
                        )
                    })
                    .collect();
                let (k, keys) = (Arc::clone(kernel), reader.kernel_keys.clone());
                let project = kernel.config().replication.project_keys.clone();
                let seq = tokio::task::spawn_blocking(move || {
                    let mut trust = standby::Trust::of(k.store.as_ref(), &keys, &project)?;
                    let parts = standby::Parts {
                        store: k.store.as_ref(),
                        vault: k.vault.as_ref(),
                        data: k.data.as_ref(),
                        snapshots: k.snapshots.as_ref(),
                    };
                    standby::apply_to(parts, batch, &mut trust)
                })
                .await?
                .map_err(|e| {
                    Ended::Restart(format!("applying the writer's records failed: {e}"))
                })?;
                reader.applied.store(seq, SeqCst);
                reader.current_if_caught_up();
                applied(kernel, changes);
                if tx
                    .send(FollowRequest {
                        msg: Some(follow_request::Msg::Ack(FollowAck { seq })),
                    })
                    .await
                    .is_err()
                {
                    return Err(Ended::Retry(
                        standby::ended("the writer", inbound.message()).await,
                    ));
                }
            }
            // A pair's witness generation, sent to its standby: nothing for a reader.
            Some(_) => {}
            None => return Ok(()),
        }
    }
}

/// After a batch was applied: the kernel takes what changed its view (grants, revocations,
/// memberships, middlewares, the partition map, processes), and its listeners and subscribers
/// get the new events.
fn applied(kernel: &Arc<Kernel>, changes: Vec<Record>) {
    let mut revoked = false;
    for record in changes {
        match record {
            Record::Grant(grant) => {
                let key = grant.key_id.clone();
                if let Err(e) = kernel
                    .check_grant(&grant)
                    .and_then(|()| kernel.keep(grant, false))
                {
                    tracing::warn!(%key, error = %e.message(), "a grant from the writer is not taken");
                }
            }
            Record::Revocation(r) => {
                tracing::warn!(key = %r.key_id, since = r.since, "key revoked (from the writer)");
                kernel
                    .revoked
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(r.key_id, r.since);
                revoked = true;
            }
            Record::Membership(m) => {
                if let Some(m) = m.membership {
                    crate::memberships::replicated(kernel, m);
                }
            }
            Record::Listener(l) => kernel.listeners.replicated(l, &kernel.memberships),
            Record::PartitionMap(map) => kernel.maps.set(map),
            Record::Process(p) => kernel.processes.replicated(p),
            _ => {}
        }
    }
    if revoked {
        kernel.update_permissions();
        kernel.changes.send_modify(|n| *n += 1);
    }
    if let Ok(head) = kernel.store.head_position() {
        kernel.durable.fetch_max(head, SeqCst);
    }
    kernel.appended.notify_waiters();
    kernel.listeners.progress.notify_waiters();
}

/// A command sent to a reader: forwarded to the writer as it came (signature, ID and vault
/// values unchanged, so a resubmit gets the stored outcome), with the grant it presents. Once
/// committed, answered only when this reader's copy holds the command's events, and, for a
/// settling submit, once the listeners connected here processed them too; `Committed.position`
/// says where the events are, for the client's next query.
pub(crate) async fn submit(
    kernel: &Arc<Kernel>,
    metadata: &MetadataMap,
    request: SubmitRequest,
) -> Result<CommandResult, Status> {
    let reader = kernel
        .reader
        .clone()
        .expect("a reader forwards its commands");
    let _running = kernel.request()?;
    let cmd = request
        .command
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("no command"))?;
    kernel.owns(&cmd.tenant_id)?;
    let sig = cmd
        .client_sig
        .as_ref()
        .ok_or_else(|| Status::unauthenticated("command is not signed"))?;
    signing::verify(sig, &signing::command_message(cmd))
        .map_err(|_| Status::unauthenticated("invalid command signature"))?;
    let _slot = kernel.request_slot(&sig.key_id)?;
    let name = config::qualified(&cmd.domain, &cmd.command_type);
    let caller = kernel.caller(&request.listener, &name, false)?;
    let wait = request.wait();
    // The writer does not know this reader's listeners: the one that asks waits only here.
    let forward = SubmitRequest {
        listener: String::new(),
        ..request
    };
    let config = kernel.config();
    let timeout = Duration::from_millis(config.submit_timeout_ms + config.settle_timeout_ms);
    let mut outgoing = Request::new(forward);
    outgoing.set_timeout(timeout + Duration::from_secs(5));
    if let Some(grant) = metadata.get(GRANT_HEADER) {
        outgoing.metadata_mut().insert(GRANT_HEADER, grant.clone());
    }
    let (writer, channel) = reader.channel().await?;
    let max = config.limits.message_bytes;
    let mut result = CommandServiceClient::new(channel)
        .max_decoding_message_size(max)
        .max_encoding_message_size(max)
        .submit(outgoing)
        .await
        .inspect_err(|status| {
            if status.code() == tonic::Code::Unavailable {
                reader.forget(&writer);
            }
        })?
        .into_inner();
    let Some(Outcome::Committed(c)) = &mut result.outcome else {
        return Ok(result);
    };
    let position = c.events.last().map_or(0, |e| e.global_position);
    c.position = position;
    until_held(
        kernel,
        &reader,
        position,
        Duration::from_millis(config.submit_timeout_ms),
    )
    .await?;
    if wait == Wait::Settled {
        let here = kernel.settle(&result, caller.as_ref()).await;
        if let Some(Outcome::Committed(c)) = &mut result.outcome {
            c.settled &= here;
        }
    }
    Ok(result)
}

/// Returns once this reader holds the event at `position`, or `reader_behind` after `timeout`.
async fn until_held(
    kernel: &Kernel,
    reader: &Reader,
    position: u64,
    timeout: Duration,
) -> Result<(), Status> {
    let held = async {
        loop {
            let appended = kernel.appended.notified();
            if kernel.durable.load(SeqCst) >= position {
                return;
            }
            // A beat a second at least: a notification missed in between is caught soon.
            let _ = tokio::time::timeout(Duration::from_millis(100), appended).await;
        }
    };
    tokio::time::timeout(timeout, held)
        .await
        .map_err(|_| behind(kernel, reader, position))
}

/// UNAVAILABLE `reader_behind` for a reader further behind its writer than
/// `--reader-max-lag-ms`: the SDKs ask another copy.
pub(crate) fn current_enough(reader: &Reader) -> Result<(), Status> {
    match reader.too_far_behind() {
        None => Ok(()),
        Some(ms) => Err(Status::unavailable(format!(
            "reader_behind: this reader has been behind its writer {} for {} s, more than its maximum lag ({} ms); {} records behind{}; ask another copy",
            reader.writer(),
            ms / 1000,
            reader.max_lag_ms,
            reader.lag(),
            if reader.following() {
                ""
            } else {
                ", and it cannot reach the writer"
            }
        ))),
    }
}

/// UNAVAILABLE `reader_behind`: this reader does not hold `position` yet; the SDKs try again,
/// with another address if they have one.
pub(crate) fn behind(kernel: &Kernel, reader: &Reader, position: u64) -> Status {
    Status::unavailable(format!(
        "reader_behind: this reader holds position {} and needs {position}; it is {} records behind its writer {}{}; try again or ask another copy",
        kernel.durable.load(SeqCst),
        reader.lag(),
        reader.writer(),
        if reader.following() {
            ""
        } else {
            ", which it cannot reach"
        }
    ))
}

/// A worker that connects to a reader: it registers (so the same code runs beside a reader),
/// is told `reader_of`, and gets no commands; they run on the writer's workers.
pub(crate) async fn idle_worker(
    kernel: Arc<Kernel>,
    metadata: &MetadataMap,
    inbound: &mut Streaming<WorkerMessage>,
    out: &crate::Outbox,
) -> Result<(), Status> {
    let reader = kernel.reader.clone().expect("only on a reader");
    let Some(In::Register(reg)) = kernel.handshake(crate::next(inbound)).await? else {
        return Err(Status::invalid_argument("expected Register"));
    };
    crate::protocol(reg.protocol)?;
    crate::workers::validate(&reg)?;
    kernel.present(metadata, &reg.key_id)?;
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    crate::send(
        out,
        Out::Challenge(Challenge {
            nonce: nonce.to_vec(),
        }),
    )
    .await?;
    let Some(In::ChallengeResponse(resp)) = kernel.handshake(crate::next(inbound)).await? else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: reg.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;
    tracing::info!(worker = %reg.worker_id, writer = %reader.writer(), "a worker connected to this reader: it gets no commands here, the writer's workers run them");
    crate::send(
        out,
        Out::Registered(Registered {
            session_id: reg.worker_id.clone(),
            reader_of: reader.writer(),
        }),
    )
    .await?;
    // Idle until it leaves.
    while crate::next(inbound).await?.is_some() {}
    Ok(())
}

/// A reactor that connects to a reader: told `reader_of`, it waits, connected, and gets
/// nothing; reactors act on the writer only.
pub(crate) async fn waiting_reactor(
    kernel: &Kernel,
    name: &str,
    out: &listeners::Outbox,
    inbound: &mut Streaming<kernable_core::proto::ListenerMessage>,
) -> Result<(), Status> {
    let writer = kernel
        .reader
        .as_ref()
        .map(|r| r.writer())
        .unwrap_or_default();
    tracing::info!(listener = %name, %writer, "a reactor connected to this reader: it waits here, the writer's reactors act");
    // Counted as connected for `/ready` while it waits, until it leaves.
    struct Waits<'a>(&'a Reader, String);
    impl Drop for Waits<'_> {
        fn drop(&mut self) {
            self.0.reactor_count(&self.1, false);
        }
    }
    let base = crate::deployments::base(name).to_owned();
    let _waits = kernel.reader.as_deref().map(|r| {
        r.reactor_count(&base, true);
        Waits(r, base)
    });
    out.send(Ok(kernable_core::proto::ListenerDelivery {
        msg: Some(kernable_core::proto::listener_delivery::Msg::Waiting(
            kernable_core::proto::Waiting {
                reader_of: writer,
                ..Default::default()
            },
        )),
    }))
    .await
    .map_err(|_| Status::cancelled("the listener left"))?;
    loop {
        tokio::select! {
            message = inbound.message() => if message?.is_none() { return Ok(()) },
            _ = out.closed() => return Ok(()),
        }
    }
}

/// The paths a reader forwards to the writer as they come: every write but a command (which
/// [`submit`] forwards), and what reads the writer's own state (files, the cache, partition
/// maps, exports and moves).
const FORWARDED: &[&str] = &[
    "/kernable.v1.CommandService/Cancel",
    "/kernable.v1.DataService/",
    "/kernable.v1.CacheService/",
    "/kernable.v1.TrustService/",
    "/kernable.v1.TrustSyncService/",
    "/kernable.v1.MembershipService/",
    "/kernable.v1.ProcessService/",
    "/kernable.v1.PartitionService/",
    "/kernable.v1.GlobalKeyService/",
    "/kernable.v1.MoveService/",
    "/kernable.v1.ExportService/",
];

/// Whether a reader forwards a request to `path` to the writer.
pub(crate) fn forwarded(path: &str) -> bool {
    FORWARDED.iter().any(|p| match p.strip_suffix('/') {
        Some(_) => path.starts_with(p),
        None => path == *p,
    })
}

/// A layer in front of a kernel's services: on a reader, requests to [`FORWARDED`] paths go to
/// the writer unchanged (headers, signature, grant and all); everything passes otherwise.
#[derive(Clone)]
pub(crate) struct Forward<S> {
    pub(crate) inner: S,
    pub(crate) reader: Option<Arc<Reader>>,
}

type Answer<R, E> = Pin<Box<dyn std::future::Future<Output = Result<R, E>> + Send>>;

impl<S, B> tower::Service<http::Request<B>> for Forward<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    B: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<StdError>,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Answer<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let reader = match &self.reader {
            Some(r) if forwarded(request.uri().path()) => Arc::clone(r),
            _ => return Box::pin(self.inner.call(request)),
        };
        Box::pin(async move {
            let (writer, channel) = match reader.channel().await {
                Ok(c) => c,
                Err(status) => return Ok(status.into_http()),
            };
            let (parts, body) = request.into_parts();
            let request = http::Request::from_parts(parts, tonic::body::Body::new(body));
            match tower::ServiceExt::oneshot(channel, request).await {
                Ok(response) => Ok(response),
                Err(e) => {
                    reader.forget(&writer);
                    Ok(away(&writer, &*e).into_http())
                }
            }
        })
    }
}

/// Where a reader's writes go: to the writer, or through a relay to the copy's pusher.
#[derive(Clone)]
pub(crate) enum Route {
    Direct(Channel),
    Relay(Arc<RelayRoute>),
}

/// A relay's address, the copy, and what seals and signs a call through it.
pub(crate) struct RelayRoute {
    channel: Channel,
    copy: String,
    key: Arc<crate::seal::CopyKey>,
    signer: SigningKey,
    grant: String,
}

impl RelayRoute {
    /// Calls made through the relay at `channel` on the writer of `copy`: sealed with its key,
    /// signed by `signer`, whose `grant` (hex) the relay checks as a reader's.
    pub(crate) fn new(
        channel: Channel,
        copy: &str,
        key: Arc<crate::seal::CopyKey>,
        signer: SigningKey,
        grant: String,
    ) -> Self {
        RelayRoute {
            channel,
            copy: copy.to_owned(),
            key,
            signer,
            grant,
        }
    }
}

/// The largest call a reader sends through a relay, and the largest answer.
const MAX_CALL_BYTES: usize = 64 << 20;

impl tower::Service<http::Request<tonic::body::Body>> for Route {
    type Response = http::Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Answer<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Route::Direct(channel) => {
                tower::Service::<http::Request<tonic::body::Body>>::poll_ready(channel, cx)
                    .map_err(Into::into)
            }
            Route::Relay(_) => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        match self {
            Route::Direct(channel) => {
                let answer = tower::Service::call(channel, request);
                Box::pin(async move { answer.await.map_err(Into::into) })
            }
            Route::Relay(relay) => Box::pin(Arc::clone(relay).call(request)),
        }
    }
}

impl RelayRoute {
    async fn call(
        self: Arc<Self>,
        request: http::Request<tonic::body::Body>,
    ) -> Result<http::Response<tonic::body::Body>, BoxError> {
        use http_body_util::BodyExt;
        let (parts, body) = request.into_parts();
        let body = match http_body_util::Limited::new(body, MAX_CALL_BYTES)
            .collect()
            .await
        {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Ok(Status::resource_exhausted(format!(
                    "the request is larger than a relay passes on ({} MiB): send it to the writer",
                    MAX_CALL_BYTES >> 20
                ))
                .into_http());
            }
        };
        let call = kernable_core::proto::ForwardedCall {
            path: parts.uri.path().to_owned(),
            headers: crate::push::headers(&parts.headers),
            body: body.to_vec(),
        };
        let mut id = [0u8; 8];
        getrandom::fill(&mut id).map_err(|_| "no randomness")?;
        let id = u64::from_le_bytes(id);
        let issued_at = now_ms();
        let sealed = crate::seal::seal_call(&self.key, &self.copy, id, issued_at, &call);
        let sig = signing::sign(
            &self.signer,
            &signing::relay_call_message(&self.copy, id, &sealed, issued_at),
        );
        let mut outgoing = Request::new(kernable_core::proto::SealedCall {
            copy: self.copy.clone(),
            id,
            sealed,
            issued_at,
            sig: Some(sig),
        });
        outgoing
            .metadata_mut()
            .insert(GRANT_HEADER, self.grant.parse()?);
        let reply = match kernable_core::proto::relay_service_client::RelayServiceClient::new(
            self.channel.clone(),
        )
        .max_decoding_message_size(MAX_CALL_BYTES + 4096)
        .max_encoding_message_size(MAX_CALL_BYTES + 4096)
        .call(outgoing)
        .await
        {
            Ok(reply) => reply.into_inner(),
            Err(status) => return Ok(status.into_http()),
        };
        let reply = crate::seal::open_reply(&self.key, &self.copy, id, &reply.sealed)
            .map_err(|e| format!("the relay's answer: {e}"))?;
        let mut response = http::Response::builder().status(u16::try_from(reply.status)?);
        for h in &reply.headers {
            response = response.header(h.name.as_str(), h.value.as_slice());
        }
        let mut trailers = http::HeaderMap::new();
        for h in &reply.trailers {
            trailers.append(
                http::HeaderName::from_bytes(h.name.as_bytes())?,
                http::HeaderValue::from_bytes(&h.value)?,
            );
        }
        let mut frames = vec![Ok::<_, std::convert::Infallible>(http_body::Frame::data(
            Bytes::from(reply.body),
        ))];
        if !trailers.is_empty() {
            frames.push(Ok(http_body::Frame::trailers(trailers)));
        }
        let body = http_body_util::StreamBody::new(tokio_stream::iter(frames));
        Ok(response.body(tonic::body::Body::new(body))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_takes_its_writers_as_a_list() {
        assert_eq!(
            addresses(" 10.0.0.1:7233, 10.0.0.2:7233,"),
            ["10.0.0.1:7233", "10.0.0.2:7233"]
        );
        assert!(addresses("").is_empty());
    }

    #[test]
    fn writes_go_to_the_writer_and_reads_stay() {
        for path in [
            "/kernable.v1.CommandService/Cancel",
            "/kernable.v1.DataService/PutData",
            "/kernable.v1.DataService/GetData",
            "/kernable.v1.CacheService/SetCache",
            "/kernable.v1.TrustService/Revoke",
            "/kernable.v1.MembershipService/Apply",
            "/kernable.v1.ProcessService/Register",
        ] {
            assert!(forwarded(path), "{path}");
        }
        for path in [
            // A command is forwarded by the service itself: it waits for its events here.
            "/kernable.v1.CommandService/Submit",
            "/kernable.v1.QueryService/Query",
            "/kernable.v1.EventService/Subscribe",
            "/kernable.v1.ListenerService/Listen",
            "/kernable.v1.WorkerGateway/Session",
            "/kernable.v1.ReplicationService/Follow",
            "/kernable.v1.CommandService/CancelAll",
        ] {
            assert!(!forwarded(path), "{path}");
        }
    }
}
