//! The kernable kernel: accepts signed commands, leases them to authenticated workers,
//! verifies their signed results and commits them to the event store.
//!
//! Workers pull work: each session runs its own dispatch loop that leases commands of the
//! types the worker registered, up to its `max_concurrency`, from the queue in kernel memory
//! (one command per aggregate at a time, see [`queue`]).
//!
//! By default a submit responds once every registered listener has processed the command's
//! events (see [`listeners`]), so a query right after it sees the new state.

pub mod config;
mod listeners;
mod metrics;
mod queue;

use std::{
    collections::HashMap,
    os::unix::fs::FileTypeExt,
    sync::{Arc, Mutex, MutexGuard, PoisonError, atomic::Ordering::Relaxed},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Accepted, Challenge, Command, CommandResult, Committed, Event, Execute, KernelMessage,
        ListenerDelivery, ListenerMessage, Registered, Rejected, Signature, Snapshot,
        SnapshotRecord, SubmitRequest, SubscribeRequest, Wait, WorkerMessage,
        command_result::Outcome,
        command_service_server::{CommandService, CommandServiceServer},
        event_service_server::{EventService, EventServiceServer},
        kernel_message::Msg as Out,
        listener_service_server::{ListenerService, ListenerServiceServer},
        worker_gateway_server::{WorkerGateway, WorkerGatewayServer},
        worker_message::Msg as In,
    },
    signing, transport,
};
use kernable_store::{Enqueued, Error as StoreError, Store};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream, UnixListenerStream};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

pub use config::Config;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Re-executions after a version conflict before the command is rejected.
const MAX_CONFLICT_RETRIES: u32 = 5;
/// Rejection code of a worker that cannot read the snapshot it got.
const SNAPSHOT_STALE: &str = "snapshot_stale";
/// How often idle workers look for expired leases.
const POLL: Duration = Duration::from_millis(100);
/// Accepted clock difference for signed subscribe requests.
const MAX_REQUEST_AGE_MS: i64 = 60_000;
/// Events read from the store per subscription round trip.
const SUBSCRIBE_BATCH: u32 = 500;

pub struct Kernel {
    config: Config,
    store: Box<dyn Store>,
    key: SigningKey,
    /// Submit calls waiting for a command's result.
    waiters: Mutex<HashMap<String, Vec<oneshot::Sender<CommandResult>>>>,
    /// Wakes idle dispatch loops when new commands arrive.
    work: Notify,
    /// Wakes subscriptions and listeners when events were committed.
    appended: Notify,
    listeners: listeners::Registry,
    queue: queue::Queue,
    metrics: metrics::Metrics,
}

impl Kernel {
    pub fn open(config: Config) -> Result<Arc<Self>, BoxError> {
        let store = kernable_store::open(&config.store)?;
        let key = signing::read_signing_key(&config.kernel_key)?;
        let listeners = listeners::Registry::load(store.listeners()?);
        let queue = queue::Queue::default();
        for command in store.pending()? {
            queue.push(command);
        }
        Ok(Arc::new(Kernel {
            config,
            store,
            key,
            waiters: Mutex::default(),
            work: Notify::new(),
            appended: Notify::new(),
            listeners,
            queue,
            metrics: metrics::Metrics::default(),
        }))
    }

    fn waiters(&self) -> MutexGuard<'_, HashMap<String, Vec<oneshot::Sender<CommandResult>>>> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes a final checkpoint and the listener positions. Leases are not persisted:
    /// after a restart, every unfinished command is delivered again right away.
    pub fn shutdown(&self) -> Result<(), BoxError> {
        self.store.checkpoint(&self.key, now_ms())?;
        listeners::flush(self)?;
        Ok(())
    }

    fn complete(&self, result: CommandResult) {
        // The aggregate is free again: its next queued command can be leased now.
        self.work.notify_waiters();
        for waiter in self
            .waiters()
            .remove(&result.command_id)
            .unwrap_or_default()
        {
            let _ = waiter.send(result.clone());
        }
    }

    /// Waits until every registered listener has acknowledged the result's events.
    /// Returns whether they did within `settle_timeout_ms`.
    async fn settle(&self, result: &CommandResult) -> bool {
        let Some(Outcome::Committed(c)) = &result.outcome else {
            return true;
        };
        let Some(last) = c.events.last().map(|e| e.global_position) else {
            return true;
        };
        let settled = async {
            loop {
                let progressed = self.listeners.progress.notified();
                if self.listeners.settled(last) {
                    return;
                }
                progressed.await;
            }
        };
        let timeout = Duration::from_millis(self.config.settle_timeout_ms);
        tokio::time::timeout(timeout, settled).await.is_ok()
    }
}

/// Serves the gRPC API on `config.listen` and writes checkpoints until dropped.
pub async fn serve(kernel: Arc<Kernel>) -> Result<(), BoxError> {
    let api = Api(Arc::clone(&kernel));
    let mut builder = Server::builder();
    if let Some(tls) = &kernel.config.tls {
        if transport::unix_path(&kernel.config.listen).is_some() {
            return Err("TLS applies to TCP only; remove [tls] or listen on host:port".into());
        }
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let client_ca = Certificate::from_pem(std::fs::read(&tls.client_ca)?);
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )?;
    }
    let router = builder
        .add_service(CommandServiceServer::new(api.clone()))
        .add_service(EventServiceServer::new(api.clone()))
        .add_service(ListenerServiceServer::new(api.clone()))
        .add_service(WorkerGatewayServer::new(api));

    let server = async {
        match transport::unix_path(&kernel.config.listen) {
            Some(path) => {
                // Replace a stale socket from a previous run, but never any other file.
                if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                    std::fs::remove_file(path)?;
                }
                let incoming = UnixListenerStream::new(UnixListener::bind(path)?);
                router.serve_with_incoming(incoming).await?;
            }
            None => {
                let incoming =
                    TcpListenerStream::new(TcpListener::bind(&kernel.config.listen).await?);
                router.serve_with_incoming(incoming).await?;
            }
        }
        Ok(())
    };
    tokio::select! {
        result = server => result,
        _ = checkpoints(&kernel) => unreachable!(),
        _ = flush_listeners(&kernel) => unreachable!(),
        result = serve_metrics(&kernel) => result,
    }
}

async fn serve_metrics(kernel: &Kernel) -> Result<(), BoxError> {
    match &kernel.config.metrics_listen {
        Some(addr) => metrics::serve(kernel, addr).await,
        None => std::future::pending().await,
    }
}

async fn flush_listeners(kernel: &Kernel) {
    let mut tick = tokio::time::interval(listeners::FLUSH);
    loop {
        tick.tick().await;
        if let Err(e) = listeners::flush(kernel) {
            tracing::error!(error = %e, "saving listener positions failed");
        }
    }
}

async fn checkpoints(kernel: &Kernel) {
    let mut tick =
        tokio::time::interval(Duration::from_millis(kernel.config.checkpoint_interval_ms));
    loop {
        tick.tick().await;
        if let Err(e) = kernel.store.checkpoint(&kernel.key, now_ms()) {
            tracing::error!(error = %e, "checkpoint failed");
        }
    }
}

#[derive(Clone)]
struct Api(Arc<Kernel>);

#[tonic::async_trait]
impl CommandService for Api {
    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<CommandResult>, Status> {
        let kernel = &self.0;
        let request = request.into_inner();
        let wait = request.wait();
        let cmd = request
            .command
            .ok_or_else(|| Status::invalid_argument("no command"))?;
        let sig = cmd
            .client_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("command is not signed"))?;
        signing::verify(sig, &signing::command_message(&cmd))
            .map_err(|_| Status::unauthenticated("invalid command signature"))?;
        if !config::valid_domain(&cmd.domain) || !config::valid_type(&cmd.command_type) {
            return Err(Status::invalid_argument(
                "a command needs a lowercase domain and a command type without dots",
            ));
        }
        let name = config::qualified(&cmd.domain, &cmd.command_type);
        if !config::allowed(&kernel.config.clients, &sig.key_id, &name) {
            return Err(Status::permission_denied(format!(
                "key may not submit {name}"
            )));
        }
        let bypass = cmd.context.as_ref().is_some_and(|c| c.bypass_authorization);
        if bypass
            && !config::allowed(
                &kernel.config.privileged,
                &sig.key_id,
                "bypass_authorization",
            )
        {
            return Err(Status::permission_denied(
                "key may not set bypass_authorization",
            ));
        }

        // Register before enqueueing, so a fast completion cannot be missed.
        let (tx, rx) = oneshot::channel();
        kernel
            .waiters()
            .entry(cmd.command_id.clone())
            .or_default()
            .push(tx);
        let done = match kernel.store.enqueue(&cmd, now_ms()).map_err(internal)? {
            Enqueued::Duplicate {
                result: Some(result),
            } => {
                kernel.waiters().remove(&cmd.command_id);
                Some(result)
            }
            Enqueued::Duplicate { result: None } => None,
            Enqueued::New => {
                kernel.queue.push(cmd.clone());
                kernel.work.notify_waiters();
                None
            }
        };
        let mut result = match done {
            Some(result) => result,
            None if wait == Wait::Accepted => {
                kernel.waiters().remove(&cmd.command_id);
                return Ok(Response::new(CommandResult {
                    command_id: cmd.command_id,
                    outcome: Some(Outcome::Accepted(Accepted {})),
                }));
            }
            None => {
                let timeout = Duration::from_millis(kernel.config.submit_timeout_ms);
                match tokio::time::timeout(timeout, rx).await {
                    Ok(Ok(result)) => result,
                    _ => {
                        return Err(Status::deadline_exceeded(
                            "command accepted but not completed yet; resubmit it to get the result",
                        ));
                    }
                }
            }
        };
        if wait == Wait::Settled {
            let started = Instant::now();
            if kernel.settle(&result).await
                && let Some(Outcome::Committed(c)) = &mut result.outcome
            {
                c.settled = true;
                kernel.metrics.settle.record(started.elapsed());
            }
        }
        Ok(Response::new(result))
    }
}

#[tonic::async_trait]
impl EventService for Api {
    type SubscribeStream = ReceiverStream<Result<Event, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let kernel = Arc::clone(&self.0);
        let req = request.into_inner();
        let sig = req
            .client_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("request is not signed"))?;
        let msg = signing::subscribe_message(req.after_position, &req.event_types, req.issued_at);
        signing::verify(sig, &msg)
            .map_err(|_| Status::unauthenticated("invalid request signature"))?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }

        let types = config::readable(&kernel.config.subscribers, &sig.key_id, &req.event_types)
            .map_err(Status::permission_denied)?;

        let (tx, rx) = mpsc::channel(SUBSCRIBE_BATCH as usize);
        tokio::spawn(stream_events(kernel, req.after_position, types, tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Sends stored events after `after` (backlog first, then live) until the subscriber leaves.
/// Always reads from the store, so a subscriber can never miss or reorder events.
async fn stream_events(
    kernel: Arc<Kernel>,
    mut after: u64,
    types: Option<Vec<String>>,
    tx: mpsc::Sender<Result<Event, Status>>,
) {
    loop {
        let appended = kernel.appended.notified();
        let batch = match kernel.store.read_all(after, SUBSCRIBE_BATCH) {
            Ok(batch) => batch,
            Err(e) => {
                let _ = tx.send(Err(internal(e))).await;
                return;
            }
        };
        if batch.is_empty() {
            tokio::select! {
                _ = appended => {}
                _ = tx.closed() => return,
            }
            continue;
        }
        for event in batch {
            after = event.global_position;
            let wanted = wanted(types.as_deref(), &event);
            if wanted && tx.send(Ok(event)).await.is_err() {
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl ListenerService for Api {
    type ListenStream = ReceiverStream<Result<ListenerDelivery, Status>>;

    async fn listen(
        &self,
        request: Request<Streaming<ListenerMessage>>,
    ) -> Result<Response<Self::ListenStream>, Status> {
        let (out, rx) = mpsc::channel(4);
        let kernel = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = listeners::run(kernel, &mut inbound, &out).await {
                let _ = out.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl WorkerGateway for Api {
    type SessionStream = ReceiverStream<Result<KernelMessage, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let (out, rx) = mpsc::channel(64);
        let kernel = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = run_session(kernel, &mut inbound, &out).await {
                let _ = out.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

type Outbox = mpsc::Sender<Result<KernelMessage, Status>>;

/// A command currently executed by the session's worker.
struct InFlight {
    command: Command,
    leased_at: Instant,
    expected_version: u64,
    /// The command was sent with a snapshot.
    with_snapshot: bool,
    attempts: u32,
    permit: OwnedSemaphorePermit,
}

struct Session {
    kernel: Arc<Kernel>,
    key_id: String,
    out: Outbox,
    in_flight: Mutex<HashMap<String, InFlight>>,
}

/// Hands the session's in-flight commands back to the queue when it ends, however it ends,
/// so another worker gets them without waiting for the leases to expire.
struct ReleaseOnDrop(Arc<Session>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let leases: Vec<String> = self
            .0
            .in_flight()
            .drain()
            .map(|(lease_id, _)| lease_id)
            .collect();
        for lease_id in leases {
            self.0.kernel.queue.release(&lease_id);
        }
        self.0.kernel.work.notify_waiters();
    }
}

/// Counts a connected worker session and logs it, until dropped.
struct SessionCount(Arc<Kernel>, String);

impl SessionCount {
    fn new(kernel: &Arc<Kernel>, worker_id: &str) -> Self {
        kernel.metrics.worker_sessions.fetch_add(1, Relaxed);
        tracing::info!(worker = worker_id, "worker connected");
        SessionCount(Arc::clone(kernel), worker_id.to_owned())
    }
}

impl Drop for SessionCount {
    fn drop(&mut self) {
        self.0.metrics.worker_sessions.fetch_sub(1, Relaxed);
        tracing::info!(worker = %self.1, "worker disconnected");
    }
}

/// Aborts a session's background task when the session ends, however it ends.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn next(inbound: &mut Streaming<WorkerMessage>) -> Result<Option<In>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

async fn send(out: &Outbox, msg: Out) -> Result<(), Status> {
    out.send(Ok(KernelMessage { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("worker disconnected"))
}

async fn run_session(
    kernel: Arc<Kernel>,
    inbound: &mut Streaming<WorkerMessage>,
    out: &Outbox,
) -> Result<(), Status> {
    // Handshake: Register -> Challenge -> ChallengeResponse -> Registered.
    let Some(In::Register(reg)) = next(inbound).await? else {
        return Err(Status::invalid_argument("expected Register"));
    };
    if !reg.command_types.iter().all(|t| config::valid_qualified(t)) {
        return Err(Status::invalid_argument(
            "register qualified command types: domain.CommandType",
        ));
    }
    let authorized = !reg.command_types.is_empty()
        && reg
            .command_types
            .iter()
            .all(|t| config::allowed(&kernel.config.workers, &reg.key_id, t));
    if !authorized {
        return Err(Status::permission_denied(
            "worker key may not handle these command types",
        ));
    }

    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    send(
        out,
        Out::Challenge(Challenge {
            nonce: nonce.to_vec(),
        }),
    )
    .await?;
    let Some(In::ChallengeResponse(resp)) = next(inbound).await? else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: reg.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;
    send(
        out,
        Out::Registered(Registered {
            session_id: reg.worker_id.clone(),
        }),
    )
    .await?;

    let _counted = SessionCount::new(&kernel, &reg.worker_id);
    let session = Arc::new(Session {
        kernel,
        key_id: reg.key_id,
        out: out.clone(),
        in_flight: Mutex::default(),
    });
    let permits = Arc::new(Semaphore::new(reg.max_concurrency.max(1) as usize));
    // Declared first, dropped last: stop dispatching, then release what is in flight.
    let _release = ReleaseOnDrop(Arc::clone(&session));
    let _dispatch = AbortOnDrop(tokio::spawn(dispatch(
        Arc::clone(&session),
        reg.command_types,
        permits,
    )));

    loop {
        match next(inbound).await? {
            None => return Ok(()),
            Some(In::Result(r)) => session.on_result(r).await?,
            Some(In::Rejection(r)) => session.on_rejection(r).await?,
            Some(_) => return Err(Status::invalid_argument("unexpected message")),
        }
    }
}

async fn dispatch(session: Arc<Session>, command_types: Vec<String>, permits: Arc<Semaphore>) {
    let types: Vec<&str> = command_types.iter().map(String::as_str).collect();
    let kernel = &session.kernel;
    let ttl = Duration::from_millis(kernel.config.lease_ttl_ms.max(0) as u64);
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (lease_id, command) = loop {
            let notified = kernel.work.notified();
            if let Some(lease) = kernel.queue.lease(&types, ttl) {
                break lease;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(POLL) => {}
            }
        };
        if !session.execute(lease_id, command, 0, permit).await {
            return;
        }
    }
}

impl Session {
    fn in_flight(&self) -> MutexGuard<'_, HashMap<String, InFlight>> {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends the command with the latest snapshot and the events after it.
    /// Returns false once the worker is gone.
    async fn execute(
        &self,
        lease_id: String,
        command: Command,
        attempts: u32,
        permit: OwnedSemaphorePermit,
    ) -> bool {
        let (tenant, domain, aggregate_type, id) = (
            &command.tenant_id,
            &command.domain,
            &command.aggregate_type,
            &command.aggregate_id,
        );
        let store = &self.kernel.store;
        let loaded = store
            .snapshot(tenant, domain, aggregate_type, id)
            .and_then(|record| {
                let snapshot = record.and_then(|r| r.snapshot);
                let after = snapshot.as_ref().map_or(0, |s| s.version);
                let history = store.read_stream_after(tenant, domain, aggregate_type, id, after)?;
                Ok((snapshot, history))
            });
        let (snapshot, history) = match loaded {
            Ok(loaded) => loaded,
            Err(e) => {
                tracing::error!(error = %e, "reading the stream failed");
                self.kernel.queue.release(&lease_id);
                self.kernel.work.notify_waiters();
                return true;
            }
        };
        let expected_version = history
            .last()
            .map(|e| e.version)
            .or(snapshot.as_ref().map(|s| s.version))
            .unwrap_or(0);
        let required = command.context.as_ref().and_then(|c| c.expected_version);
        if let Some(required) = required.filter(|v| *v != expected_version) {
            // The client based the command on a state that has changed since.
            let reason = Rejected {
                code: "version_mismatch".into(),
                message: format!(
                    "expected version {required}, the aggregate is at {expected_version}"
                ),
            };
            drop(permit);
            self.reject(&lease_id, command.command_id, reason);
            return true;
        }
        self.in_flight().insert(
            lease_id.clone(),
            InFlight {
                command: command.clone(),
                leased_at: Instant::now(),
                expected_version,
                with_snapshot: snapshot.is_some(),
                attempts,
                permit,
            },
        );
        let execute = Execute {
            lease_id,
            command: Some(command),
            history,
            expected_version,
            snapshot,
        };
        send(&self.out, Out::Execute(execute)).await.is_ok()
    }

    async fn on_result(&self, r: kernable_core::proto::Result) -> Result<(), Status> {
        let Some(f) = self.in_flight().remove(&r.lease_id) else {
            return Ok(()); // unknown or already handled lease
        };
        let sig = r.worker_sig.unwrap_or_default();
        let msg = signing::result_message(&f.command, f.expected_version, &r.events);
        let snapshot_ok = r
            .snapshot
            .as_ref()
            .is_none_or(|s| self.valid_snapshot(s, &f, r.events.len()));
        if sig.key_id != self.key_id || signing::verify(&sig, &msg).is_err() || !snapshot_ok {
            // Put it back, so it is released when the session ends.
            self.in_flight().insert(r.lease_id, f);
            return Err(Status::unauthenticated(
                "invalid result or snapshot signature",
            ));
        }
        if !self.kernel.queue.holds(&r.lease_id) {
            return Ok(()); // the lease expired; the command was handed to another worker
        }
        if !r.events.iter().all(|e| config::valid_type(&e.event_type)) {
            // Events always belong to the command's domain: a qualified or empty type is a bug.
            let reason = Rejected {
                code: "invalid_event_type".into(),
                message: "event types must be non-empty and without dots".into(),
            };
            self.reject(&r.lease_id, f.command.command_id, reason);
            return Ok(());
        }

        let command_id = &f.command.command_id;
        match self
            .kernel
            .store
            .commit(command_id, f.expected_version, &r.events, &sig, now_ms())
        {
            Ok(events) => {
                if let (Some(snapshot), Some(last)) = (r.snapshot, events.last()) {
                    let record = SnapshotRecord {
                        tenant_id: last.tenant_id.clone(),
                        domain: last.domain.clone(),
                        aggregate_type: last.aggregate_type.clone(),
                        aggregate_id: last.aggregate_id.clone(),
                        snapshot: Some(snapshot),
                        event_hash: last.hash.clone(),
                        removed: false,
                    };
                    // A snapshot is only a cache: losing one costs time, not data.
                    if let Err(e) = self.kernel.store.save_snapshot(&record) {
                        tracing::warn!(error = %e, "saving snapshot failed");
                    }
                }
                self.kernel.queue.done(&r.lease_id);
                self.kernel.metrics.committed.fetch_add(1, Relaxed);
                self.kernel.metrics.execution.record(f.leased_at.elapsed());
                self.kernel.appended.notify_waiters();
                self.kernel.complete(CommandResult {
                    command_id: f.command.command_id,
                    outcome: Some(Outcome::Committed(Committed {
                        events,
                        settled: false,
                    })),
                });
            }
            Err(StoreError::VersionConflict { .. }) if f.attempts < MAX_CONFLICT_RETRIES => {
                self.execute(r.lease_id, f.command, f.attempts + 1, f.permit)
                    .await;
            }
            Err(StoreError::VersionConflict { .. }) => {
                let reason = Rejected {
                    code: "concurrency_conflict".into(),
                    message: "aggregate kept changing; retry the command".into(),
                };
                self.reject(&r.lease_id, f.command.command_id, reason);
            }
            Err(StoreError::NotPending) => self.done(&r.lease_id),
            Err(e) => {
                tracing::error!(error = %e, "commit failed");
                self.release(&r.lease_id);
            }
        }
        Ok(())
    }

    async fn on_rejection(&self, r: kernable_core::proto::Rejection) -> Result<(), Status> {
        let Some(f) = self.in_flight().remove(&r.lease_id) else {
            return Ok(());
        };
        let reason = r.reason.unwrap_or_default();
        let sig = r.worker_sig.unwrap_or_default();
        let msg = signing::rejection_message(&f.command.command_id, &reason);
        if sig.key_id != self.key_id || signing::verify(&sig, &msg).is_err() {
            self.in_flight().insert(r.lease_id, f);
            return Err(Status::unauthenticated("invalid rejection signature"));
        }
        if reason.code == SNAPSHOT_STALE && f.with_snapshot {
            // The worker cannot read the snapshot: drop it and send all events instead.
            let c = &f.command;
            let removed = SnapshotRecord {
                tenant_id: c.tenant_id.clone(),
                domain: c.domain.clone(),
                aggregate_type: c.aggregate_type.clone(),
                aggregate_id: c.aggregate_id.clone(),
                removed: true,
                ..Default::default()
            };
            if let Err(e) = self.kernel.store.save_snapshot(&removed) {
                tracing::error!(error = %e, "removing snapshot failed");
                self.release(&r.lease_id);
                return Ok(());
            }
            self.execute(r.lease_id, f.command, f.attempts + 1, f.permit)
                .await;
            return Ok(());
        }
        self.reject(&r.lease_id, f.command.command_id, reason);
        Ok(())
    }

    /// A snapshot must describe the state right after the result's events and be signed
    /// by this session's worker.
    fn valid_snapshot(&self, s: &Snapshot, f: &InFlight, event_count: usize) -> bool {
        let msg = signing::snapshot_message(&f.command, s.version, &s.schema, &s.state);
        event_count > 0
            && s.version == f.expected_version + event_count as u64
            && s.worker_sig
                .as_ref()
                .is_some_and(|sig| sig.key_id == self.key_id && signing::verify(sig, &msg).is_ok())
    }

    fn reject(&self, lease_id: &str, command_id: String, reason: Rejected) {
        if !self.kernel.queue.holds(lease_id) {
            return;
        }
        match self.kernel.store.reject(&command_id, reason.clone()) {
            Ok(()) => {
                self.kernel.queue.done(lease_id);
                self.kernel.metrics.rejected.fetch_add(1, Relaxed);
                self.kernel.complete(CommandResult {
                    command_id,
                    outcome: Some(Outcome::Rejected(reason)),
                });
            }
            Err(StoreError::NotPending) => self.done(lease_id),
            Err(e) => {
                tracing::error!(error = %e, "reject failed");
                self.release(lease_id);
            }
        }
    }

    /// The command was completed under an earlier lease: drop it from the queue.
    fn done(&self, lease_id: &str) {
        self.kernel.queue.done(lease_id);
        self.kernel.work.notify_waiters();
    }

    /// Hands the command back for another attempt.
    fn release(&self, lease_id: &str) {
        self.kernel.queue.release(lease_id);
        self.kernel.work.notify_waiters();
    }
}

/// Whether `event` matches the type patterns of a subscription (`None`: all events).
fn wanted(types: Option<&[String]>, event: &Event) -> bool {
    types.is_none_or(|patterns| {
        let name = config::qualified(&event.domain, &event.event_type);
        patterns.iter().any(|p| config::covers(p, &name))
    })
}

fn internal(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}
