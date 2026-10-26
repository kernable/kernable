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
mod queue;

use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::FileTypeExt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Accepted, Challenge, Command, CommandResult, Committed, Event, Execute, KernelMessage,
        ListenerDelivery, ListenerMessage, Registered, Rejected, Signature, SubmitRequest,
        SubscribeRequest, Wait, WorkerMessage,
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
use tonic::{Request, Response, Status, Streaming, transport::Server};

pub use config::Config;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Re-executions after a version conflict before the command is rejected.
const MAX_CONFLICT_RETRIES: u32 = 5;
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
    let router = Server::builder()
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
    }
}

async fn flush_listeners(kernel: &Kernel) {
    let mut tick = tokio::time::interval(listeners::FLUSH);
    loop {
        tick.tick().await;
        if let Err(e) = listeners::flush(kernel) {
            eprintln!("saving listener positions failed: {e}");
        }
    }
}

async fn checkpoints(kernel: &Kernel) {
    let mut tick =
        tokio::time::interval(Duration::from_millis(kernel.config.checkpoint_interval_ms));
    loop {
        tick.tick().await;
        if let Err(e) = kernel.store.checkpoint(&kernel.key, now_ms()) {
            eprintln!("checkpoint failed: {e}");
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
        if !config::allowed(&kernel.config.clients, &sig.key_id, &cmd.command_type) {
            return Err(Status::permission_denied(format!(
                "key may not submit {}",
                cmd.command_type
            )));
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
        if wait == Wait::Settled
            && kernel.settle(&result).await
            && let Some(Outcome::Committed(c)) = &mut result.outcome
        {
            c.settled = true;
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

        let readable = kernel
            .config
            .subscribers
            .get(&sig.key_id)
            .ok_or_else(|| Status::permission_denied("key may not subscribe"))?;
        let types: Option<HashSet<String>> = if req.event_types.is_empty() {
            (!readable.iter().any(|t| t == "*")).then(|| readable.iter().cloned().collect())
        } else if req
            .event_types
            .iter()
            .all(|t| config::allowed(&kernel.config.subscribers, &sig.key_id, t))
        {
            Some(req.event_types.into_iter().collect())
        } else {
            return Err(Status::permission_denied(
                "key may not read these event types",
            ));
        };

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
    types: Option<HashSet<String>>,
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
            let wanted = types.as_ref().is_none_or(|t| t.contains(&event.event_type));
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
    expected_version: u64,
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

    /// Sends the command with its current history. Returns false once the worker is gone.
    async fn execute(
        &self,
        lease_id: String,
        command: Command,
        attempts: u32,
        permit: OwnedSemaphorePermit,
    ) -> bool {
        let history = match self.kernel.store.read_stream(
            &command.tenant_id,
            &command.aggregate_type,
            &command.aggregate_id,
        ) {
            Ok(history) => history,
            Err(e) => {
                eprintln!("read stream failed: {e}");
                self.kernel.queue.release(&lease_id);
                self.kernel.work.notify_waiters();
                return true;
            }
        };
        let expected_version = history.last().map_or(0, |e| e.version);
        self.in_flight().insert(
            lease_id.clone(),
            InFlight {
                command: command.clone(),
                expected_version,
                attempts,
                permit,
            },
        );
        let execute = Execute {
            lease_id,
            command: Some(command),
            history,
            expected_version,
        };
        send(&self.out, Out::Execute(execute)).await.is_ok()
    }

    async fn on_result(&self, r: kernable_core::proto::Result) -> Result<(), Status> {
        let Some(f) = self.in_flight().remove(&r.lease_id) else {
            return Ok(()); // unknown or already handled lease
        };
        let sig = r.worker_sig.unwrap_or_default();
        let msg = signing::result_message(&f.command, f.expected_version, &r.events);
        if sig.key_id != self.key_id || signing::verify(&sig, &msg).is_err() {
            // Put it back, so it is released when the session ends.
            self.in_flight().insert(r.lease_id, f);
            return Err(Status::unauthenticated("invalid result signature"));
        }
        if !self.kernel.queue.holds(&r.lease_id) {
            return Ok(()); // the lease expired; the command was handed to another worker
        }

        let command_id = &f.command.command_id;
        match self
            .kernel
            .store
            .commit(command_id, f.expected_version, &r.events, &sig, now_ms())
        {
            Ok(events) => {
                self.kernel.queue.done(&r.lease_id);
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
                eprintln!("commit failed: {e}");
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
        self.reject(&r.lease_id, f.command.command_id, reason);
        Ok(())
    }

    fn reject(&self, lease_id: &str, command_id: String, reason: Rejected) {
        if !self.kernel.queue.holds(lease_id) {
            return;
        }
        match self.kernel.store.reject(&command_id, reason.clone()) {
            Ok(()) => {
                self.kernel.queue.done(lease_id);
                self.kernel.complete(CommandResult {
                    command_id,
                    outcome: Some(Outcome::Rejected(reason)),
                });
            }
            Err(StoreError::NotPending) => self.done(lease_id),
            Err(e) => {
                eprintln!("reject failed: {e}");
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

fn internal(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}
