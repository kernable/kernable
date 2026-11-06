//! Test harness: an in-process kernel on a Unix socket and Rust test workers on raw stubs.
#![allow(dead_code)] // each test file uses a different subset

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ApplyRequest, ChallengeResponse, Command, CommandResult, EventSchema, Execute, Grant,
        Membership, NewEvent, Permission, Register, Rejected, Rejection, Result as WorkResult,
        Snapshot, SubmitRequest, Wait, WorkerAggregate, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, kernel_message::Msg as Out,
        membership_service_client::MembershipServiceClient,
        worker_gateway_client::WorkerGatewayClient, worker_message::Msg as In,
    },
    signing, transport,
};
use kernable_kernel::{Config, GRANT_HEADER, Kernel, serve};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

pub fn id(key: &SigningKey) -> String {
    signing::key_id(&key.verifying_key())
}

/// The root key every test kernel trusts.
pub fn root() -> SigningKey {
    key(200)
}

/// A grant for `key` signed by [`root`], newer than every grant made before.
pub fn grant(key: &SigningKey, permissions: &[(&str, &[&str])]) -> Grant {
    successor_grant(key, permissions, "")
}

/// Like [`grant`], for a key that succeeds the key ID `succeeds` (empty: none).
pub fn successor_grant(key: &SigningKey, permissions: &[(&str, &[&str])], succeeds: &str) -> Grant {
    static ISSUED: AtomicI64 = AtomicI64::new(1_000);
    grant_at(
        key,
        permissions,
        succeeds,
        ISSUED.fetch_add(1, Ordering::SeqCst),
    )
}

fn grant_at(
    key: &SigningKey,
    permissions: &[(&str, &[&str])],
    succeeds: &str,
    issued_at: i64,
) -> Grant {
    let permissions = permissions
        .iter()
        .map(|(action, types)| Permission {
            action: (*action).into(),
            types: types.iter().map(|t| (*t).into()).collect(),
        })
        .collect();
    signing::grant(&root(), &id(key), permissions, succeeds, issued_at)
}

/// What the test keys may do: client key 1 everything a client does (and manage every service), worker key 2 the
/// `orders` domain, subscriber key 4 `orders.OrderPlaced` only, listener key 5 all events (and
/// their vault values) and listener key 6 `orders.OrderPlaced` only. Issued at 1, so any
/// [`grant`] replaces them.
pub fn default_grant(key_: &SigningKey) -> Option<Grant> {
    let permissions: &[(&str, &[&str])] = match key_.to_bytes()[0] {
        1 => &[
            ("submit", &["*"]),
            ("subscribe", &["*"]),
            ("query", &["*"]),
            ("privileged", &["bypass_authorization"]),
            ("manage", &["*"]),
        ],
        2 => &[("run", &["orders.*"])],
        4 => &[("subscribe", &["orders.OrderPlaced"])],
        5 => &[
            ("listen", &["*"]),
            ("answer", &["orders.*"]),
            ("check", &["*"]),
            ("vault_read", &["*"]),
        ],
        6 => &[("listen", &["orders.OrderPlaced"])],
        _ => return None,
    };
    Some(grant_at(key_, permissions, "", 1))
}

/// `message` as a request that carries `grant`.
pub fn presenting<T>(message: T, grant: &Grant) -> tonic::Request<T> {
    use prost::Message;
    let mut request = tonic::Request::new(message);
    let value = hex(&grant.encode_to_vec()).parse().unwrap();
    request.metadata_mut().insert(GRANT_HEADER, value);
    request
}

/// `message` as a request that carries `key`'s default grant, as an SDK sends it.
pub fn with_grant<T>(message: T, key: &SigningKey) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(grant) = default_grant(key) {
        use prost::Message;
        let value = hex(&grant.encode_to_vec()).parse().unwrap();
        request.metadata_mut().insert(GRANT_HEADER, value);
    }
    request
}

pub struct Env {
    pub dir: PathBuf,
    /// Store URL, for opening the store from the test.
    pub store: String,
    pub addr: String,
    pub kernel_key: SigningKey,
    /// Snapshot store URL, for reading snapshots from the test.
    pub snapshots: String,
    pub kernel: std::sync::Arc<Kernel>,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub async fn start(name: &str) -> Env {
    start_with(name, 2_000).await
}

/// Starts a kernel that trusts [`root`] and holds the [`default_grant`]s of the test keys.
pub async fn start_with(name: &str, lease_ttl_ms: i64) -> Env {
    start_custom(name, |config, _| config.lease_ttl_ms = lease_ttl_ms).await
}

/// Like [`start`], with `customize` applied to the config (it gets the test's directory).
pub async fn start_custom(name: &str, customize: impl FnOnce(&mut Config, &Path)) -> Env {
    start_wrapped(name, customize, |store| store).await
}

/// Like [`start_custom`], with the opened store passed through `wrap` before the kernel gets it.
pub async fn start_wrapped(
    name: &str,
    customize: impl FnOnce(&mut Config, &Path),
    wrap: impl FnOnce(Box<dyn kernable_store::Store>) -> Box<dyn kernable_store::Store>,
) -> Env {
    let dir = std::env::temp_dir().join(format!("kn-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kernel_key = key(3);
    std::fs::write(dir.join("kernel.key"), hex(kernel_key.as_bytes())).unwrap();
    let store = store_url(&dir);
    let mut config = Config {
        roots: vec![id(&root())],
        store: store.clone(),
        listen: format!("unix:{}", dir.join("k.sock").display()),
        kernel_key: dir.join("kernel.key"),
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        settle_timeout_ms: 1_000,
        query_timeout_ms: 1_000,
        middleware_timeout_ms: 1_000,
        drain_timeout_ms: 1_000,
        take_over_ms: 0,
        tls: None,
        snapshots: format!("sqlite:{}", dir.join("snapshots.db").display()),
        data: format!("file:{}", dir.join("data").display()),
        cache: "memory:".into(),
        vault: None,
        metrics_listen: None,
        ui_listen: None,
        // The logic under test does not depend on it; Full is covered on its own.
        durability: kernable_kernel::config::Durability::Normal,
        limits: Default::default(),
        replication: Default::default(),
        key_command: Vec::new(),
        partition: None,
    };
    customize(&mut config, &dir);
    let addr = config.listen.clone();
    let snapshots = config.snapshots.clone();
    let opened = kernable_store::open_with(&config.store, config.durability).unwrap();
    let kernel = Kernel::open_on(config, wrap(opened)).unwrap();
    for seed in [1, 2, 4, 5, 6] {
        kernel.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    tokio::spawn(serve(std::sync::Arc::clone(&kernel)));
    wait_for(&addr).await;
    Env {
        dir,
        store,
        addr,
        kernel_key,
        kernel,
        snapshots,
    }
}

/// A free local TCP address.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// The raw HTTP response to `GET path` at `addr` (the metrics port).
pub async fn get(addr: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    response
}

/// The backend under test: `KERNABLE_TEST_STORE=file` selects the file store, else SQLite.
pub fn store_url(dir: &std::path::Path) -> String {
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => format!("file:{}", dir.join("k.log").display()),
        _ => format!("sqlite:{}", dir.join("k.db").display()),
    }
}

/// Waits until the kernel accepts connections (at the socket level, so TLS works too), for up
/// to 10 s: a loaded machine starts a kernel slowly.
pub async fn wait_for(addr: &str) {
    for _ in 0..1_000 {
        let up = match transport::unix_path(addr) {
            Some(path) => tokio::net::UnixStream::connect(path).await.is_ok(),
            None => tokio::net::TcpStream::connect(addr).await.is_ok(),
        };
        if up {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("kernel did not come up at {addr}");
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Order domain as a plain function of whether the order exists and the command. `Reindex`
/// produces no events.
pub fn order(exists: bool, cmd: &Command) -> Result<Vec<NewEvent>, Rejected> {
    let reject = |message: &str| Rejected {
        code: "invalid_state".into(),
        message: message.into(),
        ..Default::default()
    };
    let (event_type, ok) = match cmd.command_type.as_str() {
        "PlaceOrder" => ("OrderPlaced", !exists),
        "AddItem" => ("ItemAdded", exists),
        "Reindex" => return Ok(Vec::new()),
        _ => return Err(reject("unknown command")),
    };
    if !ok {
        return Err(reject(&format!("{} not allowed now", cmd.command_type)));
    }
    Ok(vec![NewEvent {
        erases: String::new(),
        vault_refs: Vec::new(),
        event_type: event_type.into(),
        payload: cmd.payload.clone(),
        schema: 1,
        ..Default::default()
    }])
}

#[derive(Clone, Copy)]
pub enum Behavior {
    Honest,
    /// Disconnects as soon as it receives a command.
    Crash,
    /// Answers each command only after this delay.
    Slow(Duration),
    /// Answers with a lease ID that is not the one it was given (an unsigned field).
    WrongLease,
    /// Signs results with a key other than its registered one.
    ForgeResult,
    /// Sends a snapshot with every result.
    Snapshots,
    /// Sends a snapshot signed with a key other than its registered one.
    ForgeSnapshot,
    /// Cannot read snapshots: rejects with `snapshot_stale` when it gets one.
    StaleSnapshots,
    /// Signs keys, facts and requirements with a key other than its registered one.
    ForgeIndex,
    /// Names a key and a requirement in its own rejections, which only the kernel may.
    NameKeyInRejections,
}

pub type VaultWriter = std::sync::Arc<
    dyn Fn(&Command) -> (Vec<kernable_core::proto::VaultValue>, String) + Send + Sync,
>;

/// The unique keys, facts and requirements a worker sends with its result for a command.
pub type IndexWriter = std::sync::Arc<
    dyn Fn(
            &Command,
        ) -> (
            Vec<kernable_core::proto::UniqueKey>,
            Vec<String>,
            Vec<kernable_core::proto::Requirement>,
        ) + Send
        + Sync,
>;

pub struct TestWorker {
    /// Key ID the worker registers with.
    pub claimed: SigningKey,
    /// Key it actually signs the challenge with.
    pub signer: SigningKey,
    pub command_types: Vec<String>,
    pub behavior: Behavior,
    /// Receives the ID of every command the worker is asked to execute.
    pub seen: Option<mpsc::UnboundedSender<String>>,
    /// Receives, per execution, the snapshot version (if any) and the number of history events.
    pub executions: Option<mpsc::UnboundedSender<(Option<u64>, usize)>>,
    /// Connects over mutual TLS with these files.
    pub tls: Option<transport::Tls>,
    /// Payload versions it declares per qualified event type, and writes.
    pub schemas: Vec<(String, u32)>,
    /// Writes every event at this version, whatever it declared.
    pub writes: Option<u32>,
    /// Vault values for the command's event, and a subject it erases (empty for none).
    pub writes_vault: Option<VaultWriter>,
    /// Receives, per execution, the vault values the kernel sent along.
    pub vaults: Option<mpsc::UnboundedSender<Vec<kernable_core::proto::VaultValue>>>,
    /// The aggregates it registers; empty: `<domain>.Order` per domain of `command_types`.
    pub aggregates: Vec<WorkerAggregate>,
    /// Its service (`Register.service`).
    pub service: String,
    /// Told once the kernel registered the worker.
    pub registered: Option<tokio::sync::oneshot::Sender<()>>,
    /// The grant it presents; `None`: the [`default_grant`] of its key.
    pub grant: Option<Grant>,
    /// Shared by workers that must never execute one aggregate at the same time.
    pub held: Option<std::sync::Arc<Held>>,
    /// The keys, facts and requirements it sends with its results.
    pub index: Option<IndexWriter>,
    /// Commands it runs at once.
    pub max_concurrency: u32,
}

/// The aggregates some workers are executing: one lease per aggregate at a time (O7).
#[derive(Default)]
pub struct Held {
    running: Mutex<HashSet<String>>,
    /// Executions of an aggregate that another execution was still running.
    pub overlaps: AtomicUsize,
}

impl TestWorker {
    pub fn honest() -> Self {
        TestWorker {
            claimed: key(2),
            signer: key(2),
            command_types: vec!["orders.PlaceOrder".into(), "orders.AddItem".into()],
            behavior: Behavior::Honest,
            seen: None,
            executions: None,
            tls: None,
            schemas: vec![],
            writes: None,
            writes_vault: None,
            vaults: None,
            aggregates: Vec::new(),
            service: String::new(),
            registered: None,
            grant: None,
            held: None,
            index: None,
            max_concurrency: 4,
        }
    }

    pub fn with(behavior: Behavior, seen: mpsc::UnboundedSender<String>) -> Self {
        TestWorker {
            behavior,
            seen: Some(seen),
            ..TestWorker::honest()
        }
    }
}

/// The aggregates of qualified command types: `<domain>.Order` per domain, as test commands use.
pub fn aggregates(command_types: &[String]) -> Vec<WorkerAggregate> {
    let mut aggregates: Vec<WorkerAggregate> = Vec::new();
    for t in command_types {
        let aggregate_type = format!("{}.Order", t.split_once('.').map_or("", |(d, _)| d));
        match aggregates
            .iter_mut()
            .find(|a| a.aggregate_type == aggregate_type)
        {
            Some(a) => a.command_types.push(t.clone()),
            None => aggregates.push(WorkerAggregate {
                aggregate_type,
                command_types: vec![t.clone()],
            }),
        }
    }
    aggregates
}

/// Runs a worker on raw generated stubs. Returns how the session ended.
pub async fn run_worker(addr: String, mut w: TestWorker) -> tonic::Status {
    let (tx, rx) = mpsc::channel(16);
    let send = |msg: In| WorkerMessage { msg: Some(msg) };
    tx.send(send(In::Register(Register {
        protocol: kernable_core::PROTOCOL,
        worker_id: "test-worker".into(),
        key_id: id(&w.claimed),
        aggregates: if w.aggregates.is_empty() {
            aggregates(&w.command_types)
        } else {
            w.aggregates.clone()
        },
        max_concurrency: w.max_concurrency,
        service: w.service.clone(),
        event_schemas: w
            .schemas
            .iter()
            .map(|(event_type, schema)| EventSchema {
                event_type: event_type.clone(),
                schema: *schema,
            })
            .collect(),
    })))
    .await
    .unwrap();

    let channel = match channel(&addr, w.tls.as_ref()).await {
        Ok(channel) => channel,
        // The kernel is away (a crash test restarts it): the caller retries.
        Err(e) => return tonic::Status::unavailable(e.to_string()),
    };
    let mut client = WorkerGatewayClient::new(channel);
    let session = match &w.grant {
        Some(grant) => presenting(ReceiverStream::new(rx), grant),
        None => with_grant(ReceiverStream::new(rx), &w.claimed),
    };
    let mut inbound = match client.session(session).await {
        Ok(r) => r.into_inner(),
        Err(status) => return status,
    };
    // Executes whose history is still coming in parts, by lease.
    let mut partial: HashMap<String, Execute> = HashMap::new();
    loop {
        let msg = match inbound.message().await {
            Ok(Some(m)) => m.msg.unwrap(),
            Ok(None) => return tonic::Status::ok("closed"),
            Err(status) => return status,
        };
        let msg = match msg {
            Out::Execute(x) if x.more => {
                partial.insert(x.lease_id.clone(), x);
                continue;
            }
            Out::ExecuteMore(rest) => {
                let Some(mut x) = partial.remove(&rest.lease_id) else {
                    continue;
                };
                x.history.extend(rest.history);
                x.vault.extend(rest.vault);
                x.more = rest.more;
                if x.more {
                    partial.insert(x.lease_id.clone(), x);
                    continue;
                }
                Out::Execute(x)
            }
            other => other,
        };
        let mut executing = None;
        let reply = match msg {
            Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&w.signer, &signing::challenge_message(&c.nonce)).sig,
            }),
            Out::Registered(_) => {
                if let Some(registered) = w.registered.take() {
                    let _ = registered.send(());
                }
                continue;
            }
            Out::ExecuteMore(_) => unreachable!("assembled above"),
            Out::Execute(x) => {
                let cmd = x.command.unwrap();
                if let Some(held) = &w.held {
                    let aggregate = format!("{}/{}", cmd.tenant_id, cmd.aggregate_id);
                    if !held.running.lock().unwrap().insert(aggregate.clone()) {
                        held.overlaps.fetch_add(1, Ordering::SeqCst);
                    }
                    executing = Some(aggregate);
                }
                if let Some(seen) = &w.seen {
                    let _ = seen.send(cmd.command_id.clone());
                }
                if let Some(vaults) = &w.vaults {
                    let _ = vaults.send(x.vault.clone());
                }
                if let Some(executions) = &w.executions {
                    let version = x.snapshot.as_ref().map(|s| s.version);
                    let _ = executions.send((version, x.history.len()));
                }
                match w.behavior {
                    Behavior::Crash => return tonic::Status::ok("crashed"),
                    Behavior::Slow(delay) => tokio::time::sleep(delay).await,
                    _ => {}
                }
                let result_key = match w.behavior {
                    Behavior::ForgeResult => key(9),
                    _ => w.signer.clone(),
                };
                let lease_id = match w.behavior {
                    // A guess at one: lease IDs are `<command ID>#<n>`.
                    Behavior::WrongLease => format!("{}0", x.lease_id),
                    _ => x.lease_id.clone(),
                };
                let exists = x.snapshot.is_some() || !x.history.is_empty();
                let outcome = match w.behavior {
                    Behavior::StaleSnapshots if x.snapshot.is_some() => Err(Rejected {
                        code: "snapshot_stale".into(),
                        message: "unknown snapshot schema".into(),
                        ..Default::default()
                    }),
                    Behavior::NameKeyInRejections => order(exists, &cmd).map_err(|r| Rejected {
                        key: "number".into(),
                        requirement: Some(Default::default()),
                        ..r
                    }),
                    _ => order(exists, &cmd),
                };
                match outcome {
                    Ok(mut events) => {
                        for e in &mut events {
                            let name = format!("{}.{}", cmd.domain, e.event_type);
                            let declared = w.schemas.iter().find(|(t, _)| *t == name);
                            e.schema = w.writes.or(declared.map(|d| d.1)).unwrap_or(1);
                        }
                        let (vault, erases) = w
                            .writes_vault
                            .as_ref()
                            .map_or_else(|| (Vec::new(), String::new()), |write| write(&cmd));
                        if let Some(first) = events.first_mut() {
                            first.vault_refs = vault.iter().map(|v| v.r#ref.clone()).collect();
                            first.erases = erases;
                        }
                        let msg = signing::result_message(&cmd, x.expected_version, &events);
                        let (keys, facts, requires) = w
                            .index
                            .as_ref()
                            .map_or_else(Default::default, |index| index(&cmd));
                        let index_sig = (!keys.is_empty()
                            || !facts.is_empty()
                            || !requires.is_empty())
                        .then(|| {
                            let signer = match w.behavior {
                                Behavior::ForgeIndex => key(9),
                                _ => result_key.clone(),
                            };
                            let msg = signing::index_message(
                                &cmd,
                                x.expected_version,
                                &keys,
                                &facts,
                                &requires,
                            );
                            signing::sign(&signer, &msg)
                        });
                        let version = x.expected_version + events.len() as u64;
                        let snapshot_key = match w.behavior {
                            Behavior::Snapshots | Behavior::StaleSnapshots => {
                                Some(w.signer.clone())
                            }
                            Behavior::ForgeSnapshot => Some(key(9)),
                            _ => None,
                        };
                        let snapshot = snapshot_key.map(|k| {
                            let state = version.to_string().into_bytes();
                            let msg = signing::snapshot_message(&cmd, version, "1", &state);
                            Snapshot {
                                version,
                                schema: "1".into(),
                                state,
                                worker_sig: Some(signing::sign(&k, &msg)),
                            }
                        });
                        In::Result(WorkResult {
                            vault,
                            lease_id,
                            events,
                            worker_sig: Some(signing::sign(&result_key, &msg)),
                            snapshot,
                            keys,
                            facts,
                            requires,
                            index_sig,
                        })
                    }
                    Err(reason) => {
                        let msg = signing::rejection_message(&cmd.command_id, &reason);
                        In::Rejection(Rejection {
                            lease_id,
                            reason: Some(reason),
                            worker_sig: Some(signing::sign(&result_key, &msg)),
                        })
                    }
                }
            }
        };
        // Done before the kernel hears of it: until then, no one else may hold the lease.
        if let (Some(held), Some(aggregate)) = (&w.held, executing) {
            held.running.lock().unwrap().remove(&aggregate);
        }
        if tx.send(send(reply)).await.is_err() {
            return tonic::Status::ok("closed");
        }
    }
}

/// Applies a membership, signed by key 1.
pub async fn apply(addr: &str, membership: Membership) -> Result<(), tonic::Status> {
    apply_as(addr, membership, &key(1)).await
}

/// Applies a membership, issued now, signed by `key_`.
pub async fn apply_as(
    addr: &str,
    mut membership: Membership,
    key_: &SigningKey,
) -> Result<(), tonic::Status> {
    membership.issued_at = issued_now();
    apply_issued(addr, membership, key_).await
}

/// Applies a membership with the `issued_at` it has, as a replay would.
pub async fn apply_issued(
    addr: &str,
    membership: Membership,
    key_: &SigningKey,
) -> Result<(), tonic::Status> {
    let request = ApplyRequest {
        sig: Some(signing::sign(
            key_,
            &signing::membership_message(&membership),
        )),
        membership: Some(membership),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(addr).await.unwrap();
    MembershipServiceClient::new(channel)
        .apply(with_grant(request, key_))
        .await
        .map(|_| ())
}

/// Now in milliseconds, later than every earlier call: the kernel refuses a membership that is
/// not newer than the applied one, and two applies can fall within one millisecond.
pub fn issued_now() -> i64 {
    static LAST: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let previous = LAST
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |last| Some(now.max(last + 1)),
        )
        .unwrap();
    now.max(previous + 1)
}

/// A membership of `service` that lists these workers, listeners and middlewares.
pub fn membership(
    service: &str,
    workers: &[&str],
    listeners: &[&str],
    middlewares: &[&str],
) -> Membership {
    let strings = |s: &[&str]| s.iter().map(|s| s.to_string()).collect();
    Membership {
        service: service.into(),
        workers: strings(workers),
        listeners: strings(listeners),
        middlewares: strings(middlewares),
        ..Default::default()
    }
}

/// Runs a worker in the background, once the kernel registered it (or refused it): commands
/// for aggregates no worker registered are refused.
pub async fn start_worker(
    addr: String,
    mut w: TestWorker,
) -> tokio::task::JoinHandle<tonic::Status> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    w.registered = Some(tx);
    let worker = tokio::spawn(run_worker(addr, w));
    let _ = rx.await;
    worker
}

pub async fn spawn_worker(env: &Env) {
    start_worker(env.addr.clone(), TestWorker::honest()).await;
}

pub fn command(id: &str, command_type: &str, aggregate_id: &str) -> Command {
    Command {
        vault_refs: Vec::new(),
        command_id: id.into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: aggregate_id.into(),
        command_type: command_type.into(),
        payload: format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        client_sig: None,
        context: None,
    }
}

pub fn signed(mut cmd: Command, key: &SigningKey) -> Command {
    cmd.client_sig = Some(signing::sign(key, &signing::command_message(&cmd)));
    cmd
}

pub async fn submit(addr: &str, cmd: Command) -> Result<CommandResult, tonic::Status> {
    submit_with(addr, cmd, Wait::Settled).await
}

pub async fn submit_with(
    addr: &str,
    cmd: Command,
    wait: Wait,
) -> Result<CommandResult, tonic::Status> {
    let channel = transport::connect(addr).await.unwrap();
    submit_on(channel, cmd, wait).await
}

/// A channel to the kernel, over mutual TLS if `tls` is set.
pub async fn channel(
    addr: &str,
    tls: Option<&transport::Tls>,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error + Send + Sync>> {
    match tls {
        Some(tls) => transport::connect_tls(addr, tls).await,
        None => Ok(transport::connect(addr).await?),
    }
}

pub async fn submit_on(
    channel: tonic::transport::Channel,
    cmd: Command,
    wait: Wait,
) -> Result<CommandResult, tonic::Status> {
    submit_as(channel, cmd, wait, "").await
}

/// Submits from the handler of `listener`, as the SDKs do.
pub async fn submit_from(
    addr: &str,
    cmd: Command,
    wait: Wait,
    listener: &str,
) -> Result<CommandResult, tonic::Status> {
    let channel = transport::connect(addr).await.unwrap();
    submit_as(channel, cmd, wait, listener).await
}

async fn submit_as(
    channel: tonic::transport::Channel,
    cmd: Command,
    wait: Wait,
    listener: &str,
) -> Result<CommandResult, tonic::Status> {
    let signer = client_key(&cmd);
    let request = SubmitRequest {
        vault: Vec::new(),
        listener: listener.into(),
        protocol: kernable_core::PROTOCOL,
        command: Some(cmd),
        wait: wait.into(),
    };
    let request = match signer {
        Some(key) => with_grant(request, &key),
        None => tonic::Request::new(request),
    };
    let result = CommandServiceClient::new(channel).submit(request).await?;
    Ok(result.into_inner())
}

/// The test key that signed `cmd`, if it is one of the keys with a default grant.
fn client_key(cmd: &Command) -> Option<SigningKey> {
    let signer = &cmd.client_sig.as_ref()?.key_id;
    [1, 2, 4, 5, 6]
        .into_iter()
        .map(key)
        .find(|k| &id(k) == signer)
}

pub fn committed_versions(result: &CommandResult) -> Vec<u64> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.iter().map(|e| e.version).collect(),
        other => panic!("expected committed, got {other:?}"),
    }
}

/// Adds a vault (a SQLite file in `dir`, `vault.db`) to a test kernel's config. By the default
/// grants, listener key 5 reads every vault value and listener key 6 none.
pub fn with_vault(config: &mut Config, dir: &Path) {
    std::fs::write(dir.join("vault.key"), hex(&[6; 32])).unwrap();
    config.vault = Some(kernable_kernel::config::VaultConfig {
        store: format!("sqlite:{}", dir.join("vault.db").display()),
        key: dir.join("vault.key"),
    });
}

/// A personal value of `subject` (a secret if empty), JSON encoded, with a random salt.
pub fn vault_value(subject: &str, value: &str) -> kernable_core::proto::VaultValue {
    let mut salt = vec![0u8; 16];
    getrandom::fill(&mut salt).unwrap();
    let value = format!("\"{value}\"").into_bytes();
    let secret = subject.is_empty();
    kernable_core::proto::VaultValue {
        r#ref: signing::vault_ref(&salt, subject, secret, &value),
        subject: subject.into(),
        secret,
        salt,
        value,
    }
}

/// A kernel process that never outlives its test: killed when dropped, also when the test
/// fails, and by a [`watchdog`] when the test process itself is killed (a timeout, Ctrl-C).
pub struct KillOnDrop {
    pub child: std::process::Child,
    watchdog: std::process::Child,
}

impl KillOnDrop {
    pub fn spawn(command: &mut std::process::Command) -> Self {
        let child = command.spawn().unwrap();
        let watchdog = watchdog(child.id());
        KillOnDrop { child, watchdog }
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = self.watchdog.kill();
        let _ = self.watchdog.wait();
    }
}

/// A shell reading a pipe from this process. However this process ends, the pipe closes and
/// the shell kills `pid`; kill the watchdog once `pid` is gone.
pub fn watchdog(pid: u32) -> std::process::Child {
    std::process::Command::new("sh")
        .args(["-c", &format!("read _; kill -9 {pid} 2>/dev/null")])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap()
}
