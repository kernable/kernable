//! Test harness: an in-process kernel on a Unix socket and Rust test workers on raw stubs.
#![allow(dead_code)] // each test file uses a different subset

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ChallengeResponse, Command, CommandResult, NewEvent, Register, Rejected, Rejection,
        Result as WorkResult, Snapshot, SubmitRequest, Wait, WorkerMessage,
        command_result::Outcome, command_service_client::CommandServiceClient,
        kernel_message::Msg as Out, worker_gateway_client::WorkerGatewayClient,
        worker_message::Msg as In,
    },
    signing, transport,
};
use kernable_kernel::{Config, Kernel, serve};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

pub fn id(key: &SigningKey) -> String {
    signing::key_id(&key.verifying_key())
}

pub struct Env {
    pub dir: PathBuf,
    /// Store URL, for opening the store from the test.
    pub store: String,
    pub addr: String,
    pub kernel_key: SigningKey,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub async fn start(name: &str) -> Env {
    start_with(name, 2_000).await
}

/// Starts a kernel that trusts client key 1 for everything, worker key 2 for Order commands,
/// subscriber key 4 for `OrderPlaced` events only, listener key 5 for all events and
/// listener key 6 for `OrderPlaced` only.
pub async fn start_with(name: &str, lease_ttl_ms: i64) -> Env {
    start_custom(name, |config, _| config.lease_ttl_ms = lease_ttl_ms).await
}

/// Like [`start`], with `customize` applied to the config (it gets the test's directory).
pub async fn start_custom(name: &str, customize: impl FnOnce(&mut Config, &Path)) -> Env {
    let dir = std::env::temp_dir().join(format!("kn-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kernel_key = key(3);
    std::fs::write(dir.join("kernel.key"), hex(kernel_key.as_bytes())).unwrap();
    let store = store_url(&dir);
    let mut config = Config {
        store: store.clone(),
        listen: format!("unix:{}", dir.join("k.sock").display()),
        kernel_key: dir.join("kernel.key"),
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        settle_timeout_ms: 1_000,
        clients: HashMap::from([(id(&key(1)), vec!["*".into()])]),
        workers: HashMap::from([(id(&key(2)), vec!["PlaceOrder".into(), "AddItem".into()])]),
        subscribers: HashMap::from([
            (id(&key(1)), vec!["*".into()]),
            (id(&key(4)), vec!["OrderPlaced".into()]),
        ]),
        listeners: HashMap::from([
            (id(&key(5)), vec!["*".into()]),
            (id(&key(6)), vec!["OrderPlaced".into()]),
        ]),
        privileged: HashMap::from([(id(&key(1)), vec!["bypass_authorization".into()])]),
        tls: None,
        metrics_listen: None,
    };
    customize(&mut config, &dir);
    let addr = config.listen.clone();
    tokio::spawn(serve(Kernel::open(config).unwrap()));
    wait_for(&addr).await;
    Env {
        dir,
        store,
        addr,
        kernel_key,
    }
}

/// The backend under test: `KERNABLE_TEST_STORE=file` selects the file store, else SQLite.
pub fn store_url(dir: &std::path::Path) -> String {
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => format!("file:{}", dir.join("k.log").display()),
        _ => format!("sqlite:{}", dir.join("k.db").display()),
    }
}

/// Waits until the kernel accepts connections (at the socket level, so TLS works too).
pub async fn wait_for(addr: &str) {
    for _ in 0..200 {
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

/// Order domain as a plain function of whether the order exists and the command.
pub fn order(exists: bool, cmd: &Command) -> Result<Vec<NewEvent>, Rejected> {
    let reject = |message: &str| Rejected {
        code: "invalid_state".into(),
        message: message.into(),
    };
    let (event_type, ok) = match cmd.command_type.as_str() {
        "PlaceOrder" => ("OrderPlaced", !exists),
        "AddItem" => ("ItemAdded", exists),
        _ => return Err(reject("unknown command")),
    };
    if !ok {
        return Err(reject(&format!("{} not allowed now", cmd.command_type)));
    }
    Ok(vec![NewEvent {
        event_type: event_type.into(),
        payload: cmd.payload.clone(),
    }])
}

#[derive(Clone, Copy)]
pub enum Behavior {
    Honest,
    /// Disconnects as soon as it receives a command.
    Crash,
    /// Answers each command only after this delay.
    Slow(Duration),
    /// Signs results with a key other than its registered one.
    ForgeResult,
    /// Sends a snapshot with every result.
    Snapshots,
    /// Sends a snapshot signed with a key other than its registered one.
    ForgeSnapshot,
    /// Cannot read snapshots: rejects with `snapshot_stale` when it gets one.
    StaleSnapshots,
}

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
}

impl TestWorker {
    pub fn honest() -> Self {
        TestWorker {
            claimed: key(2),
            signer: key(2),
            command_types: vec!["PlaceOrder".into(), "AddItem".into()],
            behavior: Behavior::Honest,
            seen: None,
            executions: None,
            tls: None,
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

/// Runs a worker on raw generated stubs. Returns how the session ended.
pub async fn run_worker(addr: String, w: TestWorker) -> tonic::Status {
    let (tx, rx) = mpsc::channel(16);
    let send = |msg: In| WorkerMessage { msg: Some(msg) };
    tx.send(send(In::Register(Register {
        worker_id: "test-worker".into(),
        key_id: id(&w.claimed),
        command_types: w.command_types,
        max_concurrency: 4,
    })))
    .await
    .unwrap();

    let channel = channel(&addr, w.tls.as_ref()).await.unwrap();
    let mut client = WorkerGatewayClient::new(channel);
    let mut inbound = match client.session(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(status) => return status,
    };
    loop {
        let msg = match inbound.message().await {
            Ok(Some(m)) => m.msg.unwrap(),
            Ok(None) => return tonic::Status::ok("closed"),
            Err(status) => return status,
        };
        let reply = match msg {
            Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&w.signer, &signing::challenge_message(&c.nonce)).sig,
            }),
            Out::Registered(_) => continue,
            Out::Execute(x) => {
                let cmd = x.command.unwrap();
                if let Some(seen) = &w.seen {
                    let _ = seen.send(cmd.command_id.clone());
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
                let exists = x.snapshot.is_some() || !x.history.is_empty();
                let outcome = match w.behavior {
                    Behavior::StaleSnapshots if x.snapshot.is_some() => Err(Rejected {
                        code: "snapshot_stale".into(),
                        message: "unknown snapshot schema".into(),
                    }),
                    _ => order(exists, &cmd),
                };
                match outcome {
                    Ok(events) => {
                        let msg = signing::result_message(&cmd, x.expected_version, &events);
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
                            lease_id: x.lease_id,
                            events,
                            worker_sig: Some(signing::sign(&result_key, &msg)),
                            snapshot,
                        })
                    }
                    Err(reason) => {
                        let msg = signing::rejection_message(&cmd.command_id, &reason);
                        In::Rejection(Rejection {
                            lease_id: x.lease_id,
                            reason: Some(reason),
                            worker_sig: Some(signing::sign(&result_key, &msg)),
                        })
                    }
                }
            }
        };
        if tx.send(send(reply)).await.is_err() {
            return tonic::Status::ok("closed");
        }
    }
}

pub fn spawn_worker(env: &Env) {
    tokio::spawn(run_worker(env.addr.clone(), TestWorker::honest()));
}

pub fn command(id: &str, command_type: &str, aggregate_id: &str) -> Command {
    Command {
        command_id: id.into(),
        tenant_id: "t1".into(),
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
    let request = SubmitRequest {
        command: Some(cmd),
        wait: wait.into(),
    };
    let result = CommandServiceClient::new(channel).submit(request).await?;
    Ok(result.into_inner())
}

pub fn committed_versions(result: &CommandResult) -> Vec<u64> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.iter().map(|e| e.version).collect(),
        other => panic!("expected committed, got {other:?}"),
    }
}
