//! Test harness: an in-process kernel on a Unix socket and Rust test workers on raw stubs.
#![allow(dead_code)] // each test file uses a different subset

use std::{collections::HashMap, path::PathBuf, time::Duration};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ChallengeResponse, Command, CommandResult, Event, NewEvent, Register, Rejected, Rejection,
        Result as WorkResult, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, kernel_message::Msg as Out,
        worker_gateway_client::WorkerGatewayClient, worker_message::Msg as In,
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

/// Starts a kernel that trusts client key 1 for everything and worker key 2 for Order commands.
pub async fn start_with(name: &str, lease_ttl_ms: i64) -> Env {
    let dir = std::env::temp_dir().join(format!("kn-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kernel_key = key(3);
    std::fs::write(dir.join("kernel.key"), hex(kernel_key.as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let config = Config {
        db: dir.join("k.db"),
        listen: addr.clone(),
        kernel_key: dir.join("kernel.key"),
        lease_ttl_ms,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        clients: HashMap::from([(id(&key(1)), vec!["*".into()])]),
        workers: HashMap::from([(id(&key(2)), vec!["PlaceOrder".into(), "AddItem".into()])]),
    };
    tokio::spawn(serve(Kernel::open(config).unwrap()));
    wait_for(&addr).await;
    Env {
        dir,
        addr,
        kernel_key,
    }
}

pub async fn wait_for(addr: &str) {
    for _ in 0..200 {
        if transport::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("kernel did not come up at {addr}");
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Order domain as a plain function of history and command.
pub fn order(history: &[Event], cmd: &Command) -> Result<Vec<NewEvent>, Rejected> {
    let reject = |message: &str| Rejected {
        code: "invalid_state".into(),
        message: message.into(),
    };
    let (event_type, ok) = match cmd.command_type.as_str() {
        "PlaceOrder" => ("OrderPlaced", history.is_empty()),
        "AddItem" => ("ItemAdded", !history.is_empty()),
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
}

impl TestWorker {
    pub fn honest() -> Self {
        TestWorker {
            claimed: key(2),
            signer: key(2),
            command_types: vec!["PlaceOrder".into(), "AddItem".into()],
            behavior: Behavior::Honest,
            seen: None,
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

    let channel = transport::connect(&addr).await.unwrap();
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
                match w.behavior {
                    Behavior::Crash => return tonic::Status::ok("crashed"),
                    Behavior::Slow(delay) => tokio::time::sleep(delay).await,
                    Behavior::Honest | Behavior::ForgeResult => {}
                }
                let result_key = match w.behavior {
                    Behavior::ForgeResult => key(9),
                    _ => w.signer.clone(),
                };
                match order(&x.history, &cmd) {
                    Ok(events) => {
                        let msg = signing::result_message(&cmd, x.expected_version, &events);
                        In::Result(WorkResult {
                            lease_id: x.lease_id,
                            events,
                            worker_sig: Some(signing::sign(&result_key, &msg)),
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
    }
}

pub fn signed(mut cmd: Command, key: &SigningKey) -> Command {
    cmd.client_sig = Some(signing::sign(key, &signing::command_message(&cmd)));
    cmd
}

pub async fn submit(addr: &str, cmd: Command) -> Result<CommandResult, tonic::Status> {
    let channel = transport::connect(addr).await.unwrap();
    let result = CommandServiceClient::new(channel).submit(cmd).await?;
    Ok(result.into_inner())
}

pub fn committed_versions(result: &CommandResult) -> Vec<u64> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.iter().map(|e| e.version).collect(),
        other => panic!("expected committed, got {other:?}"),
    }
}
