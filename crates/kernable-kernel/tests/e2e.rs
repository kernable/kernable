//! End to end: a kernel on a Unix socket, Rust test workers, signed commands.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

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
use kernable_store_sqlite::SqliteStore;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn id(key: &SigningKey) -> String {
    signing::key_id(&key.verifying_key())
}

struct Env {
    dir: PathBuf,
    addr: String,
    kernel_key: SigningKey,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Starts a kernel that trusts client key 1 for everything and worker key 2 for Order commands.
async fn start(name: &str) -> Env {
    let dir = std::env::temp_dir().join(format!("kernable-e2e-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kernel_key = key(3);
    std::fs::write(dir.join("kernel.key"), hex(kernel_key.as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let config = Config {
        db: dir.join("k.db"),
        listen: addr.clone(),
        kernel_key: dir.join("kernel.key"),
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        clients: HashMap::from([(id(&key(1)), vec!["*".into()])]),
        workers: HashMap::from([(id(&key(2)), vec!["PlaceOrder".into(), "AddItem".into()])]),
    };
    tokio::spawn(serve(Kernel::open(config).unwrap()));
    for _ in 0..100 {
        if transport::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Env {
        dir,
        addr,
        kernel_key,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Order domain as a plain function of history and command.
fn order(history: &[Event], cmd: &Command) -> Result<Vec<NewEvent>, Rejected> {
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

/// Minimal worker on raw generated stubs; `signer` may differ from `claimed` to test forgery.
/// Returns the first error the kernel sends on the session stream.
async fn run_worker(
    addr: String,
    claimed: SigningKey,
    signer: SigningKey,
    command_types: Vec<String>,
) -> tonic::Status {
    let (tx, rx) = mpsc::channel(16);
    let send = |msg: In| WorkerMessage { msg: Some(msg) };
    tx.send(send(In::Register(Register {
        worker_id: "test-worker".into(),
        key_id: id(&claimed),
        command_types,
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
                sig: signing::sign(&signer, &signing::challenge_message(&c.nonce)).sig,
            }),
            Out::Registered(_) => continue,
            Out::Execute(x) => {
                let cmd = x.command.unwrap();
                match order(&x.history, &cmd) {
                    Ok(events) => {
                        let msg = signing::result_message(&cmd, x.expected_version, &events);
                        In::Result(WorkResult {
                            lease_id: x.lease_id,
                            events,
                            worker_sig: Some(signing::sign(&signer, &msg)),
                        })
                    }
                    Err(reason) => {
                        let msg = signing::rejection_message(&cmd.command_id, &reason);
                        In::Rejection(Rejection {
                            lease_id: x.lease_id,
                            reason: Some(reason),
                            worker_sig: Some(signing::sign(&signer, &msg)),
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

fn spawn_worker(env: &Env) {
    let types = vec!["PlaceOrder".into(), "AddItem".into()];
    tokio::spawn(run_worker(env.addr.clone(), key(2), key(2), types));
}

fn command(id: &str, command_type: &str, aggregate_id: &str) -> Command {
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

fn signed(mut cmd: Command, key: &SigningKey) -> Command {
    cmd.client_sig = Some(signing::sign(key, &signing::command_message(&cmd)));
    cmd
}

async fn submit(env: &Env, cmd: Command) -> Result<CommandResult, tonic::Status> {
    let channel = transport::connect(&env.addr).await.unwrap();
    let result = CommandServiceClient::new(channel).submit(cmd).await?;
    Ok(result.into_inner())
}

fn committed_versions(result: &CommandResult) -> Vec<u64> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.iter().map(|e| e.version).collect(),
        other => panic!("expected committed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_commands_run_end_to_end() {
    let env = start("happy").await;
    spawn_worker(&env);
    let client = key(1);

    let placed = submit(&env, signed(command("c1", "PlaceOrder", "o1"), &client))
        .await
        .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    let added = submit(&env, signed(command("c2", "AddItem", "o1"), &client))
        .await
        .unwrap();
    assert_eq!(committed_versions(&added), [2]);

    // Resubmitting returns the stored result instead of running again.
    let again = submit(&env, signed(command("c2", "AddItem", "o1"), &client))
        .await
        .unwrap();
    assert_eq!(again, added);

    // Business rejection, signed by the worker.
    let dup = submit(&env, signed(command("c3", "PlaceOrder", "o1"), &client))
        .await
        .unwrap();
    assert!(matches!(dup.outcome, Some(Outcome::Rejected(r)) if r.code == "invalid_state"));

    // The log the kernel wrote verifies, including a kernel checkpoint.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let store = SqliteStore::open(env.dir.join("k.db")).unwrap();
    let report = store.verify(&id(&env.kernel_key)).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    assert_eq!(report.events, 2);
    assert!(report.checkpoints >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn unsigned_forged_and_unauthorized_commands_are_refused() {
    let env = start("clients").await;
    spawn_worker(&env);

    let unsigned = submit(&env, command("c1", "PlaceOrder", "o1"))
        .await
        .unwrap_err();
    assert_eq!(unsigned.code(), Code::Unauthenticated);

    let mut tampered = signed(command("c2", "PlaceOrder", "o1"), &key(1));
    tampered.payload = b"{\"id\":\"evil\"}".to_vec();
    assert_eq!(
        submit(&env, tampered).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let stranger = signed(command("c3", "PlaceOrder", "o1"), &key(9));
    assert_eq!(
        submit(&env, stranger).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthorized_and_impostor_workers_are_refused() {
    let env = start("workers").await;
    let types = || vec!["PlaceOrder".to_string()];

    let unknown = run_worker(env.addr.clone(), key(9), key(9), types()).await;
    assert_eq!(unknown.code(), Code::PermissionDenied);

    let too_much = run_worker(env.addr.clone(), key(2), key(2), vec!["DeleteAll".into()]).await;
    assert_eq!(too_much.code(), Code::PermissionDenied);

    // Claims the trusted worker key but cannot sign with it.
    let impostor = run_worker(env.addr.clone(), key(2), key(9), types()).await;
    assert_eq!(impostor.code(), Code::Unauthenticated);
}

/// Many concurrent commands on one aggregate across two workers: conflicts are retried,
/// and whatever is committed forms a gap-free stream.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commands_on_one_aggregate_lose_no_updates() {
    let env = Arc::new(start("concurrent").await);
    spawn_worker(&env);
    spawn_worker(&env);
    let client = key(1);
    submit(&env, signed(command("c0", "PlaceOrder", "o1"), &client))
        .await
        .unwrap();

    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for i in 1..=20 {
        let (env, outcomes, cmd) = (
            Arc::clone(&env),
            Arc::clone(&outcomes),
            signed(command(&format!("c{i}"), "AddItem", "o1"), &client),
        );
        tasks.push(tokio::spawn(async move {
            let result = submit(&env, cmd).await.unwrap();
            outcomes.lock().unwrap().push(result.outcome.unwrap());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let outcomes = outcomes.lock().unwrap();
    let committed = outcomes
        .iter()
        .filter(|o| matches!(o, Outcome::Committed(_)))
        .count();
    assert!(outcomes.iter().all(|o| match o {
        Outcome::Committed(_) => true,
        Outcome::Rejected(r) => r.code == "concurrency_conflict",
    }));
    assert!(committed > 0);

    let store = SqliteStore::open(env.dir.join("k.db")).unwrap();
    let versions: Vec<u64> = store
        .read_stream("t1", "Order", "o1")
        .unwrap()
        .iter()
        .map(|e| e.version)
        .collect();
    assert_eq!(versions, (1..=committed as u64 + 1).collect::<Vec<_>>());
}
