//! The vault: personal and secret values stay out of the signed log, reach only the workers
//! and the listeners allowed to read them, and are deleted for real: secrets once their
//! command completes, a subject's values (and the snapshots that may hold them) when an event
//! erases it.

mod common;

use std::{sync::Arc, time::Duration};

use common::*;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, Hello, ListenerMessage, SubmitRequest,
        VaultValue, Wait, command_result::Outcome, command_service_client::CommandServiceClient,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_store::{StreamId, vault::Vault};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

/// A command whose payload keeps `values` in the vault, signed by the client.
fn with_values(id: &str, command_type: &str, order: &str, values: &[&VaultValue]) -> Command {
    let mut cmd = command(id, command_type, order);
    let fields: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(i, v)| format!(r#""f{i}":{{"$vault":"{}"}}"#, v.r#ref))
        .collect();
    cmd.payload = format!("{{{}}}", fields.join(",")).into_bytes();
    cmd.vault_refs = values.iter().map(|v| v.r#ref.clone()).collect();
    signed(cmd, &key(1))
}

async fn submit_values(
    addr: &str,
    cmd: Command,
    values: &[&VaultValue],
) -> Result<CommandResult, tonic::Status> {
    let channel = transport::connect(addr).await.unwrap();
    let request = SubmitRequest {
        command: Some(cmd),
        wait: Wait::Committed.into(),
        protocol: kernable_core::PROTOCOL,
        listener: String::new(),
        vault: values.iter().map(|v| (*v).clone()).collect(),
    };
    Ok(CommandServiceClient::new(channel)
        .submit(request)
        .await?
        .into_inner())
}

fn open_vault(env: &Env) -> Vault {
    let url = format!("sqlite:{}", env.dir.join("vault.db").display());
    Vault::open(&url, &[6; 32]).unwrap()
}

fn on_disk(env: &Env, needle: &str) -> bool {
    std::fs::read_dir(&env.dir).unwrap().any(|f| {
        let bytes = std::fs::read(f.unwrap().path()).unwrap_or_default();
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_keep_personal_and_secret_values_in_the_vault() {
    let env = start_custom("vault-commands", with_vault).await;
    let (tx, mut vaults) = mpsc::unbounded_channel();
    start_worker(
        env.addr.clone(),
        TestWorker {
            vaults: Some(tx),
            ..TestWorker::honest()
        },
    )
    .await;

    let email = vault_value("k-1", "max@firma.de");
    let password = vault_value("", "correct horse");
    let cmd = with_values("c1", "PlaceOrder", "o1", &[&email, &password]);
    let placed = submit_values(&env.addr, cmd, &[&email, &password]).await;
    assert_eq!(committed_versions(&placed.unwrap()), [1]);
    // The worker got both values in clear.
    let sent = vaults.recv().await.unwrap();
    let mut got: Vec<&[u8]> = sent.iter().map(|v| v.value.as_slice()).collect();
    got.sort();
    assert_eq!(got, [&b"\"correct horse\""[..], b"\"max@firma.de\""]);

    // The secret is gone once the command completed; the personal value stays, encrypted.
    let vault = open_vault(&env);
    assert!(
        vault
            .get(std::slice::from_ref(&password.r#ref))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        vault.get(std::slice::from_ref(&email.r#ref)).unwrap().len(),
        1
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!on_disk(&env, "max@firma.de"), "no plain value in any file");
    assert!(!on_disk(&env, "correct horse"));

    // Resubmitting the completed command, without values, returns its stored outcome.
    let again = with_values("c1", "PlaceOrder", "o1", &[&email, &password]);
    let again = submit_values(&env.addr, again, &[]).await.unwrap();
    assert_eq!(committed_versions(&again), [1]);

    // Values must match their signed references, and every reference needs a value.
    let forged = VaultValue {
        value: b"\"eve@evil.example\"".to_vec(),
        ..email.clone()
    };
    let cmd = with_values("c2", "PlaceOrder", "o2", &[&email]);
    let status = submit_values(&env.addr, cmd, &[&forged]).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    let other = vault_value("k-2", "erika@firma.de");
    let cmd = with_values("c3", "PlaceOrder", "o3", &[&other]);
    let status = submit_values(&env.addr, cmd, &[]).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");

    // A kernel without a vault refuses them.
    let plain = start("vault-none").await;
    let cmd = with_values("c4", "PlaceOrder", "o4", &[&other]);
    let status = submit_values(&plain.addr, cmd, &[&other])
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
}

/// Listens with `key`, sending every batch's number of vault values to `counts`.
fn listen(env: &Env, name: &str, listener_key: u8, counts: mpsc::UnboundedSender<usize>) {
    let addr = env.addr.clone();
    let name = name.to_owned();
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name,
            key_id: id(&key(listener_key)),
            event_types: vec!["orders.OrderPlaced".into()],
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = ListenerServiceClient::new(channel)
            .listen(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(msg)) = inbound.message().await {
            let reply = match msg.msg.unwrap() {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key(listener_key), &signing::challenge_message(&c.nonce))
                        .sig,
                }),
                Out::Batch(b) => {
                    let _ = counts.send(b.vault.len());
                    In::Ack(Ack { position: b.up_to })
                }
                _ => continue,
            };
            let _ = tx.send(send(reply)).await;
        }
    });
}

/// A worker whose `PlaceOrder` events keep the customer's email in the vault (subject: the
/// payload's `customer`), and whose `AddItem` events erase the subject in the payload's `erase`.
fn customer_worker(vaults: Option<mpsc::UnboundedSender<Vec<VaultValue>>>) -> TestWorker {
    let writes: VaultWriter = Arc::new(|cmd: &Command| {
        let payload: serde_json::Value = serde_json::from_slice(&cmd.payload).unwrap();
        let field = |f: &str| payload[f].as_str().unwrap_or_default().to_owned();
        match cmd.command_type.as_str() {
            "PlaceOrder" => (
                vec![vault_value(&field("customer"), "x@firma.de")],
                String::new(),
            ),
            _ => (Vec::new(), field("erase")),
        }
    });
    TestWorker {
        behavior: Behavior::Snapshots,
        writes_vault: Some(writes),
        vaults,
        ..TestWorker::honest()
    }
}

fn order(id: &str, command_type: &str, order: &str, payload: &str) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.payload = payload.as_bytes().to_vec();
    signed(cmd, &key(1))
}

fn snapshot_of(env: &Env, order: &str) -> bool {
    let stream = StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: order.into(),
    };
    let snapshots = kernable_store::snapshots::open(&env.snapshots).unwrap();
    snapshots.get(&stream).unwrap().is_some()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erasing_event_deletes_a_subject_and_its_snapshots_everywhere() {
    let env = start_custom("vault-erase", with_vault).await;
    let eraser = grant(
        &key(2),
        &[("run", &["orders.*"]), ("vault_erase", &["orders.*"])],
    );
    env.kernel.grant(eraser).unwrap();
    let (tx, mut vaults) = mpsc::unbounded_channel();
    start_worker(env.addr.clone(), customer_worker(Some(tx))).await;
    let (reader_tx, mut reader) = mpsc::unbounded_channel();
    let (other_tx, mut other) = mpsc::unbounded_channel();
    listen(&env, "reader", 5, reader_tx);
    listen(&env, "not-a-reader", 6, other_tx);
    tokio::time::sleep(Duration::from_millis(100)).await;

    for (id, o, customer) in [
        ("c1", "o1", "k-1"),
        ("c2", "o2", "k-1"),
        ("c3", "o3", "k-2"),
    ] {
        let cmd = order(
            id,
            "PlaceOrder",
            o,
            &format!(r#"{{"customer":"{customer}"}}"#),
        );
        assert_eq!(
            committed_versions(&submit(&env.addr, cmd).await.unwrap()),
            [1]
        );
        assert_eq!(reader.recv().await, Some(1), "a reader gets the value");
        assert_eq!(other.recv().await, Some(0), "others get only the reference");
    }
    assert!(snapshot_of(&env, "o1") && snapshot_of(&env, "o2") && snapshot_of(&env, "o3"));
    while vaults.try_recv().is_ok() {}

    // Erasing k-1 from order o1's stream deletes k-1's values and the snapshots of every
    // stream that used them (o1 and o2), not those of others.
    let erase = order("c4", "AddItem", "o1", r#"{"erase":"k-1"}"#);
    assert_eq!(
        committed_versions(&submit(&env.addr, erase).await.unwrap()),
        [2]
    );
    assert!(!snapshot_of(&env, "o1") && !snapshot_of(&env, "o2"));
    assert!(snapshot_of(&env, "o3"));
    vaults.recv().await.unwrap();

    // The streams are read again in full, without the erased values.
    let add = order("c5", "AddItem", "o2", "{}");
    assert_eq!(
        committed_versions(&submit(&env.addr, add).await.unwrap()),
        [2]
    );
    assert!(vaults.recv().await.unwrap().is_empty());
    let add = order("c6", "AddItem", "o3", "{}");
    submit(&env.addr, add).await.unwrap();
    assert!(
        vaults.recv().await.is_some_and(|v| v.is_empty()),
        "o3 has a snapshot: no history"
    );
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let refs: Vec<String> = store
        .read_all(0, 10)
        .unwrap()
        .iter()
        .flat_map(|e| e.vault_refs.clone())
        .collect();
    assert_eq!(
        open_vault(&env).get(&refs).unwrap().len(),
        1,
        "only k-2's value is left"
    );
    let report = store.verify(&[id(&key(3)).to_string()]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_allowed_workers_may_erase() {
    let env = start_custom("vault-no-erase", with_vault).await;
    start_worker(env.addr.clone(), customer_worker(None)).await;
    let place = order("c1", "PlaceOrder", "o1", r#"{"customer":"k-1"}"#);
    submit(&env.addr, place).await.unwrap();
    let erase = order("c2", "AddItem", "o1", r#"{"erase":"k-1"}"#);
    let refused = submit(&env.addr, erase).await.unwrap();
    assert!(
        matches!(&refused.outcome, Some(Outcome::Rejected(r)) if r.code == "erase_not_allowed"),
        "{refused:?}"
    );
}
