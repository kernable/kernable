//! Keys: a newer grant changes rights without a restart and ends sessions that lost them; a
//! revoked key is refused and what it signed from its revocation on is flagged; a listener
//! moves to its successor key; the kernel's own key can change.

mod common;

use std::time::Duration;

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ChallengeResponse, Hello, ListenerMessage, Register, RevokeRequest, SubmitRequest, Wait,
        WorkerAggregate, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
        trust_service_client::TrustServiceClient, worker_gateway_client::WorkerGatewayClient,
        worker_message,
    },
    signing, transport,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

/// Revokes `key_id` from `since` on through the kernel's API, signed by `signer`.
async fn revoke_with(
    env: &Env,
    signer: &SigningKey,
    key_id: &str,
    since: i64,
) -> Result<(), tonic::Status> {
    let issued_at = now_ms();
    let request = RevokeRequest {
        key_id: key_id.into(),
        since,
        issued_at,
        root_sig: Some(signing::sign(
            signer,
            &signing::revoke_message(key_id, since, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    TrustServiceClient::new(channel)
        .revoke(request)
        .await
        .map(|_| ())
}

async fn revoke(env: &Env, key_id: &str, since: i64) {
    revoke_with(env, &root(), key_id, since).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_key_is_refused_at_once_and_what_it_signed_is_flagged() {
    let env = start("keys-revoke").await;
    let worker = start_worker(env.addr.clone(), TestWorker::honest()).await;
    // Key 8 submits a command before it is stolen, and one after.
    env.kernel
        .grant(grant(&key(8), &[("submit", &["*"])]))
        .unwrap();
    let before = signed(command("c1", "PlaceOrder", "o1"), &key(8));
    assert_eq!(
        committed_versions(&submit(&env.addr, before).await.unwrap()),
        [1]
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let stolen_at = now_ms();
    let after = signed(command("c2", "AddItem", "o1"), &key(8));
    assert_eq!(
        committed_versions(&submit(&env.addr, after).await.unwrap()),
        [2]
    );

    // Only a root key may revoke.
    let refused = revoke_with(&env, &key(1), &id(&key(8)), stolen_at).await;
    assert_eq!(refused.unwrap_err().code(), Code::PermissionDenied);

    // Revoking the worker key (2) and key 8: the worker's session ends at once.
    revoke(&env, &id(&key(8)), stolen_at).await;
    revoke(&env, &id(&key(2)), now_ms() + 3_600_000).await;
    let ended = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended.code(), Code::PermissionDenied, "{ended:?}");
    let again = signed(command("c3", "AddItem", "o1"), &key(8));
    assert_eq!(
        submit(&env.addr, again).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    let worker = run_worker(env.addr.clone(), TestWorker::honest()).await;
    assert_eq!(worker.code(), Code::PermissionDenied);

    // A revocation is never undone: a new grant for the key is refused.
    let regranted = env.kernel.grant(grant(&key(8), &[("submit", &["*"])]));
    assert_eq!(regranted.unwrap_err().code(), Code::PermissionDenied);

    // verify flags c2 (after the theft), not c1 (before).
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let report = store.verify(&[id(&key(3))]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    assert_eq!(
        report.suspects,
        [format!("command c2: signed by revoked key {}", id(&key(8)))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_of_a_revoked_client_that_wait_in_the_queue_are_rejected() {
    let env = start("keys-pending").await;
    env.kernel
        .grant(grant(&key(8), &[("submit", &["*"])]))
        .unwrap();
    // The worker is away: the command waits in the queue.
    start_worker(env.addr.clone(), TestWorker::honest())
        .await
        .abort();
    let queued = signed(command("c1", "PlaceOrder", "o1"), &key(8));
    let accepted = submit_with(&env.addr, queued.clone(), Wait::Accepted).await;
    assert!(matches!(
        accepted.unwrap().outcome,
        Some(Outcome::Accepted(_))
    ));
    revoke(&env, &id(&key(8)), now_ms() - 60_000).await;

    spawn_worker(&env).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let result = store.result("c1").unwrap().unwrap();
    assert!(
        matches!(&result.outcome, Some(Outcome::Rejected(r)) if r.code == "key_revoked"),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_newer_grant_takes_rights_away_without_a_restart() {
    let env = start("keys-narrower").await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit(&env.addr, place).await.unwrap()),
        [1]
    );
    let narrower = grant(&key(1), &[("submit", &["orders.PlaceOrder"])]);
    env.kernel.grant(narrower).unwrap();
    // The submit carries key 1's older, wider grant: it is ignored.
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    assert_eq!(
        submit(&env.addr, add).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grants_must_be_signed_by_a_root_key_for_the_presenting_key() {
    let env = start("keys-grants").await;
    let mut forged = grant(&key(8), &[("submit", &["*"])]);
    forged.root_sig = Some(signing::sign(&key(1), &signing::grant_message(&forged)));
    assert_eq!(
        env.kernel.grant(forged).unwrap_err().code(),
        Code::PermissionDenied
    );
    let mut tampered = grant(&key(8), &[("submit", &["orders.PlaceOrder"])]);
    tampered.permissions[0].types = vec!["*".into()];
    assert_eq!(
        env.kernel.grant(tampered).unwrap_err().code(),
        Code::Unauthenticated
    );
    let unknown = grant(&key(8), &[("write", &["*"])]);
    assert_eq!(
        env.kernel.grant(unknown).unwrap_err().code(),
        Code::InvalidArgument
    );

    // A request may carry only its signer's grant: key 2 registers with key 1's grant.
    let (tx, rx) = mpsc::channel(1);
    let register = Register {
        protocol: kernable_core::PROTOCOL,
        worker_id: "w".into(),
        key_id: id(&key(2)),
        aggregates: vec![WorkerAggregate {
            aggregate_type: "orders.Order".into(),
            command_types: vec!["orders.PlaceOrder".into()],
        }],
        ..Default::default()
    };
    tx.send(WorkerMessage {
        msg: Some(worker_message::Msg::Register(register)),
    })
    .await
    .unwrap();
    let mut session = tonic::Request::new(ReceiverStream::new(rx));
    *session.metadata_mut() = with_grant((), &key(1)).metadata().clone();
    let mut workers = WorkerGatewayClient::new(transport::connect(&env.addr).await.unwrap());
    let refused = match workers.session(session).await {
        Ok(stream) => stream.into_inner().message().await.unwrap_err(),
        Err(status) => status,
    };
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert_eq!(refused.message(), "the grant is for another key");
}

/// Connects a listener named `name` with `key`; its start position, or why it was refused.
async fn listen(env: &Env, name: &str, listener_key: u8) -> Result<u64, tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: id(&key(listener_key)),
        ..Default::default()
    };
    tx.send(send(In::Hello(hello))).await.unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await?
        .into_inner();
    loop {
        match inbound.message().await?.and_then(|m| m.msg) {
            Some(Out::Challenge(c)) => {
                let sig = signing::sign(&key(listener_key), &signing::challenge_message(&c.nonce));
                let msg = In::ChallengeResponse(ChallengeResponse { sig: sig.sig });
                tx.send(send(msg)).await.unwrap();
            }
            Some(Out::Ready(r)) => return Ok(r.after_position),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_moves_to_its_successor_key_and_keeps_its_place() {
    let env = start("keys-successor").await;
    // Registered with key 5 at position 0, then saved at a later position by the kernel.
    env.kernel
        .grant(grant(&key(6), &[("listen", &["*"])]))
        .unwrap();
    assert_eq!(listen(&env, "audit", 5).await.unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Another key is refused, unless it is the registered key's successor.
    let refused = listen(&env, "audit", 6).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);
    let successor = successor_grant(&key(6), &[("listen", &["*"])], &id(&key(5)));
    env.kernel.grant(successor).unwrap();
    assert_eq!(listen(&env, "audit", 6).await.unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let audit = store.listeners().unwrap();
    assert_eq!(audit[0].key_id, id(&key(6)), "the registration moved");
    // ... and the old key no longer fits.
    assert_eq!(
        listen(&env, "audit", 5).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_key_can_change() {
    let env = start("keys-kernel").await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, place).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await; // a checkpoint with key 3

    std::fs::write(env.dir.join("kernel.key"), hex(key(4).as_bytes())).unwrap();
    env.kernel.reload().unwrap();
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    submit(&env.addr, add).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await; // a checkpoint with key 4

    let store = kernable_store::open_read_only(&env.store).unwrap();
    let old_only = store.verify(&[id(&key(3))]).unwrap();
    assert!(
        old_only
            .problems
            .iter()
            .any(|p| p.contains("signed by unknown key")),
        "{:?}",
        old_only.problems
    );
    let both = store.verify(&[id(&key(3)), id(&key(4))]).unwrap();
    assert!(both.is_ok(), "{:?}", both.problems);
    assert!(both.checkpoints >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sighup_reads_the_kernel_key_of_the_running_kernel_again() {
    use std::process::{Command as Process, Stdio};
    let dir = std::env::temp_dir().join(format!("kn-sighup-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let store = store_url(&dir);
    let mut kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .args([
                "start",
                "--root",
                &id(&root()),
                "--store",
                &store,
                "--listen",
                &addr,
            ])
            .args(["--checkpoint-interval-ms", "50"])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    wait_for(&addr).await;
    // The worker and the client present their grants, as the SDKs do.
    start_worker(addr.clone(), TestWorker::honest()).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit(&addr, place).await.unwrap()),
        [1]
    );
    tokio::time::sleep(Duration::from_millis(200)).await; // a checkpoint with key 3

    std::fs::write(dir.join("kernel.key"), hex(key(4).as_bytes())).unwrap();
    let hup = Process::new("kill")
        .args(["-HUP", &kernel.child.id().to_string()])
        .status();
    assert!(hup.unwrap().success());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    submit(&addr, add).await.unwrap();
    until("a checkpoint with key 4", || {
        let store = kernable_store::open_read_only(&store).unwrap();
        !store.verify(&[id(&key(3))]).unwrap().is_ok()
    })
    .await;
    assert!(kernel.child.try_wait().unwrap().is_none(), "still running");
    drop(kernel);

    let store = kernable_store::open_read_only(&store).unwrap();
    let both = store.verify(&[id(&key(3)), id(&key(4))]).unwrap();
    assert!(both.is_ok(), "{:?}", both.problems);
    let old_only = store.verify(&[id(&key(3))]).unwrap();
    assert!(!old_only.is_ok(), "checkpoints signed by the new key");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn start_dev_creates_keys_and_a_grant_that_allows_everything_and_turns_the_vault_on() {
    use std::process::{Command as Process, Stdio};
    let dir = std::env::temp_dir().join(format!("kn-dev-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let addr = free_addr(); // the default, 127.0.0.1:7233, may be taken
    let kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .args(["start", "--dev", "--listen", &addr])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    wait_for(&addr).await;
    assert!(dir.join("vault.key").exists());
    assert!(dir.join("vault.db").exists(), "the vault is on");

    // app.key, with the app.grant next to it, may submit anything: it passes the grant check
    // and is refused only because no worker registered the aggregate.
    let app = signing::read_signing_key(dir.join("app.key")).unwrap();
    let grant = std::fs::read_to_string(dir.join("app.grant")).unwrap();
    let mut request = tonic::Request::new(SubmitRequest {
        command: Some(signed(command("c1", "PlaceOrder", "o1"), &app)),
        protocol: kernable_core::PROTOCOL,
        wait: Wait::Accepted.into(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("kernable-grant", grant.trim().parse().unwrap());
    let channel = transport::connect(&addr).await.unwrap();
    let status = CommandServiceClient::new(channel)
        .submit(request)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");
    drop(kernel);

    // A vault key that is not 32 bytes in hex stops the start, and says so.
    std::fs::write(dir.join("vault.key"), "abcd\n").unwrap();
    let out = Process::new(env!("CARGO_BIN_EXE_kernable"))
        .current_dir(&dir)
        .args(["start", "--dev", "--listen", &free_addr()])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains("the vault key must be 32 bytes, hex encoded"),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revocation_signed_outside_its_window_is_refused() {
    let env = start("revoke-window").await;
    for skew in [-120_000, 120_000] {
        let issued_at = now_ms() + skew;
        let target = id(&key(1));
        let request = RevokeRequest {
            key_id: target.clone(),
            since: 0,
            issued_at,
            root_sig: Some(signing::sign(
                &root(),
                &signing::revoke_message(&target, 0, issued_at),
            )),
            protocol: kernable_core::PROTOCOL,
        };
        let channel = transport::connect(&env.addr).await.unwrap();
        let refused = TrustServiceClient::new(channel)
            .revoke(request)
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unauthenticated, "{skew}");
    }
}

#[test]
fn the_ui_listens_on_a_loopback_address_only() {
    use std::process::{Command as Process, Stdio};
    let dir = std::env::temp_dir().join(format!("kn-ui-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let log = dir.join("stderr.txt");
    let mut kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .args(["start", "--dev", "--listen", &free_addr()])
            .args(["--ui-listen", "0.0.0.0:0"])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap()),
    );
    // It must stop on its own; a kernel still running after 10 s serves the UI to everyone.
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = kernel.child.try_wait().unwrap() {
            break Some(status);
        }
        if started.elapsed() > Duration::from_secs(10) {
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let stderr = std::fs::read_to_string(&log).unwrap();
    assert!(status.is_some_and(|s| !s.success()), "{stderr}");
    assert!(stderr.contains("loopback address only"), "{stderr}");
    drop(kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A new key's processes connect at the same moment, each presenting the key's grant for the
/// first time: none may be refused while another one's presentation takes effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_grant_presented_by_several_requests_at_once_lets_each_of_them_in() {
    let env = start("grant-race").await;
    for seed in 20..60u8 {
        let client = key(seed);
        let granted = grant(&client, &[("submit", &["orders.*"])]);
        let calls = (0..8).map(|i| {
            let (addr, granted, client) = (env.addr.clone(), granted.clone(), client.clone());
            tokio::spawn(async move {
                let id = format!("k{seed}-{i}");
                let request = SubmitRequest {
                    command: Some(signed(command(&id, "PlaceOrder", &id), &client)),
                    wait: Wait::Committed.into(),
                    protocol: kernable_core::PROTOCOL,
                    ..Default::default()
                };
                let channel = transport::connect(&addr).await.unwrap();
                let mut commands = CommandServiceClient::new(channel);
                let call = commands.submit(presenting(request, &granted));
                tokio::time::timeout(Duration::from_millis(500), call).await
            })
        });
        for call in calls.collect::<Vec<_>>() {
            // No worker runs orders: the command waits (or is refused as unknown), but the key
            // is never refused.
            if let Ok(Err(status)) = call.await.unwrap() {
                assert_ne!(
                    status.code(),
                    Code::PermissionDenied,
                    "key {seed}: {status:?}"
                );
            }
        }
    }
}

/// Tens of thousands of grants: the kernel starts in seconds, not minutes (it built every
/// permission again for each grant it loaded).
#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_with_many_grants_starts_at_once() {
    const N: u32 = 20_000;
    let started = std::time::Instant::now();
    let env = start_wrapped(
        "keys-many-grants",
        |_, _| {},
        |store| {
            for i in 0..N {
                let mut seed = [0u8; 32];
                seed[..4].copy_from_slice(&i.to_be_bytes());
                seed[31] = 77;
                let k = ed25519_dalek::SigningKey::from_bytes(&seed);
                store.save_grant(&grant(&k, &[("query", &["*"])])).unwrap();
            }
            store
        },
    )
    .await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(30), "the start took {took:?}");
    // All of them count.
    let mut seed = [0u8; 32];
    seed[..4].copy_from_slice(&(N - 1).to_be_bytes());
    seed[31] = 77;
    let last = ed25519_dalek::SigningKey::from_bytes(&seed);
    assert!(env.kernel.role().is_object());
    assert!(
        kernable_store::open_read_only(&env.store)
            .unwrap()
            .grants()
            .unwrap()
            .iter()
            .any(|g| g.key_id == id(&last))
    );
}
