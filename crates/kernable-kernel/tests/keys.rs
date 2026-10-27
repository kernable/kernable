//! Keys: a reload changes allowlists without a restart and ends sessions that lost their
//! rights; a revoked key is refused and what it signed from its revocation on is flagged; a
//! listener moves to its successor key; the kernel's own key can change.

mod common;

use std::time::Duration;

use common::*;
use kernable_core::{
    proto::{
        ChallengeResponse, Hello, ListenerMessage, Wait, command_result::Outcome,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_kernel::Config;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

fn config(env: &Env) -> Config {
    (*env.kernel.config()).clone()
}

/// `ms` since the epoch as a UTC time, to the second, for `[revoked]`.
fn utc(ms: i64) -> String {
    let secs = ms / 1000;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_key_is_refused_at_once_and_what_it_signed_is_flagged() {
    let env = start("keys-revoke").await;
    let worker = tokio::spawn(run_worker(env.addr.clone(), TestWorker::honest()));
    // Key 8 submits a command before it is stolen, and one after.
    let mut allowed = config(&env);
    allowed.clients.insert(id(&key(8)), vec!["*".into()]);
    env.kernel.reload(allowed.clone()).unwrap();
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

    // Revoking the worker key (2) and key 8: the worker's session ends at once.
    let mut revoked = allowed;
    revoked.revoked.insert(id(&key(8)), utc(stolen_at));
    revoked
        .revoked
        .insert(id(&key(2)), utc(now_ms() + 3_600_000));
    env.kernel.reload(revoked.clone()).unwrap();
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

    // Revocations are kept: taking them out of the configuration does not undo them.
    let mut forgiven = revoked;
    forgiven.revoked.clear();
    env.kernel.reload(forgiven).unwrap();
    let again = signed(command("c4", "AddItem", "o1"), &key(8));
    assert_eq!(
        submit(&env.addr, again).await.unwrap_err().code(),
        Code::PermissionDenied
    );

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
    let mut allowed = config(&env);
    allowed.clients.insert(id(&key(8)), vec!["*".into()]);
    env.kernel.reload(allowed.clone()).unwrap();
    // No worker yet: the command waits in the queue.
    let queued = signed(command("c1", "PlaceOrder", "o1"), &key(8));
    let accepted = submit_with(&env.addr, queued.clone(), Wait::Accepted).await;
    assert!(matches!(
        accepted.unwrap().outcome,
        Some(Outcome::Accepted(_))
    ));
    let mut revoked = allowed;
    revoked.revoked.insert(id(&key(8)), utc(now_ms() - 60_000));
    env.kernel.reload(revoked).unwrap();

    spawn_worker(&env);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let result = store.result("c1").unwrap().unwrap();
    assert!(
        matches!(&result.outcome, Some(Outcome::Rejected(r)) if r.code == "key_revoked"),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_takes_rights_away_without_a_restart() {
    let env = start("keys-reload").await;
    spawn_worker(&env);
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit(&env.addr, place).await.unwrap()),
        [1]
    );
    let mut narrower = config(&env);
    narrower
        .clients
        .insert(id(&key(1)), vec!["orders.PlaceOrder".into()]);
    let kept = env.kernel.reload(narrower).unwrap();
    assert!(kept.is_empty(), "{kept:?}");
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    assert_eq!(
        submit(&env.addr, add).await.unwrap_err().code(),
        Code::PermissionDenied
    );

    // Settings that need a restart are kept, and reported.
    let mut moved = config(&env);
    moved.listen = "unix:/elsewhere.sock".into();
    assert_eq!(env.kernel.reload(moved).unwrap(), ["listen"]);
    assert_eq!(env.kernel.config().listen, env.addr);
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
    let mut registered = env.kernel.config().as_ref().clone();
    registered.listeners.insert(id(&key(6)), vec!["*".into()]);
    env.kernel.reload(registered.clone()).unwrap();
    assert_eq!(listen(&env, "audit", 5).await.unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Another key is refused, unless it is the registered key's successor.
    let refused = listen(&env, "audit", 6).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);
    let mut successor = registered;
    successor.successors.insert(id(&key(5)), id(&key(6)));
    env.kernel.reload(successor).unwrap();
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
    spawn_worker(&env);
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, place).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await; // a checkpoint with key 3

    std::fs::write(env.dir.join("kernel-2.key"), hex(key(4).as_bytes())).unwrap();
    let mut rotated = config(&env);
    rotated.kernel_key = env.dir.join("kernel-2.key");
    env.kernel.reload(rotated).unwrap();
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
async fn sighup_reloads_the_configuration_of_the_running_kernel() {
    use std::process::{Command as Process, Stdio};
    let dir = std::env::temp_dir().join(format!("kn-sighup-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("k.sock").display());
    let toml = |clients: &str| {
        format!(
            "store = \"{}\"\nlisten = \"{addr}\"\nkernel_key = \"kernel.key\"\n\
             [clients]\n{clients}\n[workers]\n\"{}\" = [\"orders.*\"]\n",
            store_url(&dir),
            id(&key(2)),
        )
    };
    std::fs::write(
        dir.join("kernable.toml"),
        toml(&format!("\"{}\" = [\"*\"]", id(&key(1)))),
    )
    .unwrap();
    let mut kernel = Process::new(env!("CARGO_BIN_EXE_kernable-kernel"))
        .arg(dir.join("kernable.toml"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(&addr).await;
    tokio::spawn(run_worker(addr.clone(), TestWorker::honest()));
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit(&addr, place).await.unwrap()),
        [1]
    );

    std::fs::write(dir.join("kernable.toml"), toml("")).unwrap();
    let hup = Process::new("kill")
        .args(["-HUP", &kernel.id().to_string()])
        .status();
    assert!(hup.unwrap().success());
    let refused = async {
        loop {
            let add = signed(command("c2", "AddItem", "o1"), &key(1));
            match submit(&addr, add).await {
                Err(status) if status.code() == Code::PermissionDenied => return,
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), refused)
        .await
        .expect("reloaded");
    assert!(kernel.try_wait().unwrap().is_none(), "still running");
    let _ = kernel.kill();
    let _ = kernel.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
