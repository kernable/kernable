//! A pair of kernels and its witness: the standby takes the primary's place on its
//! own once the primary is gone, the new primary commits alone without a standby, and the old
//! primary comes back as its standby.

mod common;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::Duration,
};

use common::*;
use kernable_core::proto::{CommandResult, Event, command_result::Outcome};
use kernable_kernel::{
    Config, Kernel,
    config::{Pair, Replication},
    node, serve,
    witness::{self, Witness, WitnessConfig},
};
use prost::Message;
use tokio::{net::UnixListener, sync::oneshot, task::JoinHandle};

const LEASE_MS: u64 = 600;

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A socket in front of a kernel's: cut, every connection through it ends, as when the
/// kernel's machine is gone.
struct Plug {
    accept: JoinHandle<()>,
    open: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl Plug {
    fn new(front: &Path, back: &Path) -> Self {
        let _ = std::fs::remove_file(front);
        let listener = UnixListener::bind(front).unwrap();
        let open: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let (back, kept) = (back.to_owned(), Arc::clone(&open));
        let accept = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                let back = back.clone();
                kept.lock().unwrap().push(tokio::spawn(async move {
                    if let Ok(mut kernel) = tokio::net::UnixStream::connect(&back).await {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut kernel).await;
                    }
                }));
            }
        });
        Plug { accept, open }
    }

    fn cut(&self) {
        self.accept.abort();
        for connection in self.open.lock().unwrap().drain(..) {
            connection.abort();
        }
    }
}

fn node_config(dir: &Path, name: &str, kernel_seed: u8, node_seed: u8) -> Config {
    let kernel_key = dir.join(format!("{name}-kernel.key"));
    std::fs::write(&kernel_key, hex(key(kernel_seed).as_bytes())).unwrap();
    let node_key = dir.join(format!("{name}-node.key"));
    std::fs::write(&node_key, hex(key(node_seed).as_bytes())).unwrap();
    let grant = grant(&key(node_seed), &[("replicate", &["*"])]);
    std::fs::write(
        dir.join(format!("{name}-node.grant")),
        hex(&grant.encode_to_vec()),
    )
    .unwrap();
    let store = match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => format!("file:{}", dir.join(format!("{name}.log")).display()),
        _ => format!("sqlite:{}", dir.join(format!("{name}.db")).display()),
    };
    let listen = format!("unix:{}", dir.join(format!("{name}.sock")).display());
    // The other kernel and every client reach it through its plug.
    let advertise = format!("unix:{}", dir.join(format!("{name}-plug.sock")).display());
    Config {
        roots: vec![id(&root())],
        store,
        listen: listen.clone(),
        kernel_key,
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        settle_timeout_ms: 1_000,
        query_timeout_ms: 1_000,
        middleware_timeout_ms: 1_000,
        drain_timeout_ms: 1_000,
        tls: None,
        snapshots: format!(
            "sqlite:{}",
            dir.join(format!("{name}-snapshots.db")).display()
        ),
        data: format!("file:{}", dir.join(format!("{name}-data")).display()),
        cache: "memory:".into(),
        vault: None,
        metrics_listen: None,
        ui_listen: None,
        durability: kernable_kernel::config::Durability::Normal,
        limits: Default::default(),
        replication: Replication {
            sync: true,
            follow: None,
            pair: Some(Pair {
                witness: format!("unix:{}", dir.join("witness.sock").display()),
                node_key,
                advertise,
                standby_timeout_ms: LEASE_MS,
                kernel_keys: vec![id(&key(3)), id(&key(8))],
            }),
            backup: false,
        },
        key_command: Vec::new(),
        partition: None,
    }
}

/// Runs a kernel of the pair; sends its kernel once it is the primary, and ends with why once
/// the witness deposes it. Aborting the task stops it, server and lease alike: as if its
/// machine was gone.
fn run_node(config: Config) -> (JoinHandle<String>, oneshot::Receiver<Arc<Kernel>>) {
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let primary = node::become_primary(config).await.unwrap();
        let _ = tx.send(Arc::clone(&primary.kernel));
        tokio::select! {
            result = serve(primary.kernel) => panic!("the server stopped: {result:?}"),
            why = primary.lease => why.to_string(),
        }
    });
    (task, rx)
}

fn events(result: &CommandResult) -> Vec<Event> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.clone(),
        other => panic!("not committed: {other:?}"),
    }
}

fn place(i: u32) -> kernable_core::proto::Command {
    signed(
        command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
        &key(1),
    )
}

async fn until(what: &str, done: impl Fn() -> bool) {
    for _ in 0..1_000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting: {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pair_fails_over_on_its_own_and_the_old_primary_comes_back_as_its_standby() {
    let dir = std::env::temp_dir().join(format!("kn-pair-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = Dir(dir);
    let state = dir.0.join("witness.state");
    tokio::spawn(witness::run(WitnessConfig {
        listen: format!("unix:{}", dir.0.join("witness.sock").display()),
        roots: vec![id(&root())],
        state: state.clone(),
        lease_ms: LEASE_MS,
        tls: None,
    }));
    wait_for(&format!("unix:{}", dir.0.join("witness.sock").display())).await;
    let witnessed = || {
        Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state()
    };

    // A asks first and is the primary of term 1; B follows it.
    let (a_config, b_config) = (
        node_config(&dir.0, "a", 3, 11),
        node_config(&dir.0, "b", 8, 12),
    );
    let plug = |name: &str| {
        Plug::new(
            &dir.0.join(format!("{name}-plug.sock")),
            &dir.0.join(format!("{name}.sock")),
        )
    };
    let (a_plug, _b_plug) = (plug("a"), plug("b"));
    let addr = |c: &Config| c.replication.pair.as_ref().unwrap().advertise.clone();
    let (a_addr, b_addr) = (addr(&a_config), addr(&b_config));
    let (a_task, a_primary) = run_node(a_config.clone());
    let a = a_primary.await.unwrap();
    let (_b_task, b_primary) = run_node(b_config.clone());
    for seed in [1, 2] {
        a.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    until("the witness knows B is in sync", || {
        witnessed().in_sync == id(&key(12))
    })
    .await;
    start_worker(a_addr.clone(), TestWorker::honest()).await;
    // An SDK client given both kernels, the standby first: it answers not_primary, and the
    // client turns to the primary by itself.
    let sdk_key = dir.0.join("sdk.key");
    std::fs::write(&sdk_key, hex(key(1).as_bytes())).unwrap();
    std::fs::write(
        dir.0.join("sdk.grant"),
        hex(&default_grant(&key(1)).unwrap().encode_to_vec()),
    )
    .unwrap();
    let sdk =
        kernable::Kernable::new(kernable::Options::new(&sdk_key).url(format!("{b_addr},{a_addr}")))
            .unwrap();
    let sdk_submit = |command: &str| {
        kernable::Submit::new("orders", "Order", "sdk-1", command)
            .data(serde_json::json!({}))
            .wait(kernable::Wait::Committed)
    };
    let placed = sdk.submit(sdk_submit("PlaceOrder")).await.unwrap();
    assert!(
        matches!(placed, kernable::Outcome::Committed(ref e) if e.len() == 1),
        "{placed:?}"
    );
    let mut told = Vec::new();
    for i in 0..10 {
        let result = submit(&a_addr, place(i)).await.unwrap();
        let held = kernable_store::open_read_only(&b_config.store)
            .unwrap()
            .result(&format!("c{i}"))
            .unwrap()
            .expect("the standby holds what a client was told");
        assert_eq!(events(&held), events(&result));
        told.push(result);
    }

    // A's machine is gone: B claims its place once A's lease ran out.
    a_task.abort();
    a_plug.cut();
    let weak_a: Weak<Kernel> = Arc::downgrade(&a);
    drop(a);
    let b = tokio::time::timeout(Duration::from_secs(10), b_primary)
        .await
        .expect("B takes over within seconds")
        .unwrap();
    let kept = witnessed();
    assert_eq!(
        (kept.term, kept.primary.as_str()),
        (2, id(&key(12)).as_str())
    );

    // B has no standby: after --standby-timeout-ms it commits alone, and serves what A told.
    start_worker(b_addr.clone(), TestWorker::honest()).await;
    let again = submit(&b_addr, place(3)).await.unwrap();
    assert_eq!(events(&again), events(&told[3]));
    let added = submit(&b_addr, signed(command("c10", "AddItem", "o3"), &key(1)))
        .await
        .unwrap();
    assert_eq!(committed_versions(&added), [2]);
    assert_eq!(witnessed().in_sync, "", "the witness knows B commits alone");
    // The SDK client finds the new primary on its own: A cannot be reached.
    let added = sdk.submit(sdk_submit("AddItem")).await.unwrap();
    assert!(
        matches!(added, kernable::Outcome::Committed(ref e) if e[0].version == 2),
        "{added:?}"
    );

    // A comes back: the witness names B, so A follows it, catches up, and B waits for it again.
    until("A's stores are closed", || weak_a.strong_count() == 0).await;
    let (_a_task, _a_primary) = run_node(a_config.clone());
    until("the witness knows A is in sync", || {
        witnessed().in_sync == id(&key(11))
    })
    .await;
    let later = submit(&b_addr, place(11)).await.unwrap();
    let held = kernable_store::open_read_only(&a_config.store)
        .unwrap()
        .result("c11")
        .unwrap()
        .expect("A, the standby now, holds what B told");
    assert_eq!(events(&held), events(&later));
    assert!(b.readiness().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_primary_alone_tells_nothing_once_its_lease_runs_out() {
    let dir = std::env::temp_dir().join(format!("kn-pair-alone-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = Dir(dir);
    let state = dir.0.join("witness.state");
    // The kernel reaches the witness through a plug (`witness.sock` in its config).
    tokio::spawn(witness::run(WitnessConfig {
        listen: format!("unix:{}", dir.0.join("witness-real.sock").display()),
        roots: vec![id(&root())],
        state: state.clone(),
        lease_ms: LEASE_MS,
        tls: None,
    }));
    wait_for(&format!(
        "unix:{}",
        dir.0.join("witness-real.sock").display()
    ))
    .await;
    let witness_plug =
        |dir: &Path| Plug::new(&dir.join("witness.sock"), &dir.join("witness-real.sock"));
    let cut = witness_plug(&dir.0);

    let config = node_config(&dir.0, "a", 3, 11);
    let _a_plug = Plug::new(&dir.0.join("a-plug.sock"), &dir.0.join("a.sock"));
    let addr = config.replication.pair.as_ref().unwrap().advertise.clone();
    let (_task, primary) = run_node(config);
    let a = primary.await.unwrap();
    // No standby: after --standby-timeout-ms the primary commits alone.
    for seed in [1, 2] {
        a.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    start_worker(addr.clone(), TestWorker::honest()).await;
    submit(&addr, place(0)).await.unwrap();

    // The witness is out of reach: once the lease ran out, a write is not told.
    cut.cut();
    tokio::time::sleep(Duration::from_millis(LEASE_MS)).await;
    let waiting = tokio::spawn({
        let addr = addr.clone();
        async move { submit(&addr, place(1)).await }
    });
    tokio::time::sleep(Duration::from_millis(3 * LEASE_MS)).await;
    assert!(!waiting.is_finished(), "told without a lease");
    assert!(a.readiness().is_err(), "not ready without a lease");

    // Back in reach, the lease is renewed and the write is told.
    let _back = witness_plug(&dir.0);
    let told = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("told once the lease is back")
        .unwrap()
        .unwrap();
    assert_eq!(committed_versions(&told), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_primary_the_witness_replaced_stops() {
    let dir = std::env::temp_dir().join(format!("kn-pair-deposed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = Dir(dir);
    let state = dir.0.join("witness.state");
    tokio::spawn(witness::run(WitnessConfig {
        listen: format!("unix:{}", dir.0.join("witness.sock").display()),
        roots: vec![id(&root())],
        state: state.clone(),
        lease_ms: LEASE_MS,
        tls: None,
    }));
    wait_for(&format!("unix:{}", dir.0.join("witness.sock").display())).await;
    // A reaches the witness through a plug of its own, and B reaches A through A's plug.
    let mut a_config = node_config(&dir.0, "a", 3, 11);
    a_config.replication.pair.as_mut().unwrap().witness =
        format!("unix:{}", dir.0.join("a-witness.sock").display());
    let b_config = node_config(&dir.0, "b", 8, 12);
    let a_witness = |dir: &Path| Plug::new(&dir.join("a-witness.sock"), &dir.join("witness.sock"));
    let cut_witness = a_witness(&dir.0);
    let a_plug = Plug::new(&dir.0.join("a-plug.sock"), &dir.0.join("a.sock"));
    let (a_task, a_primary) = run_node(a_config);
    let _a = a_primary.await.unwrap();
    let (_b_task, b_primary) = run_node(b_config);
    let witnessed = || {
        Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state()
    };
    until("B is in sync", || witnessed().in_sync == id(&key(12))).await;

    // A still runs, but neither B nor the witness reach it: B takes its place.
    cut_witness.cut();
    a_plug.cut();
    tokio::time::timeout(Duration::from_secs(10), b_primary)
        .await
        .expect("B takes over")
        .unwrap();
    assert!(!a_task.is_finished(), "A runs on, cut off");

    // A reaches the witness again, learns it was replaced, and stops.
    let _back = a_witness(&dir.0);
    let why = tokio::time::timeout(Duration::from_secs(10), a_task)
        .await
        .expect("A stops")
        .unwrap();
    assert!(why.contains("deposed"), "{why}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_standby_is_not_named_in_sync() {
    let dir = std::env::temp_dir().join(format!("kn-pair-revoked-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = Dir(dir);
    let state = dir.0.join("witness.state");
    tokio::spawn(witness::run(WitnessConfig {
        listen: format!("unix:{}", dir.0.join("witness.sock").display()),
        roots: vec![id(&root())],
        state: state.clone(),
        lease_ms: LEASE_MS,
        tls: None,
    }));
    wait_for(&format!("unix:{}", dir.0.join("witness.sock").display())).await;
    let (a_config, b_config) = (
        node_config(&dir.0, "a", 3, 11),
        node_config(&dir.0, "b", 8, 12),
    );
    let _plugs = (
        Plug::new(&dir.0.join("a-plug.sock"), &dir.0.join("a.sock")),
        Plug::new(&dir.0.join("b-plug.sock"), &dir.0.join("b.sock")),
    );
    let (_a_task, a_primary) = run_node(a_config);
    let a = a_primary.await.unwrap();
    let (_b_task, _b_primary) = run_node(b_config);
    let witnessed = || {
        Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state()
    };
    until("B is in sync", || witnessed().in_sync == id(&key(12))).await;
    a.revoke(&id(&key(12)), 0).unwrap();
    until("the witness no longer names B", || {
        witnessed().in_sync.is_empty()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_switchover_hands_the_primary_role_over_without_waiting_for_the_lease() {
    use kernable_core::{
        proto::{SwitchoverRequest, replication_service_client::ReplicationServiceClient},
        signing, transport,
    };
    let dir = std::env::temp_dir().join(format!("kn-pair-switch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = Dir(dir);
    let state = dir.0.join("witness.state");
    tokio::spawn(witness::run(WitnessConfig {
        listen: format!("unix:{}", dir.0.join("witness.sock").display()),
        roots: vec![id(&root())],
        state: state.clone(),
        lease_ms: LEASE_MS,
        tls: None,
    }));
    wait_for(&format!("unix:{}", dir.0.join("witness.sock").display())).await;
    let witnessed = || {
        Witness::open(&state, Duration::from_millis(LEASE_MS))
            .unwrap()
            .state()
    };
    let (a_config, b_config) = (
        node_config(&dir.0, "a", 3, 11),
        node_config(&dir.0, "b", 8, 12),
    );
    let addr = |c: &Config| c.replication.pair.as_ref().unwrap().advertise.clone();
    let (a_addr, b_addr) = (addr(&a_config), addr(&b_config));
    let (a_task, a_primary) = run_node(a_config.clone());
    let a = a_primary.await.unwrap();
    let (_b_task, b_primary) = run_node(b_config.clone());
    for seed in [1, 2] {
        a.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    until("the witness knows B is in sync", || {
        witnessed().in_sync == id(&key(12))
    })
    .await;
    start_worker(a_addr.clone(), TestWorker::honest()).await;
    let mut told = Vec::new();
    for i in 0..5 {
        told.push(submit(&a_addr, place(i)).await.unwrap());
    }

    let issued_at = issued_now();
    let request = SwitchoverRequest {
        issued_at,
        sig: Some(signing::sign(
            &key(1),
            &signing::switchover_message(issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let started = std::time::Instant::now();
    let result = ReplicationServiceClient::new(transport::connect(&a_addr).await.unwrap())
        .switchover(with_grant(request, &key(1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.standby, id(&key(12)));
    let why = a_task.await.unwrap();
    assert!(why.contains("handed over"), "{why}");
    let b = tokio::time::timeout(Duration::from_secs(5), b_primary)
        .await
        .expect("B takes over")
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(LEASE_MS),
        "B serves before A's lease could have run out: {:?}",
        started.elapsed()
    );
    let kept = witnessed();
    assert_eq!(
        (kept.term, kept.primary.as_str()),
        (2, id(&key(12)).as_str())
    );
    // B holds everything A told.
    start_worker(b_addr.clone(), TestWorker::honest()).await;
    for (i, result) in told.iter().enumerate() {
        let again = submit(&b_addr, place(i as u32)).await.unwrap();
        assert_eq!(events(&again), events(result));
    }
    drop((a, b));
}
