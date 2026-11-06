//! A primary with `--replication sync` and its standby (design/replication.md): every
//! write a client is told of is on the standby first (P1), the standby refuses clients, and a
//! promoted standby serves what the primary held, with the grants it got from it. Without a
//! witness the primary serves alone until its standby caught up, and hands its role over to it
//! on a switchover.

mod common;

use std::{path::Path, sync::Arc, time::Duration};

use common::*;
use kernable_core::{
    proto::{
        Command, CommandResult, DataRequest, Event, PromoteRequest, PromoteResult,
        command_result::Outcome, data_service_client::DataServiceClient,
        replication_service_client::ReplicationServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{
    Config, Kernel,
    config::{Follow, Replication},
    serve, standby,
};
use kernable_store::{
    Store,
    data::{Data, DataKey},
    invariants::Log,
};
use prost::Message;
use tonic::Code;

struct Dir(std::path::PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("kn-repl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A kernel's configuration in `dir`, its files named after `name`.
fn config(dir: &Path, name: &str, kernel_seed: u8) -> Config {
    let kernel_key = dir.join(format!("{name}-kernel.key"));
    std::fs::write(&kernel_key, hex(key(kernel_seed).as_bytes())).unwrap();
    let store = match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => format!("file:{}", dir.join(format!("{name}.log")).display()),
        _ => format!("sqlite:{}", dir.join(format!("{name}.db")).display()),
    };
    Config {
        roots: vec![id(&root())],
        store,
        listen: format!("unix:{}", dir.join(format!("{name}.sock")).display()),
        kernel_key,
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        settle_timeout_ms: 1_000,
        query_timeout_ms: 1_000,
        middleware_timeout_ms: 1_000,
        drain_timeout_ms: 1_000,
        take_over_ms: 0,
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
        replication: Replication::default(),
        key_command: Vec::new(),
        partition: None,
    }
}

/// The standby's configuration: it follows `primary`, with a key granted `replicate`.
fn standby_config(dir: &Path, primary: &Config) -> Config {
    let mut config = config(dir, "standby", 8);
    let standby = key(7);
    let key_path = dir.join("standby.key");
    std::fs::write(&key_path, hex(standby.as_bytes())).unwrap();
    let grant = grant(&standby, &[("replicate", &["*"])]);
    std::fs::write(dir.join("standby.grant"), hex(&grant.encode_to_vec())).unwrap();
    config.replication.follow = Some(Follow {
        primary: primary.listen.clone(),
        key: key_path,
        primary_kernel_keys: vec![id(&key(3))],
    });
    config
}

/// The primary told the standby it is in sync: every write from then on waits for it.
fn in_sync(standby: &Config) -> bool {
    let (_, path) = kernable_store::backend(&standby.store);
    Path::new(&format!("{path}.in-sync")).exists()
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

fn events(result: &CommandResult) -> Vec<Event> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.clone(),
        other => panic!("not committed: {other:?}"),
    }
}

fn place(i: u32) -> Command {
    signed(
        command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
        &key(1),
    )
}

/// What the standby holds now, read next to it.
fn held(standby: &Config) -> Box<dyn Store> {
    kernable_store::open_read_only(&standby.store).unwrap()
}

async fn promote(addr: &str) -> Result<PromoteResult, tonic::Status> {
    let issued_at = issued_now();
    let request = PromoteRequest {
        issued_at,
        sig: Some(signing::sign(&key(1), &signing::promote_message(issued_at))),
        protocol: kernable_core::PROTOCOL,
    };
    ReplicationServiceClient::new(transport::connect(addr).await.unwrap())
        .promote(request)
        .await
        .map(tonic::Response::into_inner)
}

/// Puts (or with `None`, deletes) file `name` of domain `shop` through the data service.
async fn file(addr: &str, name: &str, bytes: Option<&[u8]>) {
    let op = if bytes.is_some() { "put" } else { "delete" };
    let mut r = DataRequest {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: name.into(),
        data: bytes.unwrap_or_default().to_vec(),
        content_type: if bytes.is_some() {
            "text/plain".into()
        } else {
            String::new()
        },
        issued_at: issued_now(),
        client_sig: None,
        protocol: kernable_core::PROTOCOL,
        context: None,
        after: String::new(),
    };
    r.client_sig = Some(signing::sign(&key(9), &signing::data_message(op, &r)));
    let mut client = DataServiceClient::new(transport::connect(addr).await.unwrap());
    match bytes {
        Some(_) => client.put_data(r).await.unwrap(),
        None => client.delete_data(r).await.unwrap(),
    };
}

fn shop(name: &str) -> DataKey {
    DataKey {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: name.into(),
    }
}

/// Starts a kernel on `config` and serves it.
async fn open(config: &Config) -> Arc<Kernel> {
    let kernel = Kernel::open(config.clone()).unwrap();
    tokio::spawn(serve(Arc::clone(&kernel)));
    wait_for(&config.listen).await;
    kernel
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_holds_every_write_a_client_was_told_and_takes_over() {
    let dir = Dir::new("follow");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    assert!(
        primary.readiness().is_ok(),
        "ready without a standby: it serves alone until one caught up"
    );

    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2, 4, 5, 6] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    primary
        .grant(grant(&key(9), &[("data_write", &["shop.*"])]))
        .unwrap();
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;

    // Files of the data service: what the client was told is on the standby.
    file(&primary_config.listen, "invoice", Some(b"v1")).await;
    file(&primary_config.listen, "draft", Some(b"x")).await;
    file(&primary_config.listen, "invoice", Some(b"v2")).await;
    file(&primary_config.listen, "draft", None).await;
    let files = kernable_store::data::open(&standby_config.data).unwrap();
    assert_eq!(
        files.get(&shop("invoice")).unwrap(),
        Some(Data {
            content_type: "text/plain".into(),
            data: b"v2".to_vec()
        })
    );
    assert_eq!(files.get(&shop("draft")).unwrap(), None);

    let mut told = Vec::new();
    for i in 0..20 {
        let result = submit(&primary_config.listen, place(i)).await.unwrap();
        // P1: what the client was told, the standby holds.
        let stored = held(&standby_config)
            .result(&format!("c{i}"))
            .unwrap()
            .expect("the standby holds the command's result");
        assert_eq!(events(&stored), events(&result), "command c{i}");
        told.push(result);
    }

    // The standby serves no client.
    let refused = submit(&standby_config.listen, place(99)).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("not_primary"), "{refused:?}");

    // Promoted, it opens what it holds as a kernel does after a restart.
    let promoted = promote(&standby_config.listen).await.unwrap();
    assert!(promoted.seq > 0);
    following.await.unwrap().unwrap();
    let new = open(&standby_config).await;
    // The grants came with the records: the worker's key registers without a new grant.
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    let again = submit(&standby_config.listen, place(3)).await.unwrap();
    assert_eq!(
        events(&again),
        events(&told[3]),
        "a resubmit gets the stored result"
    );
    let added = submit(
        &standby_config.listen,
        signed(command("c20", "AddItem", "o3"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&added), [2], "the stream goes on");

    let ids = [id(&key(3)), id(&key(8))];
    let (old, now) = (
        Log::read(
            &*kernable_store::open_read_only(&primary_config.store).unwrap(),
            &ids,
        )
        .unwrap(),
        Log::read(&*held(&standby_config), &ids).unwrap(),
    );
    assert_eq!(old.events[..], now.events[..old.events.len()]);
    assert!(now.check().is_empty(), "{:?}", now.check());
    assert!(new.readiness().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_of_a_primary_that_ran_alone_takes_an_image_and_follows() {
    let dir = Dir::new("image");
    let mut primary_config = config(&dir.0, "primary", 3);
    // Written before replication was on: with SQLite, no standby can follow from the start.
    {
        let store = kernable_store::open(&primary_config.store).unwrap();
        for seed in [1, 2, 4, 5, 6] {
            store
                .save_grant(&default_grant(&key(seed)).unwrap())
                .unwrap();
        }
        for i in 0..5 {
            let cmd = place(i);
            store.enqueue(&cmd, 0).unwrap();
            let events = order(false, &cmd).unwrap();
            store
                .commit(&cmd.command_id, 0, &events, &Default::default(), None, 1)
                .unwrap();
        }
    }
    primary_config.replication.sync = true;
    let _primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let _following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    for i in 5..10 {
        let result = submit(&primary_config.listen, place(i)).await.unwrap();
        let stored = held(&standby_config)
            .result(&format!("c{i}"))
            .unwrap()
            .expect("held");
        assert_eq!(events(&stored), events(&result));
    }
    let ids = [id(&key(3))];
    let (p, s) = (
        Log::read(
            &*kernable_store::open_read_only(&primary_config.store).unwrap(),
            &ids,
        )
        .unwrap(),
        Log::read(&*held(&standby_config), &ids).unwrap(),
    );
    assert_eq!(p.events, s.events);
    assert_eq!(p.results, s.results);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_without_a_grant_for_replicate_is_refused() {
    let dir = Dir::new("refused");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let _primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    // Its grant allows something else.
    let grant = grant(&key(7), &[("listen", &["*"])]);
    std::fs::write(dir.0.join("standby.grant"), hex(&grant.encode_to_vec())).unwrap();
    let following = tokio::spawn(standby::run(standby_config.clone()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !in_sync(&standby_config),
        "a standby refused is never in sync"
    );
    // And a standby that never followed cannot be promoted.
    let refused = promote(&standby_config.listen).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    following.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_whose_log_diverged_moves_its_stores_aside_and_starts_over() {
    let dir = Dir::new("diverged");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    // The standby holds a history the primary never had (a former primary's own tail).
    {
        let store = kernable_store::open(&standby_config.store).unwrap();
        store.keep_record_log(true).unwrap();
        store.enqueue(&place(77), 0).unwrap();
    }
    let _following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    submit(&primary_config.listen, place(1)).await.unwrap();

    let standby = held(&standby_config);
    assert_eq!(
        standby.command("c77").unwrap(),
        None,
        "its own tail is gone"
    );
    assert!(
        standby.result("c1").unwrap().is_some(),
        "it follows the primary"
    );
    let aside: Vec<String> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".diverged-"))
        .collect();
    assert!(
        aside.iter().any(|n| n.starts_with("standby.")),
        "the diverged store is kept aside: {aside:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_whose_key_is_revoked_follows_no_longer() {
    let dir = Dir::new("revoked");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    primary.revoke(&id(&key(7)), 0).unwrap();
    until("the revoked standby is no longer in sync", || {
        primary.readiness().is_err()
    })
    .await;
    following.abort();
}

/// A socket in front of the primary's, for the standby: held, nothing it sends reaches the
/// primary until it is let go.
struct Gate {
    held: tokio::sync::watch::Sender<bool>,
}

impl Gate {
    fn new(front: &Path, back: &Path) -> Self {
        let listener = tokio::net::UnixListener::bind(front).unwrap();
        let (held, watch) = tokio::sync::watch::channel(false);
        let back = back.to_owned();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(kernel) = tokio::net::UnixStream::connect(&back).await else {
                    return;
                };
                let (mut from_standby, to_kernel) = (client.into_split(), kernel.into_split());
                let mut watch = watch.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let (mut to_kernel, mut buf) = (to_kernel.1, vec![0u8; 64 << 10]);
                    while let Ok(n @ 1..) = from_standby.0.read(&mut buf).await {
                        if watch.wait_for(|held| !held).await.is_err()
                            || to_kernel.write_all(&buf[..n]).await.is_err()
                        {
                            return;
                        }
                    }
                });
                let (mut from_kernel, mut to_standby) = (to_kernel.0, from_standby.1);
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut from_kernel, &mut to_standby).await;
                });
            }
        });
        Gate { held }
    }
}

/// A membership waits for the standby under the lock that keeps memberships in order; a second
/// one waits for that lock. On a primary of one worker thread, the second must not take the
/// thread that reads the standby's acknowledgements from the network with it: both are applied
/// once the standby acknowledges.
#[tokio::test(flavor = "multi_thread")]
async fn a_membership_waiting_for_the_standby_does_not_stall_the_primary() {
    let dir = Dir::new("one-worker");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let primary = {
        let _primary_runtime = runtime.enter();
        Kernel::open(primary_config.clone()).unwrap()
    };
    runtime.spawn(serve(Arc::clone(&primary)));
    wait_for(&primary_config.listen).await;

    let mut standby_config = standby_config(&dir.0, &primary_config);
    let gate_path = dir.0.join("gate.sock");
    let gate = Gate::new(
        &gate_path,
        Path::new(primary_config.listen.trim_start_matches("unix:")),
    );
    if let Some(follow) = standby_config.replication.follow.as_mut() {
        follow.primary = format!("unix:{}", gate_path.display());
    }
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    // Key 1's grant is stored with the first membership.
    apply(&primary_config.listen, membership("first", &[], &[], &[]))
        .await
        .unwrap();

    gate.held.send_replace(true);
    let (addr_a, addr_b) = (primary_config.listen.clone(), primary_config.listen.clone());
    let a = tokio::spawn(async move { apply(&addr_a, membership("a", &[], &[], &[])).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b = tokio::spawn(async move { apply(&addr_b, membership("b", &[], &[], &[])).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.held.send_replace(false);
    for (name, apply) in [("a", a), ("b", b)] {
        match tokio::time::timeout(Duration::from_secs(20), apply).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => panic!("membership {name} not applied in 20 s: the primary stalled"),
        }
    }
    following.abort();
    runtime.shutdown_background();
}

/// Without a witness, a primary serves alone until its standby has caught up; from then
/// on every write waits for it, and only a standby the primary told so may be promoted.
#[tokio::test(flavor = "multi_thread")]
async fn a_primary_without_a_witness_serves_alone_until_its_standby_caught_up() {
    let dir = Dir::new("catch-up");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    for seed in [1, 2] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    // No standby yet: writes are told without one.
    for i in 0..5 {
        submit(&primary_config.listen, place(i)).await.unwrap();
    }

    let standby_config = standby_config(&dir.0, &primary_config);
    // A runtime of its own: shut down, it ends the standby as its process would.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.spawn(standby::run(standby_config.clone()));
    until("the standby caught up", || in_sync(&standby_config)).await;
    for i in 0..5 {
        assert!(
            held(&standby_config)
                .result(&format!("c{i}"))
                .unwrap()
                .is_some(),
            "it holds c{i}, told alone"
        );
    }
    // From now on, what a client is told the standby holds first (P1).
    for i in 5..10 {
        let result = submit(&primary_config.listen, place(i)).await.unwrap();
        let stored = held(&standby_config)
            .result(&format!("c{i}"))
            .unwrap()
            .expect("held before it was told");
        assert_eq!(events(&stored), events(&result));
    }
    runtime.shutdown_background();

    // A standby never told it is in sync is not promoted, though it holds records: its primary
    // may have told writes it lacks. Here: its note gone, its primary out of reach.
    let (_, path) = kernable_store::backend(&standby_config.store);
    std::fs::remove_file(format!("{path}.in-sync")).unwrap();
    let mut alone = standby_config.clone();
    alone.listen = format!("unix:{}", dir.0.join("standby-2.sock").display());
    if let Some(follow) = alone.replication.follow.as_mut() {
        follow.primary = format!("unix:{}", dir.0.join("nowhere.sock").display());
    }
    // Shut down in the background, the first runtime lets go of the stores a moment later.
    let mut waiting = tokio::spawn(standby::run(alone.clone()));
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !waiting.is_finished() {
            break;
        }
        waiting = tokio::spawn(standby::run(alone.clone()));
    }
    wait_for(&alone.listen).await;
    let refused = promote(&alone.listen).await.unwrap_err();
    assert!(refused.message().contains("never caught up"), "{refused:?}");
    // Told so before it lost its primary, it is: the note stays when it starts again.
    std::fs::write(format!("{path}.in-sync"), b"in sync\n").unwrap();
    promote(&alone.listen).await.unwrap();
    waiting.await.unwrap().unwrap();
}

/// A switchover without a witness: the primary hands its role to its standby in sync,
/// which serves everything the primary told; the old primary does not start as the primary
/// again, and started to follow, it follows the new one.
#[tokio::test(flavor = "multi_thread")]
async fn a_switchover_without_a_witness_hands_the_role_to_the_standby() {
    use kernable_core::proto::SwitchoverRequest;
    let dir = Dir::new("switchover");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    // A runtime of its own: shut down, it ends the old primary as its process would.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let primary = {
        let _primary_runtime = runtime.enter();
        Kernel::open(primary_config.clone()).unwrap()
    };
    runtime.spawn(serve(Arc::clone(&primary)));
    wait_for(&primary_config.listen).await;
    let handed = runtime
        .spawn(kernable_kernel::switchover_keeper(&primary).expect("a primary without a witness"));
    for seed in [1, 2] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    let mut standby_config = standby_config(&dir.0, &primary_config);
    // With the flags it has as the primary later: a standby of its own may follow it.
    standby_config.replication.sync = true;
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    let mut told = Vec::new();
    for i in 0..5 {
        told.push(submit(&primary_config.listen, place(i)).await.unwrap());
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
    let result =
        ReplicationServiceClient::new(transport::connect(&primary_config.listen).await.unwrap())
            .switchover(with_grant(request, &key(1)))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(result.standby, id(&key(7)));
    let why = handed.await.unwrap();
    assert!(why.to_string().contains("handed over"), "{why}");
    following.await.unwrap().unwrap();

    // The old primary takes nothing more, and does not start as the primary again.
    let refused = submit(&primary_config.listen, place(99)).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused:?}");
    let err = kernable_kernel::refuse_if_handed_over(&primary_config).unwrap_err();
    assert!(
        err.to_string().contains("handed the primary role over"),
        "{err}"
    );
    // It stops, as its process does once it handed over, writing nothing more.
    primary.shutdown().unwrap();
    drop(primary);
    runtime.shutdown_background();

    // The standby serves, with everything the primary told.
    let new = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    for (i, result) in told.iter().enumerate() {
        let again = submit(&standby_config.listen, place(i as u32))
            .await
            .unwrap();
        assert_eq!(events(&again), events(result), "c{i}");
    }

    // Started to follow, the old primary follows the new one. Records it wrote after the
    // handover were never told (no standby acknowledged them): its log may differ, and it
    // then moves its stores aside and takes an image, as a pair's old primary does.
    let mut back = primary_config.clone();
    back.listen = format!("unix:{}", dir.0.join("primary-2.sock").display());
    back.replication.sync = false;
    back.replication.follow = Some(Follow {
        primary: standby_config.listen.clone(),
        key: dir.0.join("standby.key"),
        primary_kernel_keys: vec![id(&key(3)), id(&key(8))],
    });
    let rejoined = tokio::spawn(standby::run(back.clone()));
    until("the old primary follows the new one", || {
        if rejoined.is_finished() {
            panic!("the old primary stopped following");
        }
        in_sync(&back)
    })
    .await;
    assert!(
        kernable_kernel::refuse_if_handed_over(&primary_config).is_ok(),
        "following, it no longer counts as handed over"
    );
    drop(new);
}
