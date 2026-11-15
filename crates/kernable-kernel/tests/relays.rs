//! Relays (`kernable relay`) and pushers (`kernable push`): a pusher beside the writer sends its
//! records to a copy at a relay over a connection it opens itself, and readers follow the copy
//! there (`--reader <relay> --reader-copy <copy>`) as they follow a writer.

mod common;

use std::{path::Path, sync::Arc, time::Duration};

use common::*;
use kernable_kernel::{
    Config, Kernel,
    config::{Follow, Replication},
    push, relay, serve, standby,
};
use prost::Message;
use tokio::task::JoinHandle;
use tonic::Code;

fn config(dir: &Path, name: &str, kernel_seed: u8) -> Config {
    let kernel_key = dir.join(format!("{name}-kernel.key"));
    std::fs::write(&kernel_key, hex(key(kernel_seed).as_bytes())).unwrap();
    Config {
        roots: vec![id(&root())],
        store: format!("sqlite:{}", dir.join(format!("{name}.db")).display()),
        listen: format!("unix:{}", dir.join(format!("{name}.sock")).display()),
        kernel_key,
        lease_ttl_ms: 2_000,
        checkpoint_interval_ms: 50,
        submit_timeout_ms: 5_000,
        settle_timeout_ms: 2_000,
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
        admin_socket: None,
        ui_listen: None,
        durability: kernable_kernel::config::Durability::Normal,
        limits: Default::default(),
        replication: Replication {
            readers: true,
            ..Default::default()
        },
        key_command: Vec::new(),
        partition: None,
        deployment_retention_days: Default::default(),
        deployment_switch_lag: 100,
    }
}

/// A key file `<name>.key` of `seed` and its grant `<name>.grant`, allowing `read`.
fn reading_key(dir: &Path, name: &str, seed: u8) -> std::path::PathBuf {
    let k = key(seed);
    let path = dir.join(format!("{name}.key"));
    std::fs::write(&path, hex(k.as_bytes())).unwrap();
    let grant = grant(&k, &[("read", &["*"])]);
    std::fs::write(
        dir.join(format!("{name}.grant")),
        hex(&grant.encode_to_vec()),
    )
    .unwrap();
    path
}

/// The copy key in `dir`, made once.
fn copy_key(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("copy.key");
    if !path.exists() {
        std::fs::write(&path, format!("{}\n", hex(&[42; 32]))).unwrap();
    }
    path
}

/// A reader `name` of the copy `copy` at the relay `relay`.
fn reader_config(dir: &Path, name: &str, seed: u8, relay: &str, copy: &str) -> Config {
    let mut config = config(dir, name, seed);
    config.replication.readers = false;
    config.replication.follow = Some(Follow {
        primary: relay.into(),
        key: reading_key(dir, name, seed + 50),
        primary_kernel_keys: vec![id(&key(3))],
        copy: copy.into(),
        copy_key: Some(copy_key(dir)),
    });
    config.replication.reader = true;
    config
}

async fn open(config: &Config) -> Arc<Kernel> {
    let kernel = Kernel::open(config.clone()).unwrap();
    tokio::spawn(serve(Arc::clone(&kernel)));
    wait_for(&config.listen).await;
    kernel
}

async fn start_reader(config: &Config) -> Arc<Kernel> {
    tokio::time::timeout(Duration::from_secs(30), standby::run(config.clone()))
        .await
        .expect("the reader catches up")
        .unwrap();
    // As `kernable start` does: the stores' locks may take a moment to go.
    open(&kernable_kernel::promoted(config.clone())).await
}

/// The relay in `dir`, taking pushes from key 90's.
async fn start_relay(dir: &Path) -> String {
    let listen = format!("unix:{}", dir.join("relay.sock").display());
    let config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.join("copies"),
        roots: vec![id(&root())],
        pushers: vec![id(&key(90))],
        access: None,
        tls: None,
    };
    tokio::spawn(async move { relay::run(config).await.unwrap() });
    wait_for(&listen).await;
    listen
}

/// A pusher of the writer at `writer` to `copy` at `relay`, with key 90.
fn start_pusher(dir: &Path, writer: &str, relay: &str, copy: &str) -> JoinHandle<()> {
    let path = reading_key(dir, "pusher", 90);
    let pusher = push::Pusher {
        follow: vec![writer.to_owned()],
        key: key(90),
        grant_file: path.with_extension("grant"),
        relay: relay.to_owned(),
        copy: copy.to_owned(),
        copy_key: kernable_kernel::seal::CopyKey::read(&copy_key(dir)).unwrap(),
        trust: push::Trust::load(vec![id(&root())], Some(dir.join("copy.revoked"))).unwrap(),
    };
    tokio::spawn(async move {
        push::run(pusher).await.unwrap();
    })
}

fn grant_defaults(kernel: &Kernel) {
    for seed in [1, 2, 4, 5, 6] {
        kernel.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
}

fn place(i: u32) -> kernable_core::proto::Command {
    signed(
        command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
        &key(1),
    )
}

/// The newest event `kernel` holds, by its role.
fn held(kernel: &Kernel) -> u64 {
    kernel.role()["position"].as_u64().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_follows_the_writer_through_a_copy_at_a_relay() {
    let dir = Dir::new("relays-follow");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }

    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    // The writer counts the pusher among its readers.
    until_async("the pusher follows the writer", async || {
        writer.role()["readers"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
    })
    .await;

    let first = reader_config(&dir.0, "reader", 21, &relay, "shop");
    let reader = start_reader(&first).await;
    assert_eq!(reader.role()["role"], "reader");
    let at = held(&writer);
    until_async("the reader holds what the writer wrote", async || {
        held(&reader) >= at
    })
    .await;

    // What the writer writes next reaches it through the relay too.
    for i in 5..30 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let at = held(&writer);
    until_async("the reader holds the newer records", async || {
        held(&reader) >= at
    })
    .await;

    // The relay holds the records sealed: nothing of them reads there.
    let log = std::fs::read(dir.0.join("copies/shop/log")).unwrap();
    let plain = |needle: &[u8]| log.windows(needle.len()).any(|w| w == needle);
    assert!(!plain(b"PlaceOrder") && !plain(b"OrderPlaced"));
    let mut store = std::fs::read(dir.0.join("writer.db")).unwrap();
    store.extend(std::fs::read(dir.0.join("writer.db-wal")).unwrap_or_default());
    assert!(
        store.windows(10).any(|w| w == b"PlaceOrder"),
        "the writer's store holds it in plain"
    );

    // A reader with another key gets nothing it could read.
    let mut wrong = reader_config(&dir.0, "wrong", 23, &relay, "shop");
    let other = dir.0.join("other-copy.key");
    std::fs::write(&other, format!("{}\n", hex(&[43; 32]))).unwrap();
    wrong.replication.follow.as_mut().unwrap().copy_key = Some(other);
    let refused = tokio::time::timeout(Duration::from_secs(2), standby::run(wrong)).await;
    assert!(refused.is_err(), "it never catches up: {refused:?}");

    // A command sent to the reader goes through the relay, sealed, to the pusher, which makes
    // it on the writer; the reader answers once its own copy holds the events.
    let result = submit(&first.listen, place(99)).await.unwrap();
    let at = match &result.outcome {
        Some(kernable_core::proto::command_result::Outcome::Committed(c)) => c.position,
        other => panic!("not committed: {other:?}"),
    };
    assert!(held(&reader) >= at && held(&writer) >= at);
    // Sent again, it gets the stored outcome.
    let again = submit(&first.listen, place(99)).await.unwrap();
    assert_eq!(again.outcome, result.outcome);
    // Any other write goes the same way, and what the writer refuses comes back as it refused
    // it: an unsigned cancel.
    let channel = kernable_core::transport::connect(&first.listen)
        .await
        .unwrap();
    let refused = kernable_core::proto::command_service_client::CommandServiceClient::new(channel)
        .cancel(kernable_core::proto::CancelRequest {
            command_id: "c1".into(),
            tenant_id: "t1".into(),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated, "{refused:?}");
    assert!(refused.message().contains("not signed"), "{refused:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_serves_only_granted_readers_of_a_copy_it_holds() {
    let dir = Dir::new("relays-refuse");
    let relay = start_relay(&dir.0).await;
    // No copy yet: a reader waits.
    let config = reader_config(&dir.0, "reader", 21, &relay, "none");
    let early = tokio::time::timeout(Duration::from_secs(2), standby::run(config.clone())).await;
    assert!(early.is_err(), "the reader waits for the copy: {early:?}");

    // Not a pusher it takes.
    let writer_config = config_with_writer(&dir.0).await;
    let path = reading_key(&dir.0, "stranger", 91);
    let stranger = push::Pusher {
        follow: vec![writer_config.listen.clone()],
        key: key(91),
        grant_file: path.with_extension("grant"),
        relay: relay.clone(),
        copy: "shop".into(),
        copy_key: kernable_kernel::seal::CopyKey::read(&copy_key(&dir.0)).unwrap(),
        trust: push::Trust::load(Vec::new(), None).unwrap(),
    };
    let pushing = tokio::spawn(push::run(stranger));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !dir.0.join("copies").join("shop").exists(),
        "a stranger's push makes no copy"
    );
    pushing.abort();
}

async fn config_with_writer(dir: &Path) -> Config {
    let writer_config = config(dir, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    writer_config
}

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_whose_log_no_longer_continues_the_copy_empties_it() {
    let dir = Dir::new("relays-anew");
    let relay = start_relay(&dir.0).await;

    // A first writer fills the copy.
    let first = Dir::new("relays-anew-first");
    let first_config = config(&first.0, "writer", 3);
    let writer = open(&first_config).await;
    grant_defaults(&writer);
    start_worker(first_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..10 {
        submit(&first_config.listen, place(i)).await.unwrap();
    }
    let pusher = start_pusher(&dir.0, &first_config.listen, &relay, "shop");
    let old = reader_config(&dir.0, "old", 21, &relay, "shop");
    let old_reader = start_reader(&old).await;
    let at = held(&writer);
    until_async(
        "the first reader holds the first writer's records",
        async || held(&old_reader) >= at,
    )
    .await;
    pusher.abort();

    // Another writer (restored from elsewhere, say) with fewer, other records: the copy begins
    // anew from its first record.
    let second = Dir::new("relays-anew-second");
    let second_config = config(&second.0, "writer", 3);
    let writer_2 = open(&second_config).await;
    grant_defaults(&writer_2);
    start_worker(second_config.listen.clone(), TestWorker::honest()).await;
    for i in 100..103 {
        submit(&second_config.listen, place(i)).await.unwrap();
    }
    let _pusher = start_pusher(&dir.0, &second_config.listen, &relay, "shop");
    let new = reader_config(&dir.0, "new", 22, &relay, "shop");
    let new_reader = start_reader(&new).await;
    let at = held(&writer_2);
    until_async(
        "a new reader holds the second writer's records",
        async || held(&new_reader) >= at,
    )
    .await;
    assert_eq!(held(&new_reader), at, "and none of the first's");
}

/// The writer as a process in `dir`, so its store can be changed between runs.
async fn writer_process(dir: &Path) -> (KillOnDrop, String) {
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = format!("unix:{}", dir.join("writer.sock").display());
    let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"));
    process
        .current_dir(dir)
        .arg("start")
        .args([
            "--root",
            &id(&root()),
            "--store",
            &format!("sqlite:{}", dir.join("writer.db").display()),
            "--listen",
            &addr,
            "--readers",
            "on",
            "--checkpoint-interval-ms",
            "20",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let kernel = KillOnDrop::spawn(&mut process);
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    (kernel, addr)
}

/// The position of the newest event at `addr`, from a committed command's answer.
async fn place_at(addr: &str, i: u32) -> u64 {
    match submit(addr, place(i)).await.unwrap().outcome {
        Some(kernable_core::proto::command_result::Outcome::Committed(c)) => c.position,
        other => panic!("not committed: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_begins_from_an_image_when_the_writer_no_longer_keeps_its_first_records() {
    let dir = Dir::new("relays-image");
    let (mut writer, addr) = writer_process(&dir.0).await;
    for i in 0..10 {
        place_at(&addr, i).await;
    }
    writer.child.kill().unwrap();
    writer.child.wait().unwrap();
    // The writer pruned its record log, as once its standby and readers took it.
    {
        let db = rusqlite::Connection::open(dir.0.join("writer.db")).unwrap();
        let head: u64 = db
            .query_row("SELECT MAX(seq) FROM replication", [], |r| r.get(0))
            .unwrap();
        db.execute("DELETE FROM replication WHERE seq < ?1", [head - 2])
            .unwrap();
        db.execute("UPDATE replication_state SET base = ?1", [head - 2])
            .unwrap();
    }
    let (_writer, addr) = writer_process(&dir.0).await;

    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &addr, &relay, "shop");
    let reader_config = reader_config(&dir.0, "reader", 21, &relay, "shop");
    let reader = start_reader(&reader_config).await;
    let at = place_at(&addr, 10).await;
    until_async(
        "the reader holds the image and what came after",
        async || held(&reader) >= at,
    )
    .await;
    assert!(dir.0.join("copies/shop/log").exists() && !dir.0.join("copies/shop/log.new").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_ahead_of_the_copy_waits_for_it() {
    let dir = Dir::new("relays-ahead");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let relay = start_relay(&dir.0).await;
    let pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    let copied = held(&writer);
    let log = dir.0.join("copies/shop/log");
    until_async("the copy holds the first records", async || {
        std::fs::metadata(&log).is_ok_and(|m| m.len() > 0)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    pusher.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for i in 5..10 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }

    // A reader follows the writer directly first, and gets further than the copy.
    let mut direct = reader_config(&dir.0, "reader", 21, &relay, "shop");
    if let Some(f) = direct.replication.follow.as_mut() {
        f.primary = writer_config.listen.clone();
        (f.copy, f.copy_key) = (String::new(), None);
    }
    tokio::time::timeout(Duration::from_secs(30), standby::run(direct))
        .await
        .expect("it catches up from the writer")
        .unwrap();
    assert!(held(&writer) > copied);

    // Then it follows the relay: it waits until the copy holds what it holds, and goes on.
    let via_relay = reader_config(&dir.0, "reader", 21, &relay, "shop");
    let catching_up = tokio::spawn(standby::run(kernable_kernel::promoted(via_relay.clone())));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!catching_up.is_finished(), "it waits for the copy");
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    tokio::time::timeout(Duration::from_secs(30), catching_up)
        .await
        .expect("it catches up once the copy does")
        .unwrap()
        .unwrap();
    let reader = open(&kernable_kernel::promoted(via_relay)).await;
    let at = held(&writer);
    until_async("the reader holds everything", async || held(&reader) >= at).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn readers_of_a_copy_whose_pusher_is_gone_count_the_writer_as_away() {
    let dir = Dir::new("relays-silent");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    let relay = start_relay(&dir.0).await;
    let pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    let mut reader_config = reader_config(&dir.0, "reader", 21, &relay, "shop");
    reader_config.replication.reader_max_lag_ms = 1_500;
    let reader = start_reader(&reader_config).await;
    until_async("the reader is ready", async || reader.readiness().is_ok()).await;
    let query = async || {
        let mut q = kernable_core::proto::QueryRequest {
            protocol: kernable_core::PROTOCOL,
            query_id: format!("q{}", issued_now()),
            tenant_id: "t1".into(),
            domain: "orders".into(),
            query_type: "CountOrders".into(),
            payload: b"{}".to_vec(),
            issued_at: issued_now(),
            ..Default::default()
        };
        q.client_sig = Some(kernable_core::signing::sign(
            &key(1),
            &kernable_core::signing::query_message(&q),
        ));
        let channel = kernable_core::transport::connect(&reader_config.listen)
            .await
            .unwrap();
        kernable_core::proto::query_service_client::QueryServiceClient::new(channel)
            .query(with_grant(q, &key(1)))
            .await
    };
    // Current: no read model answers, but it is not refused for its lag.
    let early = query().await.unwrap_err();
    assert!(!early.message().starts_with("reader_behind"), "{early:?}");

    // The pusher is gone: the copy no longer follows the writer, and its reader says so.
    pusher.abort();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match query().await {
                Err(s) if s.message().starts_with("reader_behind") => {
                    assert_eq!(s.code(), Code::Unavailable);
                    return;
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("beyond its maximum lag the reader refuses queries");
    // The relay waits 3 s for the pusher, the reader 5 s for the relay's beat.
    tokio::time::timeout(Duration::from_secs(20), async {
        while reader.readiness().is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the reader counts the writer as away");
    tokio::time::timeout(Duration::from_secs(20), async {
        while reader.role()["state"] != "away" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("it hears nothing more and counts the writer as away");

    // Back again: the reader is ready again.
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    until_async("the reader is ready again", async || {
        reader.readiness().is_ok()
    })
    .await;
}

/// A reader `name` that has no copy key yet, its grant `grant`.
fn new_reader(
    dir: &Path,
    name: &str,
    seed: u8,
    relay: &str,
    grant: kernable_core::proto::Grant,
) -> Config {
    let mut config = reader_config(dir, name, seed, relay, "shop");
    std::fs::write(
        dir.join(format!("{name}.grant")),
        hex(&grant.encode_to_vec()),
    )
    .unwrap();
    config.replication.follow.as_mut().unwrap().copy_key =
        Some(dir.join(format!("{name}-copy.key")));
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_reader_gets_the_copy_key_on_the_word_of_the_customers_enrol_key() {
    use kernable_core::proto::Permission;
    let dir = Dir::new("relays-enrol");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..3 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    // The customer's enrol key: the root lets it grant `read`, nothing else.
    let enrol = key(80);
    let enrol_grant = grant(&enrol, &[("grant", &["read"])]);
    let by_enrol = |to: &ed25519_dalek::SigningKey| {
        kernable_core::signing::delegated_grant(
            &enrol,
            enrol_grant.clone(),
            &id(to),
            vec![Permission {
                action: "read".into(),
                types: vec!["*".into()],
            }],
            issued_now(),
        )
    };

    // A reader the enrol key granted gets the key, sealed for it, and follows.
    let config = new_reader(&dir.0, "enrolled", 31, &relay, by_enrol(&key(81)));
    let reader = start_reader(&config).await;
    let at = held(&writer);
    until_async(
        "the enrolled reader holds the writer's records",
        async || held(&reader) >= at,
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(dir.0.join("enrolled-copy.key")).unwrap(),
        std::fs::read_to_string(copy_key(&dir.0)).unwrap()
    );

    // A grant from a root the pusher does not trust: refused, at once.
    let stranger_root = key(82);
    let mut foreign = grant(&key(83), &[("read", &["*"])]);
    foreign.root_sig = Some(kernable_core::signing::sign(
        &stranger_root,
        &kernable_core::signing::grant_message(&foreign),
    ));
    let refused = standby::run(new_reader(&dir.0, "foreign", 33, &relay, foreign))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("copy key"), "{refused}");
    assert!(!dir.0.join("foreign-copy.key").exists());

    // The writer revokes the enrol key: the pusher sees it pass and refuses what it granted.
    writer.revoke(&id(&enrol), 0).unwrap();
    submit(&writer_config.listen, place(10)).await.unwrap();
    let revoked_file = dir.0.join("copy.revoked");
    until_async("the pusher noted the revocation", async || {
        std::fs::read_to_string(&revoked_file).is_ok_and(|f| f.contains(&id(&enrol)))
    })
    .await;
    let refused = standby::run(new_reader(&dir.0, "late", 34, &relay, by_enrol(&key(84))))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("revoked"), "{refused}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_relay_keeps_each_customers_copy_to_its_own_pushers_and_readers() {
    let dir = Dir::new("relays-access");
    // Two customers: shop (root 200, pusher 90) and other (root 201, pusher 91).
    let other_root = key(201);
    std::fs::write(
        dir.0.join("access.toml"),
        format!(
            "[copies.\"shop.eu\"]\npushers = [\"{}\"]\nroots = [\"{}\"]\n\n[copies.\"other.eu\"]\npushers = [\"{}\"]\nroots = [\"{}\"]\n",
            id(&key(90)),
            id(&root()),
            id(&key(91)),
            id(&other_root)
        ),
    )
    .unwrap();
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        roots: Vec::new(),
        pushers: Vec::new(),
        access: Some(dir.0.join("access.toml")),
        tls: None,
    };
    tokio::spawn(async move { relay::run(config).await.unwrap() });
    wait_for(&listen).await;

    let writer_config = config_with_writer(&dir.0).await;
    // Shop's pusher fills shop's copy; pushing to the other customer's it is refused.
    let _ours = start_pusher(&dir.0, &writer_config.listen, &listen, "shop.eu");
    let theirs = start_pusher(&dir.0, &writer_config.listen, &listen, "other.eu");
    let copies = dir.0.join("copies");
    until_async("shop's copy fills", async || {
        std::fs::metadata(copies.join("shop.eu").join("log")).is_ok_and(|m| m.len() > 0)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !copies.join("other.eu").exists(),
        "no push to another's copy"
    );
    theirs.abort();

    // Shop's reader follows shop's copy; at the other's, its grant counts for nothing.
    let ours = reader_config(&dir.0, "ours", 21, &listen, "shop.eu");
    let _reader = start_reader(&ours).await;
    let mut stray = reader_config(&dir.0, "stray", 22, &listen, "other.eu");
    stray.replication.follow.as_mut().unwrap().copy_key = Some(copy_key(&dir.0));
    let refused = tokio::time::timeout(Duration::from_secs(2), standby::run(stray)).await;
    assert!(refused.is_err(), "it never follows another customer's copy");
}
