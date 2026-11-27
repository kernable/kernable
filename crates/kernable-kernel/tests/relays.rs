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

/// The keys of the readers a test's relay serves: every reader key (`reading_key`'s seed + 50)
/// a test uses.
fn readers() -> Vec<String> {
    (51u8..=99).map(|s| id(&key(s))).collect()
}

/// A key in an access file: its ID, role and state.
type KeyLine<'a> = (String, &'a str, &'a str);

/// `--access` with these copies, each with its keys.
fn access_toml(copies: &[(&str, Vec<KeyLine>)]) -> String {
    let mut text = String::new();
    for (copy, keys) in copies {
        text.push_str(&format!("[copies.\"{copy}\".keys]\n"));
        for (k, role, state) in keys {
            text.push_str(&format!(
                "\"{k}\" = {{ role = \"{role}\", state = \"{state}\" }}\n"
            ));
        }
        text.push('\n');
    }
    text
}

/// The relay in `dir`, taking pushes from key 90's.
async fn start_relay(dir: &Path) -> String {
    let listen = format!("unix:{}", dir.join("relay.sock").display());
    let config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.join("copies"),
        readers: readers(),
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
    start_pusher_anew(dir, writer, relay, copy, None)
}

/// The same, with `--start-anew`.
fn start_pusher_anew(
    dir: &Path,
    writer: &str,
    relay: &str,
    copy: &str,
    start_anew: Option<u64>,
) -> JoinHandle<()> {
    let path = reading_key(dir, "pusher", 90);
    let pusher = push::Pusher {
        follow: vec![writer.to_owned()],
        key: key(90),
        grant_file: path.with_extension("grant"),
        relay: relay.to_owned(),
        copy: copy.to_owned(),
        copy_key: kernable_kernel::seal::CopyKey::read(&copy_key(dir)).unwrap(),
        start_anew,
        // It hands the copy key to keys the test's root granted copy=<copy>.
        trust: push::Trust::load(vec![id(&root())], None).unwrap(),
        pushed: None,
        copy_key_file: None,
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
async fn a_relay_serves_only_the_readers_it_names_a_copy_it_holds() {
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
        start_anew: None,
        trust: push::Trust::load(Vec::new(), None).unwrap(),
        pushed: None,
        copy_key_file: None,
    };
    let pushing = tokio::spawn(push::run(stranger));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !dir.0.join("copies").join("shop").exists(),
        "a stranger's push makes no copy"
    );
    pushing.abort();

    // A reader whose key it does not name gets nothing, whatever the root granted it.
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    until_async("the copy fills", async || {
        std::fs::metadata(dir.0.join("copies/shop/log")).is_ok_and(|m| m.len() > 0)
    })
    .await;
    let unnamed = reader_config(&dir.0, "unnamed", 60, &relay, "shop");
    let refused = tokio::time::timeout(Duration::from_secs(3), standby::run(unnamed)).await;
    assert!(refused.is_err(), "a reader it does not name never follows");
}

async fn config_with_writer(dir: &Path) -> Config {
    let writer_config = config(dir, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    writer_config
}

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_whose_log_no_longer_continues_the_copy_empties_it_only_when_told() {
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

    let first_at = at;

    // Another writer (restored from elsewhere, or started by mistake) with fewer, other
    // records: the copy keeps its history, and the pusher pushes nothing.
    let second = Dir::new("relays-anew-second");
    let second_config = config(&second.0, "writer", 3);
    let writer_2 = open(&second_config).await;
    grant_defaults(&writer_2);
    start_worker(second_config.listen.clone(), TestWorker::honest()).await;
    for i in 100..103 {
        submit(&second_config.listen, place(i)).await.unwrap();
    }
    let refused = start_pusher(&dir.0, &second_config.listen, &relay, "shop");
    let status = dir.0.join("copies").join("shop").join("status.json");
    let head = || -> Option<u64> {
        let s: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&status).ok()?).ok()?;
        s["head"].as_u64()
    };
    until("the relay says where the copy stands", || {
        head().is_some_and(|h| h > 0)
    })
    .await;
    let copy_at = head().unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let kept = reader_config(&dir.0, "kept", 23, &relay, "shop");
    let kept_reader = start_reader(&kept).await;
    until_async(
        "a new reader still gets the first writer's records",
        async || held(&kept_reader) >= first_at,
    )
    .await;
    // A position other than the copy's empties nothing either.
    refused.abort();
    let wrong = start_pusher_anew(
        &dir.0,
        &second_config.listen,
        &relay,
        "shop",
        Some(copy_at + 1),
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    drop(kept_reader);
    let still = reader_config(&dir.0, "still", 24, &relay, "shop");
    let still_reader = start_reader(&still).await;
    until_async(
        "a reader started after it still gets the first writer's records",
        async || held(&still_reader) >= first_at,
    )
    .await;
    wrong.abort();
    drop(still_reader);

    // Told with the copy's position: the copy begins anew from the second writer's first record.
    let _pusher = start_pusher_anew(&dir.0, &second_config.listen, &relay, "shop", Some(copy_at));
    let new = reader_config(&dir.0, "new", 22, &relay, "shop");
    let new_reader = start_reader(&new).await;
    let at = held(&writer_2);
    until_async(
        "a new reader holds the second writer's records",
        async || held(&new_reader) >= at,
    )
    .await;
    assert_eq!(held(&new_reader), at, "and none of the first's");
    // The first reader, further than the new copy reaches, holds records of the copy that is
    // gone: it stops, and started again (as its supervisor does) it starts over from the new
    // copy rather than wait for it to grow past them.
    until("the first reader stopped", || {
        Arc::strong_count(&old_reader) == 1
    })
    .await;
    drop(old_reader);
    let old_reader = start_reader(&old).await;
    until_async(
        "the first reader, started again, holds the second writer's records",
        async || held(&old_reader) == at,
    )
    .await;
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
            "--instance-key",
            "kernel.key",
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

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_relay_keeps_each_customers_copy_to_its_own_pushers_and_readers() {
    let dir = Dir::new("relays-access");
    // Two customers: shop (pusher 90, reader 71) and other (pusher 91, reader 201).
    std::fs::write(
        dir.0.join("access.toml"),
        access_toml(&[
            (
                "shop.eu",
                vec![
                    (id(&key(90)), "push", "active"),
                    (id(&key(71)), "read", "active"),
                ],
            ),
            (
                "other.eu",
                vec![
                    (id(&key(91)), "push", "active"),
                    (id(&key(201)), "read", "active"),
                ],
            ),
        ]),
    )
    .unwrap();
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: Vec::new(),
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

    // Shop's reader follows shop's copy; the other's does not name its key.
    let ours = reader_config(&dir.0, "ours", 21, &listen, "shop.eu");
    let _reader = start_reader(&ours).await;
    let mut stray = reader_config(&dir.0, "stray", 22, &listen, "other.eu");
    stray.replication.follow.as_mut().unwrap().copy_key = Some(copy_key(&dir.0));
    let refused = tokio::time::timeout(Duration::from_secs(2), standby::run(stray)).await;
    assert!(refused.is_err(), "it never follows another customer's copy");
}

/// A relay on a network address with a public CA's certificate and no client CA: pushers and
/// readers reach it as `https://host:port` with no certificate of their own (it knows them by
/// their keys); a relay that asks for mutual TLS refuses such a client.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_over_tls_needs_no_client_certificates() {
    let dir = Dir::new("relays-tls");
    let certs = dir.0.join("certs");
    kernable_core::certs::create(&certs, &[]).unwrap();
    // Stands in for the public CAs a client trusts: the CA alone, no certificate of its own.
    // (Set once for this process; the other tests reach their relays over Unix sockets, which
    // never use TLS, and need all three variables for mutual TLS.)
    unsafe { std::env::set_var("KERNABLE_TLS_CA", certs.join("ca.tls.crt")) };

    let listen = free_addr();
    let open_relay = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: readers(),
        pushers: vec![id(&key(90))],
        access: None,
        tls: Some(relay::RelayTls {
            cert: certs.join("kernel.tls.crt"),
            key: certs.join("kernel.tls.key"),
            client_ca: None,
        }),
    };
    tokio::spawn(async move { relay::run(open_relay).await.unwrap() });
    wait_for(&listen).await;
    let port = listen.rsplit(':').next().unwrap();
    let relay = format!("https://localhost:{port}");

    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    let reader = start_reader(&reader_config(&dir.0, "reader", 21, &relay, "shop")).await;
    let at = held(&writer);
    until_async(
        "the reader holds what the writer wrote, over TLS",
        async || held(&reader) >= at,
    )
    .await;

    // Plain text to it gets nowhere.
    let plain = kernable_core::transport::connect(&listen).await;
    if let Ok(channel) = plain {
        let refused = kernable_core::proto::relay_service_client::RelayServiceClient::new(channel)
            .calls(tokio_stream::iter(
                Vec::<kernable_core::proto::CallsRequest>::new(),
            ))
            .await;
        assert!(refused.is_err(), "plain text answered: {refused:?}");
    }

    // A relay that asks for mutual TLS refuses a client without a certificate.
    let strict = free_addr();
    let strict_relay = relay::RelayConfig {
        listen: strict.clone(),
        dir: dir.0.join("strict"),
        readers: readers(),
        pushers: vec![id(&key(90))],
        access: None,
        tls: Some(relay::RelayTls {
            cert: certs.join("kernel.tls.crt"),
            key: certs.join("kernel.tls.key"),
            client_ca: Some(certs.join("ca.tls.crt")),
        }),
    };
    tokio::spawn(async move { relay::run(strict_relay).await.unwrap() });
    wait_for(&strict).await;
    let port = strict.rsplit(':').next().unwrap();
    let refused = async {
        let channel = kernable_core::transport::connect_https_pinging(
            &format!("https://localhost:{port}"),
            Some(&certs.join("ca.tls.crt")),
            None,
            None,
        )
        .await?;
        kernable_core::proto::relay_service_client::RelayServiceClient::new(channel)
            .calls(tokio_stream::iter(
                Vec::<kernable_core::proto::CallsRequest>::new(),
            ))
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    assert!(
        refused.is_err(),
        "mutual TLS let a client without a certificate in"
    );
}

/// A client that never says hello, or never answers the challenge, holds its stream only for
/// the handshake's time; a name that is no copy's is refused without being repeated back.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_ends_handshakes_that_do_not_finish_and_repeats_no_bad_names() {
    use kernable_core::proto::{
        PushHello, PushRequest, push_request, relay_service_client::RelayServiceClient,
    };
    let dir = Dir::new("relays-handshake");
    let relay = start_relay(&dir.0).await;
    let client = || async {
        RelayServiceClient::new(kernable_core::transport::connect(&relay).await.unwrap())
    };

    fn signed<S>(stream: S, copy: &str) -> tonic::Request<S> {
        let mut request = tonic::Request::new(stream);
        relay::sign_stream(&mut request, &key(90), "push", copy).unwrap();
        request
    }
    // No hello at all.
    let started = std::time::Instant::now();
    let mut stream = client()
        .await
        .push(signed(tokio_stream::pending::<PushRequest>(), "shop"))
        .await
        .unwrap()
        .into_inner();
    let ended = stream.message().await.unwrap_err();
    assert_eq!(ended.code(), Code::DeadlineExceeded, "{ended:?}");
    let took = started.elapsed();
    assert!(
        took >= relay::HANDSHAKE && took < relay::HANDSHAKE + Duration::from_secs(5),
        "{took:?}"
    );

    // A hello, then silence: the challenge goes unanswered.
    let hello = PushRequest {
        msg: Some(push_request::Msg::Hello(PushHello {
            key_id: id(&key(90)),
            copy: "shop".into(),
            protocol: kernable_core::PROTOCOL,
        })),
    };
    let mut stream = client()
        .await
        .push(signed(
            tokio_stream::StreamExt::chain(
                tokio_stream::iter(vec![hello]),
                tokio_stream::pending(),
            ),
            "shop",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        stream.message().await.unwrap().is_some(),
        "the challenge comes"
    );
    let ended = stream.message().await.unwrap_err();
    assert_eq!(ended.code(), Code::DeadlineExceeded, "{ended:?}");

    // Names that are no copy's, never echoed.
    // (A name of megabytes never gets this far: HTTP/2 refuses headers that large.)
    for name in ["../../etc/passwd", "Shop", &"a".repeat(128)] {
        let hello = PushRequest {
            msg: Some(push_request::Msg::Hello(PushHello {
                key_id: id(&key(90)),
                copy: name.to_owned(),
                protocol: kernable_core::PROTOCOL,
            })),
        };
        // Signed for that name: refused before any message, the name not repeated.
        let mut request = tonic::Request::new(tokio_stream::iter(vec![hello]));
        let sig = kernable_core::signing::sign(
            &key(90),
            &kernable_core::signing::relay_stream_message("push", name, &id(&key(90)), now()),
        );
        let headers = request.metadata_mut();
        headers.insert(
            kernable_core::signing::RELAY_KEY_HEADER,
            id(&key(90)).parse().unwrap(),
        );
        headers.insert(
            kernable_core::signing::RELAY_COPY_HEADER,
            tonic::metadata::MetadataValue::try_from(name).unwrap_or_else(|_| "x".parse().unwrap()),
        );
        headers.insert(
            kernable_core::signing::RELAY_AT_HEADER,
            now().to_string().parse().unwrap(),
        );
        headers.insert(
            kernable_core::signing::RELAY_SIG_HEADER,
            hex(&sig.sig).parse().unwrap(),
        );
        let refused = client().await.push(request).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert!(
            !refused.message().contains(&name[..4]),
            "{}",
            refused.message()
        );
    }
    assert!(!dir.0.join("etc").exists() && !dir.0.join("copies/Shop").exists());
}

/// A reader far behind acknowledges batch after batch while it catches up; the relay takes the
/// acknowledgements as they come, so HTTP/2 never ends the connection for too many small frames
/// (it did after about a hundred, every stream on the connection with it).
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_catches_up_hundreds_of_batches_on_one_connection() {
    let log = captured_log();

    let dir = Dir::new("relays-many-batches");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    // One commit at a time, apart: each its own batch at the relay.
    for i in 0..300 {
        submit(&writer_config.listen, place(i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let at = held(&writer);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let reader = start_reader(&reader_config(&dir.0, "reader", 21, &relay, "shop")).await;
    until_async("the reader holds all 300", async || held(&reader) >= at).await;
    let text = log.text();
    let mine: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(relay.as_str()) && l.contains("failed"))
        .collect();
    assert!(mine.is_empty(), "the reader's stream broke: {mine:#?}");
}

/// The process's warnings, kept for the tests that read them (set once per test binary).
#[derive(Clone)]
struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for Captured {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured_log() -> Captured {
    static LOG: std::sync::OnceLock<Captured> = std::sync::OnceLock::new();
    LOG.get_or_init(|| {
        let log = Captured(Arc::default());
        let writer = log.clone();
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(move || writer.clone())
                .finish(),
        );
        log
    })
    .clone()
}

/// A copy at its storage limit: the pusher says so, not that "the channel closed".
#[tokio::test(flavor = "multi_thread")]
async fn a_pusher_tells_why_the_relay_ended_its_push() {
    let log = captured_log();
    let dir = Dir::new("relays-full");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    std::fs::create_dir_all(dir.0.join("copies/shop")).unwrap();
    std::fs::write(dir.0.join("copies/shop/max-bytes"), "3000").unwrap();
    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    for i in 0..30 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    until_async("the pusher names the limit", async || {
        log.text()
            .lines()
            .any(|l| l.contains("pushing failed") && l.contains("the copy is full"))
    })
    .await;
    let text = log.text();
    let closed: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(relay.as_str()) && l.contains("channel closed"))
        .collect();
    assert!(closed.is_empty(), "{closed:#?}");
}

/// A reader the relay refuses (its root not in the access list) says so once, as an error, keeps
/// asking, and follows as soon as the operator lets it in, without a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_reader_says_so_once_and_follows_once_let_in() {
    let log = captured_log();
    let dir = Dir::new("relays-let-in");
    let access = |reader: bool| {
        let mut keys = vec![(id(&key(90)), "push", "active")];
        if reader {
            keys.push((id(&key(74)), "read", "active"));
        }
        std::fs::write(dir.0.join("access.toml"), access_toml(&[("shop.eu", keys)])).unwrap()
    };
    access(false);
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: Vec::new(),
        pushers: Vec::new(),
        access: Some(dir.0.join("access.toml")),
        tls: None,
    };
    tokio::spawn(async move { relay::run(config).await.unwrap() });
    wait_for(&listen).await;
    let writer_config = config_with_writer(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &listen, "shop.eu");
    until_async("the copy fills", async || {
        std::fs::metadata(dir.0.join("copies/shop.eu/log")).is_ok_and(|m| m.len() > 0)
    })
    .await;

    // The relay does not name its key yet: refused.
    let reader = reader_config(&dir.0, "late", 24, &listen, "shop.eu");
    let following = tokio::spawn(standby::run(reader.clone()));
    tokio::time::sleep(Duration::from_secs(4)).await;
    let refused = log
        .text()
        .lines()
        .filter(|l| l.contains("ERROR") && l.contains("refused") && l.contains(listen.as_str()))
        .count();
    assert_eq!(refused, 1, "once, as an error:\n{}", log.text());
    assert!(!following.is_finished(), "it keeps asking");

    // The operator names its key: it catches up, still the same process.
    access(true);
    tokio::time::timeout(Duration::from_secs(70), following)
        .await
        .expect("it follows once let in")
        .unwrap()
        .unwrap();
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A stream at a relay is known by its request before any message: unsigned, signed by a key
/// the relay does not take, stale, or for another copy than its hello, it is refused at once,
/// and a big first message of a stranger is never read.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_knows_a_stream_by_its_signed_request_before_reading_it() {
    use kernable_core::proto::{
        PushHello, PushRequest, push_request, relay_service_client::RelayServiceClient,
    };
    use kernable_core::signing::{
        self, RELAY_AT_HEADER, RELAY_COPY_HEADER, RELAY_KEY_HEADER, RELAY_SIG_HEADER,
    };
    let dir = Dir::new("relays-signed");
    let relay = start_relay(&dir.0).await;
    let client = || async {
        RelayServiceClient::new(kernable_core::transport::connect(&relay).await.unwrap())
            .max_encoding_message_size(512 << 20)
    };
    let big = || PushRequest {
        msg: Some(push_request::Msg::Hello(PushHello {
            key_id: "x".repeat(64 << 20),
            ..Default::default()
        })),
    };
    let request = |key: &ed25519_dalek::SigningKey, copy: &str, at: i64| {
        let mut r = tonic::Request::new(tokio_stream::iter(vec![big()]));
        let sig = signing::sign(
            key,
            &signing::relay_stream_message("push", copy, &id(key), at),
        );
        let h = r.metadata_mut();
        h.insert(RELAY_KEY_HEADER, id(key).parse().unwrap());
        h.insert(RELAY_COPY_HEADER, copy.parse().unwrap());
        h.insert(RELAY_AT_HEADER, at.to_string().parse().unwrap());
        h.insert(RELAY_SIG_HEADER, hex(&sig.sig).parse().unwrap());
        r
    };

    // Unsigned.
    let started = std::time::Instant::now();
    let e = client()
        .await
        .push(tokio_stream::iter(vec![big()]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unauthenticated, "{e:?}");
    // A key the relay does not take (key 91; it takes 90's pushes).
    let e = client()
        .await
        .push(request(&key(91), "shop", now()))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied, "{e:?}");
    // Its own key, but stale.
    let e = client()
        .await
        .push(request(&key(90), "shop", now() - 120_000))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unauthenticated, "{e:?}");
    // Signed for another rpc's message: the signature does not hold.
    let mut r = request(&key(90), "shop", now());
    r.metadata_mut()
        .insert(RELAY_COPY_HEADER, "other".parse().unwrap());
    let e = client().await.push(r).await.unwrap_err();
    assert_eq!(e.code(), Code::Unauthenticated, "{e:?}");
    // None of the 64 MB first messages was read: all refused in well under the time to send one.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // Signed right, but the hello names another copy than the request.
    let mut r = tonic::Request::new(tokio_stream::iter(vec![PushRequest {
        msg: Some(push_request::Msg::Hello(PushHello {
            key_id: id(&key(90)),
            copy: "elsewhere".into(),
            protocol: kernable_core::PROTOCOL,
        })),
    }]));
    relay::sign_stream(&mut r, &key(90), "push", "shop").unwrap();
    let e = client()
        .await
        .push(r)
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
}

/// A copy's status.json (every ten seconds) names its pusher and its readers with how far each
/// was sent, for whoever operates the relay; nothing of a record's content.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_writes_who_is_connected_to_each_copy() {
    let dir = Dir::new("relays-status");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    let reader = start_reader(&reader_config(&dir.0, "reader", 21, &relay, "shop")).await;
    let at = held(&writer);
    until_async("the reader holds it", async || held(&reader) >= at).await;

    let path = dir.0.join("copies/shop/status.json");
    let status = || -> serde_json::Value {
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default()
    };
    // Written every ten seconds: up to 25 s for it.
    let named = || {
        let s = status();
        s["pusher"]["key"] == id(&key(90))
            && s["readers"].as_array().is_some_and(|r| {
                // Record numbers, as the relay counts them: the reader was sent the whole copy.
                r.iter().any(|r| {
                    r["key"] == id(&key(71)) && r["position"].as_u64() == s["head"].as_u64()
                })
            })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    while !named() {
        assert!(
            std::time::Instant::now() < deadline,
            "status.json: {}",
            status()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(status()["head"].as_u64() > Some(0), "{}", status());
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains("PlaceOrder") && !text.contains("OrderPlaced"),
        "{text}"
    );
}

/// A copy taken out of the access file (its customer unlinked it): the streams already open end
/// within seconds, the pusher's and the reader's, without a restart.
#[tokio::test(flavor = "multi_thread")]
async fn streams_end_once_their_copy_leaves_the_access_file() {
    let log = captured_log();
    let dir = Dir::new("relays-unlinked");
    let access = |linked: bool| {
        let text = if linked {
            access_toml(&[(
                "shop.eu",
                vec![
                    (id(&key(90)), "push", "active"),
                    (id(&key(71)), "read", "active"),
                ],
            )])
        } else {
            String::new()
        };
        std::fs::write(dir.0.join("access.toml"), text).unwrap()
    };
    access(true);
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let relay_config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: Vec::new(),
        pushers: Vec::new(),
        access: Some(dir.0.join("access.toml")),
        tls: None,
    };
    tokio::spawn(async move { relay::run(relay_config).await.unwrap() });
    wait_for(&listen).await;
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &listen, "shop.eu");
    let reader = start_reader(&reader_config(&dir.0, "reader", 21, &listen, "shop.eu")).await;
    let at = held(&writer);
    until_async("the reader holds it", async || held(&reader) >= at).await;

    access(false);
    let ended = |what: &str| {
        log.text()
            .lines()
            .any(|l| l.contains(what) && l.contains("holds no copy shop.eu"))
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    // The reader may be between two streams as the copy leaves (just caught up, say): then its
    // next stream is refused rather than ended; either way it gets nothing more.
    while !(ended("a push ended") && (ended("a reader's stream ended") || ended("refused"))) {
        assert!(
            std::time::Instant::now() < deadline,
            "both streams end within 15 s:\n{}",
            log.text()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A key's state in the access file: a stopped reader and a revoked pusher are refused within
/// seconds, on the streams they hold, without a restart, and each says why; an active key goes
/// on.
#[tokio::test(flavor = "multi_thread")]
async fn stopped_and_revoked_keys_are_refused_on_the_streams_they_hold() {
    let log = captured_log();
    let dir = Dir::new("relays-states");
    let access = |reader: &str, other: &str, pusher: &str| {
        let keys = vec![
            (id(&key(90)), "push", pusher),
            (id(&key(71)), "read", reader),
            (id(&key(72)), "read", other),
        ];
        std::fs::write(
            dir.0.join("access.toml"),
            access_toml(&[("states.eu", keys)]),
        )
        .unwrap()
    };
    access("active", "active", "active");
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let relay_config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: Vec::new(),
        pushers: Vec::new(),
        access: Some(dir.0.join("access.toml")),
        tls: None,
    };
    tokio::spawn(async move { relay::run(relay_config).await.unwrap() });
    wait_for(&listen).await;
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &listen, "states.eu");
    let reader = start_reader(&reader_config(&dir.0, "reader", 21, &listen, "states.eu")).await;
    let other = start_reader(&reader_config(&dir.0, "other", 22, &listen, "states.eu")).await;
    let at = held(&writer);
    until_async("both readers hold it", async || {
        held(&reader) >= at && held(&other) >= at
    })
    .await;

    // The first reader stopped: its stream ends, and the other reader goes on.
    access("stopped", "active", "active");
    let said = |what: &str| log.text().lines().any(|l| l.contains(what));
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !said("is stopped for states.eu") {
        assert!(
            std::time::Instant::now() < deadline,
            "the stopped reader is refused:\n{}",
            log.text()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // What was on its way before the stream ended may still arrive: then nothing more.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = held(&reader);
    submit(&writer_config.listen, place(10)).await.unwrap();
    let at = held(&writer);
    until_async("the active reader goes on", async || held(&other) >= at).await;
    assert_eq!(
        held(&reader),
        before,
        "the stopped reader gets nothing more:\n{}",
        log.text()
    );

    // The pusher revoked: its push ends, and nothing new reaches the copy.
    access("stopped", "active", "revoked");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !said("is revoked for states.eu") {
        assert!(
            std::time::Instant::now() < deadline,
            "the revoked pusher is refused:\n{}",
            log.text()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    submit(&writer_config.listen, place(11)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        held(&other) < held(&writer),
        "a revoked pusher pushes nothing more"
    );
}

/// A writer from its copy (`--become-writer`): a new kernel catches up from the relay's copy,
/// waits while the relay takes another pusher, and once the old pusher is stopped and its own
/// named, becomes the writer on the same history; its pusher continues the copy where it stands,
/// and a reader of the copy gets the new writer's records.
#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_becomes_the_writer_of_its_copy_once_the_relay_takes_its_pusher() {
    let log = captured_log();
    let dir = Dir::new("relays-become");
    // The old writer's pusher 90; readers 71 (a reader) and 75 (the new writer, following, and
    // pushing with the same key once the relay names it the copy's pusher).
    let access = |old: &str, new: Option<&str>| {
        let mut keys = vec![
            (id(&key(90)), "push", old),
            (id(&key(71)), "read", "active"),
            (id(&key(73)), "read", "active"),
        ];
        keys.push(match new {
            Some(state) => (id(&key(75)), "push", state),
            None => (id(&key(75)), "read", "active"),
        });
        std::fs::write(
            dir.0.join("access.toml"),
            access_toml(&[("become.eu", keys)]),
        )
        .unwrap()
    };
    access("active", None);
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let relay_config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: Vec::new(),
        pushers: Vec::new(),
        access: Some(dir.0.join("access.toml")),
        tls: None,
    };
    tokio::spawn(async move { relay::run(relay_config).await.unwrap() });
    wait_for(&listen).await;
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    let worker = start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let old_pusher = start_pusher(&dir.0, &writer_config.listen, &listen, "become.eu");
    let old_at = held(&writer);

    // The new writer: a reader of the copy with its own kernel key (25); it follows, and then
    // pushes, with its key 75.
    let mut becoming = reader_config(&dir.0, "becoming", 25, &listen, "become.eu");
    becoming.replication.readers = true;
    becoming.replication.become_writer = true;
    // The root approves its kernel key for the copy (`kernable approve`).
    let project = root();
    let approval = kernable_core::proto::WriterApproval {
        copy: "become.eu".into(),
        kernel_key_id: id(&key(25)),
        issued_at: 1,
        root_sig: Some(kernable_core::signing::sign(
            &project,
            &kernable_core::signing::writer_approval_message("become.eu", &id(&key(25)), 1),
        )),
    };
    let approval_path = dir.0.join("become.eu.approval");
    std::fs::write(&approval_path, hex(&approval.encode_to_vec())).unwrap();
    becoming.replication.approval = Some(approval_path);
    let taking = tokio::spawn(standby::run(becoming.clone()));
    let said = |what: &str| log.text().lines().any(|l| l.contains(what));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !said("not the writer yet") {
        assert!(
            std::time::Instant::now() < deadline,
            "it waits for the relay:\n{}",
            log.text()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        !taking.is_finished(),
        "not the writer while the relay takes another pusher"
    );

    // The old writer's pusher stopped, the new one's named: it takes the copy over.
    access("stopped", Some("active"));
    tokio::time::timeout(Duration::from_secs(30), taking)
        .await
        .expect("it becomes the writer")
        .unwrap()
        .unwrap();
    old_pusher.abort();
    drop(worker);
    let new_writer = open(&kernable_kernel::promoted(becoming.clone())).await;
    assert_eq!(held(&new_writer), old_at, "the same history");
    assert_eq!(new_writer.role()["role"], "writer", "{}", new_writer.role());
    grant_defaults(&new_writer);
    start_worker(becoming.listen.clone(), TestWorker::honest()).await;
    let pusher = push::Pusher {
        follow: vec![becoming.listen.clone()],
        key: key(75),
        grant_file: dir.0.join("becoming.grant"),
        relay: listen.clone(),
        copy: "become.eu".into(),
        copy_key: kernable_kernel::seal::CopyKey::read(&copy_key(&dir.0)).unwrap(),
        start_anew: None,
        trust: push::Trust::load(Vec::new(), None).unwrap(),
        pushed: None,
        copy_key_file: None,
    };
    tokio::spawn(async move { push::run(pusher).await.unwrap() });
    for i in 20..23 {
        submit(&becoming.listen, place(i)).await.unwrap();
    }
    let new_at = held(&new_writer);
    assert!(new_at > old_at);

    // A reader of the copy that knows the old writer's kernel key and trusts the root (not the
    // new writer's key) holds the new writer's records: the takeover told it. The copy went on,
    // it did not begin anew.
    let reading = reader_config(&dir.0, "reading", 21, &listen, "become.eu");
    let reader = start_reader(&reading).await;
    until_async("the reader holds the new writer's records", async || {
        held(&reader) >= new_at
    })
    .await;
    // One that trusts another root, and not the new writer's key, stops at the takeover.
    let mut unknowing = reader_config(&dir.0, "unknowing", 23, &listen, "become.eu");
    unknowing.roots.push(id(&key(98)));
    unknowing.roots.retain(|r| *r != id(&root()));
    let stopped = tokio::time::timeout(Duration::from_secs(5), standby::run(unknowing)).await;
    assert!(
        !matches!(stopped, Ok(Ok(()))),
        "it does not take a writer it was not told of: {stopped:?}"
    );
    assert!(
        log.text()
            .lines()
            .any(|l| l.contains("took the partition over") && l.contains("not signed by a root")),
        "and says what to give it: {}",
        log.text()
    );
    let anew = log
        .text()
        .lines()
        .any(|l| l.contains("begins anew") && l.contains("become.eu"));
    assert!(!anew, "the copy continued: {}", log.text());
}

/// A new instance without the copy key asks the copy's pusher through the relay: its grant from
/// the root allows `copy=shop`, so the pusher seals the key to the instance's own key and signs
/// it; the relay carries it and never holds it. One whose grant does not allow it gets nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_reader_gets_the_copy_key_sealed_to_its_key_by_its_grant() {
    let log = captured_log();
    let dir = Dir::new("relays-handover");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..3 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    // The relay takes the writer's own key as the copy's pusher, as with one key per kernel.
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let relay_config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: readers(),
        pushers: vec![id(&key(3))],
        access: None,
        tls: None,
    };
    tokio::spawn(async move { relay::run(relay_config).await.unwrap() });
    wait_for(&listen).await;
    let grant_file = reading_key(&dir.0, "writer-instance", 3).with_extension("grant");
    let pusher = push::Pusher {
        follow: vec![writer_config.listen.clone()],
        key: key(3),
        grant_file,
        relay: listen.clone(),
        copy: "shop".into(),
        copy_key: kernable_kernel::seal::CopyKey::read(&copy_key(&dir.0)).unwrap(),
        start_anew: None,
        trust: push::Trust::load(vec![id(&root())], None).unwrap(),
        pushed: None,
        copy_key_file: None,
    };
    tokio::spawn(async move { push::run(pusher).await.unwrap() });

    // Granted read and copy=shop by the root, with no copy key file yet.
    let mut newbie = reader_config(&dir.0, "newbie", 21, &listen, "shop");
    let newbie_key = dir.0.join("newbie-copy.key");
    if let Some(f) = newbie.replication.follow.as_mut() {
        f.copy_key = Some(newbie_key.clone());
    }
    let granted = grant(&key(71), &[("read", &["*"]), ("copy", &["shop"])]);
    std::fs::write(dir.0.join("newbie.grant"), hex(&granted.encode_to_vec())).unwrap();
    let reader = start_reader(&newbie).await;
    let at = held(&writer);
    until_async("the new reader holds what the writer wrote", async || {
        held(&reader) >= at
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(&newbie_key).unwrap(),
        std::fs::read_to_string(copy_key(&dir.0)).unwrap(),
        "the copy key, handed over"
    );

    // Granted read only: the pusher refuses, and the reader says why and waits.
    let mut plain = reader_config(&dir.0, "plain", 23, &listen, "shop");
    if let Some(f) = plain.replication.follow.as_mut() {
        f.copy_key = Some(dir.0.join("plain-copy.key"));
    }
    let waiting = tokio::time::timeout(Duration::from_secs(4), standby::run(plain)).await;
    assert!(waiting.is_err(), "it keeps asking: {waiting:?}");
    assert!(!dir.0.join("plain-copy.key").exists());
    assert!(
        log.text()
            .lines()
            .any(|l| l.contains("the copy key") && l.contains("copy=shop")),
        "and says why: {}",
        log.text()
    );
}

/// A reader given no writer key takes the one the root approved for its copy (`--approvals`, as
/// a joined node writes them), and only one a root it trusts signed.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_takes_its_writer_from_the_roots_approval() {
    let dir = Dir::new("relays-approved");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..3 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let relay = start_relay(&dir.0).await;
    let _pusher = start_pusher(&dir.0, &writer_config.listen, &relay, "shop");
    let approvals = dir.0.join("approvals");
    std::fs::create_dir_all(&approvals).unwrap();
    let approve = |by: &ed25519_dalek::SigningKey| kernable_core::proto::WriterApproval {
        copy: "shop".into(),
        kernel_key_id: id(&key(3)),
        issued_at: 1,
        root_sig: Some(kernable_core::signing::sign(
            by,
            &kernable_core::signing::writer_approval_message("shop", &id(&key(3)), 1),
        )),
    };
    // Signed by another key than the root: no writer it trusts, it does not catch up.
    std::fs::write(
        approvals.join("shop.approval"),
        hex(&approve(&key(98)).encode_to_vec()),
    )
    .unwrap();
    let mut reading = reader_config(&dir.0, "approved", 21, &relay, "shop");
    reading.replication.approvals = Some(approvals.clone());
    if let Some(f) = reading.replication.follow.as_mut() {
        f.primary_kernel_keys.clear();
    }
    let refused = tokio::time::timeout(Duration::from_secs(3), standby::run(reading.clone())).await;
    assert!(!matches!(refused, Ok(Ok(()))), "{refused:?}");
    // The root's: it does.
    std::fs::write(
        approvals.join("shop.approval"),
        hex(&approve(&root()).encode_to_vec()),
    )
    .unwrap();
    let _ = std::fs::remove_dir_all(dir.0.join("approved.db"));
    let mut fresh = reader_config(&dir.0, "approved2", 23, &relay, "shop");
    fresh.replication.approvals = Some(approvals);
    if let Some(f) = fresh.replication.follow.as_mut() {
        f.primary_kernel_keys.clear();
    }
    let reader = start_reader(&fresh).await;
    let at = held(&writer);
    until_async(
        "the reader holds the approved writer's records",
        async || held(&reader) >= at,
    )
    .await;
}

/// A rotated copy key (`kernable relay rotate`): the pusher pushes with the new one and the copy
/// begins anew under it; a reader finding another key asks for the new one and, granted
/// `copy=shop`, is handed it and catches up.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_copy_key_reaches_granted_readers_and_the_copy_begins_anew_under_it() {
    let log = captured_log();
    let dir = Dir::new("relays-rotate");
    let writer_config = config(&dir.0, "writer", 3);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..3 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let listen = format!("unix:{}", dir.0.join("relay.sock").display());
    let relay_config = relay::RelayConfig {
        listen: listen.clone(),
        dir: dir.0.join("copies"),
        readers: readers(),
        pushers: vec![id(&key(3))],
        access: None,
        tls: None,
    };
    tokio::spawn(async move { relay::run(relay_config).await.unwrap() });
    wait_for(&listen).await;
    let grant_file = reading_key(&dir.0, "writer-instance", 3).with_extension("grant");
    let key_file = copy_key(&dir.0);
    let (relay_at, writer_at) = (listen.clone(), writer_config.listen.clone());
    let (pushed, file) = (dir.0.join("copy.key.pushed"), key_file.clone());
    // As `kernable push` does: started again with the new key once the file holds it.
    tokio::spawn(async move {
        loop {
            let pusher = push::Pusher {
                follow: vec![writer_at.clone()],
                key: key(3),
                grant_file: grant_file.clone(),
                relay: relay_at.clone(),
                copy: "shop".into(),
                copy_key: kernable_kernel::seal::CopyKey::read(&file).unwrap(),
                start_anew: None,
                trust: push::Trust::load(vec![id(&root())], None).unwrap(),
                pushed: Some(pushed.clone()),
                copy_key_file: Some(file.clone()),
            };
            match push::run(pusher).await {
                Err(e) if e.downcast_ref::<push::KeyChanged>().is_some() => continue,
                other => panic!("the pusher stopped: {other:?}"),
            }
        }
    });

    let mut newbie = reader_config(&dir.0, "newbie", 21, &listen, "shop");
    let newbie_key = dir.0.join("newbie-copy.key");
    if let Some(f) = newbie.replication.follow.as_mut() {
        f.copy_key = Some(newbie_key.clone());
    }
    let granted = grant(&key(71), &[("read", &["*"]), ("copy", &["shop"])]);
    std::fs::write(dir.0.join("newbie.grant"), hex(&granted.encode_to_vec())).unwrap();
    let reader = start_reader(&newbie).await;
    let at = held(&writer);
    until_async("the reader holds what the writer wrote", async || {
        held(&reader) >= at
    })
    .await;
    // Rotated: a new key in the pusher's file, the old one kept beside it.
    let old = std::fs::read_to_string(&key_file).unwrap();
    std::fs::rename(&key_file, dir.0.join("copy.key.old")).unwrap();
    kernable_kernel::seal::CopyKey::create(&key_file).unwrap();
    let new = std::fs::read_to_string(&key_file).unwrap();
    assert_ne!(old, new);
    for i in 3..6 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    // The pusher takes the new key within seconds; the copy begins anew under it, and the
    // running reader stops (it is to be started again).
    let new_id = kernable_kernel::seal::CopyKey::read(&key_file)
        .unwrap()
        .id();
    let noted = dir.0.join("copy.key.pushed");
    until_async("the pusher pushes with the new key", async || {
        std::fs::read_to_string(&noted).is_ok_and(|t| t.trim() == new_id)
    })
    .await;
    drop(reader);
    // Started again (as a node does), with its old key: it asks for the new one, and catches up
    // anew under it.
    let mut again = reader_config(&dir.0, "again", 23, &listen, "shop");
    let again_key = dir.0.join("again-copy.key");
    std::fs::write(&again_key, &old).unwrap();
    if let Some(f) = again.replication.follow.as_mut() {
        f.copy_key = Some(again_key.clone());
    }
    let granted = grant(&key(73), &[("read", &["*"]), ("copy", &["shop"])]);
    std::fs::write(dir.0.join("again.grant"), hex(&granted.encode_to_vec())).unwrap();
    let reader = start_reader(&again).await;
    let newbie_key = again_key;
    let at = held(&writer);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !(std::fs::read_to_string(&newbie_key).ok().as_deref() == Some(new.as_str())
        && held(&reader) >= at)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the reader has the new key and what the writer wrote ({} of {at}, key {:?}, new {new:?}): {}",
            held(&reader),
            std::fs::read_to_string(&newbie_key).ok(),
            log.text()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
