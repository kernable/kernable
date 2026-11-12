//! Readers (`--reader`): read-only copies that follow a writer (`--readers on`) over the
//! replication stream, answer queries from their own copy, forward commands, and wait for what
//! they forward before they answer, so a query after a command sees it on every copy. Nothing
//! outward runs on a reader, and a reader follows whichever kernel is the primary.

mod common;

use std::{path::Path, sync::Arc, time::Duration};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, CommandResult, Hello, ListenerMessage, PromoteRequest, QueryAnswer,
        QueryRequest, QueryResult, Wait, command_result::Outcome, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient, query_answer,
        query_result, query_service_client::QueryServiceClient,
        replication_service_client::ReplicationServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{
    Config, Kernel,
    backup::{self, Follower, crypt::BackupKey, target},
    config::{Follow, FromBackup, Replication},
    serve, standby,
};
use prost::Message;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

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
        replication: Replication::default(),
        key_command: Vec::new(),
        partition: None,
        deployment_retention_days: Default::default(),
        deployment_switch_lag: 100,
    }
}

/// The writer: it keeps readers (and, with `standby`, waits for its standby).
fn writer_config(dir: &Path, standby: bool) -> Config {
    let mut config = config(dir, "writer", 3);
    config.replication.readers = true;
    config.replication.sync = standby;
    config
}

/// A reader `name` of the kernels at `writers` (comma-separated), with a key granted
/// `replicate`.
fn reader_config(dir: &Path, name: &str, seed: u8, writers: &str) -> Config {
    let mut config = config(dir, name, seed);
    let reader = key(seed + 50);
    let key_path = dir.join(format!("{name}.key"));
    std::fs::write(&key_path, hex(reader.as_bytes())).unwrap();
    let grant = grant(&reader, &[("replicate", &["*"])]);
    std::fs::write(
        dir.join(format!("{name}.grant")),
        hex(&grant.encode_to_vec()),
    )
    .unwrap();
    config.replication.follow = Some(Follow {
        primary: writers.into(),
        key: key_path,
        primary_kernel_keys: vec![id(&key(3)), id(&key(8))],
    });
    config.replication.reader = true;
    config
}

/// The standby of the writer, with the flags it serves with once promoted.
fn standby_config(dir: &Path, writer: &Config) -> Config {
    let mut config = config(dir, "standby", 8);
    let standby = key(7);
    let key_path = dir.join("standby.key");
    std::fs::write(&key_path, hex(standby.as_bytes())).unwrap();
    let grant = grant(&standby, &[("replicate", &["*"])]);
    std::fs::write(dir.join("standby.grant"), hex(&grant.encode_to_vec())).unwrap();
    config.replication.follow = Some(Follow {
        primary: writer.listen.clone(),
        key: key_path,
        primary_kernel_keys: vec![id(&key(3))],
    });
    config.replication.readers = true;
    config
}

/// Starts a kernel on `config` and serves it.
async fn open(config: &Config) -> Arc<Kernel> {
    let kernel = Kernel::open(config.clone()).unwrap();
    tokio::spawn(serve(Arc::clone(&kernel)));
    wait_for(&config.listen).await;
    kernel
}

/// Starts a reader as `kernable start --reader` does: it catches up, then serves its copy.
async fn start_reader(config: &Config) -> Arc<Kernel> {
    tokio::time::timeout(Duration::from_secs(30), standby::run(config.clone()))
        .await
        .expect("the reader catches up")
        .unwrap();
    open(config).await
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

fn position(result: &CommandResult) -> u64 {
    match &result.outcome {
        Some(Outcome::Committed(c)) => {
            assert_eq!(
                c.position,
                c.events.last().map_or(0, |e| e.global_position),
                "the position is the last event's"
            );
            c.position
        }
        other => panic!("not committed: {other:?}"),
    }
}

/// A listener at `addr` with key 5: a read model counting `orders.OrderPlaced` and answering
/// `orders.CountOrders`, or with `reactor`, a reactor. Every message it gets goes to `seen`.
fn listener(
    addr: &str,
    name: &str,
    reactor: bool,
    seen: mpsc::UnboundedSender<Out>,
) -> JoinHandle<tonic::Status> {
    let (addr, name, key) = (addr.to_owned(), name.to_owned(), key(5));
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name,
            key_id: id(&key),
            order: if reactor { 10 } else { 0 },
            event_types: vec!["orders.OrderPlaced".into()],
            from_position: (!reactor).then_some(0),
            query_types: if reactor {
                Vec::new()
            } else {
                vec!["orders.CountOrders".into()]
            },
            reactor,
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = match ListenerServiceClient::new(channel)
            .listen(with_grant(ReceiverStream::new(rx), &key))
            .await
        {
            Ok(r) => r.into_inner(),
            Err(status) => return status,
        };
        let mut count = 0;
        loop {
            let msg = match inbound.message().await {
                Ok(Some(m)) => m.msg.unwrap(),
                Ok(None) => return tonic::Status::ok("closed"),
                Err(status) => return status,
            };
            let _ = seen.send(msg.clone());
            let reply = match msg {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Ready(_) | Out::Waiting(_) => continue,
                Out::Batch(b) => {
                    count += b.events.len();
                    In::Ack(Ack {
                        position: b.up_to,
                        ..Default::default()
                    })
                }
                Out::Query(call) => In::Answer(QueryAnswer {
                    call_id: call.call_id,
                    outcome: Some(query_answer::Outcome::Response(count.to_string().into())),
                }),
                Out::Check(_) => unreachable!("not a middleware"),
            };
            if tx.send(send(reply)).await.is_err() {
                return tonic::Status::ok("closed");
            }
        }
    })
}

fn read_model(addr: &str, name: &str) -> JoinHandle<tonic::Status> {
    let (seen, _) = mpsc::unbounded_channel();
    listener(addr, name, false, seen)
}

fn count_query(min_position: u64, key: &SigningKey) -> QueryRequest {
    let mut q = QueryRequest {
        protocol: kernable_core::PROTOCOL,
        query_id: format!("q{}", now_ms()),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        query_type: "CountOrders".into(),
        payload: b"{}".to_vec(),
        min_position,
        issued_at: issued_now(),
        ..Default::default()
    };
    q.client_sig = Some(signing::sign(key, &signing::query_message(&q)));
    q
}

/// How many orders the read model at `addr` counted, once it holds `min_position`.
async fn count(addr: &str, min_position: u64) -> Result<usize, tonic::Status> {
    let channel = transport::connect(addr).await.unwrap();
    let result: QueryResult = QueryServiceClient::new(channel)
        .query(with_grant(count_query(min_position, &key(1)), &key(1)))
        .await?
        .into_inner();
    match result.outcome {
        Some(query_result::Outcome::Response(r)) => {
            Ok(String::from_utf8_lossy(&r).parse().unwrap())
        }
        other => panic!("expected an answer, got {other:?}"),
    }
}

/// The newest event `kernel` holds, by its role.
fn held(kernel: &Kernel) -> u64 {
    kernel.role()["position"].as_u64().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn two_readers_answer_queries_and_see_a_command_sent_through_either() {
    let dir = Dir::new("readers-two");
    let writer_config = writer_config(&dir.0, false);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    for i in 0..5 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }

    let first = reader_config(&dir.0, "reader-1", 21, &writer_config.listen);
    let second = reader_config(&dir.0, "reader-2", 22, &writer_config.listen);
    let reader_1 = start_reader(&first).await;
    let reader_2 = start_reader(&second).await;
    assert_eq!(reader_1.role()["role"], "reader");
    assert_eq!(writer.role()["role"], "writer");
    let _models = [
        read_model(&first.listen, "orders-count"),
        read_model(&second.listen, "orders-count"),
    ];
    until_async("both readers are ready", async || {
        reader_1.readiness().is_ok() && reader_2.readiness().is_ok()
    })
    .await;
    assert_eq!(count(&first.listen, 0).await.unwrap(), 5);
    assert_eq!(count(&second.listen, 0).await.unwrap(), 5);

    // A command sent through a reader runs on the writer, and the reader answers once its own
    // copy holds the events and its read model processed them.
    let result = submit(&first.listen, place(5)).await.unwrap();
    let at = position(&result);
    assert!(at > 0);
    match &result.outcome {
        Some(Outcome::Committed(c)) => assert!(c.settled, "settled on the reader too"),
        _ => unreachable!(),
    }
    assert!(held(&reader_1) >= at);
    assert_eq!(
        count(&first.listen, 0).await.unwrap(),
        6,
        "read your writes"
    );
    // The other reader sees it with the position: it waits until it holds it.
    assert_eq!(count(&second.listen, at).await.unwrap(), 6);
    // A resubmit through the other reader gets the original outcome.
    let again = submit(&second.listen, place(5)).await.unwrap();
    assert_eq!(position(&again), at);
    assert_eq!(count(&second.listen, at).await.unwrap(), 6);

    // A position no copy holds: the reader says it is behind, after the query timeout.
    let beyond = count(&second.listen, at + 1_000).await.unwrap_err();
    assert_eq!(beyond.code(), Code::Unavailable, "{beyond:?}");
    assert!(beyond.message().starts_with("reader_behind"), "{beyond:?}");

    // A reader keeps no followers and is never promoted.
    let issued_at = issued_now();
    let promote = ReplicationServiceClient::new(transport::connect(&first.listen).await.unwrap())
        .promote(PromoteRequest {
            issued_at,
            sig: Some(signing::sign(&key(1), &signing::promote_message(issued_at))),
            protocol: kernable_core::PROTOCOL,
        })
        .await
        .unwrap_err();
    assert_eq!(promote.code(), Code::FailedPrecondition, "{promote:?}");
    assert!(promote.message().contains("never promoted"), "{promote:?}");

    // The writer lists both.
    let readers = writer.role()["readers"].as_array().unwrap().len();
    assert_eq!(readers, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_reactor_or_worker_runs_on_a_reader() {
    let dir = Dir::new("readers-reactor");
    let writer_config = writer_config(&dir.0, false);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    let reader_config = reader_config(&dir.0, "reader", 21, &writer_config.listen);
    let _reader = start_reader(&reader_config).await;

    let (seen, mut got) = mpsc::unbounded_channel();
    let _reactor = listener(&reader_config.listen, "mailer", true, seen);
    let waiting = loop {
        match tokio::time::timeout(Duration::from_secs(5), got.recv())
            .await
            .expect("the reader answers the reactor")
            .unwrap()
        {
            Out::Waiting(w) => break w,
            Out::Challenge(_) => continue,
            other => panic!("a reactor on a reader gets nothing but Waiting: {other:?}"),
        }
    };
    assert_eq!(waiting.reader_of, writer_config.listen);
    for i in 0..3 {
        submit(&reader_config.listen, place(i)).await.unwrap();
    }
    // Nothing reaches it, however long it stays.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), got.recv())
            .await
            .is_err(),
        "the reactor got something on the reader"
    );

    // A worker that connects to the reader is told so and gets no commands: the writer's
    // worker runs them all.
    let (worker_seen, mut executed) = mpsc::unbounded_channel();
    let _idle = start_worker(
        reader_config.listen.clone(),
        TestWorker::with(Behavior::Honest, worker_seen),
    )
    .await;
    for i in 3..6 {
        submit(&reader_config.listen, place(i)).await.unwrap();
    }
    assert!(
        executed.try_recv().is_err(),
        "a worker on the reader ran a command"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_reader_starts_from_the_backup_and_catches_up_from_the_writer() {
    let dir = Dir::new("readers-backup");
    let mut writer_config = writer_config(&dir.0, false);
    writer_config.replication.backup = true;
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;

    // A backup follower writes the writer to a directory.
    let to = format!("file:{}", dir.0.join("backup").display());
    let follower_key = key(9);
    let follower_grant = grant(&follower_key, &[("backup", &["*"])]);
    writer.grant(follower_grant.clone()).unwrap();
    let follower = Follower {
        addrs: vec![writer_config.listen.clone()],
        key: follower_key,
        grant: hex(&follower_grant.encode_to_vec()),
        grant_file: None,
        target: Arc::from(target::open(&to, None).unwrap()),
        crypt: Arc::new(BackupKey::new(&[7; 32])),
        segment_every: Duration::from_millis(100),
        base_every: Duration::from_secs(3600),
        keep: 3,
        check_every: None,
        status_file: None,
    };
    let _backing_up = tokio::spawn(backup::run(follower));
    for i in 0..20 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }
    let head = kernable_store::open_read_only(&writer_config.store)
        .unwrap()
        .record_head()
        .unwrap()
        .seq;
    until("the backup holds the writer's records", || {
        let t = target::open(&to, None).unwrap();
        backup::generations(t.as_ref(), &BackupKey::new(&[7; 32]))
            .unwrap()
            .iter()
            .filter_map(|g| {
                let base = g.base.as_ref()?;
                Some(g.segments.last().map_or(base.seq, |s| s.last))
            })
            .max()
            .is_some_and(|reached| reached >= head)
    })
    .await;
    // Written after the backup's newest record: these come from the writer.
    for i in 20..25 {
        submit(&writer_config.listen, place(i)).await.unwrap();
    }

    let key_file = dir.0.join("backup.key");
    std::fs::write(&key_file, hex(&[7; 32])).unwrap();
    let mut reader_config = reader_config(&dir.0, "reader", 21, &writer_config.listen);
    reader_config.replication.from_backup = Some(FromBackup {
        target: to.clone(),
        key: key_file,
        vault_key: None,
    });
    let reader = start_reader(&reader_config).await;
    let _model = read_model(&reader_config.listen, "orders-count");
    let last = submit(&writer_config.listen, place(25)).await.unwrap();
    assert_eq!(
        count(&reader_config.listen, position(&last)).await.unwrap(),
        26
    );
    assert!(reader.readiness().is_ok());

    // A reader whose backup cannot be read takes a full image from the writer instead.
    let mut broken = self::reader_config(&dir.0, "reader-broken", 22, &writer_config.listen);
    broken.replication.from_backup = Some(FromBackup {
        target: format!("file:{}", dir.0.join("no-backup-here").display()),
        key: dir.0.join("backup.key"),
        vault_key: None,
    });
    let _fallback = start_reader(&broken).await;
    let _model = read_model(&broken.listen, "orders-count");
    assert_eq!(count(&broken.listen, position(&last)).await.unwrap(), 26);
}

/// The writer's machine is lost: the standby is promoted, and the reader, given both
/// addresses, follows the new primary on, without starting over.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_follows_the_standby_after_a_failover() {
    let dir = Dir::new("readers-failover");
    let writer_config = writer_config(&dir.0, true);
    // A runtime of its own: shut down, it is the writer's lost machine.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let writer = {
        let _entered = runtime.enter();
        Kernel::open(writer_config.clone()).unwrap()
    };
    runtime.spawn(serve(Arc::clone(&writer)));
    wait_for(&writer_config.listen).await;
    grant_defaults(&writer);
    let standby_config = standby_config(&dir.0, &writer_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || {
        let (_, path) = kernable_store::backend(&standby_config.store);
        Path::new(&format!("{path}.in-sync")).exists()
    })
    .await;
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;

    let both = format!("{},{}", writer_config.listen, standby_config.listen);
    let reader_config = reader_config(&dir.0, "reader", 21, &both);
    let reader = start_reader(&reader_config).await;
    let _model = read_model(&reader_config.listen, "orders-count");
    let mut told = 0;
    for i in 0..10 {
        told = position(&submit(&reader_config.listen, place(i)).await.unwrap());
    }
    assert_eq!(count(&reader_config.listen, told).await.unwrap(), 10);

    // The writer is lost; the standby is promoted and serves.
    drop(writer);
    runtime.shutdown_background();
    let issued_at = issued_now();
    ReplicationServiceClient::new(transport::connect(&standby_config.listen).await.unwrap())
        .promote(PromoteRequest {
            issued_at,
            sig: Some(signing::sign(&key(1), &signing::promote_message(issued_at))),
            protocol: kernable_core::PROTOCOL,
        })
        .await
        .unwrap();
    following.await.unwrap().unwrap();
    let primary = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;

    // Meanwhile the reader answered from its copy; it follows the new primary now.
    assert_eq!(count(&reader_config.listen, 0).await.unwrap(), 10);
    let mut after = 0;
    for i in 10..15 {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match submit(&reader_config.listen, place(i)).await {
                    Ok(r) => return r,
                    // The reader turns to the new primary within a few tries.
                    Err(e) if e.code() == Code::Unavailable => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => panic!("{e:?}"),
                }
            }
        })
        .await
        .expect("the reader reaches the new primary");
        after = position(&result);
    }
    assert!(after > told);
    assert_eq!(count(&reader_config.listen, after).await.unwrap(), 15);
    assert_eq!(reader.role()["writer"], standby_config.listen.as_str());
    assert_eq!(primary.role()["readers"].as_array().unwrap().len(), 1);
}

/// Under load on the writer, a reader falls behind by little and catches up; every command a
/// client was told of is visible on the reader with its position.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_keeps_up_under_load() {
    let dir = Dir::new("readers-load");
    let writer_config = writer_config(&dir.0, false);
    let writer = open(&writer_config).await;
    grant_defaults(&writer);
    start_worker(writer_config.listen.clone(), TestWorker::honest()).await;
    let reader_config = reader_config(&dir.0, "reader", 21, &writer_config.listen);
    let reader = start_reader(&reader_config).await;
    let _model = read_model(&reader_config.listen, "orders-count");

    let lag = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let watching = {
        let (reader, lag) = (Arc::clone(&reader), Arc::clone(&lag));
        tokio::spawn(async move {
            loop {
                let now = reader.role()["lag"].as_u64().unwrap();
                lag.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    let clients: Vec<_> = (0..8u32)
        .map(|c| {
            let addr = writer_config.listen.clone();
            tokio::spawn(async move {
                let mut last = 0;
                for i in 0..50 {
                    let result = submit_with(&addr, place(c * 1000 + i), Wait::Committed)
                        .await
                        .unwrap();
                    last = position(&result);
                }
                last
            })
        })
        .collect();
    let mut newest = 0;
    for client in clients {
        newest = newest.max(client.await.unwrap());
    }
    // Every command is on the reader once it reached the newest position.
    assert_eq!(count(&reader_config.listen, newest).await.unwrap(), 400);
    until("the reader holds the writer's head", || {
        reader.role()["lag"].as_u64() == Some(0)
    })
    .await;
    watching.abort();
    let most = lag.load(std::sync::atomic::Ordering::SeqCst);
    eprintln!("the reader was at most {most} records behind under load");
    assert!(
        most < 8_192,
        "a reader stays within one window of the writer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_without_readers_on_refuses_a_reader() {
    let dir = Dir::new("readers-refused");
    let mut writer_config = writer_config(&dir.0, false);
    writer_config.replication.readers = false;
    let _writer = open(&writer_config).await;
    let reader_config = reader_config(&dir.0, "reader", 21, &writer_config.listen);
    // It keeps trying; it never catches up.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), standby::run(reader_config))
            .await
            .is_err()
    );
}
