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
        CacheRequest, Command, CommandResult, DataRequest, Event, PromoteRequest, PromoteResult,
        cache_service_client::CacheServiceClient, command_result::Outcome,
        data_service_client::DataServiceClient,
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
    let dir = Dir::new("repl-follow");
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

/// A cache request of key 9, signed, on `shop` in tenant `t`.
fn cache(op: &str, name: &str, value: &[u8]) -> CacheRequest {
    let mut r = CacheRequest {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: name.into(),
        value: value.to_vec(),
        ttl_ms: 0,
        issued_at: now_ms(),
        client_sig: None,
        protocol: kernable_core::PROTOCOL,
        context: None,
    };
    r.client_sig = Some(signing::sign(&key(9), &signing::cache_message(op, &r)));
    r
}

/// The version of the snapshot of order `o1` the kernel of `config` keeps, if any.
fn snapshot_of_o1(config: &Config) -> Option<u64> {
    let stream = kernable_store::StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o1".into(),
    };
    let snapshots = kernable_store::snapshots::open(&config.snapshots).ok()?;
    snapshots
        .get(&stream, "")
        .ok()??
        .snapshot
        .map(|s| s.version)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_takes_over_with_the_primarys_snapshots_and_cache() {
    let dir = Dir::new("repl-warm");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2, 4, 5, 6] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    primary
        .grant(grant(&key(9), &[("cache", &["shop.*"])]))
        .unwrap();
    start_worker(
        primary_config.listen.clone(),
        TestWorker {
            behavior: Behavior::Snapshots,
            ..TestWorker::honest()
        },
    )
    .await;
    for i in 0..4 {
        let kind = if i == 0 { "PlaceOrder" } else { "AddItem" };
        let cmd = signed(command(&format!("c{i}"), kind, "o1"), &key(1));
        committed_versions(&submit(&primary_config.listen, cmd).await.unwrap());
    }
    let mut cache_client =
        CacheServiceClient::new(transport::connect(&primary_config.listen).await.unwrap());
    cache_client
        .set_cache(cache("set", "quote", b"42"))
        .await
        .unwrap();
    cache_client
        .set_cache(cache("set", "draft", b"x"))
        .await
        .unwrap();
    cache_client
        .delete_cache(cache("delete", "draft", b""))
        .await
        .unwrap();

    // The primary's snapshot reaches the standby once it holds its event.
    assert_eq!(snapshot_of_o1(&primary_config), Some(4));
    until("the standby keeps o1's snapshot", || {
        snapshot_of_o1(&standby_config) == Some(4)
    })
    .await;

    // Promoted, the new primary starts warm: the snapshot and the cache values are there.
    tokio::time::sleep(Duration::from_millis(300)).await;
    promote(&standby_config.listen).await.unwrap();
    following.await.unwrap().unwrap();
    let _new = open(&standby_config).await;
    assert_eq!(snapshot_of_o1(&standby_config), Some(4));
    let mut cache_client =
        CacheServiceClient::new(transport::connect(&standby_config.listen).await.unwrap());
    let quote = cache_client
        .get_cache(cache("get", "quote", b""))
        .await
        .unwrap()
        .into_inner();
    assert!(quote.found, "the cache value set on the primary");
    assert_eq!(quote.value, b"42");
    let draft = cache_client
        .get_cache(cache("get", "draft", b""))
        .await
        .unwrap()
        .into_inner();
    assert!(!draft.found, "deleted on the primary, gone here too");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_of_a_primary_that_ran_alone_takes_an_image_and_follows() {
    let dir = Dir::new("repl-image");
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

/// A scheduled command is a pending command like any other: on the standby before the client
/// is told it is accepted, and run by the promoted standby when it is due.
#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_command_reaches_the_standby_and_runs_after_a_promotion() {
    let dir = Dir::new("repl-scheduled");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2, 4, 5, 6] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    let worker = start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    let placed = submit(&primary_config.listen, place(1)).await.unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    let due = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1_500;
    let mut later = command("s1", "AddItem", "o1");
    later.deliver_at = due;
    let accepted = submit(&primary_config.listen, signed(later, &key(1)))
        .await
        .unwrap();
    assert!(
        matches!(accepted.outcome, Some(Outcome::Accepted(_))),
        "{accepted:?}"
    );
    // Nothing runs on the old primary any more.
    worker.abort();
    let pending = held(&standby_config).pending().unwrap();
    assert!(
        pending
            .iter()
            .any(|p| p.command.command_id == "s1" && p.command.deliver_at == due),
        "{pending:?}"
    );

    promote(&standby_config.listen).await.unwrap();
    following.await.unwrap().unwrap();
    let _new = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    until("the promoted standby ran it", || {
        held(&standby_config).result("s1").unwrap().is_some()
    })
    .await;
    let ran = held(&standby_config).result("s1").unwrap().unwrap();
    assert_eq!(committed_versions(&ran), [2]);
    let at = events(&ran)[0].created_at;
    assert!(at >= due, "run at {at}, due at {due}");
}

/// A process registered on the primary is on the standby, versions and state, and the promoted
/// standby knows it.
#[tokio::test(flavor = "multi_thread")]
async fn processes_reach_the_standby() {
    use kernable_core::proto::{ProcessRegistration, process_service_client::ProcessServiceClient};
    let dir = Dir::new("repl-processes");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2, 4, 5, 6] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    let deployer = grant(
        &key(21),
        &[
            ("process", &["orders.*"]),
            ("submit", &["orders.*"]),
            ("listen", &["orders.*"]),
        ],
    );
    primary.grant(deployer.clone()).unwrap();
    start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    let model = serde_json::to_vec(&serde_json::json!({
        "name": "orders.follow",
        "start": {"on": "orders.OrderPlaced", "key": "aggregateId", "data": {"order": "aggregateId"}},
        "steps": [
            {"name": "add", "kind": "command", "command": "orders.Order.AddItem", "aggregate": "case.order", "next": "done"},
            {"name": "done", "kind": "end"}
        ]
    }))
    .unwrap();
    let register = |model: Vec<u8>| {
        let (addr, deployer) = (primary_config.listen.clone(), deployer.clone());
        async move {
            let issued_at = issued_now();
            let request = ProcessRegistration {
                sig: Some(signing::sign(
                    &key(21),
                    &signing::process_message(&model, issued_at),
                )),
                model,
                issued_at,
                protocol: kernable_core::PROTOCOL,
            };
            ProcessServiceClient::new(transport::connect(&addr).await.unwrap())
                .register(presenting(request, &deployer))
                .await
                .map(tonic::Response::into_inner)
        }
    };
    let registered = register(model.clone()).await.unwrap();
    assert_eq!((registered.version, registered.new), (1, true));
    // As whom it acts, too.
    let acting = kernable_core::proto::RequestContext {
        sender_tenant: "platform".into(),
        sender_identity: "proc-1".into(),
        sender_session: "key-1".into(),
        ..Default::default()
    };
    let issued_at = issued_now();
    let request = kernable_core::proto::ProcessActing {
        name: "orders.follow".into(),
        context: Some(acting.clone()),
        issued_at,
        sig: Some(signing::sign(
            &key(21),
            &signing::process_act_message("orders.follow", Some(&acting), issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(&primary_config.listen).await.unwrap())
        .act(presenting(request, &deployer))
        .await
        .unwrap();
    let state = held(&standby_config)
        .processes()
        .unwrap()
        .into_iter()
        .find(|r| r.version == 0)
        .expect("the state is on the standby");
    assert_eq!(state.acting, Some(acting));
    let on_standby: Vec<_> = held(&standby_config)
        .processes()
        .unwrap()
        .into_iter()
        .filter(|r| r.version > 0)
        .collect();
    assert_eq!(on_standby.len(), 1, "{on_standby:?}");
    assert_eq!(on_standby[0].model, model);

    promote(&standby_config.listen).await.unwrap();
    following.await.unwrap().unwrap();
    let _new = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    let issued_at = issued_now();
    let request = ProcessRegistration {
        sig: Some(signing::sign(
            &key(21),
            &signing::process_message(&model, issued_at),
        )),
        model: model.clone(),
        issued_at,
        protocol: kernable_core::PROTOCOL,
    };
    let again =
        ProcessServiceClient::new(transport::connect(&standby_config.listen).await.unwrap())
            .register(presenting(request, &deployer))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(
        (again.version, again.new),
        (1, false),
        "the promoted standby holds version 1"
    );
}

/// A case waiting on the primary goes on on the promoted standby: its events, the engine's
/// position and the case's running fact are on the standby before anyone is told.
#[tokio::test(flavor = "multi_thread")]
async fn a_case_goes_on_after_a_failover() {
    use kernable_core::proto::{ProcessRegistration, process_service_client::ProcessServiceClient};
    let dir = Dir::new("repl-case-failover");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    for seed in [1, 2, 4, 5, 6] {
        primary.grant(default_grant(&key(seed)).unwrap()).unwrap();
    }
    let deployer = grant(
        &key(21),
        &[
            ("process", &["orders.*"]),
            ("submit", &["orders.*"]),
            ("listen", &["orders.*"]),
        ],
    );
    primary.grant(deployer.clone()).unwrap();
    let worker = start_worker(primary_config.listen.clone(), TestWorker::honest()).await;
    let model = serde_json::to_vec(&serde_json::json!({
        "name": "orders.follow",
        "start": {"on": "orders.OrderPlaced", "key": "aggregateId", "data": {"order": "aggregateId"}},
        "steps": [
            {"name": "added", "kind": "wait", "event": "orders.ItemAdded", "key": "aggregateId", "next": "more"},
            {"name": "more", "kind": "command", "command": "orders.Order.AddItem", "aggregate": "case.order", "next": "done"},
            {"name": "done", "kind": "end"}
        ]
    }))
    .unwrap();
    let issued_at = issued_now();
    let request = ProcessRegistration {
        sig: Some(signing::sign(
            &key(21),
            &signing::process_message(&model, issued_at),
        )),
        model,
        issued_at,
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(&primary_config.listen).await.unwrap())
        .register(presenting(request, &deployer))
        .await
        .unwrap();
    submit(&primary_config.listen, place(1)).await.unwrap();
    let waiting = || {
        held(&standby_config)
            .read_stream("t1", "process", "orders.follow", "o1#1")
            .unwrap()
            .iter()
            .any(|e| e.event_type == "StepReached")
    };
    until("the case waits, on the standby too", waiting).await;
    worker.abort();

    promote(&standby_config.listen).await.unwrap();
    following.await.unwrap().unwrap();
    let _new = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    let added = submit(
        &standby_config.listen,
        signed(command("c-add", "AddItem", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&added), [2]);
    let ended = || {
        held(&standby_config)
            .read_stream("t1", "process", "orders.follow", "o1#1")
            .unwrap()
            .iter()
            .any(|e| e.event_type == "ProcessEnded")
    };
    until("the case ended on the promoted standby", ended).await;
    let events = held(&standby_config)
        .read_stream("t1", "orders", "Order", "o1")
        .unwrap();
    assert_eq!(events.len(), 3, "the process added one more item");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_without_a_grant_for_replicate_is_refused() {
    let dir = Dir::new("repl-refused");
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

/// A standby whose newest records are its own, from when it was the primary (a command it
/// enqueued), takes exactly those back and follows on, without setting its stores aside or
/// copying everything again (design/rewind.md).
#[tokio::test(flavor = "multi_thread")]
async fn a_standby_takes_back_its_own_newest_records_and_follows_on() {
    let dir = Dir::new("repl-rewound");
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
    assert!(aside.is_empty(), "nothing set aside: {aside:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_whose_log_diverged_moves_its_stores_aside_and_starts_over() {
    let dir = Dir::new("repl-diverged");
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    // The standby holds a history the primary never had (a former primary's own tail), one
    // it cannot take back: a data file's change (the file is outside the log).
    {
        let store = kernable_store::open(&standby_config.store).unwrap();
        store.keep_record_log(true).unwrap();
        store.enqueue(&place(77), 0).unwrap();
        store.data_changed("t1", "orders", "invoice").unwrap();
    }
    // A copy set aside before: it goes, only the newest stays.
    let (_, store_path) = kernable_store::backend(&standby_config.store);
    std::fs::write(format!("{store_path}.diverged-1"), b"older").unwrap();
    std::fs::write(format!("{store_path}-wal.diverged-1"), b"older").unwrap();
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
    assert!(
        !aside.iter().any(|n| n.ends_with(".diverged-1")),
        "the older copy is gone: {aside:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_whose_key_is_revoked_follows_no_longer() {
    let dir = Dir::new("repl-revoked");
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
    let dir = Dir::new("repl-one-worker");
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
    let dir = Dir::new("repl-catch-up");
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
    // A primary now: no note of a standby in sync stays beside its stores (a move back to a
    // server it left would otherwise read it as in sync at once).
    assert!(
        !Path::new(&format!("{path}.in-sync")).exists(),
        "the note outlived the promotion"
    );
}

/// A switchover without a witness: the primary hands its role to its standby in sync,
/// which serves everything the primary told; the old primary does not start as the primary
/// again, and started to follow, it follows the new one.
#[tokio::test(flavor = "multi_thread")]
async fn a_switchover_without_a_witness_hands_the_role_to_the_standby() {
    use kernable_core::proto::SwitchoverRequest;
    let dir = Dir::new("repl-switchover");
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

/// One request to a kernel's local socket: the whole response.
async fn local(socket: &Path, method: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::UnixStream::connect(socket).await.unwrap();
    conn.write_all(format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    response
}

/// A switchover on the primary's local socket, with no key: the same checks as over the
/// network. Refused without a standby (`kernable switchover --socket`), and with one in sync
/// it hands the role over without losing a told write.
#[tokio::test(flavor = "multi_thread")]
async fn a_switchover_on_the_local_socket_needs_no_key_and_makes_the_same_checks() {
    let dir = Dir::new("repl-switchover-local");
    // Alone, without --replication sync: refused, by the CLI too.
    let mut alone = config(&dir.0, "alone", 3);
    let alone_socket = dir.0.join("alone-admin.sock");
    alone.admin_socket = Some(alone_socket.display().to_string());
    let _alone = open(&alone).await;
    until("the local socket is up", || alone_socket.exists()).await;
    assert!(
        local(&alone_socket, "GET", "/api/switchover")
            .await
            .starts_with("HTTP/1.1 405")
    );
    let refused = local(&alone_socket, "POST", "/api/switchover").await;
    assert!(refused.starts_with("HTTP/1.1 409"), "{refused}");
    assert!(refused.contains("without --replication sync"), "{refused}");
    let socket = alone_socket.display().to_string();
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
            .args(["switchover", "--socket", &socket])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("without --replication sync"),
        "{out:?}"
    );

    // A primary with a standby in sync hands over, asked on its socket.
    let mut primary_config = config(&dir.0, "primary", 3);
    primary_config.replication.sync = true;
    let primary_socket = dir.0.join("primary-admin.sock");
    primary_config.admin_socket = Some(primary_socket.display().to_string());
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
    standby_config.replication.sync = true;
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || in_sync(&standby_config)).await;
    let mut told = Vec::new();
    for i in 0..5 {
        told.push(submit(&primary_config.listen, place(i)).await.unwrap());
    }
    let answer = local(&primary_socket, "POST", "/api/switchover").await;
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.contains(&id(&key(7))), "names the standby: {answer}");
    let why = handed.await.unwrap();
    assert!(why.to_string().contains("handed over"), "{why}");
    following.await.unwrap().unwrap();
    primary.shutdown().unwrap();
    drop(primary);
    runtime.shutdown_background();

    // The standby serves, with everything the primary told.
    let _new = open(&standby_config).await;
    start_worker(standby_config.listen.clone(), TestWorker::honest()).await;
    for (i, result) in told.iter().enumerate() {
        let again = submit(&standby_config.listen, place(i as u32))
            .await
            .unwrap();
        assert_eq!(events(&again), events(result), "c{i}");
    }
}

/// A standby whose own tail completed a command that brought vault values (design/rewind.md):
/// the command is put back to pending if the vault still holds its values, and the standby
/// takes the tail back; with them gone (a crash dropped them), it starts over.
#[tokio::test(flavor = "multi_thread")]
async fn a_completion_with_vault_values_is_taken_back_while_they_are_held() {
    // The file log keeps no undo window: it cannot tell confirmed completions from others, so
    // their secrets go when the stores open, and such a tail starts over.
    if std::env::var("KERNABLE_TEST_STORE").as_deref() == Ok("file") {
        return;
    }
    for values_held in [true, false] {
        let dir = Dir::new(&format!("repl-rewind-values-{values_held}"));
        let mut primary_config = config(&dir.0, "primary", 3);
        primary_config.replication.sync = true;
        let mut standby_config = standby_config(&dir.0, &primary_config);
        let vault_dir = dir.0.join("standby-vault");
        std::fs::create_dir_all(&vault_dir).unwrap();
        with_vault(&mut standby_config, &vault_dir);
        // Both hold the command as enqueued (the same record); the standby, a former primary,
        // also committed it, a record the primary never got.
        let value = vault_value("", "4111-1111");
        let mut command = place(88);
        command.vault_refs = vec![value.r#ref.clone()];
        for config in [&primary_config, &standby_config] {
            let store = kernable_store::open(&config.store).unwrap();
            store.keep_record_log(true).unwrap();
            store.enqueue(&command, 1).unwrap();
        }
        {
            let store = kernable_store::open(&standby_config.store).unwrap();
            store.keep_record_log(true).unwrap();
            store
                .commit(&command.command_id, 0, &[], &worker_sig(), None, 2)
                .unwrap();
            let vault_config = standby_config.vault.as_ref().unwrap();
            let vault = kernable_store::vault::Vault::open(&vault_config.store, &[6; 32]).unwrap();
            if values_held {
                vault
                    .put(
                        std::slice::from_ref(&value),
                        &command.vault_refs,
                        &kernable_store::vault::Owner {
                            command_id: &command.command_id,
                            tenant: &command.tenant_id,
                            stream: None,
                        },
                        1,
                    )
                    .unwrap();
            }
        }
        let _primary = open(&primary_config).await;
        let _following = tokio::spawn(standby::run(standby_config.clone()));
        until("the standby is in sync", || in_sync(&standby_config)).await;
        let aside: Vec<String> = std::fs::read_dir(&dir.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".diverged-"))
            .collect();
        let standby = held(&standby_config);
        if values_held {
            assert!(aside.is_empty(), "taken back, nothing set aside: {aside:?}");
            assert_eq!(
                standby.result(&command.command_id).unwrap(),
                None,
                "pending again"
            );
        } else {
            assert!(!aside.is_empty(), "its values gone, it started over");
        }
    }
}

/// A rewind cut short by a crash after the store and before the vault (design/rewind.md):
/// when the stores open again, the values of the commands it took back go, those of commands
/// the store still holds stay.
#[test]
fn a_rewind_a_crash_cut_short_is_finished_when_the_stores_open() {
    let dir = Dir::new("repl-rewind-note");
    let mut config = config(&dir.0, "standby", 8);
    with_vault(&mut config, &dir.0);
    let (gone, kept) = (vault_value("", "gone"), vault_value("", "kept"));
    let store = kernable_store::open(&config.store).unwrap();
    store.keep_record_log(true).unwrap();
    let mut held = place(2);
    held.vault_refs = vec![kept.r#ref.clone()];
    store.enqueue(&held, 1).unwrap();
    let vault_config = config.vault.as_ref().unwrap();
    let vault = kernable_store::vault::Vault::open(&vault_config.store, &[6; 32]).unwrap();
    for (value, id) in [(&gone, "c1"), (&kept, "c2")] {
        vault
            .put(
                std::slice::from_ref(value),
                std::slice::from_ref(&value.r#ref),
                &kernable_store::vault::Owner {
                    command_id: id,
                    tenant: "t1",
                    stream: None,
                },
                1,
            )
            .unwrap();
    }
    let (_, path) = kernable_store::backend(&config.store);
    std::fs::write(format!("{path}.rewinding"), "c1\nc2").unwrap();
    standby::finish_rewind(&config.store, store.as_ref(), Some(&vault)).unwrap();
    let left: Vec<String> = vault
        .describe(&[gone.r#ref.clone(), kept.r#ref.clone()])
        .unwrap()
        .into_iter()
        .map(|(r, ..)| r)
        .collect();
    assert_eq!(left, std::slice::from_ref(&kept.r#ref));
    assert!(!Path::new(&format!("{path}.rewinding")).exists());
}

fn worker_sig() -> kernable_core::proto::Signature {
    kernable_core::proto::Signature {
        key_id: "worker".into(),
        sig: vec![0; 64],
    }
}
