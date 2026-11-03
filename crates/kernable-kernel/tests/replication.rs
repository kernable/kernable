//! A primary with `--replication sync` and its standby (design/replication.md): every
//! write a client is told of is on the standby first (P1), the standby refuses clients, and a
//! promoted standby serves what the primary held, with the grants it got from it.

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
        primary.readiness().unwrap_err().contains("standby"),
        "not ready without a standby"
    );

    let standby_config = standby_config(&dir.0, &primary_config);
    let following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || primary.readiness().is_ok()).await;
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
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    let _following = tokio::spawn(standby::run(standby_config.clone()));
    until("the standby is in sync", || primary.readiness().is_ok()).await;
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
    let primary = open(&primary_config).await;
    let standby_config = standby_config(&dir.0, &primary_config);
    // Its grant allows something else.
    let grant = grant(&key(7), &[("listen", &["*"])]);
    std::fs::write(dir.0.join("standby.grant"), hex(&grant.encode_to_vec())).unwrap();
    let following = tokio::spawn(standby::run(standby_config.clone()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(primary.readiness().is_err(), "no standby is in sync");
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
    until("the standby is in sync", || primary.readiness().is_ok()).await;
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
    until("the standby is in sync", || primary.readiness().is_ok()).await;
    primary.revoke(&id(&key(7)), 0).unwrap();
    until("the revoked standby is no longer in sync", || {
        primary.readiness().is_err()
    })
    .await;
    following.abort();
}
