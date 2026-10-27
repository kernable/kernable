//! Compatibility: stores written in data format 1 keep working, and requests of a protocol
//! version the kernel does not speak are refused with a clear message.
//!
//! The stores in `tests/fixtures/format-1` are frozen: never regenerate them. A new data format
//! adds a new directory (written with `KERNABLE_WRITE_FIXTURES=1`, which only fills directories
//! that do not exist yet) and keeps the old ones, which `kernable migrate` must then upgrade.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use kernable_core::{
    PROTOCOL,
    proto::{
        Attribute, Command, Hello, ListenerMessage, ListenerRecord, Middleware, NewEvent,
        QueryRequest, Register, Rejected, RequestContext, Snapshot, SnapshotRecord, SubmitRequest,
        SubscribeRequest, Wait, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, event_service_client::EventServiceClient,
        listener_message, listener_service_client::ListenerServiceClient,
        query_service_client::QueryServiceClient, worker_gateway_client::WorkerGatewayClient,
        worker_message,
    },
    signing, transport,
};
use kernable_store::Store;
use tokio_stream::StreamExt;

const AT: i64 = 1_700_000_000_000;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-1")
}

/// What every format-1 fixture holds: two committed commands on `o1` (with a request context,
/// event schemas 2 and 1 and a snapshot), a rejected one, a pending one on `o2`, two listeners
/// (one a middleware) and a checkpoint.
fn fill(store: &dyn Store) {
    let context = RequestContext {
        sender_identity: "i-1".into(),
        correlation_id: "r-1".into(),
        attributes: vec![Attribute {
            key: "channel".into(),
            value: "web".into(),
        }],
        ..Default::default()
    };
    let cmd = |id: &str, command_type: &str, aggregate: &str| {
        let mut cmd = command(id, command_type, aggregate);
        cmd.context = Some(context.clone());
        signed(cmd, &key(1))
    };
    let commit = |cmd: &Command, version: u64, event: (&str, u32, &str)| {
        store.enqueue(cmd, AT).unwrap();
        let events = [NewEvent {
            event_type: event.0.into(),
            schema: event.1,
            payload: event.2.as_bytes().to_vec(),
        }];
        let msg = signing::result_message(cmd, version, &events);
        store
            .commit(
                &cmd.command_id,
                version,
                &events,
                &signing::sign(&key(2), &msg),
                AT,
            )
            .unwrap();
    };
    let c1 = cmd("c1", "PlaceOrder", "o1");
    commit(
        &c1,
        0,
        ("OrderPlaced", 2, r#"{"customer":"k-1","currency":"EUR"}"#),
    );
    commit(
        &cmd("c2", "AddItem", "o1"),
        1,
        ("ItemAdded", 1, r#"{"sku":"apple"}"#),
    );
    let state = b"2".to_vec();
    let snapshot_msg = signing::snapshot_message(&c1, 2, "1", &state);
    let o1 = store.read_stream("t1", "orders", "Order", "o1").unwrap();
    store
        .save_snapshot(&SnapshotRecord {
            tenant_id: "t1".into(),
            domain: "orders".into(),
            aggregate_type: "Order".into(),
            aggregate_id: "o1".into(),
            snapshot: Some(Snapshot {
                version: 2,
                schema: "1".into(),
                state,
                worker_sig: Some(signing::sign(&key(2), &snapshot_msg)),
            }),
            event_hash: o1[1].hash.clone(),
            removed: false,
        })
        .unwrap();
    let c3 = cmd("c3", "AddItem", "o9");
    store.enqueue(&c3, AT).unwrap();
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "AddItem not allowed now".into(),
    };
    store.reject("c3", reason).unwrap();
    store.enqueue(&cmd("c4", "PlaceOrder", "o2"), AT).unwrap();
    let listener = |name: &str, types: &str, middleware: Option<Middleware>| ListenerRecord {
        name: name.into(),
        key_id: id(&key(5)),
        order: 0,
        event_types: vec![types.into()],
        position: 2,
        removed: false,
        middleware,
    };
    let guard = Middleware {
        order: 0,
        types: vec!["identity.*".into()],
    };
    store
        .save_listeners(&[
            listener("orders", "orders.*", None),
            listener("guard", "identity.*", Some(guard)),
        ])
        .unwrap();
    store.checkpoint(&key(3), AT).unwrap();
}

/// Copies the fixture of `backend` into `dir` and returns the copy's store URL.
fn copy(backend: &str, file: &str, dir: &Path) -> String {
    let fixture = fixtures().join(file);
    if std::env::var_os("KERNABLE_WRITE_FIXTURES").is_some() && !fixtures().exists() {
        std::fs::create_dir_all(fixtures()).unwrap();
    }
    if std::env::var_os("KERNABLE_WRITE_FIXTURES").is_some() && !fixture.exists() {
        let url = format!("{backend}:{}", fixture.display());
        fill(kernable_store::open(&url).unwrap().as_ref());
        if backend == "sqlite" {
            // One self-contained file: fold the write-ahead log into it.
            let conn = rusqlite::Connection::open(&fixture).unwrap();
            conn.pragma_update(None, "journal_mode", "DELETE").unwrap();
        }
    }
    let target = dir.join(file);
    std::fs::copy(&fixture, &target).unwrap();
    format!("{backend}:{}", target.display())
}

fn outcome(result: &kernable_core::proto::CommandResult) -> &Outcome {
    result.outcome.as_ref().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn stores_of_format_1_still_open_verify_and_run() {
    for (backend, file) in [("sqlite", "k.db"), ("file", "k.log")] {
        let mut url = String::new();
        let env = start_custom(&format!("compat-{backend}"), |config, dir| {
            url = copy(backend, file, dir);
            config.store = url.clone();
        })
        .await;

        let store = kernable_store::open_read_only(&url).unwrap();
        let report = store.verify(&id(&key(3))).unwrap();
        assert!(report.is_ok(), "{backend}: {:?}", report.problems);
        let counts = (report.events, report.commands, report.checkpoints);
        assert_eq!((counts, report.snapshots), ((2, 4, 1), 1), "{backend}");
        assert_eq!(
            store.event_schemas().unwrap(),
            [
                ("orders.ItemAdded".to_string(), 1),
                ("orders.OrderPlaced".to_string(), 2)
            ]
        );
        let events = store.read_all(0, 10).unwrap();
        let context = events[0].context.as_ref().unwrap();
        assert_eq!(context.attributes[0].value, "web");
        let rejected = store.result("c3").unwrap().unwrap();
        assert!(matches!(outcome(&rejected), Outcome::Rejected(r) if r.code == "invalid_state"));
        let listeners = store.listeners().unwrap();
        assert_eq!(listeners.len(), 2);
        assert!(
            listeners[0].middleware.is_some(),
            "{backend}: guard is a middleware"
        );
        drop(store);

        // A worker of today runs the pending command and continues the stream after the snapshot.
        let (tx, mut executions) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(run_worker(
            env.addr.clone(),
            TestWorker {
                schemas: vec![("orders.OrderPlaced".into(), 2)],
                executions: Some(tx),
                ..TestWorker::honest()
            },
        ));
        let pending = signed(
            Command {
                context: Some(RequestContext {
                    sender_identity: "i-1".into(),
                    correlation_id: "r-1".into(),
                    attributes: vec![Attribute {
                        key: "channel".into(),
                        value: "web".into(),
                    }],
                    ..Default::default()
                }),
                ..command("c4", "PlaceOrder", "o2")
            },
            &key(1),
        );
        let c4 = submit_with(&env.addr, pending, Wait::Committed)
            .await
            .unwrap();
        assert_eq!(committed_versions(&c4), [1], "{backend}");
        let c5 = signed(command("c5", "AddItem", "o1"), &key(1));
        let c5 = submit_with(&env.addr, c5, Wait::Committed).await.unwrap();
        assert_eq!(committed_versions(&c5), [3], "{backend}");
        let seen: Vec<_> = std::iter::from_fn(|| executions.try_recv().ok()).collect();
        assert_eq!(seen, [(None, 0), (Some(2), 0)], "{backend}");

        let store = kernable_store::open_read_only(&url).unwrap();
        let report = store.verify(&id(&key(3))).unwrap();
        assert!(report.is_ok(), "{backend}: {:?}", report.problems);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn other_protocol_versions_are_refused_everywhere() {
    let env = start("compat-protocol").await;
    for (version, advice) in [(0, "update the SDK"), (PROTOCOL + 1, "update the kernel")] {
        let channel = transport::connect(&env.addr).await.unwrap();
        let mut refusals = Vec::new();

        let submit = SubmitRequest {
            protocol: version,
            ..Default::default()
        };
        refusals.push(
            CommandServiceClient::new(channel.clone())
                .submit(submit)
                .await
                .err(),
        );
        let query = QueryRequest {
            protocol: version,
            ..Default::default()
        };
        refusals.push(
            QueryServiceClient::new(channel.clone())
                .query(query)
                .await
                .err(),
        );
        let subscribe = SubscribeRequest {
            protocol: version,
            ..Default::default()
        };
        let subscribed = EventServiceClient::new(channel.clone())
            .subscribe(subscribe)
            .await;
        refusals.push(subscribed.err());

        let register = WorkerMessage {
            msg: Some(worker_message::Msg::Register(Register {
                protocol: version,
                ..Default::default()
            })),
        };
        let session = WorkerGatewayClient::new(channel.clone())
            .session(tokio_stream::iter([register]))
            .await;
        refusals.push(match session {
            Ok(r) => r.into_inner().next().await.and_then(Result::err),
            Err(status) => Some(status),
        });
        let hello = ListenerMessage {
            msg: Some(listener_message::Msg::Hello(Hello {
                protocol: version,
                ..Default::default()
            })),
        };
        let listen = ListenerServiceClient::new(channel)
            .listen(tokio_stream::iter([hello]))
            .await;
        refusals.push(match listen {
            Ok(r) => r.into_inner().next().await.and_then(Result::err),
            Err(status) => Some(status),
        });

        for (i, refusal) in refusals.into_iter().enumerate() {
            let status = refusal.unwrap_or_else(|| panic!("request {i} was not refused"));
            assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "{i}: {status:?}"
            );
            assert!(status.message().contains(advice), "{i}: {status:?}");
        }
    }
}
