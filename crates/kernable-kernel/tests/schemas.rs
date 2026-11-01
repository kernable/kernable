//! Event schemas: events carry the payload version their worker declared, and a worker that
//! would write an older version than the store holds is refused.

mod common;

use common::*;
use kernable_core::{
    proto::{NewEvent, Rejected, command_result::Outcome},
    signing,
};

fn worker(schemas: &[(&str, u32)], writes: Option<u32>) -> TestWorker {
    TestWorker {
        schemas: schemas.iter().map(|(t, v)| (t.to_string(), *v)).collect(),
        writes,
        ..TestWorker::honest()
    }
}

fn place(id: &str, order: &str) -> kernable_core::proto::Command {
    signed(command(id, "PlaceOrder", order), &key(1))
}

fn schemas(result: &kernable_core::proto::CommandResult) -> Vec<u32> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.iter().map(|e| e.schema).collect(),
        other => panic!("expected committed, got {other:?}"),
    }
}

fn rejection(result: &kernable_core::proto::CommandResult) -> &Rejected {
    match &result.outcome {
        Some(Outcome::Rejected(r)) => r,
        other => panic!("expected rejected, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn events_keep_their_schema_and_an_outdated_worker_cannot_write() {
    let env = start("schema-outdated").await;
    let v2 = start_worker(env.addr.clone(), worker(&[("orders.OrderPlaced", 2)], None)).await;
    let placed = submit(&env.addr, place("c1", "o1")).await.unwrap();
    assert_eq!(schemas(&placed), [2]);
    v2.abort();

    // An old worker that declares the type is refused when it registers.
    let declared = run_worker(env.addr.clone(), worker(&[("orders.OrderPlaced", 1)], None)).await;
    assert_eq!(declared.code(), tonic::Code::FailedPrecondition);
    assert!(declared.message().contains("schema 2"), "{declared:?}");

    // One that does not declare it is refused at its first result; the command waits.
    let addr = env.addr.clone();
    let pending = tokio::spawn(async move { submit(&addr, place("c2", "o2")).await });
    let silent = run_worker(env.addr.clone(), worker(&[], None)).await;
    assert_eq!(silent.code(), tonic::Code::FailedPrecondition);

    // A current worker runs it.
    start_worker(env.addr.clone(), worker(&[("orders.OrderPlaced", 2)], None)).await;
    assert_eq!(schemas(&pending.await.unwrap().unwrap()), [2]);

    let store = kernable_store::open_read_only(&env.store).unwrap();
    let stored: Vec<_> = store.read_all(0, 10).unwrap();
    assert_eq!(stored.iter().map(|e| e.schema).collect::<Vec<_>>(), [2, 2]);
    assert_eq!(
        store.event_schemas().unwrap(),
        [("orders.OrderPlaced".to_string(), 2)]
    );
    let report = store.verify(&[id(&env.kernel_key).to_string()]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_remembers_stored_schemas_across_restarts() {
    let env = start_custom("schema-restart", |config, _| {
        // Events written by an earlier kernel.
        let store = kernable_store::open(&config.store).unwrap();
        let cmd = command("c0", "PlaceOrder", "o0");
        store.enqueue(&cmd, 0).unwrap();
        let events = [NewEvent {
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: "OrderPlaced".into(),
            payload: b"{}".to_vec(),
            schema: 3,
        }];
        let sig = signing::sign(&key(2), &signing::result_message(&cmd, 0, &events));
        store.commit("c0", 0, &events, &sig, None, 0).unwrap();
    })
    .await;
    let refused = run_worker(env.addr.clone(), worker(&[("orders.OrderPlaced", 2)], None)).await;
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);

    start_worker(env.addr.clone(), worker(&[("orders.OrderPlaced", 3)], None)).await;
    assert_eq!(
        schemas(&submit(&env.addr, place("c1", "o1")).await.unwrap()),
        [3]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_writes_exactly_what_it_declared() {
    let env = start("schema-declared").await;
    let liar = start_worker(
        env.addr.clone(),
        worker(&[("orders.OrderPlaced", 2)], Some(3)),
    )
    .await;
    let result = submit(&env.addr, place("c1", "o1")).await.unwrap();
    assert_eq!(rejection(&result).code, "invalid_event_schema");
    liar.abort();

    // Undeclared types are written at 1, never at 0.
    let zero = start_worker(env.addr.clone(), worker(&[], Some(0))).await;
    let result = submit(&env.addr, place("c2", "o2")).await.unwrap();
    assert_eq!(rejection(&result).code, "invalid_event_schema");
    zero.abort();

    for bad in [
        &[("orders.OrderPlaced", 0)][..],
        &[("OrderPlaced", 1)],
        &[("identity.AccountRegistered", 1)], // not a domain of its commands
    ] {
        let status = run_worker(env.addr.clone(), worker(bad, None)).await;
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{bad:?}");
    }
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(store.head_position().unwrap(), 0);
}
