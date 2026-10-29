//! Registered aggregates: commands no worker registered are refused, the kernel is not ready
//! while a registered aggregate has no worker, and a service unregisters what it no longer
//! declares.

mod common;

use std::time::Duration;

use common::*;
use kernable_core::proto::{Command, WorkerAggregate, command_result::Outcome};
use tonic::Code;

fn aggregate(aggregate_type: &str, command_types: &[&str]) -> WorkerAggregate {
    WorkerAggregate {
        aggregate_type: aggregate_type.into(),
        command_types: command_types.iter().map(|t| t.to_string()).collect(),
    }
}

fn shop(aggregates: Vec<WorkerAggregate>, declared: &[&str]) -> TestWorker {
    TestWorker {
        aggregates,
        service: "shop".into(),
        declared: declared.iter().map(|a| a.to_string()).collect(),
        ..TestWorker::honest()
    }
}

fn invoice(id: &str) -> Command {
    Command {
        aggregate_type: "Invoice".into(),
        ..command(id, "SendInvoice", "i1")
    }
}

/// Waits until the kernel's readiness is `expected`.
async fn readiness(env: &Env, expected: Result<(), &str>) {
    let expected = expected.map_err(str::to_owned);
    for _ in 0..100 {
        if env.kernel.readiness() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(env.kernel.readiness(), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_no_worker_registered_are_refused_and_wait_while_it_is_away() {
    let env = start("workers-registered").await;
    let refused = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code(), Code::NotFound, "{refused:?}");
    readiness(&env, Ok(())).await; // nothing registered, nothing missing

    let worker = TestWorker {
        command_types: vec!["orders.PlaceOrder".into()],
        ..TestWorker::honest()
    };
    let running = start_worker(env.addr.clone(), worker).await;
    let placed = submit(
        &env.addr,
        signed(command("c2", "PlaceOrder", "o1"), &key(1)),
    )
    .await;
    assert!(matches!(
        placed.unwrap().outcome,
        Some(Outcome::Committed(_))
    ));
    // A command type the aggregate does not have, and an aggregate nobody registered.
    for cmd in [command("c3", "AddItem", "o1"), invoice("c4")] {
        let refused = submit(&env.addr, signed(cmd, &key(1))).await.unwrap_err();
        assert_eq!(refused.code(), Code::NotFound, "{refused:?}");
    }

    // While the worker is away, the kernel is not ready, and commands wait for it.
    running.abort();
    readiness(&env, Err("waiting for workers: orders.Order")).await;
    let queued = signed(command("c5", "PlaceOrder", "o2"), &key(1));
    let accepted = submit_with(&env.addr, queued, kernable_core::proto::Wait::Accepted).await;
    assert!(matches!(
        accepted.unwrap().outcome,
        Some(Outcome::Accepted(_))
    ));
    start_worker(env.addr.clone(), TestWorker::honest()).await;
    readiness(&env, Ok(())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_unregisters_the_aggregates_it_no_longer_declares() {
    let env = start("workers-service").await;
    let both = ["orders.Order", "orders.Invoice"];
    let v1 = shop(
        vec![
            aggregate("orders.Order", &["orders.PlaceOrder"]),
            aggregate("orders.Invoice", &["orders.SendInvoice"]),
        ],
        &both,
    );
    start_worker(env.addr.clone(), v1).await.abort();
    readiness(
        &env,
        Err("waiting for workers: orders.Invoice, orders.Order"),
    )
    .await;

    // A worker must declare its own aggregates.
    let undeclared = shop(vec![aggregate("orders.Order", &["orders.PlaceOrder"])], &[]);
    let status = run_worker(env.addr.clone(), undeclared).await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");

    // Version 2 has no invoices any more: they are refused, and nobody waits for them.
    let v2 = shop(
        vec![aggregate("orders.Order", &["orders.PlaceOrder"])],
        &["orders.Order"],
    );
    start_worker(env.addr.clone(), v2).await;
    readiness(&env, Ok(())).await;
    let refused = submit(&env.addr, signed(invoice("c1"), &key(1)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::NotFound, "{refused:?}");

    let store = kernable_store::open_read_only(&env.store).unwrap();
    let workers = store.workers().unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(
        (
            workers[0].aggregate_type.as_str(),
            workers[0].service.as_str()
        ),
        ("orders.Order", "shop")
    );
    assert!(workers[0].last_seen > 0);
}
