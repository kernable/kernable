//! Registered aggregates: commands no worker registered are refused, the kernel is not ready
//! while a registered aggregate has no worker, and a service's membership decides which
//! aggregates it waits for.

mod common;

use std::time::Duration;

use common::*;
use kernable_core::proto::{Command, Wait, WorkerAggregate, command_result::Outcome};
use tonic::Code;

fn aggregate(aggregate_type: &str, command_types: &[&str]) -> WorkerAggregate {
    WorkerAggregate {
        aggregate_type: aggregate_type.into(),
        command_types: command_types.iter().map(|t| t.to_string()).collect(),
    }
}

fn shop(aggregates: Vec<WorkerAggregate>) -> TestWorker {
    TestWorker {
        aggregates,
        service: "shop".into(),
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
    let accepted = submit_with(&env.addr, queued, Wait::Accepted).await;
    assert!(matches!(
        accepted.unwrap().outcome,
        Some(Outcome::Accepted(_))
    ));
    start_worker(env.addr.clone(), TestWorker::honest()).await;
    readiness(&env, Ok(())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_membership_decides_which_aggregates_the_kernel_waits_for() {
    let env = start("workers-membership").await;
    let (order, invoices) = (
        aggregate("orders.Order", &["orders.PlaceOrder"]),
        aggregate("orders.Invoice", &["orders.SendInvoice"]),
    );

    // Listed before any worker registered: its commands wait, and so does readiness.
    apply(
        &env.addr,
        membership("shop", &["orders.Order", "orders.Invoice"], &[], &[]),
    )
    .await
    .unwrap();
    readiness(
        &env,
        Err("waiting for workers: orders.Invoice, orders.Order"),
    )
    .await;
    let queued = signed(invoice("c1"), &key(1));
    let accepted = submit_with(&env.addr, queued, Wait::Accepted).await;
    assert!(matches!(
        accepted.unwrap().outcome,
        Some(Outcome::Accepted(_))
    ));
    let v1 = start_worker(env.addr.clone(), shop(vec![order.clone(), invoices])).await;
    readiness(&env, Ok(())).await;

    // An aggregate the membership does not list is dynamic: it runs, but nobody waits for it
    // and it is forgotten once its last worker leaves.
    let refunds = shop(vec![aggregate("orders.Refund", &["orders.Refund"])]);
    let refunds = start_worker(env.addr.clone(), refunds).await;
    let refund = Command {
        aggregate_type: "Refund".into(),
        ..command("c2", "Refund", "r1")
    };
    // It ran: the test worker knows no refunds and rejects it.
    let refunded = submit(&env.addr, signed(refund.clone(), &key(1)))
        .await
        .unwrap();
    assert!(
        matches!(&refunded.outcome, Some(Outcome::Rejected(r)) if r.message == "unknown command"),
        "{refunded:?}"
    );
    refunds.abort();
    readiness(&env, Ok(())).await;
    let refund = Command {
        command_id: "c3".into(),
        ..refund
    };
    let refused = submit(&env.addr, signed(refund, &key(1)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::NotFound, "{refused:?}");

    // The next version has no invoices any more: they are unregistered once no worker runs them.
    v1.abort();
    readiness(
        &env,
        Err("waiting for workers: orders.Invoice, orders.Order"),
    )
    .await;
    apply(&env.addr, membership("shop", &["orders.Order"], &[], &[]))
        .await
        .unwrap();
    readiness(&env, Err("waiting for workers: orders.Order")).await;
    start_worker(env.addr.clone(), shop(vec![order])).await;
    readiness(&env, Ok(())).await;
    let refused = submit(&env.addr, signed(invoice("c4"), &key(1)))
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
