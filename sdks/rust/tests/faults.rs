//! Faults around listeners: a first connect that fails, a batch whose future is dropped, and
//! handlers that panic. Nothing may be skipped or stuck, nothing from before a reactor existed
//! may be replayed, and a panic fails only its batch, query or check.

mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::{TestKernel, eventually};
use kernable::{Aggregate, Domain, Error, Kernable, NewEvent, Query, Submit, Wait, domain};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
struct By {
    by: i64,
}

fn counters() -> (Domain, Aggregate<i64>) {
    let counters = domain("counters");
    let counter = counters.aggregate("Counter", 0i64);
    counter.apply("Incremented", |n, data: By, _evt, _ctx| *n += data.by);
    counter.command("Increment", |_, data: By, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Incremented", By { by: data.by })])
    });
    (counters, counter)
}

async fn increment(kernable: &Kernable, id: &str, by: i64) {
    kernable
        .submit(
            Submit::new("counters", "Counter", id, "Increment")
                .data(By { by })
                .wait(Wait::Committed),
        )
        .await
        .unwrap();
}

fn start_worker(kernable: &Kernable, counter: &Aggregate<i64>) -> kernable::Worker {
    let worker = kernable.worker(counter);
    let w = worker.clone();
    tokio::spawn(async move { w.run().await });
    worker
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_reactor_whose_first_connect_fails_still_starts_at_the_newest_event() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (counters, counter) = counters();
    let worker = start_worker(&kernable, &counter);
    worker.ready().await;
    for by in [1, 2, 3] {
        increment(&kernable, "c1", by).await;
    }

    // The kernel is away when the reactor connects for the first time.
    kernel.kill();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    let mailer = counters
        .reactor("mailer")
        .reconnect(Duration::from_millis(50))
        .on("counters.Incremented", move |data: By, _evt, _ctx| {
            s.lock().unwrap().push(data.by);
            async { Ok(()) }
        });
    let reactor = kernable.listener(&mailer);
    let r = reactor.clone();
    tokio::spawn(async move { r.run().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    kernel.start().await;
    reactor.ready().await;
    increment(&kernable, "c1", 4).await;
    eventually(|| !seen.lock().unwrap().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        *seen.lock().unwrap(),
        [4],
        "only what happened after it existed"
    );
    reactor.stop();
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_whose_future_was_dropped_does_not_leave_the_store_stuck() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (counters, counter) = counters();
    let worker = start_worker(&kernable, &counter);
    let url = format!("sqlite:{}", kernel.dir.join("totals.db").display());
    let first = Arc::new(AtomicBool::new(true));
    let f = Arc::clone(&first);
    let totals = counters
        .read_model("totals")
        .store(&url, "1")
        .reconnect(Duration::from_millis(50))
        .on("counters.Incremented", move |data: By, evt, ctx| {
            let f = Arc::clone(&f);
            async move {
                let store = ctx.store.expect("a store");
                let total = store.get::<i64>("totals", &evt.aggregate_id)?.unwrap_or(0) + data.by;
                store.set("totals", &evt.aggregate_id, &total)?;
                if f.swap(false, Ordering::SeqCst) {
                    std::future::pending::<()>().await; // dropped right here
                }
                Ok(())
            }
        });
    let model = kernable.listener(&totals);
    let m = model.clone();
    let running = tokio::spawn(async move { m.run().await });
    model.ready().await;
    worker.ready().await;
    increment(&kernable, "c1", 1).await;
    eventually(|| !first.load(Ordering::SeqCst)).await;
    // The caller gives up on run(), e.g. with a timeout, in the middle of the batch.
    running.abort();
    let _ = running.await;

    let m = model.clone();
    tokio::spawn(async move { m.run().await });
    let total = || {
        let store = model.opened_store().unwrap();
        store.get::<i64>("totals", "c1").unwrap()
    };
    // Read through the store's own connection, an open transaction would show its writes:
    // a second event must also arrive and commit.
    increment(&kernable, "c1", 2).await;
    eventually(|| total() == Some(3)).await;
    assert_eq!(
        total(),
        Some(3),
        "the batch was handled again, once, then the next"
    );
    model.stop();
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_panics_fails_its_batch_or_query_not_the_listener() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (counters, counter) = counters();
    let worker = start_worker(&kernable, &counter);
    let handled = Arc::new(Mutex::new(Vec::new()));
    let panicked = Arc::new(AtomicBool::new(false));
    let (h, p) = (Arc::clone(&handled), Arc::clone(&panicked));
    let totals = counters
        .read_model("totals")
        .reconnect(Duration::from_millis(50))
        .on("counters.Incremented", move |data: By, _evt, _ctx| {
            let (h, p) = (Arc::clone(&h), Arc::clone(&p));
            async move {
                if !p.swap(true, Ordering::SeqCst) {
                    panic!("a bug in the handler");
                }
                h.lock().unwrap().push(data.by);
                Ok(())
            }
        })
        .query(
            "counters.Broken",
            |_: serde_json::Value, _qry, _ctx| async move {
                if true {
                    panic!("a bug in the query");
                }
                Ok(0)
            },
        );
    let model = kernable.listener(&totals);
    let m = model.clone();
    let running = tokio::spawn(async move { m.run().await });
    model.ready().await;
    worker.ready().await;

    increment(&kernable, "c1", 1).await;
    eventually(|| !handled.lock().unwrap().is_empty()).await;
    assert_eq!(
        *handled.lock().unwrap(),
        [1],
        "handled again after the panic"
    );
    let answer = kernable
        .query::<i64>(Query::new("counters", "Broken").data(json!({})))
        .await;
    assert!(
        matches!(&answer, Err(Error::Rejected(r)) if r.code == "handler_error"),
        "{answer:?}"
    );
    assert!(!running.is_finished(), "the listener runs on");
    model.stop();
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_reactor_with_a_store_starts_at_the_newest_event() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (counters, counter) = counters();
    let worker = start_worker(&kernable, &counter);
    worker.ready().await;
    for by in [1, 2] {
        increment(&kernable, "c1", by).await;
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    let url = format!("sqlite:{}", kernel.dir.join("mailer.db").display());
    let mailer = counters.reactor("stored-mailer").store(&url, "1").on(
        "counters.Incremented",
        move |data: By, _evt, _ctx| {
            s.lock().unwrap().push(data.by);
            async { Ok(()) }
        },
    );
    let reactor = kernable.listener(&mailer);
    let r = reactor.clone();
    tokio::spawn(async move { r.run().await });
    reactor.ready().await;
    increment(&kernable, "c1", 3).await;
    eventually(|| !seen.lock().unwrap().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*seen.lock().unwrap(), [3]);
    reactor.stop();
    worker.stop();
}
