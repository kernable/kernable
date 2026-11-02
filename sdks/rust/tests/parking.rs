//! Reactors park events their handlers give up on, and get them again when an operator asks.

mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::{TestKernel, eventually};
use kernable::{Error, ListenerHandle, NewEvent, Rejected, Submit, Worker, domain};
use serde_json::Value;

fn spawn_worker(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

fn spawn(listener: &ListenerHandle) -> tokio::task::JoinHandle<()> {
    let listener = listener.clone();
    tokio::spawn(async move { listener.run().await })
}

/// The lines of `kernable listeners parked`, once there are `n`.
async fn parked(kernel: &TestKernel, n: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for _ in 0..100 {
        let (ok, out) = kernel.cli(&["listeners", "parked", "{store}"]);
        assert!(ok);
        lines = out.lines().map(str::to_owned).collect();
        if lines.len() == n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    lines
}

/// A shop whose `Ship` is rejected for an order that was not placed, and a `Request` that
/// asks for an order to be shipped.
fn shop() -> (kernable::Domain, kernable::Aggregate<bool>) {
    let shop = domain("shop");
    let order = shop.aggregate("Order", false);
    order.apply("Placed", |placed, _: Value, _evt, _ctx| *placed = true);
    order.command("Place", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Placed", ())])
    });
    order.command("Ship", |placed, _: Value, _cmd, _ctx| {
        if !*placed {
            return Err(Rejected::new("not placed").code("not_placed"));
        }
        Ok(vec![NewEvent::new("Shipped", ())])
    });
    order.command("Request", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Requested", ())])
    });
    (shop, order)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_follow_up_parks_the_event_and_the_reactor_moves_on() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (shop, order) = shop();
    let shipped = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&shipped);
    let shipper = shop
        .reactor("shipper")
        .on("Requested", move |_: Value, evt, ctx| {
            let s = Arc::clone(&s);
            async move {
                let ship = Submit::new("shop", "Order", &evt.aggregate_id, "Ship")
                    .id(format!("ship-{}", evt.aggregate_id));
                ctx.submit(ship).await?;
                s.lock().unwrap().push(evt.aggregate_id);
                Ok(())
            }
        });
    let worker = kernable.worker(&order);
    let handle = kernable.listener(&shipper);
    let running = [spawn_worker(&worker), spawn(&handle)];
    worker.ready().await;
    handle.ready().await;

    let submit = |id: &str, command: &str| Submit::new("shop", "Order", id, command);
    kernable.submit(submit("o1", "Request")).await.unwrap();
    kernable.submit(submit("o2", "Place")).await.unwrap();
    kernable.submit(submit("o2", "Request")).await.unwrap();
    // o1 was never placed: its shipping is rejected, parked at once; o2 ships.
    assert_eq!(*shipped.lock().unwrap(), ["o2"]);
    let lines = parked(&kernel, 1).await;
    assert!(
        lines[0].starts_with("shipper position 1 shop.Requested not_placed attempts 1 parked "),
        "{lines:?}"
    );
    assert!(lines[0].ends_with(": not placed"), "{lines:?}");

    // A follow-up without a fixed ID is refused before it is sent.
    let no_id = shop
        .reactor("no-id")
        .on("Placed", |_: Value, evt, ctx| async move {
            let ship = Submit::new("shop", "Order", &evt.aggregate_id, "Ship");
            let refused = ctx.submit(ship).await;
            assert!(matches!(&refused, Err(Error::Config(m)) if m.contains("fixed ID")));
            Ok(())
        });
    let no_id = kernable.listener(&no_id);
    let checked = spawn(&no_id);
    no_id.ready().await;
    kernable.submit(submit("o3", "Place")).await.unwrap();
    no_id.stop();
    checked.await.unwrap();

    worker.stop();
    handle.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_handler_parks_after_its_attempts_and_a_retry_handles_it() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (shop, order) = shop();
    let down = Arc::new(AtomicBool::new(true));
    let mailed = Arc::new(Mutex::new(Vec::new()));
    let (d, m) = (Arc::clone(&down), Arc::clone(&mailed));
    // With a store: a parked event's writes go, the rest of the batch stays.
    let mailer = shop
        .reactor("mailer")
        .park_after(2)
        .reconnect(Duration::from_millis(20))
        .store("memory:", "1")
        .on("Placed", move |_: Value, evt, ctx| {
            let (d, m) = (Arc::clone(&d), Arc::clone(&m));
            async move {
                let store = ctx.store.unwrap();
                store.set("tried", &evt.aggregate_id, &true)?;
                if evt.aggregate_id == "o1" && d.load(Ordering::SeqCst) {
                    return Err("the mail server is down".into());
                }
                m.lock().unwrap().push(evt.aggregate_id);
                Ok(())
            }
        });
    let worker = kernable.worker(&order);
    let handle = kernable.listener(&mailer);
    let running = [spawn_worker(&worker), spawn(&handle)];
    worker.ready().await;
    handle.ready().await;

    let place = |id: &str| Submit::new("shop", "Order", id, "Place");
    kernable.submit(place("o1")).await.unwrap();
    kernable.submit(place("o2")).await.unwrap();
    eventually(|| mailed.lock().unwrap().len() == 1).await;
    assert_eq!(*mailed.lock().unwrap(), ["o2"]);
    let lines = parked(&kernel, 1).await;
    assert!(
        lines[0].starts_with("mailer position 1 shop.Placed handler_error attempts 2 parked "),
        "{lines:?}"
    );
    assert!(lines[0].ends_with(": handler failed"), "{lines:?}");
    let store = handle.opened_store().unwrap();
    assert_eq!(store.get::<bool>("tried", "o1").unwrap(), None);
    assert_eq!(store.get::<bool>("tried", "o2").unwrap(), Some(true));

    // Fixed: the operator retries it, and it is handled.
    down.store(false, Ordering::SeqCst);
    let (ok, out) = kernel.cli(&["listeners", "retry", "{url}", "app.key", "mailer", "--all"]);
    assert!(ok, "{out}");
    assert_eq!(out.trim(), "retrying 1 parked events of mailer: 1");
    eventually(|| mailed.lock().unwrap().len() == 2).await;
    assert_eq!(*mailed.lock().unwrap(), ["o2", "o1"]);
    assert!(parked(&kernel, 0).await.is_empty());
    assert_eq!(store.get::<bool>("tried", "o1").unwrap(), Some(true));
    let (ok, out) = kernel.cli(&["listeners", "dismiss", "{url}", "app.key", "mailer", "1"]);
    assert!(!ok, "nothing left to dismiss");
    assert_eq!(out.trim(), "no parked event 1 of mailer");

    worker.stop();
    handle.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_model_never_parks_by_default() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (shop, order) = shop();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    let orders = shop
        .read_model("orders")
        .reconnect(Duration::from_millis(20))
        .on("Placed", move |_: Value, evt, _ctx| {
            let s = Arc::clone(&s);
            async move {
                s.lock().unwrap().push(evt.aggregate_id.clone());
                if evt.aggregate_id == "o1" {
                    return Err(Rejected::new("cannot show it").into());
                }
                Ok(())
            }
        });
    let worker = kernable.worker(&order);
    let handle = kernable.listener(&orders);
    let running = [spawn_worker(&worker), spawn(&handle)];
    worker.ready().await;
    handle.ready().await;
    let place =
        |id: &str| Submit::new("shop", "Order", id, "Place").wait(kernable::Wait::Committed);
    kernable.submit(place("o1")).await.unwrap();
    kernable.submit(place("o2")).await.unwrap();
    // It keeps getting o1; o2 waits behind it.
    eventually(|| seen.lock().unwrap().len() >= 5).await;
    assert!(seen.lock().unwrap().iter().all(|id| id == "o1"));
    assert!(parked(&kernel, 0).await.is_empty());

    worker.stop();
    handle.stop();
    for r in running {
        r.await.unwrap();
    }
}
