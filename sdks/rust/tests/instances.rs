//! Instances: one listener run by several processes under one name. A single reactor runs
//! on one instance and the next takes over; a replicated read model runs on every instance.

mod common;

use std::sync::{Arc, Mutex};

use common::{TestKernel, eventually};
use kernable::{ListenerHandle, NewEvent, Submit, Worker, domain};
use serde_json::Value;

type Seen = Arc<Mutex<Vec<String>>>;

fn spawn_worker(worker: &Worker) -> tokio::task::JoinHandle<()> {
    let worker = worker.clone();
    tokio::spawn(async move { worker.run().await })
}

fn spawn(listener: &ListenerHandle) -> tokio::task::JoinHandle<()> {
    let listener = listener.clone();
    tokio::spawn(async move { listener.run().await })
}

/// The shop as a process on machine `node` defines it: its own domain, the same names.
fn shop_on(
    node: &str,
    seen: &Seen,
) -> (
    kernable::Aggregate<u32>,
    kernable::Listener,
    kernable::Listener,
) {
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", 0u32);
    cart.command("Add", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Added", ())])
    });
    let (s, n) = (Arc::clone(seen), node.to_owned());
    let mailer = shop
        .reactor("mailer")
        .instance(node)
        .on("Added", move |_: Value, evt, _ctx| {
            s.lock().unwrap().push(format!("{n}:{}", evt.aggregate_id));
            async { Ok(()) }
        });
    let (s, n) = (Arc::clone(seen), node.to_owned());
    let carts = shop
        .read_model("carts")
        .instance(node)
        .on("Added", move |_: Value, evt, _ctx| {
            s.lock()
                .unwrap()
                .push(format!("{n}:carts:{}", evt.aggregate_id));
            async { Ok(()) }
        });
    (cart, mailer, carts)
}

fn seen_now(seen: &Seen) -> Vec<String> {
    seen.lock().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_reactor_runs_on_one_instance_and_the_next_takes_over() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let seen: Seen = Arc::default();
    let (cart, mailer_a, _) = shop_on("node-a", &seen);
    let (_, mailer_b, _) = shop_on("node-b", &seen);
    let worker = kernable.worker(&cart);
    let _w = spawn_worker(&worker);
    worker.ready().await;
    let a = kernable.listener(&mailer_a);
    let a_task = spawn(&a);
    a.ready().await;
    // node-b waits, and counts as started: it does not hold up its process.
    let b = kernable.listener(&mailer_b);
    let _b_task = spawn(&b);
    b.ready().await;
    kernable
        .submit(Submit::new("shop", "Cart", "c1", "Add"))
        .await
        .unwrap();
    assert_eq!(seen_now(&seen), ["node-a:c1"]);

    a.stop();
    a_task.await.unwrap();
    kernable
        .submit(Submit::new("shop", "Cart", "c2", "Add"))
        .await
        .unwrap();
    eventually(|| seen_now(&seen).len() == 2).await;
    assert_eq!(
        seen_now(&seen),
        ["node-a:c1", "node-b:c2"],
        "c1 is not mailed again"
    );
    b.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn every_instance_of_a_read_model_gets_every_event() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "1000"]);
    kernel.start().await;
    let kernable = kernel.client();
    let seen: Seen = Arc::default();
    let (cart, _, carts_a) = shop_on("node-a", &seen);
    let (_, _, carts_b) = shop_on("node-b", &seen);
    let worker = kernable.worker(&cart);
    let _w = spawn_worker(&worker);
    worker.ready().await;
    let (a, b) = (kernable.listener(&carts_a), kernable.listener(&carts_b));
    let (a_task, _b_task) = (spawn(&a), spawn(&b));
    a.ready().await;
    b.ready().await;
    kernable
        .submit(Submit::new("shop", "Cart", "c1", "Add"))
        .await
        .unwrap();
    eventually(|| seen_now(&seen).len() == 2).await;
    let mut both = seen_now(&seen);
    both.sort();
    assert_eq!(both, ["node-a:carts:c1", "node-b:carts:c1"]);

    // node-a is gone: node-b settles alone.
    a.stop();
    a_task.await.unwrap();
    kernable
        .submit(Submit::new("shop", "Cart", "c2", "Add"))
        .await
        .unwrap();
    assert!(seen_now(&seen).contains(&"node-b:carts:c2".to_owned()));
    b.stop();
}
