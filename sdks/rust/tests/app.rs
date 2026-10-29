//! An app made of whole domains: its membership comes from what the domains define, and `run()`
//! applies it and runs everything. Ported from the TypeScript SDK's tests.

mod common;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use common::TestKernel;
use kernable::{
    Aggregate, App, Domain, Error, Listener, Manifest, MiddlewareOptions, NewEvent, Query, Submit,
    domain,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Id {
    id: String,
}

struct Shop {
    shop: Domain,
    billing: Domain,
    cart: Aggregate<u32>,
    carts: Listener,
    invoices: Listener,
    guard: Listener,
}

fn shop_and_billing() -> Shop {
    let shop = domain("shop");
    let cart = shop.aggregate("Cart", 0u32);
    cart.command("Add", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Added", ())])
    });
    let counts = Arc::new(Mutex::new(HashMap::<String, u32>::new()));
    let c = Arc::clone(&counts);
    let carts = shop
        .read_model("carts")
        .on("Added", move |_: Value, evt, _ctx| {
            *c.lock().unwrap().entry(evt.aggregate_id).or_default() += 1;
            async { Ok(()) }
        })
        .query("Count", move |data: Id, _qry, _ctx| {
            let count = counts.lock().unwrap().get(&data.id).copied().unwrap_or(0);
            async move { Ok(count) }
        });
    shop.reactor("push")
        .live()
        .on("Added", |_: Value, _evt, _ctx| async { Ok(()) });
    let billing = domain("billing");
    let invoices = billing
        .reactor("invoices")
        .on("shop.Added", |_: Value, _evt, _ctx| async { Ok(()) });
    let guard = billing
        .read_model("guard")
        .on("shop.Added", |_: Value, _evt, _ctx| async { Ok(()) })
        .middleware(MiddlewareOptions::new(&["shop.*"]), |_| async { Ok(()) });
    Shop {
        shop,
        billing,
        cart,
        carts,
        invoices,
        guard,
    }
}

#[tokio::test]
async fn an_app_lists_its_domains_aggregates_listeners_and_middlewares_but_no_live_ones() {
    let s = shop_and_billing();
    // Keys only: the kernel is never started.
    let kernel = TestKernel::new(&["shop.*"], &[]);
    let kernable = kernel.client();
    let app = kernable
        .app(
            App::new()
                .service("shop")
                .version("1.0.0")
                .description("Carts")
                .metadata("team", "checkout")
                .domain(&s.shop)
                .domain(&s.billing),
        )
        .unwrap();
    let expected = Manifest::new()
        .service("shop")
        .worker(&s.cart)
        .listener(&s.carts)
        .listener(&s.invoices)
        .middleware(&s.guard)
        .metadata("version", "1.0.0")
        .metadata("description", "Carts")
        .metadata("team", "checkout");
    assert_eq!(app.manifest(), &expected);
    assert!(app.worker().is_some());
    assert_eq!(app.listeners().len(), 4, "the live one runs too");
    assert!(matches!(
        kernable.app(App::new().domain(&s.shop)),
        Err(Error::Config(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_applies_its_membership_and_runs_everything() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let s = shop_and_billing();
    let kernable = kernel.client();
    let app = kernable
        .app(
            App::new()
                .service("shop")
                .version("1.0.0")
                .domain(&s.shop)
                .domain(&s.billing),
        )
        .unwrap();
    let running = {
        let app = app.clone();
        tokio::spawn(async move { app.run().await })
    };
    tokio::time::timeout(Duration::from_secs(10), app.ready())
        .await
        .expect("the app connects");
    let outcome = kernable
        .submit(Submit::new("shop", "Cart", "c1", "Add"))
        .await
        .unwrap();
    assert!(outcome.is_committed());
    let count: u32 = kernable
        .query(Query::new("shop", "Count").data(json!({ "id": "c1" })))
        .await
        .unwrap();
    assert_eq!(count, 1);
    app.stop();
    running.await.unwrap().unwrap();
}

/// Set for the child process of [`an_app_prints_one_overview_once_everything_is_connected`].
const OVERVIEW_CHILD: &str = "KERNABLE_TEST_OVERVIEW_CHILD";

/// Runs an app until it is ready, in a child process whose stderr the parent test reads.
/// Does nothing in a normal test run.
#[tokio::test(flavor = "multi_thread")]
async fn overview_child() {
    if std::env::var_os(OVERVIEW_CHILD).is_none() {
        return;
    }
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let shop = domain("shop");
    shop.aggregate("Order", 0u32)
        .command("Place", |_, _: Value, _cmd, _ctx| Ok(vec![]));
    shop.read_model("orders")
        .on("Placed", |_: Value, _evt, _ctx| async { Ok(()) })
        .query("GetOrder", |_: Value, _qry, _ctx| async { Ok(Value::Null) });
    let app = kernable
        .app(
            App::new()
                .service("shop")
                .version("1.0.0")
                .description("Orders")
                .domain(&shop),
        )
        .unwrap();
    let running = {
        let app = app.clone();
        tokio::spawn(async move { app.run().await })
    };
    tokio::time::timeout(Duration::from_secs(10), app.ready())
        .await
        .expect("the app connects");
    tokio::time::sleep(Duration::from_millis(100)).await;
    app.stop();
    running.await.unwrap().unwrap();
}

#[test]
fn an_app_prints_one_overview_once_everything_is_connected() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "overview_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(OVERVIEW_CHILD, "1")
        .env("KERNABLE_LOG", "info")
        .env_remove("NO_COLOR")
        .output()
        .unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(out.status.success(), "the child runs: {stderr}");
    let lines: Vec<&str> = stderr.split('\n').collect();
    assert_eq!(lines[0], "kernable · shop 1.0.0 · Orders", "{stderr}");
    let kernel = lines[1]
        .strip_prefix("  kernel      unix:/tmp/kn-rs-")
        .expect("the kernel line");
    let (_, key) = kernel.split_once(" · key app (").expect("the key");
    assert!(key.ends_with("…)") && key.chars().count() == 14, "{key}");
    assert_eq!(
        lines[2..8],
        [
            "  membership  applied: 1 worker, 1 listener",
            "",
            "  worker      Order   runs Place",
            "  listener    orders  from 0 · reads Placed",
            "                      answers GetOrder",
            "",
        ]
    );
    let ms = lines[8]
        .strip_prefix("  ready in ")
        .and_then(|rest| rest.strip_suffix(" ms"))
        .expect("the ready line");
    assert!(ms.parse::<u64>().is_ok(), "{ms}");
    assert_eq!(
        lines[9..],
        ["", ""],
        "one overview, no connection lines: {stderr}"
    );
}
