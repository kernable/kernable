//! TLS from the environment. Its own test binary: every `Kernable::new` reads these variables,
//! so setting them would leak into tests running alongside.

mod common;

use common::TestKernel;
use kernable::{Error, Kernable, NewEvent, Outcome, Submit, Wait, domain};
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn tls_from_the_environment_takes_all_three_variables_or_none() {
    let kernel = TestKernel::with_tls(&["shop.*"]);
    kernel.start().await;
    let shop = domain("shop");
    let order = shop.aggregate("Order", false);
    order.command("Place", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Placed", json!({}))])
    });

    let tls = kernel.tls();
    let vars = [
        ("KERNABLE_TLS_CA", &tls.ca),
        ("KERNABLE_TLS_CERT", &tls.cert),
        ("KERNABLE_TLS_KEY", &tls.key),
    ];
    for (var, path) in vars {
        unsafe { std::env::set_var(var, path) };
    }
    let from_env = Kernable::new(kernel.options()).unwrap();
    let worker = from_env.worker(&order);
    let w = worker.clone();
    tokio::spawn(async move { w.run().await });
    worker.ready().await;
    let placed = from_env
        .submit(Submit::new("shop", "Order", "o1", "Place").wait(Wait::Committed))
        .await;
    unsafe { std::env::remove_var("KERNABLE_TLS_KEY") };
    let partial = Kernable::new(kernel.options()).err();
    for (var, _) in vars {
        unsafe { std::env::remove_var(var) };
    }
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
    assert!(matches!(partial, Some(Error::Config(_))), "{partial:?}");
    worker.stop();
}
