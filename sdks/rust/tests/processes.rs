//! Business processes: an app registers its domains' processes once its worker is ready, and lists
//! them in its membership; the kernel numbers the versions.

mod common;

use common::TestKernel;
use kernable::process::{Send, Start};
use kernable::{App, NewEvent, domain};
use serde_json::Value;

#[tokio::test(flavor = "multi_thread")]
async fn an_app_registers_its_processes_and_a_changed_model_is_the_next_version() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();

    let shop = domain("shop");
    let order = shop.aggregate("order", 0u32);
    order.command("Submit", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("OrderSubmitted", ())])
    });
    order.command("Confirm", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("OrderConfirmed", ())])
    });
    let fulfilment = shop
        .process("fulfilment")
        .start_named("OrderSubmitted", Start::key("aggregate_id"))
        .command_named("confirm", "order.Confirm", Send::aggregate("case.order_id"))
        .end("done");
    let app = kernable
        .app(App::new().service("shop").domain(&shop))
        .unwrap();
    // The membership lists the process.
    assert_eq!(
        app.manifest(),
        &kernable::Manifest::new()
            .service("shop")
            .worker(&order)
            .process(&fulfilment)
    );
    let running = {
        let app = app.clone();
        tokio::spawn(async move { app.run().await })
    };
    app.ready().await;

    // Ready: the app registered version 1. The same model again registers nothing new.
    let again = kernable.register_process(&fulfilment).await.unwrap();
    assert_eq!(
        (again.name.as_str(), again.version, again.new),
        ("shop.fulfilment", 1, false)
    );

    // A changed model is version 2.
    let changed = domain("shop")
        .process("fulfilment")
        .start_named("OrderSubmitted", Start::key("aggregate_id"))
        .command_named("confirm", "order.Confirm", Send::aggregate("case.order_id"))
        .command_named("again", "order.Confirm", Send::aggregate("case.order_id"))
        .end("done");
    let v2 = kernable.register_process(&changed).await.unwrap();
    assert_eq!((v2.version, v2.new), (2, true));

    app.stop();
    running.await.unwrap().unwrap();
}
