//! Client options: each one takes effect, from code or from the environment.

mod common;

use std::{path::PathBuf, time::Duration};

use common::TestKernel;
use kernable::{Error, Kernable, NewEvent, Options, Outcome, Submit, Wait, domain};
use serde_json::json;

fn order() -> kernable::Aggregate<bool> {
    let shop = domain("shop");
    // Snapshots off (0): commands still run.
    let order = shop.aggregate("Order", false).snapshot_every(0);
    order.command("Place", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Placed", json!({}))])
    });
    order
}

async fn place(kernable: &Kernable, id: &str) -> Result<Outcome, Error> {
    kernable
        .submit(Submit::new("shop", "Order", id, "Place").wait(Wait::Committed))
        .await
}

fn start(kernable: &Kernable) -> kernable::Worker {
    let worker = kernable.worker(&order());
    let w = worker.clone();
    tokio::spawn(async move { w.run().await });
    worker
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_may_be_given_as_a_str_string_path_or_path_buf() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let key = kernel.app_key.clone();
    let path = key.display().to_string();
    // From another directory, the default (app.key here) would not be found.
    for options in [
        Options::from(path.as_str()),
        Options::from(path.clone()),
        Options::from(key.as_path()),
        Options::from(PathBuf::from(&key)),
    ] {
        let kernable = Kernable::new(options.url(&kernel.url));
        assert!(kernable.is_ok(), "{:?}", kernable.err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grant_file_anywhere_is_sent_along() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    // Not next to the key: only Options::grant finds it. Before any request presented it, the
    // kernel holds no grant for the key.
    let moved = kernel.dir.join("elsewhere.grant");
    std::fs::rename(kernel.dir.join("app.grant"), &moved).unwrap();
    let without = Kernable::new(kernel.options()).unwrap();
    let denied = place(&without, "o1").await;
    assert!(
        matches!(&denied, Err(Error::Status(s)) if s.code() == tonic::Code::PermissionDenied),
        "{denied:?}"
    );
    let kernable = Kernable::new(kernel.options().grant(&moved)).unwrap();
    let worker = start(&kernable);
    worker.ready().await;
    let placed = place(&kernable, "o1").await;
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_client_timeout_bounds_every_request() {
    let kernel = TestKernel::new(&["shop.*"], &[]); // never started
    let kernable = Kernable::new(kernel.options().timeout(Duration::from_millis(300))).unwrap();
    let started = std::time::Instant::now();
    let refused = place(&kernable, "o1").await;
    assert!(
        matches!(&refused, Err(Error::Status(s)) if s.code() == tonic::Code::DeadlineExceeded),
        "{refused:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_returns_the_command_id() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let worker = start(&kernable);
    worker.ready().await;
    let id = kernable
        .enqueue(Submit::new("shop", "Order", "o1", "Place").id("cmd-7"))
        .await
        .unwrap();
    assert_eq!(id, "cmd-7");
    let generated = kernable
        .enqueue(Submit::new("shop", "Order", "o2", "Place"))
        .await
        .unwrap();
    assert_eq!(generated.len(), 36, "a UUID: {generated}");
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_from_options_or_the_environment() {
    let kernel = TestKernel::with_tls(&["shop.*"]);
    kernel.start().await;
    let kernable = Kernable::new(kernel.options().tls(kernel.tls())).unwrap();
    let worker = start(&kernable);
    worker.ready().await;
    let placed = place(&kernable, "o1").await;
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");

    // The environment: all three variables, or none. Only this test of this binary reads them.
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
    let placed = place(&from_env, "o2").await;
    unsafe { std::env::remove_var("KERNABLE_TLS_KEY") };
    let partial = Kernable::new(kernel.options()).err();
    for (var, _) in vars {
        unsafe { std::env::remove_var(var) };
    }
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
    assert!(matches!(partial, Some(Error::Config(_))), "{partial:?}");
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_submit_does_not_wait_for_listeners() {
    let kernel = TestKernel::new(&["shop.*"], &["--settle-timeout-ms", "3000"]);
    kernel.start().await;
    let kernable = kernel.client();
    let worker = start(&kernable);
    // A read model that never gets past its first event.
    let stalled = domain("shop")
        .read_model("stalled")
        .on("shop.Placed", |_: serde_json::Value, _evt, _ctx| {
            std::future::pending::<Result<(), kernable::BoxError>>()
        });
    let listener = kernable.listener(&stalled);
    let l = listener.clone();
    tokio::spawn(async move { l.run().await });
    worker.ready().await;
    listener.ready().await;
    let started = std::time::Instant::now();
    let placed = place(&kernable, "o1").await;
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
    assert!(
        started.elapsed() < Duration::from_millis(1_500),
        "{:?}",
        started.elapsed()
    );
    worker.stop();
    listener.stop();
}

#[test]
fn a_query_outcome_says_whether_it_was_answered() {
    use kernable::{QueryOutcome, Rejected};
    assert!(QueryOutcome::Answered(json!(1)).is_answered());
    assert!(!QueryOutcome::Rejected(Rejected::new("no")).is_answered());
}

#[test]
fn a_rejection_reads_as_its_message() {
    let rejected = kernable::Rejected::new("out of stock").code("empty");
    assert_eq!(rejected.to_string(), "out of stock");
}
