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
async fn tls_from_options() {
    let kernel = TestKernel::with_tls(&["shop.*"]);
    kernel.start().await;
    let kernable = Kernable::new(kernel.options().tls(kernel.tls())).unwrap();
    let worker = start(&kernable);
    worker.ready().await;
    let placed = place(&kernable, "o1").await;
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
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

/// Passes what `r` reads on to `w`; while `frozen`, nothing, and the connection stays open.
async fn pump(
    mut r: impl tokio::io::AsyncRead + Unpin,
    mut w: impl tokio::io::AsyncWrite + Unpin,
    frozen: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 16 << 10];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if frozen.load(std::sync::atomic::Ordering::SeqCst) {
            // Cut off: what comes in is dropped silently, nothing is closed.
            return std::future::pending().await;
        }
        if w.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// A TCP proxy to the kernel's socket at `to` that can be frozen, as when the network cuts a
/// kernel off without a word (a partition): its address and the switch.
async fn freezable_proxy(to: PathBuf) -> (String, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let frozen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let f = std::sync::Arc::clone(&frozen);
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let Ok(unix) = tokio::net::UnixStream::connect(&to).await else {
                continue;
            };
            let (tr, tw) = tcp.into_split();
            let (ur, uw) = unix.into_split();
            tokio::spawn(pump(tr, uw, std::sync::Arc::clone(&f)));
            tokio::spawn(pump(ur, tw, std::sync::Arc::clone(&f)));
        }
    });
    (format!("http://{addr}"), frozen)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pair_client_turns_to_the_other_kernel_within_seconds_when_one_is_cut_off() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let socket = PathBuf::from(kernel.url.trim_start_matches("unix:"));
    let (proxied, frozen) = freezable_proxy(socket).await;
    // Two addresses of the same kernel: the first through the proxy, the second direct.
    let kernable = Kernable::new(
        kernel
            .options()
            .url(format!("{proxied},{}", kernel.url))
            .timeout(Duration::from_secs(30)),
    )
    .unwrap();
    // The worker connects directly: what is measured is the client's turn. (Through the proxy,
    // the one kernel behind both addresses would lease the next command to the frozen worker
    // session until its lease ran out; a kernel that takes over never had that session.)
    let direct = kernel.client();
    let _worker = start(&direct);
    // Through the proxy, once the worker is there.
    let mut first = place(&kernable, "o1").await;
    for _ in 0..100 {
        if matches!(first, Ok(Outcome::Committed(_))) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        first = place(&kernable, "o1").await;
    }
    assert!(matches!(first, Ok(Outcome::Committed(_))), "{first:?}");
    // The proxy freezes: the connection the client holds looks open and answers nothing.
    frozen.store(true, std::sync::atomic::Ordering::SeqCst);
    let started = std::time::Instant::now();
    let placed = place(&kernable, "o2").await;
    assert!(matches!(placed, Ok(Outcome::Committed(_))), "{placed:?}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the client turned to the other address after {:?}",
        started.elapsed()
    );
}
