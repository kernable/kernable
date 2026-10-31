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
use kernable::{
    Aggregate, Domain, Error, Kernable, NewEvent, Options, Outcome, Query, Submit, Wait, domain,
};
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
    start_worker_of(kernable, counter)
}

fn start_worker_of(kernable: &Kernable, aggregate: &Aggregate<i64>) -> kernable::Worker {
    let worker = kernable.worker(aggregate);
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

#[test]
fn plain_text_goes_only_to_a_kernel_on_this_machine_unless_allowed() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _entered = runtime.enter(); // a channel needs a runtime, though it connects lazily
    let client = |url: &str, insecure: bool| {
        let options = Options::new(&kernel.app_key).url(url);
        Kernable::new(if insecure {
            options.insecure()
        } else {
            options
        })
    };
    for url in [
        "unix:/tmp/k.sock",
        "http://127.0.0.1:7233",
        "localhost:7233",
        "[::1]:7233",
        "127.0.0.2:7233",
    ] {
        assert!(client(url, false).is_ok(), "{url}");
    }
    for url in [
        "http://10.0.0.5:7233",
        "kernel.internal:7233",
        "http://127.0.0.1.example.com:7233",
    ] {
        let refused = client(url, false).err();
        assert!(
            matches!(&refused, Some(Error::Config(m)) if m.contains("not on this machine")),
            "{url}: {refused:?}"
        );
    }
    assert!(client("http://10.0.0.5:7233", true).is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_waits_for_the_kernel_to_come_back_until_its_deadline() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let (_, counter) = counters();
    let worker = start_worker(&kernable, &counter);
    worker.ready().await;
    kernel.kill();
    let submit = |id: &str| {
        Submit::new("counters", "Counter", "c1", "Increment")
            .data(By { by: 1 })
            .id(id)
            .wait(Wait::Committed)
    };
    let started = std::time::Instant::now();
    let refused = kernable
        .submit(submit("i0").timeout(Duration::from_millis(300)))
        .await;
    assert!(
        matches!(&refused, Err(Error::Status(s)) if s.code() == tonic::Code::DeadlineExceeded),
        "{refused:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    // Back while the request waits: it goes through.
    let waiting = {
        let kernable = kernable.clone();
        tokio::spawn(async move { kernable.submit(submit("i1")).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    kernel.start().await;
    let outcome = waiting.await.unwrap();
    assert!(matches!(outcome, Ok(Outcome::Committed(_))), "{outcome:?}");
    worker.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_command_handler_is_rejected_without_its_text() {
    let kernel = TestKernel::new(&["counters.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let counters = domain("counters");
    let leaky = counters.aggregate("Leaky", 0i64);
    leaky.command("Do", |_, _: serde_json::Value, _cmd, _ctx| {
        panic!("password hunter2 rejected by db://admin@10.0.0.5")
    });
    let worker = start_worker_of(&kernable, &leaky);
    worker.ready().await;
    let outcome = kernable
        .submit(Submit::new("counters", "Leaky", "l1", "Do").wait(Wait::Committed))
        .await
        .unwrap();
    match outcome {
        Outcome::Rejected(r) => {
            assert_eq!(
                (r.code.as_str(), r.message.as_str()),
                ("handler_error", "handler failed")
            );
        }
        other => panic!("expected a rejection, got {other:?}"),
    }
    worker.stop();
}

/// A kernel that misbehaves: repeats a batch with a new event, then sends one that goes back.
struct FakeKernel {
    hellos: Arc<Mutex<Vec<Option<u64>>>>,
    acks: Arc<Mutex<Vec<u64>>>,
}

#[tonic::async_trait]
impl kernable_core::proto::listener_service_server::ListenerService for FakeKernel {
    type ListenStream = tokio_stream::wrappers::ReceiverStream<
        Result<kernable_core::proto::ListenerDelivery, tonic::Status>,
    >;

    async fn listen(
        &self,
        request: tonic::Request<tonic::Streaming<kernable_core::proto::ListenerMessage>>,
    ) -> Result<tonic::Response<Self::ListenStream>, tonic::Status> {
        use kernable_core::proto::{
            Batch, Event, ListenerDelivery, Ready, listener_delivery::Msg as Out,
            listener_message::Msg as In,
        };
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (hellos, acks) = (Arc::clone(&self.hellos), Arc::clone(&self.acks));
        tokio::spawn(async move {
            let event = |position: u64| Event {
                global_position: position,
                tenant_id: "t1".into(),
                domain: "counters".into(),
                aggregate_type: "Counter".into(),
                aggregate_id: "c1".into(),
                event_type: "Incremented".into(),
                version: position,
                schema: 1,
                payload: format!(r#"{{"by":{position}}}"#).into_bytes(),
                ..Event::default()
            };
            let send = |msg: Out| tx.send(Ok(ListenerDelivery { msg: Some(msg) }));
            let Ok(Some(first)) = inbound.message().await else {
                return;
            };
            let Some(In::Hello(hello)) = first.msg else {
                return;
            };
            hellos.lock().unwrap().push(hello.from_position);
            if hellos.lock().unwrap().len() > 1 {
                return; // the reconnect: seen, that is all
            }
            let _ = send(Out::Ready(Ready {
                after_position: 0,
                head_position: 0,
            }))
            .await;
            let batches = [vec![1, 2], vec![1, 2, 3], vec![2]];
            let ups = [2, 3, 2];
            for (events, up_to) in batches.into_iter().zip(ups) {
                let batch = Batch {
                    events: events.into_iter().map(event).collect(),
                    up_to,
                    vault: Vec::new(),
                };
                let _ = send(Out::Batch(batch)).await;
                match inbound.message().await {
                    Ok(Some(m)) => {
                        if let Some(In::Ack(a)) = m.msg {
                            acks.lock().unwrap().push(a.position);
                        }
                    }
                    _ => return,
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_handles_each_event_once_when_the_kernel_repeats_batches_or_goes_back() {
    let keys = TestKernel::new(&["counters.*"], &[]); // only for its key files
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let fake = FakeKernel {
        hellos: Arc::default(),
        acks: Arc::default(),
    };
    let (hellos, acks) = (Arc::clone(&fake.hellos), Arc::clone(&fake.acks));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                kernable_core::proto::listener_service_server::ListenerServiceServer::new(fake),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let kernable =
        Kernable::new(Options::new(&keys.app_key).url(format!("http://{addr}"))).unwrap();
    let handled = Arc::new(Mutex::new(Vec::new()));
    let h = Arc::clone(&handled);
    let model = domain("counters")
        .read_model("fake")
        .reconnect(Duration::from_millis(50))
        .on("counters.Incremented", move |data: By, _evt, _ctx| {
            h.lock().unwrap().push(data.by);
            async { Ok(()) }
        });
    let handle = kernable.listener(&model);
    let h = handle.clone();
    tokio::spawn(async move { h.run().await });
    eventually(|| hellos.lock().unwrap().len() >= 2).await;
    assert_eq!(*handled.lock().unwrap(), [1, 2, 3], "each event once");
    assert_eq!(
        *acks.lock().unwrap(),
        [2, 3],
        "no ack for the batch that went back"
    );
    assert_eq!(
        *hellos.lock().unwrap(),
        [Some(0), Some(3)],
        "the reconnect resumes after what was handled"
    );
    handle.stop();
}

const HANGS_CHILD: &str = "KERNABLE_TEST_HANGS_CHILD";
static RELEASED: AtomicBool = AtomicBool::new(false);

/// Hangs a command past its lease and a query past 5 s, in a child process whose stderr the
/// parent test reads. Does nothing in a normal test run.
#[tokio::test(flavor = "multi_thread")]
async fn hangs_child() {
    if std::env::var_os(HANGS_CHILD).is_none() {
        return;
    }
    let kernel = TestKernel::new(
        &["counters.*"],
        &["--lease-ttl-ms", "500", "--query-timeout-ms", "8000"],
    );
    kernel.start().await;
    let kernable = kernel.client();
    let counters = domain("counters");
    let slow = counters.aggregate("Slow", 0i64);
    slow.command("Do", |_, _: serde_json::Value, _cmd, _ctx| {
        while !RELEASED.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(vec![])
    });
    let answers = counters.read_model("slow-answers").query(
        "counters.Ask",
        |_: serde_json::Value, _qry, _ctx| async {
            while !RELEASED.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(1)
        },
    );
    let worker = start_worker_of(&kernable, &slow);
    let listener = kernable.listener(&answers);
    let l = listener.clone();
    tokio::spawn(async move { l.run().await });
    worker.ready().await;
    listener.ready().await;
    kernable
        .enqueue(Submit::new("counters", "Slow", "s1", "Do"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let k = kernable.clone();
    tokio::spawn(async move { k.query::<i64>(Query::new("counters", "Ask")).await });
    tokio::time::sleep(Duration::from_millis(5_500)).await;
    RELEASED.store(true, Ordering::SeqCst);
    worker.stop();
    listener.stop();
}

#[test]
fn a_handler_that_hangs_is_reported() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["hangs_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(HANGS_CHILD, "1")
        .env("KERNABLE_LOG", "error")
        .output()
        .unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(out.status.success(), "the child runs: {stderr}");
    assert!(
        stderr.contains("Slow/s1 still running after its lease ended"),
        "{stderr}"
    );
    assert!(
        stderr.contains("still handling the query counters.Ask after 5 s"),
        "{stderr}"
    );
}
