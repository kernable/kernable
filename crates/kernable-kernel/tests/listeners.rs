//! Ordered listeners and the settling submit: lower orders first, submits wait for everyone
//! that counts: not listeners their service retired, not live ones.

mod common;

use std::time::Duration;

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, Batch, ChallengeResponse, CommandResult, Hello, ListenerMessage, Middleware, Wait,
        command_result::Outcome, listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, Streaming};

struct Listener {
    acks: mpsc::Sender<ListenerMessage>,
    inbound: Streaming<kernable_core::proto::ListenerDelivery>,
    /// Position the kernel starts delivering after.
    start: u64,
}

impl Listener {
    async fn next(&mut self) -> Batch {
        match tokio::time::timeout(Duration::from_secs(2), self.inbound.message()).await {
            Ok(Ok(Some(m))) => match m.msg.unwrap() {
                Out::Batch(b) => b,
                other => panic!("expected a batch, got {other:?}"),
            },
            other => panic!("no batch within 2 s: {other:?}"),
        }
    }

    async fn nothing_for(&mut self, ms: u64) {
        let got = tokio::time::timeout(Duration::from_millis(ms), self.inbound.message()).await;
        assert!(got.is_err(), "unexpected delivery: {got:?}");
    }

    async fn ack(&self, position: u64) {
        let msg = In::Ack(Ack { position });
        self.acks
            .send(ListenerMessage { msg: Some(msg) })
            .await
            .unwrap();
    }
}

struct Opts {
    key: SigningKey,
    signer: SigningKey,
    order: i32,
    types: Vec<String>,
    from: Option<u64>,
    /// The service and the names it declares (`Hello.service`).
    service: (&'static str, Vec<&'static str>),
    live: bool,
    middlewares: Vec<Middleware>,
}

fn opts(order: i32) -> Opts {
    Opts {
        key: key(5),
        signer: key(5),
        order,
        types: Vec::new(),
        from: None,
        service: ("", Vec::new()),
        live: false,
        middlewares: Vec::new(),
    }
}

/// Options of a listener of `service` that declares `declared`.
fn of(service: &'static str, declared: &[&'static str]) -> Opts {
    Opts {
        service: (service, declared.to_vec()),
        ..opts(0)
    }
}

async fn listen(env: &Env, name: &str, o: Opts) -> Result<Listener, Status> {
    let (tx, rx) = mpsc::channel(16);
    let hello = In::Hello(Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: id(&o.key),
        order: o.order,
        event_types: o.types,
        from_position: o.from,
        service: o.service.0.into(),
        declared: o.service.1.iter().map(|n| n.to_string()).collect(),
        live: o.live,
        middlewares: o.middlewares,
        ..Default::default()
    });
    tx.send(ListenerMessage { msg: Some(hello) }).await.unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await?
        .into_inner();
    loop {
        match inbound.message().await?.and_then(|m| m.msg) {
            Some(Out::Challenge(c)) => {
                let sig = signing::sign(&o.signer, &signing::challenge_message(&c.nonce)).sig;
                let msg = In::ChallengeResponse(ChallengeResponse { sig });
                tx.send(ListenerMessage { msg: Some(msg) }).await.unwrap();
            }
            Some(Out::Ready(r)) => {
                return Ok(Listener {
                    acks: tx,
                    inbound,
                    start: r.after_position,
                });
            }
            other => panic!("unexpected handshake message {other:?}"),
        }
    }
}

fn spawn_submit(env: &Env, id: &str, command_type: &str) -> JoinHandle<CommandResult> {
    let addr = env.addr.clone();
    let cmd = signed(command(id, command_type, "o1"), &key(1));
    tokio::spawn(async move { submit(&addr, cmd).await.unwrap() })
}

fn settled(result: &CommandResult) -> bool {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.settled,
        other => panic!("expected committed, got {other:?}"),
    }
}

fn positions(batch: &Batch) -> Vec<u64> {
    batch.events.iter().map(|e| e.global_position).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn lower_orders_go_first_and_submit_waits_for_every_listener() {
    let env = start("listener-order").await;
    spawn_worker(&env).await;
    let mut auth = listen(&env, "auth", opts(0)).await.unwrap();
    let mut reactor = listen(&env, "reactor", opts(10)).await.unwrap();

    let pending = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&auth.next().await), [1]);
    // The reactor must not see the event before the read model has processed it.
    reactor.nothing_for(200).await;
    auth.ack(1).await;
    assert_eq!(positions(&reactor.next().await), [1]);
    // The command is committed, but not every listener is done yet.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!pending.is_finished());
    reactor.ack(1).await;
    assert!(settled(&pending.await.unwrap()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_settles_only_on_the_listeners_before_it() {
    let env = start("listener-caller").await;
    spawn_worker(&env).await;
    let mut model = listen(&env, "model", opts(0)).await.unwrap();
    let mut reactor = listen(&env, "reactor", opts(10)).await.unwrap();
    let _later = listen(&env, "later", opts(20)).await.unwrap();

    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit_with(&env.addr, place, Wait::Committed)
        .await
        .unwrap();
    assert_eq!(positions(&model.next().await), [1]);
    model.ack(1).await;
    // The reactor handles event 1 and, from its handler, submits a command that settles.
    assert_eq!(positions(&reactor.next().await), [1]);
    let addr = env.addr.clone();
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    let follow_up =
        tokio::spawn(async move { submit_from(&addr, add, Wait::Settled, "reactor").await });
    assert_eq!(positions(&model.next().await), [2]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!follow_up.is_finished(), "it waits for the read model");
    model.ack(2).await;
    // ... but neither for the reactor itself nor for later listeners, which are still at 0.
    let result = follow_up.await.unwrap().unwrap();
    assert!(settled(&result));

    let unknown = signed(command("c3", "AddItem", "o1"), &key(1));
    let status = submit_from(&env.addr, unknown, Wait::Settled, "nobody")
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_is_ready_once_every_listener_is_connected_and_caught_up() {
    let metrics = free_addr();
    let env = start_custom("listener-ready", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit_with(&env.addr, place, Wait::Committed)
        .await
        .unwrap();
    let ready = |body: String| body.starts_with("HTTP/1.1 200");
    assert!(ready(get(&metrics, "/ready").await), "no listeners yet");

    // A read model rebuilding from the first event.
    let from_start = Opts {
        from: Some(0),
        ..opts(0)
    };
    let mut model = listen(&env, "model", from_start).await.unwrap();
    let waiting = get(&metrics, "/ready").await;
    assert!(waiting.starts_with("HTTP/1.1 503"), "{waiting}");
    assert!(
        waiting.ends_with("waiting for listeners: model\n"),
        "{waiting}"
    );
    assert!(
        get(&metrics, "/metrics")
            .await
            .contains("\nkernable_ready 0\n")
    );

    assert_eq!(positions(&model.next().await), [1]);
    model.ack(1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(ready(get(&metrics, "/ready").await));
    assert!(
        get(&metrics, "/metrics")
            .await
            .contains("\nkernable_ready 1\n")
    );

    // A registered listener that went away makes the kernel not ready; health is unaffected.
    drop(model);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!ready(get(&metrics, "/ready").await));
    assert!(get(&metrics, "/health").await.starts_with("HTTP/1.1 200"));
}

#[tokio::test(flavor = "multi_thread")]
async fn committed_and_accepted_submits_do_not_wait_and_a_stalled_listener_times_out() {
    let env = start("listener-modes").await;
    spawn_worker(&env).await;
    let _stalled = listen(&env, "stalled", opts(0)).await.unwrap();

    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let committed = submit_with(&env.addr, place, Wait::Committed)
        .await
        .unwrap();
    assert!(!settled(&committed));

    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    let accepted = submit_with(&env.addr, add.clone(), Wait::Accepted)
        .await
        .unwrap();
    assert_eq!(
        accepted.outcome,
        Some(Outcome::Accepted(Default::default()))
    );

    // Resubmitting returns the result; the stalled listener lets the wait time out (1 s).
    let started = std::time::Instant::now();
    let result = submit(&env.addr, add).await.unwrap();
    assert!(!settled(&result));
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disconnected_listener_still_counts_and_resumes_where_it_left_off() {
    let env = start("listener-resume").await;
    spawn_worker(&env).await;
    let mut orders = listen(&env, "orders", opts(0)).await.unwrap();
    let first = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&orders.next().await), [1]);
    orders.ack(1).await;
    assert!(settled(&first.await.unwrap()));
    drop(orders);

    let second = spawn_submit(&env, "c2", "AddItem").await.unwrap();
    assert!(
        !settled(&second),
        "an absent listener has not processed the event"
    );

    let mut orders = listen(&env, "orders", opts(0)).await.unwrap();
    assert_eq!(orders.start, 1);
    assert_eq!(positions(&orders.next().await), [2]);
    drop(orders);

    // An in-memory read model rebuilds from the beginning.
    let rebuild = Opts {
        from: Some(0),
        ..opts(0)
    };
    let mut orders = listen(&env, "orders", rebuild).await.unwrap();
    assert_eq!(positions(&orders.next().await), [1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn events_of_other_types_do_not_hold_back_a_filtered_listener() {
    let env = start("listener-filter").await;
    spawn_worker(&env).await;
    let placed_only = Opts {
        key: key(6),
        signer: key(6),
        types: vec!["orders.OrderPlaced".into()],
        ..opts(0)
    };
    let mut listener = listen(&env, "placed", placed_only).await.unwrap();
    let mut later = listen(&env, "later", opts(10)).await.unwrap();

    let place = spawn_submit(&env, "c1", "PlaceOrder");
    let batch = listener.next().await;
    assert_eq!((positions(&batch), batch.up_to), (vec![1], 1));
    listener.ack(1).await;
    later.next().await;
    later.ack(1).await;
    assert!(settled(&place.await.unwrap()));

    // ItemAdded is not for this listener: it is skipped without a round trip.
    let add = spawn_submit(&env, "c2", "AddItem");
    assert_eq!(positions(&later.next().await), [2]);
    later.ack(2).await;
    assert!(settled(&add.await.unwrap()));
    listener.nothing_for(100).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_listeners_are_refused() {
    let env = start("listener-refused").await;
    let code = |r: Result<Listener, Status>| r.err().map(|s| s.code());

    let stranger = Opts {
        key: key(9),
        signer: key(9),
        ..opts(0)
    };
    assert_eq!(
        code(listen(&env, "x", stranger).await),
        Some(Code::PermissionDenied)
    );
    let impostor = Opts {
        signer: key(9),
        ..opts(0)
    };
    assert_eq!(
        code(listen(&env, "x", impostor).await),
        Some(Code::Unauthenticated)
    );
    assert_eq!(
        code(listen(&env, "x", opts(-10)).await),
        Some(Code::InvalidArgument)
    );
    let too_much = Opts {
        key: key(6),
        signer: key(6),
        types: vec!["orders.ItemAdded".into()],
        ..opts(0)
    };
    assert_eq!(
        code(listen(&env, "x", too_much).await),
        Some(Code::PermissionDenied)
    );

    let _first = listen(&env, "orders", opts(0)).await.unwrap();
    assert_eq!(
        code(listen(&env, "orders", opts(0)).await),
        Some(Code::AlreadyExists)
    );
    let other_key = Opts {
        key: key(6),
        signer: key(6),
        ..opts(0)
    };
    drop(_first);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        code(listen(&env, "orders", other_key).await),
        Some(Code::PermissionDenied)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_listener_starts_at_the_newest_event_and_then_resumes() {
    let env = start("listener-head").await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit_with(&env.addr, place, Wait::Committed)
        .await
        .unwrap();

    // It never reacts to what happened before it existed.
    let mut mails = listen(&env, "mails", opts(10)).await.unwrap();
    assert_eq!(mails.start, 1);
    spawn_submit(&env, "c2", "AddItem");
    assert_eq!(positions(&mails.next().await), [2]);
    mails.ack(2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(mails);

    // What happens while it is away it gets when it is back.
    let add = signed(command("c3", "AddItem", "o1"), &key(1));
    submit_with(&env.addr, add, Wait::Committed).await.unwrap();
    let mut mails = listen(&env, "mails", opts(10)).await.unwrap();
    assert_eq!(mails.start, 2);
    assert_eq!(positions(&mails.next().await), [3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_retires_what_it_no_longer_declares() {
    let metrics = free_addr();
    let env = start_custom("listener-service", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
    let ready = |body: String| body.starts_with("HTTP/1.1 200");
    let carts = listen(&env, "carts", of("shop", &["carts", "guard"]))
        .await
        .unwrap();
    let guard = Opts {
        middlewares: vec![Middleware {
            order: 0,
            types: vec!["orders.PlaceOrder".into()],
            when: 0,
        }],
        ..of("shop", &["carts", "guard"])
    };
    let guard = listen(&env, "guard", guard).await.unwrap();
    assert!(ready(get(&metrics, "/ready").await));
    drop(guard);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting = get(&metrics, "/ready").await;
    assert!(
        waiting.ends_with("waiting for listeners: guard\n"),
        "{waiting}"
    );

    // The next version of the service no longer has the guard: nobody waits for it.
    drop(carts);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let carts = listen(&env, "carts", of("shop", &["carts"])).await.unwrap();
    assert!(ready(get(&metrics, "/ready").await));
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let guard = store.listeners().unwrap();
    let guard = guard.iter().find(|l| l.name == "guard").unwrap();
    assert!(guard.retired && !guard.middlewares.is_empty());
    // But its middleware keeps refusing until it is removed: security fails closed.
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let refused = submit(&env.addr, place).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable);

    // A listener of another service, or without one, is never retired by it.
    let other = listen(&env, "other", of("billing", &["other"]))
        .await
        .unwrap();
    let plain = listen(&env, "plain", opts(0)).await.unwrap();
    drop((other, plain));
    drop(carts);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _carts = listen(&env, "carts", of("shop", &["carts"])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waiting = get(&metrics, "/ready").await;
    assert!(
        waiting.ends_with("waiting for listeners: other, plain\n"),
        "{waiting}"
    );

    // A service must declare the listener that says so.
    let undeclared = listen(&env, "stray", of("shop", &["carts"])).await;
    assert_eq!(undeclared.err().unwrap().code(), Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_listener_starts_at_the_newest_event_and_counts_for_nobody() {
    let metrics = free_addr();
    let env = start_custom("listener-live", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit_with(&env.addr, place, Wait::Committed)
        .await
        .unwrap();
    // It takes no part in its service's declaration.
    let live = || Opts {
        live: true,
        from: Some(0),
        ..of("shop", &["model"])
    };
    let mut push = listen(&env, "push", live()).await.unwrap();
    assert_eq!(push.start, 1, "from_position is ignored");
    // It does not hold up a settling submit, even without acknowledging.
    let added = spawn_submit(&env, "c2", "AddItem").await.unwrap();
    assert!(settled(&added));
    assert_eq!(positions(&push.next().await), [2]);
    drop(push);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(get(&metrics, "/ready").await.starts_with("HTTP/1.1 200"));
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert!(store.listeners().unwrap().is_empty(), "never stored");

    // Not a middleware, and not under the name of a registered listener.
    let checking = Opts {
        middlewares: vec![Middleware {
            order: 0,
            types: vec!["*".into()],
            when: 0,
        }],
        ..live()
    };
    let refused = listen(&env, "push", checking).await;
    assert_eq!(refused.err().unwrap().code(), Code::InvalidArgument);
    let model = listen(&env, "model", opts(0)).await.unwrap();
    drop(model);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let taken = listen(&env, "model", live()).await;
    assert_eq!(taken.err().unwrap().code(), Code::AlreadyExists);
}
