//! Ordered listeners and the settling submit: lower orders first, submits wait for everyone
//! that counts: not listeners their service retired, not live ones.

mod common;

use std::time::Duration;

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, Batch, ChallengeResponse, CommandResult, Hello, ListenerMessage, Middleware, Parked,
        RetryRequest, Wait, When, command_result::Outcome, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
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
        self.ack_parking(position, &[]).await;
    }

    /// Acknowledges `position`, giving up on the events at `parked`.
    async fn ack_parking(&self, position: u64, parked: &[u64]) {
        let parked = parked
            .iter()
            .map(|&position| Parked {
                position,
                code: "not_found".into(),
                message: "no such customer".into(),
                attempts: 5,
            })
            .collect();
        let msg = In::Ack(Ack { position, parked });
        self.acks
            .send(ListenerMessage { msg: Some(msg) })
            .await
            .unwrap();
    }

    /// The error that ends the session, within 2 s.
    async fn ended(&mut self) -> Status {
        let ended = tokio::time::timeout(Duration::from_secs(2), self.inbound.message())
            .await
            .unwrap();
        ended.unwrap_err()
    }
}

/// Asks the kernel to deliver the parked events of `listener` at `positions` again (all, if
/// empty), or to dismiss them, signed by `key`.
async fn retry(
    env: &Env,
    key: &SigningKey,
    listener: &str,
    positions: &[u64],
    dismiss: bool,
) -> Result<Vec<u64>, Status> {
    let issued_at = issued_now();
    let sig = signing::sign(
        key,
        &signing::retry_message(listener, positions, dismiss, issued_at),
    );
    let request = RetryRequest {
        listener: listener.into(),
        positions: positions.to_vec(),
        dismiss,
        issued_at,
        sig: Some(sig),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    let result = ListenerServiceClient::new(channel)
        .retry(with_grant(request, key))
        .await?;
    Ok(result.into_inner().positions)
}

/// The positions of the events `name` has parked, as stored (after a flush).
async fn stored_parked(env: &Env, name: &str) -> Vec<(u64, bool)> {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let listeners = store.listeners().unwrap();
    let l = listeners.iter().find(|l| l.name == name).unwrap();
    l.parked.iter().map(|p| (p.position, p.retry)).collect()
}

struct Opts {
    key: SigningKey,
    signer: SigningKey,
    order: i32,
    types: Vec<String>,
    from: Option<u64>,
    /// Its service (`Hello.service`).
    service: &'static str,
    live: bool,
    middlewares: Vec<Middleware>,
    protocol: u32,
}

fn opts(order: i32) -> Opts {
    Opts {
        key: key(5),
        signer: key(5),
        order,
        types: Vec::new(),
        from: None,
        service: "",
        live: false,
        middlewares: Vec::new(),
        protocol: kernable_core::PROTOCOL,
    }
}

/// Options of a listener of `service`.
fn of(service: &'static str) -> Opts {
    Opts { service, ..opts(0) }
}

async fn listen(env: &Env, name: &str, o: Opts) -> Result<Listener, Status> {
    let (tx, rx) = mpsc::channel(16);
    let hello = In::Hello(Hello {
        protocol: o.protocol,
        name: name.into(),
        key_id: id(&o.key),
        order: o.order,
        event_types: o.types,
        from_position: o.from,
        service: o.service.into(),
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
async fn a_membership_decides_which_listeners_count_and_which_middlewares_check() {
    let metrics = free_addr();
    let env = start_custom("listener-membership", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
    let ready = |body: String| body.starts_with("HTTP/1.1 200");
    let guarding = || Opts {
        middlewares: vec![Middleware {
            order: 0,
            types: vec!["orders.PlaceOrder".into()],
            when: 0,
        }],
        ..of("shop")
    };

    // Only a key granted manage applies it; then the kernel waits for what it lists, also
    // before it ever connected.
    let shop = membership("shop", &[], &["carts"], &["guard"]);
    let denied = apply_as(&env.addr, shop.clone(), &key(2))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    apply(&env.addr, shop).await.unwrap();
    let waiting = get(&metrics, "/ready").await;
    assert!(
        waiting.ends_with("waiting for listeners: carts, guard\n"),
        "{waiting}"
    );
    let carts = listen(&env, "carts", of("shop")).await.unwrap();
    let guard = listen(&env, "guard", guarding()).await.unwrap();
    assert!(ready(get(&metrics, "/ready").await));

    // A listener of the service that it does not list counts for nobody, and cannot check.
    let extra = listen(&env, "extra", of("shop")).await.unwrap();
    drop(extra);
    let rogue = listen(&env, "rogue", guarding()).await;
    assert_eq!(rogue.err().unwrap().code(), Code::PermissionDenied);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(ready(get(&metrics, "/ready").await));

    // A listed middleware that is away refuses what it checks: security fails closed.
    drop(guard);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let refused = submit(&env.addr, place.clone()).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable);

    // The next version no longer lists the guard: its check goes, and nobody waits for it.
    apply(&env.addr, membership("shop", &[], &["carts"], &[]))
        .await
        .unwrap();
    assert!(ready(get(&metrics, "/ready").await));
    let placed = submit(&env.addr, place).await.unwrap();
    assert!(matches!(placed.outcome, Some(Outcome::Committed(_))));
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let listeners = store.listeners().unwrap();
    let guard = listeners.iter().find(|l| l.name == "guard").unwrap();
    assert!(
        guard.middlewares.is_empty(),
        "its position stays, its check goes"
    );

    // A listener without a service always counts.
    let plain = listen(&env, "plain", opts(0)).await.unwrap();
    drop((plain, carts));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waiting = get(&metrics, "/ready").await;
    assert!(
        waiting.ends_with("waiting for listeners: carts, plain\n"),
        "{waiting}"
    );
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
    // Its service's membership does not need to list it.
    let live = || Opts {
        live: true,
        from: Some(0),
        ..of("shop")
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

#[tokio::test(flavor = "multi_thread")]
async fn an_ack_beyond_the_delivered_events_ends_the_session() {
    let env = start("listener-ack-beyond").await;
    spawn_worker(&env).await;
    let mut orders = listen(&env, "orders", opts(0)).await.unwrap();
    let _first = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&orders.next().await), [1]);
    orders.ack(2).await;
    let ended = tokio::time::timeout(Duration::from_secs(2), orders.inbound.message())
        .await
        .unwrap();
    assert_eq!(ended.unwrap_err().code(), Code::InvalidArgument);
    // Nothing of it was kept: the listener resumes after what it had acknowledged, nothing.
    let orders = listen(&env, "orders", opts(0)).await.unwrap();
    assert_eq!(orders.start, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_whose_grant_no_longer_covers_it_is_disconnected() {
    let env = start("listener-narrowed").await;
    let check = || Middleware {
        order: 1,
        types: vec!["orders.*".into()],
        when: When::Before.into(),
    };
    let guard = Opts {
        middlewares: vec![check()],
        ..opts(0)
    };
    let mut guard = listen(&env, "guard", guard).await.unwrap();
    let mut audit = listen(&env, "audit", opts(0)).await.unwrap();

    // Key 5 keeps listen but loses check: the middleware goes, the plain listener stays.
    env.kernel
        .grant(grant(&key(5), &[("listen", &["*"])]))
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), guard.inbound.message())
        .await
        .unwrap();
    assert_eq!(ended.unwrap_err().code(), Code::PermissionDenied);
    audit.nothing_for(300).await;

    // Then it loses listen too.
    env.kernel
        .grant(grant(&key(5), &[("query", &["*"])]))
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), audit.inbound.message())
        .await
        .unwrap();
    assert_eq!(ended.unwrap_err().code(), Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parked_event_stays_parked_until_a_retry_gets_past_it() {
    let metrics = free_addr();
    let env = start_custom("listener-parked", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
    let mut mailer = listen(&env, "mailer", opts(10)).await.unwrap();
    let placed = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&mailer.next().await), [1]);
    // Giving up on an event processes it: the submit settles, the listener moves on.
    mailer.ack_parking(1, &[1]).await;
    assert!(settled(&placed.await.unwrap()));
    assert_eq!(stored_parked(&env, "mailer").await, [(1, false)]);
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let parked = store.listeners().unwrap()[0].parked[0].clone();
    assert_eq!(
        (
            parked.event_type.as_str(),
            parked.code.as_str(),
            parked.attempts
        ),
        ("orders.OrderPlaced", "not_found", 5)
    );
    drop(store);
    let scraped = get(&metrics, "/metrics").await;
    assert!(
        scraped.contains("kernable_listener_parked{listener=\"mailer\"} 1"),
        "{scraped}"
    );
    let added = spawn_submit(&env, "c2", "AddItem");
    assert_eq!(positions(&mailer.next().await), [2]);
    mailer.ack(2).await;
    added.await.unwrap();

    // A retry comes alone, up to the last delivered position; parked again, it stays.
    assert_eq!(
        retry(&env, &key(1), "mailer", &[1], false).await.unwrap(),
        [1]
    );
    let again = mailer.next().await;
    assert_eq!((again.events.len(), again.up_to), (0, 2));
    assert_eq!(again.retries[0].global_position, 1);
    mailer.ack_parking(2, &[1]).await;
    assert_eq!(stored_parked(&env, "mailer").await, [(1, false)]);
    mailer.nothing_for(200).await;

    // Handled: it is gone.
    assert_eq!(
        retry(&env, &key(1), "mailer", &[], false).await.unwrap(),
        [1]
    );
    assert_eq!(mailer.next().await.retries.len(), 1);
    mailer.ack(2).await;
    assert_eq!(stored_parked(&env, "mailer").await, []);
    assert!(
        retry(&env, &key(1), "mailer", &[], false)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn parked_events_are_checked() {
    let env = start("listener-parked-checked").await;
    spawn_worker(&env).await;
    let mut mailer = listen(&env, "mailer", opts(10)).await.unwrap();
    let _placed = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&mailer.next().await), [1]);
    mailer.ack(1).await;
    let _added = spawn_submit(&env, "c2", "AddItem");
    assert_eq!(positions(&mailer.next().await), [2]);
    // Only events of the acknowledged batches: not one acknowledged before.
    mailer.ack_parking(2, &[1]).await;
    assert_eq!(mailer.ended().await.code(), Code::InvalidArgument);

    // Only a key that manages the listener's service retries; only registered listeners.
    let refused = retry(&env, &key(5), "mailer", &[], false).await;
    assert_eq!(refused.unwrap_err().code(), Code::PermissionDenied);
    let unknown = retry(&env, &key(1), "nobody", &[], false).await;
    assert_eq!(unknown.unwrap_err().code(), Code::NotFound);
    let mut forged = RetryRequest {
        listener: "mailer".into(),
        issued_at: issued_now(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut client = ListenerServiceClient::new(channel);
    let unsigned = client.retry(with_grant(forged.clone(), &key(1))).await;
    assert_eq!(unsigned.unwrap_err().code(), Code::Unauthenticated);
    let msg = signing::retry_message("mailer", &[], false, forged.issued_at);
    forged.sig = Some(signing::sign(&key(1), &msg));
    forged.dismiss = true;
    let changed = client.retry(with_grant(forged, &key(1))).await;
    assert_eq!(changed.unwrap_err().code(), Code::Unauthenticated);

    // A live listener has nothing to park into.
    let live = Opts {
        live: true,
        ..opts(10)
    };
    let mut push = listen(&env, "push", live).await.unwrap();
    let _shipped = spawn_submit(&env, "c3", "AddItem");
    assert_eq!(positions(&push.next().await), [3]);
    push.ack_parking(3, &[3]).await;
    assert_eq!(push.ended().await.code(), Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_waits_for_the_listener_and_survives_a_restart() {
    let env = start("listener-parked-restart").await;
    spawn_worker(&env).await;
    let mut mailer = listen(&env, "mailer", opts(10)).await.unwrap();
    let placed = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&mailer.next().await), [1]);
    mailer.ack_parking(1, &[1]).await;
    placed.await.unwrap();
    let added = spawn_submit(&env, "c2", "AddItem");
    assert_eq!(positions(&mailer.next().await), [2]);
    mailer.ack_parking(2, &[2]).await;
    added.await.unwrap();
    drop(mailer);

    // Asked for while it is away; dismissed ones are gone. Both are stored at once.
    assert_eq!(
        retry(&env, &key(1), "mailer", &[1], false).await.unwrap(),
        [1]
    );
    assert_eq!(
        retry(&env, &key(1), "mailer", &[2], true).await.unwrap(),
        [2]
    );
    assert!(
        retry(&env, &key(1), "mailer", &[2], true)
            .await
            .unwrap()
            .is_empty()
    );
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let parked = store.listeners().unwrap()[0].parked.clone();
    assert_eq!(
        parked
            .iter()
            .map(|p| (p.position, p.retry))
            .collect::<Vec<_>>(),
        [(1, true)]
    );
    drop(store);

    let from = env.dir.clone();
    let after = start_custom("listener-parked-restarted", move |_, dir| {
        for file in ["k.db", "k.db-wal", "k.log"] {
            if from.join(file).exists() {
                std::fs::copy(from.join(file), dir.join(file)).unwrap();
            }
        }
    })
    .await;
    let mut mailer = listen(&after, "mailer", opts(10)).await.unwrap();
    let again = mailer.next().await;
    assert_eq!(again.up_to, mailer.start);
    assert_eq!(again.retries[0].global_position, 1);
    mailer.ack(again.up_to).await;
    assert_eq!(stored_parked(&after, "mailer").await, []);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_of_protocol_3_gets_no_retries() {
    let env = start("listener-parked-old").await;
    spawn_worker(&env).await;
    let mut mailer = listen(&env, "mailer", opts(10)).await.unwrap();
    let placed = spawn_submit(&env, "c1", "PlaceOrder");
    assert_eq!(positions(&mailer.next().await), [1]);
    mailer.ack_parking(1, &[1]).await;
    placed.await.unwrap();
    drop(mailer);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let old = Opts {
        protocol: 3,
        ..opts(10)
    };
    let mut mailer = listen(&env, "mailer", old).await.unwrap();
    assert_eq!(
        retry(&env, &key(1), "mailer", &[], false).await.unwrap(),
        [1]
    );
    // It would acknowledge the retry without handling it: it never gets one.
    mailer.nothing_for(300).await;
    assert_eq!(stored_parked(&env, "mailer").await, [(1, true)]);
}
