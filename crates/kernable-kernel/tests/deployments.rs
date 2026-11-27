//! Deployments: workers and listeners declare the code version they run. Events and snapshots
//! record it, the service's membership names the deployment that serves, an older deployment
//! refuses commands on aggregates holding events it does not know, read models run per
//! deployment side by side, and the report says what a deployment left behind.

mod common;

use std::time::Duration;

use common::*;
use kernable_core::{
    proto::{
        Ack, Batch, ChallengeResponse, CommandResult, Hello, ListenerMessage, Skipped, Wait,
        command_result::Outcome, listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

/// A worker of `deployment` in service `shop`, declaring `ItemAdded` at `schema`.
fn worker(deployment: &str, schema: u32) -> (TestWorker, mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let w = TestWorker {
        deployment: deployment.into(),
        service: "shop".into(),
        schemas: vec![
            ("orders.OrderPlaced".into(), 1),
            ("orders.ItemAdded".into(), schema),
        ],
        ..TestWorker::with(Behavior::Honest, tx)
    };
    (w, rx)
}

/// The membership of `shop` naming `deployment`, forced: a deployment whose workers do not
/// know the history's events never takes over by itself (`unknown_history` in its warm-up);
/// forced, the kernel refuses commands aggregate by aggregate.
async fn serve(env: &Env, deployment: &str) {
    let mut m = membership("shop", &["orders.Order"], &[], &[]);
    m.deployment = deployment.into();
    apply_switching(&env.addr, m, true, false, false)
        .await
        .unwrap();
}

fn store(env: &Env) -> Box<dyn kernable_store::Store> {
    kernable_store::open_read_only(&env.store).unwrap()
}

fn rejected(result: &CommandResult) -> (&str, &str) {
    match &result.outcome {
        Some(Outcome::Rejected(r)) => (r.code.as_str(), r.message.as_str()),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

fn events(result: &CommandResult) -> Vec<kernable_core::proto::Event> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.clone(),
        other => panic!("expected committed, got {other:?}"),
    }
}

async fn run(env: &Env, id: &str, command_type: &str, aggregate: &str) -> CommandResult {
    let cmd = signed(command(id, command_type, aggregate), &key(1));
    submit_with(&env.addr, cmd, Wait::Committed).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_refuses_aggregates_with_newer_events_and_serves_the_others() {
    let env = start("deployments-rollback").await;
    serve(&env, "v2").await;
    let (v1, mut seen1) = worker("v1", 1);
    let (v2, mut seen2) = worker("v2", 2);
    start_worker(env.addr.clone(), v1).await;
    start_worker(env.addr.clone(), v2).await;

    // v2 serves: it runs the commands, and its events say so, at its schema.
    run(&env, "a1", "PlaceOrder", "a").await;
    let added = events(&run(&env, "a2", "AddItem", "a").await);
    assert_eq!((added[0].deployment.as_str(), added[0].schema), ("v2", 2));
    assert_eq!(seen2.recv().await.unwrap(), "a1");
    assert!(seen1.try_recv().is_err(), "v1 idles while v2 serves");

    // Back to v1: v1 runs the commands, v2 idles; v1 may write schema 1 again.
    serve(&env, "v1").await;
    run(&env, "b1", "PlaceOrder", "b").await;
    let b = events(&run(&env, "b2", "AddItem", "b").await);
    assert_eq!((b[0].deployment.as_str(), b[0].schema), ("v1", 1));
    assert_eq!(seen1.recv().await.unwrap(), "b1");

    // Aggregate a holds ItemAdded at schema 2: v1 does not see the command at all.
    let refused = run(&env, "a3", "AddItem", "a").await;
    let (code, message) = rejected(&refused);
    assert_eq!(code, "unknown_history");
    assert!(
        message.contains("orders.ItemAdded at schema 2"),
        "{message}"
    );
    assert_eq!(seen1.recv().await.unwrap(), "b2");
    assert!(seen1.try_recv().is_err(), "the worker never got a3");

    // Rolled forward again, the aggregate takes commands.
    serve(&env, "v2").await;
    events(&run(&env, "a4", "AddItem", "a").await);

    // The report from v2's start: what it wrote, what is new there.
    let report = kernable_kernel::deployments::since(store(&env).as_ref(), 0, Some("v2")).unwrap();
    let types = report["types"].as_array().unwrap();
    let item = types
        .iter()
        .find(|t| t["type"] == "orders.ItemAdded")
        .unwrap();
    assert_eq!(item["schema"], 2);
    assert_eq!(item["events"], 2);
    assert_eq!(item["aggregates"], 1);
    assert_eq!(item["newType"], true, "nothing before position 0");
    // From the position after v2's first events: schema 1 events of v1 came later, schema 2
    // already existed.
    let all = kernable_kernel::deployments::since(store(&env).as_ref(), 2, None).unwrap();
    let rows: Vec<(String, u64, String, bool)> = all["types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                t["type"].as_str().unwrap().to_owned(),
                t["schema"].as_u64().unwrap(),
                t["deployment"].as_str().unwrap().to_owned(),
                t["newSchema"].as_bool().unwrap() || t["newType"].as_bool().unwrap(),
            )
        })
        .collect();
    assert!(rows.contains(&("orders.ItemAdded".into(), 1, "v1".into(), false)));
    assert!(rows.contains(&("orders.ItemAdded".into(), 2, "v2".into(), false)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_type_another_deployment_introduced_is_unknown_to_an_older_one() {
    let env = start("deployments-types").await;
    // Workers that declare OrderPlaced only: ItemAdded, which they can write, is not declared.
    let narrow = |deployment: &str| {
        let (mut w, _) = worker(deployment, 1);
        w.schemas.retain(|(t, _)| t == "orders.OrderPlaced");
        w
    };
    start_worker(env.addr.clone(), narrow("v1")).await;
    start_worker(env.addr.clone(), worker("v2", 1).0).await;
    serve(&env, "v1").await;
    run(&env, "a1", "PlaceOrder", "a").await;
    // v2 introduces ItemAdded, after v1's first event.
    serve(&env, "v2").await;
    events(&run(&env, "a2", "AddItem", "a").await);
    serve(&env, "v1").await;
    let refused = run(&env, "a3", "AddItem", "a").await;
    let (code, message) = rejected(&refused);
    assert_eq!(code, "unknown_history");
    assert!(
        message.contains("orders.ItemAdded (version 2), a type deployment v1 does not know"),
        "{message}"
    );
    // Aggregates without it are served.
    run(&env, "b1", "PlaceOrder", "b").await;

    // A deployment that wrote nothing yet knows what existed before it: a redeploy of the same
    // code under a new name is not refused.
    start_worker(env.addr.clone(), narrow("v3")).await;
    serve(&env, "v3").await;
    events(&run(&env, "a4", "AddItem", "a").await);

    // Once v1 itself wrote the type, it is one of its own.
    serve(&env, "v1").await;
    events(&run(&env, "b2", "AddItem", "b").await);
    events(&run(&env, "a5", "AddItem", "a").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn each_deployment_gets_only_its_own_snapshots() {
    let env = start("deployments-snapshots").await;
    let (tx, mut executions) = mpsc::unbounded_channel();
    let (seen, _) = mpsc::unbounded_channel();
    let snapshotting = |deployment: &str| TestWorker {
        behavior: Behavior::Snapshots,
        deployment: deployment.into(),
        service: "shop".into(),
        executions: Some(tx.clone()),
        seen: Some(seen.clone()),
        ..TestWorker::honest()
    };
    serve(&env, "v2").await;
    start_worker(env.addr.clone(), snapshotting("v1")).await;
    start_worker(env.addr.clone(), snapshotting("v2")).await;
    run(&env, "c1", "PlaceOrder", "o").await;
    run(&env, "c2", "AddItem", "o").await;
    // v2 made snapshots; it gets its newest.
    assert_eq!(executions.recv().await.unwrap(), (None, 0));
    assert_eq!(executions.recv().await.unwrap(), (Some(1), 0));
    // v1 never gets v2's snapshot: all events instead.
    serve(&env, "v1").await;
    run(&env, "c3", "AddItem", "o").await;
    assert_eq!(executions.recv().await.unwrap(), (None, 2));
    // v1's own snapshot now sits beside v2's.
    serve(&env, "v2").await;
    run(&env, "c4", "AddItem", "o").await;
    assert_eq!(executions.recv().await.unwrap(), (Some(2), 1));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snapshots = kernable_store::snapshots::open(&env.snapshots).unwrap();
    let mut held: Vec<(String, u64)> = snapshots
        .infos()
        .unwrap()
        .into_iter()
        .map(|i| (i.deployment, i.version))
        .collect();
    held.sort();
    assert_eq!(held, [("v1".into(), 3), ("v2".into(), 4)]);
}

struct Listener {
    acks: mpsc::Sender<ListenerMessage>,
    inbound: Streaming<kernable_core::proto::ListenerDelivery>,
    start: u64,
}

impl Listener {
    /// The next message within 2 s.
    async fn message(&mut self) -> Result<Option<Out>, tonic::Status> {
        match tokio::time::timeout(Duration::from_secs(2), self.inbound.message()).await {
            Ok(m) => m.map(|m| m.and_then(|m| m.msg)),
            Err(_) => panic!("no message within 2 s"),
        }
    }

    /// Nothing arrives for `ms`.
    async fn quiet(&mut self, ms: u64) {
        if let Ok(m) = tokio::time::timeout(Duration::from_millis(ms), self.inbound.message()).await
        {
            panic!("expected nothing, got {m:?}");
        }
    }

    async fn next(&mut self) -> Batch {
        match tokio::time::timeout(Duration::from_secs(2), self.inbound.message()).await {
            Ok(Ok(Some(m))) => match m.msg.unwrap() {
                Out::Batch(b) => b,
                other => panic!("expected a batch, got {other:?}"),
            },
            other => panic!("no batch within 2 s: {other:?}"),
        }
    }

    async fn ack(&self, position: u64, skipped: Vec<Skipped>) {
        let msg = In::Ack(Ack {
            position,
            parked: Vec::new(),
            skipped,
        });
        self.acks
            .send(ListenerMessage { msg: Some(msg) })
            .await
            .unwrap();
    }

    /// Acknowledges batches until it has seen `position`.
    async fn through(&mut self, position: u64) -> Vec<u64> {
        let mut seen = Vec::new();
        loop {
            let batch = self.next().await;
            seen.extend(batch.events.iter().map(|e| e.global_position));
            self.ack(batch.up_to, Vec::new()).await;
            if batch.up_to >= position {
                return seen;
            }
        }
    }
}

/// A read model `name` of `deployment` in service `shop` (it answers a query, so it is
/// registered per deployment), or a reactor when `reactor`.
async fn listen(env: &Env, name: &str, deployment: &str, reactor: bool) -> Listener {
    let mut l = connect(env, name, deployment, reactor).await;
    l.start = match l.inbound.message().await.unwrap().and_then(|m| m.msg) {
        Some(Out::Ready(r)) => r.after_position,
        other => panic!("expected Ready, got {other:?}"),
    };
    l
}

/// The same, up to the handshake: what the kernel says next (`Ready`, `Waiting`) is the
/// test's to read.
async fn connect(env: &Env, name: &str, deployment: &str, reactor: bool) -> Listener {
    let (tx, rx) = mpsc::channel(16);
    let hello = In::Hello(Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: id(&key(5)),
        service: "shop".into(),
        deployment: deployment.into(),
        query_types: if reactor {
            Vec::new()
        } else {
            vec!["orders.GetOrder".into()]
        },
        ..Default::default()
    });
    tx.send(ListenerMessage { msg: Some(hello) }).await.unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(with_grant(ReceiverStream::new(rx), &key(5)))
        .await
        .unwrap()
        .into_inner();
    match inbound.message().await.unwrap().and_then(|m| m.msg) {
        Some(Out::Challenge(c)) => {
            let sig = signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig;
            let msg = In::ChallengeResponse(ChallengeResponse { sig });
            tx.send(ListenerMessage { msg: Some(msg) }).await.unwrap();
        }
        other => panic!("unexpected handshake message {other:?}"),
    }
    Listener {
        acks: tx,
        inbound,
        start: 0,
    }
}

fn settled(result: &CommandResult) -> bool {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.settled,
        other => panic!("expected committed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_models_run_per_deployment_and_only_the_serving_one_counts() {
    let metrics = free_addr();
    let env = start_custom("deployments-listeners", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    let mut m = membership("shop", &[], &["carts", "mailer"], &[]);
    m.deployment = "v1".into();
    apply(&env.addr, m.clone()).await.unwrap();
    spawn_worker(&env).await;
    let mut carts1 = listen(&env, "carts", "v1", false).await;
    let mut mailer = listen(&env, "mailer", "v1", true).await;
    for (i, id) in ["c1", "c2"].iter().enumerate() {
        let cmd = signed(
            command(id, if i == 0 { "PlaceOrder" } else { "AddItem" }, "o1"),
            &key(1),
        );
        let submitting = tokio::spawn({
            let addr = env.addr.clone();
            async move { submit(&addr, cmd).await.unwrap() }
        });
        carts1.through(i as u64 + 1).await;
        mailer.through(i as u64 + 1).await;
        assert!(settled(&submitting.await.unwrap()));
    }

    // v2's read model of the same name: its own registration, from the first event.
    let mut carts2 = listen(&env, "carts", "v2", false).await;
    assert_eq!(carts2.start, 0, "a new deployment's read model rebuilds");
    // It counts for nobody while v1 serves: a submit settles without it.
    let cmd = signed(command("c3", "AddItem", "o1"), &key(1));
    let submitting = tokio::spawn({
        let addr = env.addr.clone();
        async move { submit(&addr, cmd).await.unwrap() }
    });
    carts1.through(3).await;
    mailer.through(3).await;
    assert!(settled(&submitting.await.unwrap()));
    assert_eq!(
        carts2.through(3).await,
        [1, 2, 3],
        "every event, for itself"
    );

    // The reactor keeps one registration, and only the serving deployment's acts: v2's waits
    // while v1 serves, also with v1's away.
    drop(mailer);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut mailer2 = connect(&env, "mailer", "v2", true).await;
    match mailer2.message().await.unwrap() {
        Some(Out::Waiting(w)) => assert_eq!(w.serving, "v1"),
        other => panic!("expected Waiting, got {other:?}"),
    }
    mailer2.quiet(200).await;

    // v2 serves now: its reactor goes on after v1's position, its read model counts, v1's no
    // longer does.
    m.deployment = "v2".into();
    apply(&env.addr, m).await.unwrap();
    match mailer2.message().await.unwrap() {
        Some(Out::Ready(r)) => assert_eq!(r.after_position, 3, "nothing is handled twice"),
        other => panic!("expected Ready, got {other:?}"),
    }
    drop(carts1);
    let cmd = signed(command("c4", "AddItem", "o1"), &key(1));
    let submitting = tokio::spawn({
        let addr = env.addr.clone();
        async move { submit(&addr, cmd).await.unwrap() }
    });
    carts2.through(4).await;
    mailer2.through(4).await;
    assert!(
        settled(&submitting.await.unwrap()),
        "v1's carts is away and counts for nobody"
    );

    // Skipped events are counted per listener.
    let cmd = signed(command("c5", "AddItem", "o1"), &key(1));
    let submitting = tokio::spawn({
        let addr = env.addr.clone();
        async move { submit(&addr, cmd).await.unwrap() }
    });
    let batch = carts2.next().await;
    let skipped = vec![Skipped {
        position: 5,
        event_type: "orders.ItemAdded".into(),
        schema: 3,
    }];
    carts2.ack(batch.up_to, skipped).await;
    mailer2.through(5).await;
    submitting.await.unwrap();
    let scraped = get(&metrics, "/metrics").await;
    assert!(
        scraped.contains("kernable_listener_skipped{listener=\"carts@v2\"} 1"),
        "{scraped}"
    );

    // The stored registrations: one per deployment for the read model, one for the reactor.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let mut names: Vec<String> = store
        .listeners()
        .unwrap()
        .into_iter()
        .map(|l| l.name)
        .collect();
    names.sort();
    assert_eq!(names, ["carts@v1", "carts@v2", "mailer"]);
}

/// Applies `m` as an app with switch `caught-up` does (`no_rollback`) until the kernel takes
/// it, for up to 2 s.
async fn apply_eventually(env: &Env, m: &kernable_core::proto::Membership) {
    for _ in 0..100 {
        if apply_switching(&env.addr, m.clone(), false, false, true)
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    apply_switching(&env.addr, m.clone(), false, false, true)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_deployment_takes_over_only_once_caught_up_and_only_an_operator_rolls_back() {
    let metrics = free_addr();
    let env = start_custom("deployments-warm-up", |config, _| {
        config.metrics_listen = Some(metrics.clone());
        config.deployment_switch_lag = 0;
    })
    .await;
    let mut m1 = membership("shop", &["orders.Order"], &["carts"], &[]);
    m1.deployment = "v1".into();
    apply(&env.addr, m1.clone()).await.unwrap();
    start_worker(env.addr.clone(), worker("v1", 1).0).await;
    let mut carts1 = listen(&env, "carts", "v1", false).await;
    run(&env, "a1", "PlaceOrder", "a").await;
    carts1.through(1).await;

    // v2 connects beside v1: its read model rebuilds, its worker is not there yet.
    let mut carts2 = listen(&env, "carts", "v2", false).await;
    let mut m2 = m1.clone();
    m2.deployment = "v2".into();
    let refused = apply(&env.addr, m2.clone()).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    let why = refused.message();
    assert!(why.starts_with("not_caught_up: deployment v2"), "{why}");
    assert!(why.contains("carts@v2 at 0 of 1"), "{why}");
    assert!(
        why.contains("no worker of orders.Order is connected"),
        "{why}"
    );
    let ready = get(&metrics, "/ready?deployment=v2").await;
    assert!(ready.starts_with("HTTP/1.1 503"), "{ready}");
    assert!(ready.contains("carts@v2 at 0 of 1"), "{ready}");
    // An app that leaves the switch to an operator is refused as well, whatever the state.
    let kept = apply_switching(&env.addr, m2.clone(), false, true, false)
        .await
        .unwrap_err();
    assert!(
        kept.message()
            .starts_with("not_serving: shop is served by deployment v1")
    );

    // Caught up and its worker connected: it takes over; v1 keeps its read model.
    start_worker(env.addr.clone(), worker("v2", 1).0).await;
    carts2.through(1).await;
    // The listener's acknowledgement and the worker's registration reach the kernel a moment
    // after: asked until then.
    let mut ready = String::new();
    for _ in 0..100 {
        ready = get(&metrics, "/ready?deployment=v2").await;
        if ready.starts_with("HTTP/1.1 200") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready.starts_with("HTTP/1.1 200"), "{ready}");
    apply_eventually(&env, &m2).await;
    let written = events(&run(&env, "a2", "AddItem", "a").await);
    assert_eq!(written[0].deployment, "v2");
    carts2.through(2).await;

    // An app of v2 that restarts applies again: the serving deployment, nothing switches.
    apply_switching(&env.addr, m2.clone(), false, false, true)
        .await
        .unwrap();
    // An app of v1 that restarts (switch caught-up, no_rollback) does not switch back.
    let restarted = apply_switching(&env.addr, m1.clone(), false, false, true)
        .await
        .unwrap_err();
    assert_eq!(restarted.code(), tonic::Code::FailedPrecondition);
    assert!(
        restarted
            .message()
            .starts_with("served_before: deployment v1 served shop before and v2 serves it now"),
        "{}",
        restarted.message()
    );
    let written = events(&run(&env, "a3", "AddItem", "a").await);
    assert_eq!(written[0].deployment, "v2");

    // Back to v1, which served before, by an operator: at once, although its read model is
    // behind.
    let mut back = m1.clone();
    back.metadata = vec![kernable_core::proto::Attribute {
        key: "rollback".into(),
        value: "1".into(),
    }];
    apply(&env.addr, back).await.unwrap();

    // A deployment nothing of which connected waits, unless forced.
    let mut m3 = m1.clone();
    m3.deployment = "v3".into();
    let refused = apply(&env.addr, m3.clone()).await.unwrap_err();
    assert!(
        refused.message().contains("carts has not connected"),
        "{}",
        refused.message()
    );
    let ready = get(&metrics, "/ready?deployment=v3").await;
    assert!(ready.contains("nothing of it is connected"), "{ready}");
    apply_switching(&env.addr, m3, true, false, false)
        .await
        .unwrap();
    drop((carts1, carts2));
}

/// Submits a command for `aggregate` through spawn_worker's honest worker, waiting until it
/// committed (not settled: the test's listeners acknowledge).
async fn commit(env: &Env, id: &str, aggregate: &str) {
    let cmd = signed(command(id, "PlaceOrder", aggregate), &key(1));
    submit_with(&env.addr, cmd, Wait::Committed).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_serving_deployment_s_reactors_act_and_a_new_one_starts_at_the_switch() {
    let ui = free_addr();
    let env = start_custom("deployments-reactors", |config, _| {
        config.ui_listen = Some(ui.clone());
        config.deployment_switch_lag = 0;
    })
    .await;
    let mut m1 = membership("shop", &[], &["mailer"], &[]);
    m1.deployment = "v1".into();
    apply(&env.addr, m1.clone()).await.unwrap();
    spawn_worker(&env).await;
    let mut mailer1 = listen(&env, "mailer", "v1", true).await;
    commit(&env, "c1", "o1").await;
    mailer1.through(1).await;

    // v2 warms up beside v1: its reactors, of v1's name and of a new one, wait and get nothing.
    let mut mailer2 = connect(&env, "mailer", "v2", true).await;
    let mut notifier2 = connect(&env, "notifier", "v2", true).await;
    for r in [&mut mailer2, &mut notifier2] {
        match r.message().await.unwrap() {
            Some(Out::Waiting(w)) => assert_eq!(w.serving, "v1"),
            other => panic!("expected Waiting, got {other:?}"),
        }
    }
    commit(&env, "c2", "o2").await;
    mailer1.through(2).await;
    mailer2.quiet(300).await;
    notifier2.quiet(0).await;
    // After a kernel restart or a reconnect it would race v1's: it still waits.
    drop(mailer1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    commit(&env, "c3", "o3").await;
    mailer2.quiet(300).await;
    let mut mailer1 = listen(&env, "mailer", "v1", true).await;
    assert_eq!(mailer1.start, 2);
    mailer1.through(3).await;
    // Its acknowledgement reaches the kernel a moment later; v2 goes on after what v1
    // acknowledged, so the switch waits for it (else v2 would get event 3 again, rightly).
    let acknowledged = async || {
        let state = get(&ui, "/api/state").await;
        let (_, body) = state.split_once("\r\n\r\n").unwrap();
        let state: serde_json::Value = serde_json::from_str(body).unwrap();
        state["listeners"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["name"] == "mailer" && l["deployment"] == "v1" && l["position"] == 3)
    };
    let mut done = false;
    for _ in 0..100 {
        if acknowledged().await {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(done, "v1's acknowledgement of 3 never reached the kernel");

    // The warm-up counts a waiting reactor as ready: it takes over at the switch.
    let mut m2 = m1.clone();
    m2.deployment = "v2".into();
    m2.listeners = vec!["mailer".into(), "notifier".into()];
    apply(&env.addr, m2).await.unwrap();
    // v1's reactor is ended; v2's goes on after v1's position, the new one starts at the switch.
    let ended = loop {
        match mailer1.message().await {
            Ok(Some(Out::Batch(_))) => continue,
            other => break other,
        }
    };
    let status = ended.expect_err("v1's reactor is ended");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        status
            .message()
            .contains("deployment v1 no longer serves shop"),
        "{status:?}"
    );
    for r in [&mut mailer2, &mut notifier2] {
        match r.message().await.unwrap() {
            Some(Out::Ready(ready)) => assert_eq!(ready.after_position, 3),
            other => panic!("expected Ready, got {other:?}"),
        }
    }
    commit(&env, "c4", "o4").await;
    assert_eq!(mailer2.through(4).await, [4]);
    assert_eq!(notifier2.through(4).await, [4]);

    // v1's reactor connecting again waits in turn.
    let mut again = connect(&env, "mailer", "v1", true).await;
    match again.message().await.unwrap() {
        Some(Out::Waiting(w)) => assert_eq!(w.serving, "v2"),
        other => panic!("expected Waiting, got {other:?}"),
    }
    again.quiet(200).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_deployment_with_unknown_history_or_a_failing_read_model_does_not_take_over() {
    let metrics = free_addr();
    let env = start_custom("deployments-failed", |config, _| {
        config.metrics_listen = Some(metrics.clone());
        config.deployment_switch_lag = 0;
    })
    .await;
    let mut m1 = membership("shop", &["orders.Order"], &["carts"], &[]);
    m1.deployment = "v1".into();
    apply(&env.addr, m1.clone()).await.unwrap();
    start_worker(env.addr.clone(), worker("v1", 2).0).await;
    let mut carts1 = listen(&env, "carts", "v1", false).await;
    run(&env, "a1", "PlaceOrder", "a").await;
    run(&env, "a2", "AddItem", "a").await;
    carts1.through(2).await;

    // v2 reads ItemAdded only up to schema 1: caught up, it still may not take over.
    start_worker(env.addr.clone(), worker("v2", 1).0).await;
    let mut carts2 = listen(&env, "carts", "v2", false).await;
    carts2.through(2).await;
    let mut m2 = m1.clone();
    m2.deployment = "v2".into();
    let refused = apply(&env.addr, m2.clone()).await.unwrap_err();
    let why = refused.message();
    assert!(why.starts_with("not_caught_up: deployment v2"), "{why}");
    assert!(
        why.contains("unknown_history: the history holds orders.ItemAdded at schema 2, and deployment v2's workers read it only up to schema 1"),
        "{why}"
    );
    let ready = get(&metrics, "/ready?deployment=v2").await;
    assert!(
        ready.starts_with("HTTP/1.1 503") && ready.contains("unknown_history"),
        "{ready}"
    );
    let written = events(&run(&env, "a3", "AddItem", "a").await);
    assert_eq!(written[0].deployment, "v1", "v1 serves on");

    // v3's read model fails at the first event: its connection ends three times there.
    start_worker(env.addr.clone(), worker("v3", 2).0).await;
    for _ in 0..3 {
        let mut carts3 = listen(&env, "carts", "v3", false).await;
        assert_eq!(carts3.start, 0);
        carts3.next().await;
        drop(carts3);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut m3 = m1.clone();
    m3.deployment = "v3".into();
    let mut carts3 = listen(&env, "carts", "v3", false).await;
    let refused = apply(&env.addr, m3.clone()).await.unwrap_err();
    assert!(
        refused.message().contains(
            "carts@v3 fails at the event after position 0 (orders.OrderPlaced at schema 1): its connection ended 3 times there"
        ),
        "{}",
        refused.message()
    );
    // Once it gets past the event (it is fixed, say), it catches up and takes over.
    carts3.through(3).await;
    apply_eventually(&env, &m3).await;
    let written = events(&run(&env, "a4", "AddItem", "a").await);
    assert_eq!(written[0].deployment, "v3");
    drop((carts1, carts2));
}

#[test]
fn a_deployment_is_dropped_by_hand_without_touching_the_events() {
    let dir = Dir::new("deployments-drop");
    let store = kernable_store::open(&store_url(&dir.0)).unwrap();
    let registered = |name: &str| kernable_core::proto::ListenerRecord {
        name: name.into(),
        key_id: id(&key(5)),
        ..Default::default()
    };
    store
        .save_listeners(&[
            registered("carts@v1"),
            registered("carts@v2"),
            registered("mailer"),
        ])
        .unwrap();
    let snapshots = kernable_store::snapshots::open("memory:").unwrap();
    let record = |deployment: &str| kernable_core::proto::SnapshotRecord {
        aggregate_id: "o1".into(),
        deployment: deployment.into(),
        snapshot: Some(Default::default()),
        ..Default::default()
    };
    snapshots.save(&record("v1")).unwrap();
    snapshots.save(&record("v2")).unwrap();
    let (names, dropped) =
        kernable_kernel::deployments::drop(store.as_ref(), snapshots.as_ref(), "v1").unwrap();
    assert_eq!((names, dropped), (vec!["carts@v1".to_owned()], 1));
    let mut left: Vec<String> = store
        .listeners()
        .unwrap()
        .into_iter()
        .map(|l| l.name)
        .collect();
    left.sort();
    assert_eq!(left, ["carts@v2", "mailer"]);
    assert_eq!(snapshots.infos().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_deployments_are_refused() {
    let env = start("deployments-invalid").await;
    let (mut w, _) = worker("bad version", 1);
    w.deployment = "bad version".into();
    let status = run_worker(env.addr.clone(), w).await;
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let mut m = membership("shop", &[], &[], &[]);
    m.deployment = "a@b".into();
    let refused = apply(&env.addr, m).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}

/// One request to the kernel's local socket: the whole response.
async fn local(socket: &std::path::Path, method: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::UnixStream::connect(socket).await.unwrap();
    conn.write_all(format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    response
}

/// `kernable <args>`: the exit code and what it printed.
async fn cli(args: &[&str]) -> (i32, String) {
    let args: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
            .args(&args)
            .output()
            .unwrap();
        (
            out.status.code().unwrap(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_local_socket_serves_readiness_and_drops_an_old_deployment_while_the_kernel_runs() {
    use std::os::unix::fs::PermissionsExt;
    let env = start_custom("deployments-socket", |config, dir| {
        config.admin_socket = Some(dir.join("ops/admin.sock").display().to_string());
        config.deployment_switch_lag = 0;
    })
    .await;
    let socket = env.dir.join("ops/admin.sock");
    let mode = |p: &std::path::Path| {
        std::fs::metadata(p)
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or(0)
    };
    // Bound, then narrowed to its user (inside a directory only its user may enter).
    for _ in 0..100 {
        if mode(&socket) == 0o600 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mode(&socket), 0o600, "the kernel's user only");
    assert_eq!(mode(&env.dir.join("ops")), 0o700);
    let socket_arg = socket.display().to_string();

    let mut m1 = membership("shop", &["orders.Order"], &["carts"], &[]);
    m1.deployment = "v1".into();
    apply(&env.addr, m1.clone()).await.unwrap();
    let worker1 = start_worker(env.addr.clone(), worker("v1", 1).0).await;
    let mut carts1 = listen(&env, "carts", "v1", false).await;
    run(&env, "a1", "PlaceOrder", "a").await;
    carts1.through(1).await;

    // v2 warms up beside v1; the socket says how far it is, as the metrics address does.
    let worker2 = start_worker(env.addr.clone(), worker("v2", 1).0).await;
    let mut carts2 = listen(&env, "carts", "v2", false).await;
    let ready = local(&socket, "GET", "/ready?deployment=v2").await;
    assert!(ready.starts_with("HTTP/1.1 503"), "{ready}");
    assert!(ready.contains("carts@v2 at 0 of 1"), "{ready}");
    carts2.through(1).await;
    // The listener handled the event; its acknowledgement reaches the kernel a moment later.
    let mut ready = String::new();
    for _ in 0..100 {
        ready = local(&socket, "GET", "/ready?deployment=v2").await;
        if ready.starts_with("HTTP/1.1 200") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready.starts_with("HTTP/1.1 200"), "{ready}");
    let warm = local(&socket, "GET", "/api/deployments/warm-up?deployment=v2").await;
    assert!(warm.contains("\"caughtUp\":true"), "{warm}");
    assert!(
        local(&socket, "GET", "/health")
            .await
            .starts_with("HTTP/1.1 200")
    );
    let mut m2 = m1.clone();
    m2.deployment = "v2".into();
    apply_eventually(&env, &m2).await;
    let listed = local(&socket, "GET", "/api/deployments").await;
    assert!(listed.contains("\"deployment\":\"v2\""), "{listed}");

    // Never the serving one, never one still running.
    let refused = local(
        &socket,
        "POST",
        "/api/deployments/drop?deployment=v2&force=true",
    )
    .await;
    assert!(refused.starts_with("HTTP/1.1 409"), "{refused}");
    assert!(refused.contains("\"error\":\"serving\""), "{refused}");
    let refused = local(
        &socket,
        "POST",
        "/api/deployments/drop?deployment=v1&force=true",
    )
    .await;
    assert!(refused.contains("\"error\":\"running\""), "{refused}");
    assert!(
        refused.contains("workers of orders.Order are connected"),
        "{refused}"
    );
    worker1.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let refused = local(
        &socket,
        "POST",
        "/api/deployments/drop?deployment=v1&force=true",
    )
    .await;
    assert!(refused.contains("\"error\":\"running\""), "{refused}");
    assert!(refused.contains("listener carts@v1"), "{refused}");
    drop(carts1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Stopped, but it served: only forced, as the kernel cannot know an operator's grace.
    let (code, printed) = cli(&["deployments", "drop", "--socket", &socket_arg, "v1"]).await;
    assert_eq!(code, 3, "{printed}");
    assert!(printed.contains("served_before"), "{printed}");
    let (code, printed) = cli(&[
        "deployments",
        "drop",
        "--socket",
        &socket_arg,
        "v1",
        "--force",
        "--json",
    ])
    .await;
    assert_eq!(code, 0, "{printed}");
    let dropped: serde_json::Value = serde_json::from_str(&printed).unwrap();
    assert_eq!(dropped["deployment"], "v1");
    assert_eq!(dropped["readModels"], serde_json::json!(["carts@v1"]));
    assert_eq!(dropped["forced"], true);
    assert!(dropped["snapshots"].is_u64());
    let names: Vec<String> = store(&env)
        .listeners()
        .unwrap()
        .into_iter()
        .map(|l| l.name)
        .collect();
    assert!(!names.contains(&"carts@v1".to_owned()), "{names:?}");
    // The events stay; v2 serves on.
    assert_eq!(
        events(&run(&env, "a2", "AddItem", "a").await)[0].deployment,
        "v2"
    );
    carts2.through(2).await;

    // The list as JSON, while the kernel runs.
    let (code, printed) = cli(&["deployments", &env.store, "--json"]).await;
    assert_eq!(code, 0, "{printed}");
    let list: serde_json::Value = serde_json::from_str(&printed).unwrap();
    let names: Vec<&str> = list["deployments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["deployment"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["v1", "v2"]);
    assert_eq!(list["services"][0]["deployment"], "v2");

    // Only these, and the drop only by POST.
    let wrong = local(&socket, "GET", "/api/deployments/drop?deployment=v1").await;
    assert!(wrong.starts_with("HTTP/1.1 405"), "{wrong}");
    let wrong = local(&socket, "GET", "/api/events").await;
    assert!(wrong.starts_with("HTTP/1.1 404"), "{wrong}");
    let wrong = local(&socket, "POST", "/api/deployments/drop?deployment=a@b").await;
    assert!(wrong.starts_with("HTTP/1.1 400"), "{wrong}");
    drop((worker2, carts2));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_kernel_s_deployments_are_dropped_with_the_same_refusals() {
    let dir = Dir::new("deployments-drop-stopped");
    let url = store_url(&dir.0);
    {
        let store = kernable_store::open(&url).unwrap();
        // v0 served once, v1 serves now, pr-1 never wrote anything.
        for (i, deployment) in ["v0", "v1"].iter().enumerate() {
            let cmd = command(&format!("c{i}"), "PlaceOrder", &format!("o{i}"));
            store.enqueue(&cmd, 1_000).unwrap();
            let event = kernable_core::proto::NewEvent {
                event_type: "OrderPlaced".into(),
                schema: 1,
                deployment: (*deployment).into(),
                ..Default::default()
            };
            store
                .commit(
                    &cmd.command_id,
                    0,
                    &[event],
                    &Default::default(),
                    None,
                    1_000,
                )
                .unwrap();
        }
        let mut m = membership("shop", &["orders.Order"], &[], &[]);
        m.deployment = "v1".into();
        store
            .save_membership(&kernable_core::proto::MembershipRecord {
                membership: Some(m),
                ..Default::default()
            })
            .unwrap();
    }
    let snapshots = format!("sqlite:{}", dir.0.join("snapshots.db").display());
    let drop = |deployment: &str, force: bool| {
        let mut args = vec![
            "deployments".to_owned(),
            "drop".into(),
            url.clone(),
            deployment.into(),
            "--snapshots".into(),
            snapshots.clone(),
        ];
        if force {
            args.push("--force".into());
        }
        async move {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            cli(&args).await
        }
    };
    let (code, printed) = drop("v1", true).await;
    assert_eq!(code, 3, "{printed}");
    assert!(
        printed.contains("serving: deployment v1 serves shop"),
        "{printed}"
    );
    let (code, printed) = drop("v0", false).await;
    assert_eq!(code, 3, "{printed}");
    assert!(printed.contains("served_before"), "{printed}");
    let (code, printed) = drop("v0", true).await;
    assert_eq!(code, 0, "{printed}");
    let (code, printed) = drop("pr-1", false).await;
    assert_eq!(code, 0, "a deployment that never wrote an event: {printed}");
    assert!(printed.contains("dropped deployment pr-1"), "{printed}");
}
