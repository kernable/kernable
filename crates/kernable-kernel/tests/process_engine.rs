//! The process engine (design/processes.md, stage 2): the design's example run end to end, with
//! a shop and a payment service written with the Rust SDK against an in-process kernel.

mod common;

use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable::{Kernable, NewEvent, Options, Rejected as SdkRejected, Submit};
use kernable_core::{
    proto::{
        Event, Grant, ProcessRegistered, ProcessRegistration, ProcessResolution,
        process_service_client::ProcessServiceClient,
    },
    signing, transport,
};
use serde_json::{Value, json};
use tonic::Code;

const PROCESS: &str = "shop.fulfilment";

/// The example of `proto/test-vectors/process-model.json`, with its timeout at `timeout_ms`.
fn example(timeout_ms: u64) -> Value {
    let file: Value = serde_json::from_str(include_str!(
        "../../../proto/test-vectors/process-model.json"
    ))
    .unwrap();
    let mut model = file["model"].clone();
    model["steps"][4]["timeoutMs"] = json!(timeout_ms);
    model
}

/// The key of the shop's app: may do everything.
fn app_key() -> SigningKey {
    key(30)
}

fn app_grant() -> Grant {
    grant(&app_key(), &[("*", &["*"])])
}

/// The shop and the payments, as the example needs them; what they do is set from the test.
struct Shop {
    shop: kernable::Domain,
    order: kernable::Aggregate<u32>,
    payment: kernable::Aggregate<u32>,
    approvals: kernable::Listener,
    /// The code `Ship` rejects with, if any.
    ship_rejects: Arc<Mutex<Option<String>>>,
    /// What `NeedsApproval` answers.
    needs_approval: Arc<AtomicBool>,
    /// Every command the order aggregate ran: type, order, data.
    ran: Arc<Mutex<Vec<(String, String, Value)>>>,
}

fn shop() -> Shop {
    let shop = kernable::domain("shop");
    let payments = kernable::domain("payments");
    let order = shop.aggregate("order", 0u32);
    let ship_rejects: Arc<Mutex<Option<String>>> = Arc::default();
    let ran: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
    order.apply("OrderSubmitted", |s: &mut u32, _: Value, _, _| *s += 1);
    for (command, event) in [
        ("Submit", "OrderSubmitted"),
        ("Confirm", "OrderConfirmed"),
        ("Approve", "OrderApproved"),
        ("Ship", "OrderShipped"),
        ("NotifyDelay", "DelayNotified"),
        ("Cancel", "OrderCancelled"),
    ] {
        let ran = Arc::clone(&ran);
        let rejects = Arc::clone(&ship_rejects);
        order.command(command, move |_, data: Value, cmd, _| {
            if command == "Ship"
                && let Some(code) = rejects.lock().unwrap().clone()
            {
                return Err(SdkRejected::new("cannot ship now").code(code));
            }
            ran.lock()
                .unwrap()
                .push((command.to_owned(), cmd.aggregate_id.clone(), data.clone()));
            Ok(vec![NewEvent::new(event, data)])
        });
    }
    let payment = payments.aggregate("payment", 0u32);
    payment.command("Pay", |_, data: Value, _, _| {
        Ok(vec![NewEvent::new(
            "PaymentReceived",
            json!({"orderId": data["orderId"], "at": 42}),
        )])
    });
    let needs_approval = Arc::new(AtomicBool::new(false));
    let answer = Arc::clone(&needs_approval);
    let approvals = shop
        .read_model("approvals")
        .on(
            "OrderSubmitted",
            |_: Value, _: kernable::StoredEventMeta, _| async { Ok(()) },
        )
        .query("NeedsApproval", move |_: Value, _, _| {
            let answer = answer.load(Ordering::SeqCst);
            async move { Ok(answer) }
        });
    Shop {
        shop,
        order,
        payment,
        approvals,
        ship_rejects,
        needs_approval,
        ran,
    }
}

impl Shop {
    /// Connects the shop's worker, the payments' worker and the read model to the kernel at
    /// `addr`; returns a client.
    async fn connect(&self, dir: &Path, addr: &str) -> Kernable {
        let key_path = dir.join("app.key");
        std::fs::write(&key_path, hex(app_key().as_bytes())).unwrap();
        std::fs::write(
            dir.join("app.grant"),
            hex(&prost::Message::encode_to_vec(&app_grant())),
        )
        .unwrap();
        let k = Kernable::new(Options::new(&key_path).url(addr).tenant("t1")).unwrap();
        for aggregate_worker in [k.worker(&self.order), k.worker(&self.payment)] {
            let running = aggregate_worker.clone();
            tokio::spawn(async move { running.run().await });
            aggregate_worker.ready().await;
        }
        let listener = k.listener(&self.approvals);
        let running = listener.clone();
        tokio::spawn(async move { running.run().await });
        listener.ready().await;
        let _ = &self.shop;
        k
    }

    fn ran(&self, command: &str) -> Vec<(String, Value)> {
        self.ran
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, _, _)| c == command)
            .map(|(_, o, d)| (o.clone(), d.clone()))
            .collect()
    }
}

/// A kernel with the shop running, the example registered (its timeout `timeout_ms`).
async fn shop_kernel(name: &str, timeout_ms: u64) -> (Env, Shop, Kernable) {
    let env = start(name).await;
    env.kernel.grant(app_grant()).unwrap();
    let shop = shop();
    let k = shop.connect(&env.dir, &env.addr).await;
    let registered = register(&env.addr, &example(timeout_ms)).await.unwrap();
    assert_eq!(registered.version, 1);
    (env, shop, k)
}

async fn register(addr: &str, model: &Value) -> Result<ProcessRegistered, tonic::Status> {
    let model = serde_json::to_vec(model).unwrap();
    let issued_at = issued_now();
    let request = ProcessRegistration {
        sig: Some(signing::sign(
            &app_key(),
            &signing::process_message(&model, issued_at),
        )),
        model,
        issued_at,
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(addr).await.unwrap())
        .register(presenting(request, &app_grant()))
        .await
        .map(tonic::Response::into_inner)
}

async fn resolve(addr: &str, case: &str, action: &str) -> Result<String, tonic::Status> {
    let issued_at = issued_now();
    let request = ProcessResolution {
        process: PROCESS.into(),
        case_id: case.into(),
        tenant_id: String::new(),
        action: action.into(),
        issued_at,
        sig: Some(signing::sign(
            &app_key(),
            &signing::process_resolve_message(PROCESS, case, "", action, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(addr).await.unwrap())
        .resolve(presenting(request, &app_grant()))
        .await
        .map(|r| r.into_inner().tenant_id)
}

async fn submit_order(k: &Kernable, order: &str, total: u64) {
    k.submit(Submit::new("shop", "order", order, "Submit").data(json!({"total": total})))
        .await
        .unwrap();
}

async fn pay(k: &Kernable, order: &str) {
    k.submit(
        Submit::new("payments", "payment", &format!("p-{order}"), "Pay")
            .data(json!({"orderId": order})),
    )
    .await
    .unwrap();
}

async fn approve(k: &Kernable, order: &str) {
    k.submit(Submit::new("shop", "order", order, "Approve"))
        .await
        .unwrap();
}

/// The events of case `case` of the example, in tenant t1.
fn case_events(store: &str, case: &str) -> Vec<Event> {
    kernable_store::open_read_only(store)
        .unwrap()
        .read_stream("t1", "process", PROCESS, case)
        .unwrap()
}

fn payload(e: &Event) -> Value {
    serde_json::from_slice(&e.payload).unwrap()
}

/// Waits (10 s at most) until the case holds an event `event_type` that `matches`.
async fn until_case(store: &str, case: &str, what: &str, matches: impl Fn(&Event) -> bool) {
    for _ in 0..500 {
        if case_events(store, case).iter().any(&matches) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "case {case} never got {what}: {:?}",
        timeline(&case_events(store, case))
    );
}

async fn until_reached(store: &str, case: &str, step: &str) {
    until_case(store, case, &format!("to step {step}"), |e| {
        e.event_type == "StepReached" && payload(e)["step"] == step
    })
    .await;
}

async fn until_ended(store: &str, case: &str) -> String {
    until_case(store, case, "an end", |e| e.event_type == "ProcessEnded").await;
    let events = case_events(store, case);
    payload(events.last().unwrap())["end"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The steps a case reached, in order.
fn steps(store: &str, case: &str) -> Vec<String> {
    case_events(store, case)
        .iter()
        .filter(|e| e.event_type == "StepReached")
        .map(|e| payload(e)["step"].as_str().unwrap().to_owned())
        .collect()
}

fn timeline(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| format!("{} {}", e.event_type, String::from_utf8_lossy(&e.payload)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_example_runs_from_the_order_to_the_shipment() {
    let metrics = free_addr();
    let env = start_custom("pe-happy", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    env.kernel.grant(app_grant()).unwrap();
    let shop = shop();
    let k = shop.connect(&env.dir, &env.addr).await;
    register(&env.addr, &example(60_000)).await.unwrap();

    submit_order(&k, "o1", 500).await;
    until_reached(&env.store, "o1#1", "paid").await;
    assert_eq!(shop.ran("Confirm"), [("o1".into(), json!({}))]);
    let started = payload(&case_events(&env.store, "o1#1")[0]);
    assert_eq!(started["version"], 1);
    assert_eq!(started["key"], "o1");
    assert_eq!(started["data"], json!({"orderId": "o1", "total": 500}));
    let page = get(&metrics, "/metrics").await;
    assert!(
        page.contains("kernable_process_cases{process=\"shop.fulfilment\",state=\"waiting\"} 1"),
        "{page}"
    );

    pay(&k, "o1").await;
    assert_eq!(until_ended(&env.store, "o1#1").await, "done");
    assert_eq!(
        steps(&env.store, "o1#1"),
        ["confirm", "large", "paid", "ship", "done"]
    );
    assert_eq!(shop.ran("Ship").len(), 1);
    // The payment's data went into the case; its timeout was withdrawn.
    let events = case_events(&env.store, "o1#1");
    let paid = events
        .iter()
        .find(|e| e.event_type == "StepCompleted" && payload(e)["step"] == "paid")
        .unwrap();
    assert_eq!(payload(paid)["data"], json!({"paidAt": 42}));
    assert_eq!(payload(paid)["outcome"], "event");
    let timer = events.iter().find(|e| e.event_type == "TimerSet").unwrap();
    let timer_id = payload(timer)["commandId"].as_str().unwrap().to_owned();
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let withdrawn = store.result(&timer_id).unwrap().unwrap();
    assert!(
        matches!(&withdrawn.outcome, Some(kernable_core::proto::command_result::Outcome::Rejected(r)) if r.code == "cancelled"),
        "{withdrawn:?}"
    );
    // The commands it sent carry the process as their sender.
    let sent = events
        .iter()
        .find(|e| e.event_type == "CommandSent")
        .unwrap();
    let confirm = store
        .command(payload(sent)["commandId"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let context = confirm.command.context.unwrap();
    assert_eq!(context.sender_identity, "process:shop.fulfilment");
    assert_eq!(context.correlation_id, "shop.fulfilment/o1#1");
    // Case events are the kernel's own, and the store verifies.
    let report = store.verify(&[id(&env.kernel_key)]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    assert!(
        events
            .iter()
            .all(|e| e.producer_sig.as_ref().unwrap().key_id == id(&env.kernel_key))
    );
    let page = get(&metrics, "/metrics").await;
    assert!(
        page.contains("kernable_process_cases{process=\"shop.fulfilment\",state=\"ended\"} 1"),
        "{page}"
    );
    assert!(
        page.contains(
            "kernable_process_step_seconds_count{process=\"shop.fulfilment\",step=\"paid\"} 1"
        ),
        "{page}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_order_asks_whether_it_needs_approval() {
    let (env, shop, k) = shop_kernel("pe-approval", 60_000).await;
    shop.needs_approval.store(true, Ordering::SeqCst);
    submit_order(&k, "big", 1500).await;
    until_reached(&env.store, "big#1", "approved").await;
    approve(&k, "big").await;
    until_reached(&env.store, "big#1", "paid").await;
    pay(&k, "big").await;
    assert_eq!(until_ended(&env.store, "big#1").await, "done");
    assert_eq!(
        steps(&env.store, "big#1"),
        [
            "confirm", "large", "approval", "approved", "paid", "ship", "done"
        ]
    );

    // Answered `false`, it goes on to the payment.
    shop.needs_approval.store(false, Ordering::SeqCst);
    submit_order(&k, "big2", 2000).await;
    until_reached(&env.store, "big2#1", "paid").await;
    assert_eq!(
        steps(&env.store, "big2#1"),
        ["confirm", "large", "approval", "paid"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_payment_before_the_case_waits_for_it_counts_when_it_does() {
    let (env, shop, k) = shop_kernel("pe-early", 60_000).await;
    shop.needs_approval.store(true, Ordering::SeqCst);
    submit_order(&k, "o7", 5000).await;
    until_reached(&env.store, "o7#1", "approved").await;
    // Paid while the order still waits for its approval: kept for the case.
    pay(&k, "o7").await;
    until_case(&env.store, "o7#1", "the payment kept", |e| {
        e.event_type == "EventKept"
    })
    .await;
    approve(&k, "o7").await;
    assert_eq!(until_ended(&env.store, "o7#1").await, "done");
    assert_eq!(
        steps(&env.store, "o7#1"),
        [
            "confirm", "large", "approval", "approved", "paid", "ship", "done"
        ]
    );
    assert!(
        case_events(&env.store, "o7#1")
            .iter()
            .all(|e| e.event_type != "TimerSet"),
        "no timeout set: the payment was there"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unpaid_order_is_cancelled_when_its_time_is_up() {
    let (env, shop, k) = shop_kernel("pe-timeout", 400).await;
    submit_order(&k, "o2", 100).await;
    assert_eq!(until_ended(&env.store, "o2#1").await, "done");
    assert_eq!(
        steps(&env.store, "o2#1"),
        ["confirm", "large", "paid", "cancel", "done"]
    );
    assert_eq!(
        shop.ran("Cancel"),
        [("o2".into(), json!({"reason": "unpaid"}))]
    );
    let events = case_events(&env.store, "o2#1");
    assert!(events.iter().any(|e| e.event_type == "TimerFired"));
    // A payment after the case ended moves nothing.
    pay(&k, "o2").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(case_events(&env.store, "o2#1").len(), events.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejection_with_a_path_takes_it_and_another_is_an_incident() {
    let (env, shop, k) = shop_kernel("pe-rejections", 60_000).await;
    *shop.ship_rejects.lock().unwrap() = Some("out_of_stock".into());
    submit_order(&k, "o3", 100).await;
    until_reached(&env.store, "o3#1", "paid").await;
    pay(&k, "o3").await;
    assert_eq!(until_ended(&env.store, "o3#1").await, "done");
    assert_eq!(
        steps(&env.store, "o3#1"),
        ["confirm", "large", "paid", "ship", "notify", "done"]
    );
    assert_eq!(shop.ran("NotifyDelay").len(), 1);

    // Any other code stops the case: an incident, until an operator resolves it.
    *shop.ship_rejects.lock().unwrap() = Some("warehouse_closed".into());
    for order in ["r1", "r2", "r3"] {
        submit_order(&k, order, 100).await;
        until_reached(&env.store, &format!("{order}#1"), "paid").await;
        pay(&k, order).await;
        until_case(&env.store, &format!("{order}#1"), "an incident", |e| {
            e.event_type == "IncidentRaised" && payload(e)["code"] == "warehouse_closed"
        })
        .await;
    }
    let refused = resolve(&env.addr, "o3#1", "retry").await.unwrap_err();
    assert_eq!(refused.code(), Code::NotFound, "{refused:?}");

    // Retry: the step runs again (a new command), and succeeds now.
    *shop.ship_rejects.lock().unwrap() = None;
    assert_eq!(resolve(&env.addr, "r1#1", "retry").await.unwrap(), "t1");
    assert_eq!(until_ended(&env.store, "r1#1").await, "done");
    let sent: Vec<String> = case_events(&env.store, "r1#1")
        .iter()
        .filter(|e| e.event_type == "CommandSent" && payload(e)["step"] == "ship")
        .map(|e| payload(e)["commandId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0], sent[1], "a new attempt, a new command ID");

    // Skip: on to the step's next, without shipping.
    assert_eq!(resolve(&env.addr, "r2#1", "skip").await.unwrap(), "t1");
    assert_eq!(until_ended(&env.store, "r2#1").await, "done");
    assert_eq!(
        steps(&env.store, "r2#1"),
        ["confirm", "large", "paid", "ship", "done"]
    );

    // Cancel: ended `cancelled`; with the command line.
    std::fs::write(env.dir.join("ops.key"), hex(app_key().as_bytes())).unwrap();
    std::fs::write(
        env.dir.join("ops.grant"),
        hex(&prost::Message::encode_to_vec(&app_grant())),
    )
    .unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
        .current_dir(&env.dir)
        .args(["processes", "cancel", &env.addr, "ops.key", PROCESS, "r3#1"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "cancelled case r3#1 of shop.fulfilment (tenant t1)"
    );
    assert_eq!(until_ended(&env.store, "r3#1").await, "cancelled");
    let resolved = case_events(&env.store, "r3#1")
        .iter()
        .find(|e| e.event_type == "IncidentResolved")
        .map(payload)
        .unwrap();
    assert_eq!(resolved["action"], "cancel");
    assert_eq!(resolved["by"], id(&app_key()));

    // The command line lists the cases and shows one.
    let cases = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
        .args([
            "processes",
            "cases",
            &env.store,
            PROCESS,
            "--state",
            "ended",
        ])
        .output()
        .unwrap();
    let listed = String::from_utf8_lossy(&cases.stdout);
    assert!(
        listed
            .lines()
            .any(|l| l.starts_with("t1 r3#1 ended cancelled")),
        "{listed}"
    );
    assert!(
        listed.lines().any(|l| l.starts_with("t1 o3#1 ended done")),
        "{listed}"
    );
    let case = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
        .args(["processes", "case", &env.store, PROCESS, "r3#1"])
        .output()
        .unwrap();
    let shown = String::from_utf8_lossy(&case.stdout);
    assert!(shown.starts_with("tenant t1\n"), "{shown}");
    assert!(shown.contains(" v1 ProcessStarted {"), "{shown}");
    assert!(shown.contains("IncidentRaised"), "{shown}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_case_runs_per_key_and_the_next_starts_after_it_ended() {
    let (env, _shop, k) = shop_kernel("pe-keys", 400).await;
    submit_order(&k, "o4", 100).await;
    until_reached(&env.store, "o4#1", "paid").await;
    // Submitted again while its case runs: ignored.
    submit_order(&k, "o4", 100).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(case_events(&env.store, "o4#2").is_empty());
    assert_eq!(until_ended(&env.store, "o4#1").await, "done");
    // Once it ended, the key's next case starts.
    submit_order(&k, "o4", 100).await;
    until_reached(&env.store, "o4#2", "paid").await;
    assert_eq!(payload(&case_events(&env.store, "o4#2")[0])["key"], "o4");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_removed_process_starts_no_case_and_a_new_version_leaves_running_ones_on_theirs() {
    let (env, _shop, k) = shop_kernel("pe-versions", 60_000).await;
    submit_order(&k, "v1", 100).await;
    until_reached(&env.store, "v1#1", "paid").await;

    // Version 2 ships without waiting for the payment; the running case stays on version 1.
    let mut v2 = example(60_000);
    v2["steps"][1]["otherwise"] = json!("ship");
    assert_eq!(register(&env.addr, &v2).await.unwrap().version, 2);
    submit_order(&k, "v2", 100).await;
    assert_eq!(until_ended(&env.store, "v2#1").await, "done");
    assert_eq!(payload(&case_events(&env.store, "v2#1")[0])["version"], 2);
    assert_eq!(
        steps(&env.store, "v2#1"),
        ["confirm", "large", "ship", "done"]
    );
    pay(&k, "v1").await;
    assert_eq!(until_ended(&env.store, "v1#1").await, "done");
    assert_eq!(
        steps(&env.store, "v1#1"),
        ["confirm", "large", "paid", "ship", "done"]
    );

    // Removed: no new cases; a running one goes on (on version 3, the first model again).
    assert_eq!(
        register(&env.addr, &example(60_000)).await.unwrap().version,
        3
    );
    submit_order(&k, "v3", 100).await;
    until_reached(&env.store, "v3#1", "paid").await;
    let issued_at = issued_now();
    let removal = kernable_core::proto::ProcessRemoval {
        name: PROCESS.into(),
        issued_at,
        sig: Some(signing::sign(
            &app_key(),
            &signing::process_remove_message(PROCESS, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(&env.addr).await.unwrap())
        .remove(presenting(removal, &app_grant()))
        .await
        .unwrap();
    submit_order(&k, "v4", 100).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(case_events(&env.store, "v4#1").is_empty());
    pay(&k, "v3").await;
    assert_eq!(until_ended(&env.store, "v3#1").await, "done");
}

#[tokio::test(flavor = "multi_thread")]
async fn cases_go_on_after_a_restart() {
    let (env, shop, k) = shop_kernel("pe-restart", 1_500).await;
    submit_order(&k, "w1", 100).await;
    submit_order(&k, "w2", 100).await;
    until_reached(&env.store, "w1#1", "paid").await;
    until_reached(&env.store, "w2#1", "paid").await;

    // Another kernel on a copy of the store, as after a restart: the waits and timers held.
    let from = env.dir.clone();
    let back = start_custom("pe-restart-back", move |_, dir| {
        for file in ["k.db", "k.db-wal", "k.log"] {
            if from.join(file).exists() {
                std::fs::copy(from.join(file), dir.join(file)).unwrap();
            }
        }
    })
    .await;
    back.kernel.grant(app_grant()).unwrap();
    let k2 = shop.connect(&back.dir, &back.addr).await;
    drop(k);
    pay(&k2, "w1").await;
    assert_eq!(until_ended(&back.store, "w1#1").await, "done");
    // w2's timeout falls due on the kernel that runs now.
    assert_eq!(until_ended(&back.store, "w2#1").await, "done");
    assert_eq!(
        steps(&back.store, "w2#1"),
        ["confirm", "large", "paid", "cancel", "done"]
    );
}

/// A process of the test workers' `orders` domain: a placed order waits for an item, then the
/// process adds one more.
fn follow() -> Value {
    json!({
        "name": "orders.follow",
        "start": {"on": "orders.OrderPlaced", "key": "aggregateId", "data": {"order": "aggregateId"}},
        "steps": [
            {"name": "added", "kind": "wait", "event": "orders.ItemAdded", "key": "aggregateId", "next": "more"},
            {"name": "more", "kind": "command", "command": "orders.Order.AddItem", "aggregate": "case.order", "next": "done"},
            {"name": "done", "kind": "end"}
        ]
    })
}

fn of_tenant(
    id: &str,
    command_type: &str,
    order: &str,
    tenant: &str,
) -> kernable_core::proto::Command {
    let mut cmd = command(id, command_type, order);
    cmd.tenant_id = tenant.into();
    signed(cmd, &key(1))
}

fn follow_events(store: &str, tenant: &str, case: &str) -> Vec<Event> {
    kernable_store::open_read_only(store)
        .unwrap()
        .read_stream(tenant, "process", "orders.follow", case)
        .unwrap()
}

async fn until_follow_ended(store: &str, tenant: &str, case: &str) {
    for _ in 0..500 {
        if follow_events(store, tenant, case)
            .iter()
            .any(|e| e.event_type == "ProcessEnded")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "case {case} of {tenant} never ended: {:?}",
        timeline(&follow_events(store, tenant, case))
    );
}

fn node_key(dir: &Path) -> std::path::PathBuf {
    let node_key = dir.join("node.key");
    std::fs::write(&node_key, hex(key(13).as_bytes())).unwrap();
    let grant = grant(&key(13), &[("replicate", &["*"])]);
    std::fs::write(
        dir.join("node.grant"),
        hex(&prost::Message::encode_to_vec(&grant)),
    )
    .unwrap();
    node_key
}

#[tokio::test(flavor = "multi_thread")]
async fn each_partition_runs_the_cases_of_its_tenants() {
    use kernable_core::proto::{Partition, PartitionMap, TenantPartition};
    let p1 = start_custom("pe-p1", |config, dir| {
        config.partition = Some(kernable_kernel::config::PartitionConfig {
            name: "p1".into(),
            control: None,
            node_key: node_key(dir),
        });
    })
    .await;
    let p1_addr = p1.addr.clone();
    let p2 = start_custom("pe-p2", |config, dir| {
        config.partition = Some(kernable_kernel::config::PartitionConfig {
            name: "p2".into(),
            control: Some(p1_addr.clone()),
            node_key: node_key(dir),
        });
    })
    .await;
    for env in [&p1, &p2] {
        env.kernel
            .grant(grant(&key(13), &[("replicate", &["*"])]))
            .unwrap();
        env.kernel.grant(app_grant()).unwrap();
    }
    let mut map = PartitionMap {
        epoch: 1,
        partitions: vec![
            Partition {
                name: "p1".into(),
                addresses: vec![p1.addr.clone()],
            },
            Partition {
                name: "p2".into(),
                addresses: vec![p2.addr.clone()],
            },
        ],
        tenants: vec![TenantPartition {
            tenant: "acme".into(),
            partition: "p2".into(),
        }],
        default: "p1".into(),
        control: "p1".into(),
        issued_at: issued_now(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    map.sig = Some(signing::sign(&key(1), &signing::partitions_message(&map)));
    kernable_core::proto::partition_service_client::PartitionServiceClient::new(
        transport::connect(&p1.addr).await.unwrap(),
    )
    .apply(with_grant(map, &key(1)))
    .await
    .unwrap();
    for env in [&p1, &p2] {
        start_worker_registered(env.addr.clone()).await;
    }
    // Registered on the control partition; the other takes it from there.
    assert_eq!(register(&p1.addr, &follow()).await.unwrap().version, 1);
    let mut taken = None;
    for _ in 0..200 {
        match register(&p2.addr, &follow()).await {
            Ok(r) => {
                taken = Some(r);
                break;
            }
            Err(s) if s.code() == Code::FailedPrecondition => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(s) => panic!("{s:?}"),
        }
    }
    let taken = taken.expect("p2 took the process");
    assert_eq!((taken.version, taken.new), (1, false));
    let mut changed = follow();
    changed["steps"][1]["data"] = json!({"x": {"value": 1}});
    let refused = register(&p2.addr, &changed).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");

    // t1 lives on p1, acme on p2: each partition runs its own tenants' cases.
    for (env, tenant) in [(&p1, "t1"), (&p2, "acme")] {
        let placed = submit(
            &env.addr,
            of_tenant(&format!("{tenant}-c1"), "PlaceOrder", "o1", tenant),
        )
        .await
        .unwrap();
        assert_eq!(committed_versions(&placed), [1]);
        let added = submit(
            &env.addr,
            of_tenant(&format!("{tenant}-c2"), "AddItem", "o1", tenant),
        )
        .await
        .unwrap();
        assert_eq!(committed_versions(&added), [2]);
        until_follow_ended(&env.store, tenant, "o1#1").await;
    }
    assert!(follow_events(&p1.store, "acme", "o1#1").is_empty());
    assert!(follow_events(&p2.store, "t1", "o1#1").is_empty());
}
