//! Middleware: listeners that check commands and queries before the kernel accepts or routes
//! them, in chain order, failing closed; and after, seeing their outcome.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, CacheRequest, ChallengeResponse, DataRequest, Hello, ListenerMessage, ListenerRecord,
        Middleware, MiddlewareCall, QueryAnswer, QueryRequest, Rejected, RequestContext, Verdict,
        Wait, When, cache_service_client::CacheServiceClient, command_result::Outcome,
        data_service_client::DataServiceClient, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
        middleware_call, middleware_call::Request, query_answer, query_result,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

/// Names of the middlewares that checked a request, in the order they did; after a request
/// with what came of it (see [`seen`]).
type Log = Arc<Mutex<Vec<String>>>;

/// What a test middleware knows when it checks a request: the request and the number of
/// `orders.OrderPlaced` events it has processed.
type Decide = fn(&Request, usize) -> Option<Rejected>;

struct Spec {
    name: &'static str,
    key: SigningKey,
    order: i32,
    types: Vec<String>,
    decide: Decide,
    /// Its order as a listener (events), not in the chain.
    listener_order: i32,
    /// Acknowledges batches; a listener that does not holds back higher listener orders.
    acks: bool,
    /// Before or after requests; `None`: no middleware, only a read model.
    when: Option<When>,
    /// Answers `orders.CountOrders` with the number of orders it has seen.
    answers: bool,
    /// Takes this long to decide.
    deliberates: Duration,
}

fn spec(name: &'static str, order: i32, decide: Decide) -> Spec {
    Spec {
        name,
        key: key(5),
        order,
        types: vec!["*".into()],
        decide,
        listener_order: 0,
        acks: true,
        when: Some(When::Before),
        answers: false,
        deliberates: Duration::ZERO,
    }
}

/// A middleware after requests.
fn after(name: &'static str, order: i32, decide: Decide) -> Spec {
    Spec {
        when: Some(When::After),
        ..spec(name, order, decide)
    }
}

/// A read model answering `orders.CountOrders`, no middleware.
fn counter() -> Spec {
    Spec {
        when: None,
        answers: true,
        ..spec("counter", 0, pass)
    }
}

/// How a test middleware logs a call: its name, and after a request what came of it.
fn seen(name: &str, call: &MiddlewareCall) -> String {
    let outcome = match &call.outcome {
        None if call.vault.is_empty() => return name.into(),
        None => return format!("{name} with {} vault values", call.vault.len()),
        Some(middleware_call::Outcome::CommandResult(r)) => match &r.outcome {
            Some(Outcome::Committed(c)) => format!("committed {}", c.events.len()),
            Some(Outcome::Rejected(r)) => format!("rejected {}", r.code),
            _ => "?".into(),
        },
        Some(middleware_call::Outcome::QueryResult(r)) => match &r.outcome {
            Some(query_result::Outcome::Response(b)) => {
                format!("answered {}", String::from_utf8_lossy(b))
            }
            Some(query_result::Outcome::Rejected(r)) => format!("rejected {}", r.code),
            None => "?".into(),
        },
    };
    let id = match &call.request {
        Some(Request::Command(c)) => &c.command_id,
        Some(Request::Query(q)) => &q.query_id,
        Some(Request::Data(d)) => &d.key,
        Some(Request::Cache(c)) => &c.key,
        Some(Request::Cancel(c)) => &c.command_id,
        None => "",
    };
    format!("{name} after {id} {outcome}")
}

fn pass(_: &Request, _: usize) -> Option<Rejected> {
    None
}

fn refuse(code: &str) -> Option<Rejected> {
    Some(Rejected {
        code: code.into(),
        message: "refused by a test middleware".into(),
        ..Default::default()
    })
}

/// Runs a middleware that also counts `orders.OrderPlaced`, rebuilding from position 0, until
/// the kernel ends the stream (returns its status) or `stop` is dropped.
fn middleware(env: &Env, spec: Spec, log: &Log) -> (JoinHandle<tonic::Status>, mpsc::Sender<()>) {
    let addr = env.addr.clone();
    let log = Arc::clone(log);
    let (stop, mut stopped) = mpsc::channel::<()>(1);
    let task = tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name: spec.name.into(),
            key_id: id(&spec.key),
            order: spec.listener_order,
            event_types: vec!["orders.OrderPlaced".into()],
            from_position: Some(0),
            query_types: if spec.answers {
                vec!["orders.CountOrders".into()]
            } else {
                Vec::new()
            },
            middlewares: spec
                .when
                .map(|when| Middleware {
                    order: spec.order,
                    types: spec.types,
                    when: when.into(),
                })
                .into_iter()
                .collect(),
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = match ListenerServiceClient::new(channel)
            .listen(ReceiverStream::new(rx))
            .await
        {
            Ok(r) => r.into_inner(),
            Err(status) => return status,
        };
        let mut seen = 0;
        loop {
            let msg = tokio::select! {
                m = inbound.message() => m,
                _ = stopped.recv() => return tonic::Status::ok("stopped"),
            };
            let msg = match msg {
                Ok(Some(m)) => m.msg.unwrap(),
                Ok(None) => return tonic::Status::ok("closed"),
                Err(status) => return status,
            };
            let reply = match msg {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&spec.key, &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Ready(_) | Out::Waiting(_) => continue,
                Out::Batch(b) => {
                    seen += b.events.len();
                    if !spec.acks {
                        continue;
                    }
                    In::Ack(Ack {
                        position: b.up_to,
                        ..Default::default()
                    })
                }
                Out::Check(call) => {
                    tokio::time::sleep(spec.deliberates).await;
                    log.lock().unwrap().push(self::seen(spec.name, &call));
                    In::Verdict(Verdict {
                        call_id: call.call_id,
                        rejected: (spec.decide)(&call.request.unwrap(), seen),
                    })
                }
                Out::Query(call) => In::Answer(QueryAnswer {
                    call_id: call.call_id,
                    outcome: Some(query_answer::Outcome::Response(seen.to_string().into())),
                }),
            };
            if tx.send(send(reply)).await.is_err() {
                return tonic::Status::ok("closed");
            }
        }
    });
    (task, stop)
}

async fn settle_in() {
    tokio::time::sleep(Duration::from_millis(150)).await;
}

/// Waits until `log` holds `n` entries (a middleware after commands is told in the background).
async fn logged(log: &Log, n: usize) -> Vec<String> {
    for _ in 0..100 {
        if log.lock().unwrap().len() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    log.lock().unwrap().clone()
}

async fn ask(
    env: &Env,
    q: QueryRequest,
) -> Result<kernable_core::proto::QueryResult, tonic::Status> {
    let channel = transport::connect(&env.addr).await.unwrap();
    QueryServiceClient::new(channel)
        .query(q)
        .await
        .map(|r| r.into_inner())
}

fn rejected_code(result: &kernable_core::proto::CommandResult) -> Option<&str> {
    match &result.outcome {
        Some(Outcome::Rejected(r)) => Some(&r.code),
        _ => None,
    }
}

fn query(query_type: &str, key: &SigningKey) -> QueryRequest {
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut q = QueryRequest {
        target: None,
        listener: String::new(),
        wait_only: false,
        protocol: kernable_core::PROTOCOL,
        query_id: "q1".into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        query_type: query_type.into(),
        payload: b"{}".to_vec(),
        context: None,
        min_position: 0,
        issued_at,
        client_sig: None,
    };
    q.client_sig = Some(signing::sign(key, &signing::query_message(&q)));
    q
}

#[tokio::test(flavor = "multi_thread")]
async fn the_chain_runs_in_order_and_the_first_rejection_refuses() {
    let env = start("mw-chain").await;
    spawn_worker(&env).await;
    let log = Log::default();
    // Registered in reverse: the chain is ordered by `order`, then by name.
    fn no_o2(r: &Request, _: usize) -> Option<Rejected> {
        match r {
            Request::Command(c) if c.aggregate_id == "o2" => refuse("forbidden"),
            Request::Query(_) => refuse("no_queries"),
            _ => None,
        }
    }
    let _late = middleware(&env, spec("late", 5, pass), &log);
    let _deny = middleware(&env, spec("b-deny", 1, no_o2), &log);
    let _first = middleware(&env, spec("a-first", 1, pass), &log);
    settle_in().await;

    let ok = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&ok), [1]);
    assert_eq!(*log.lock().unwrap(), ["a-first", "b-deny", "late"]);

    log.lock().unwrap().clear();
    let refused = submit(
        &env.addr,
        signed(command("c2", "PlaceOrder", "o2"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(rejected_code(&refused), Some("forbidden"));
    assert_eq!(*log.lock().unwrap(), ["a-first", "b-deny"]);
    // A refused command was never accepted: nothing is stored for it.
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert!(store.result("c2").unwrap().is_none());

    // Queries go through the same chain, before any read model is asked.
    let channel = transport::connect(&env.addr).await.unwrap();
    let answered = QueryServiceClient::new(channel)
        .query(query("CountOrders", &key(1)))
        .await
        .unwrap()
        .into_inner();
    assert!(
        matches!(answered.outcome, Some(query_result::Outcome::Rejected(r)) if r.code == "no_queries")
    );
}

/// F8: `listener` in a submit or query is not signed, so anyone may set it. It only narrows
/// what the request itself waits for and where its query may go; every middleware still checks
/// the request, whichever listener it names.
#[tokio::test(flavor = "multi_thread")]
async fn naming_a_listener_never_skips_a_middleware() {
    let env = start("mw-unsigned-listener").await;
    spawn_worker(&env).await;
    let log = Log::default();
    fn deny(r: &Request, _: usize) -> Option<Rejected> {
        match r {
            Request::Command(c) if c.aggregate_id == "o2" => refuse("forbidden"),
            Request::Query(_) => refuse("no_queries"),
            _ => None,
        }
    }
    let _first = middleware(&env, spec("a-first", 1, pass), &log);
    let _deny = middleware(&env, spec("b-deny", 2, deny), &log);
    let _counter = middleware(&env, counter(), &log);
    let audit = Spec {
        when: None,
        ..spec("audit", 0, pass)
    };
    let _audit = middleware(&env, audit, &log);
    settle_in().await;

    for name in ["counter", "audit"] {
        log.lock().unwrap().clear();
        let refused = submit_from(
            &env.addr,
            signed(command("c2", "PlaceOrder", "o2"), &key(1)),
            Wait::Committed,
            name,
        )
        .await
        .unwrap();
        assert_eq!(rejected_code(&refused), Some("forbidden"), "as {name}");
        assert_eq!(*log.lock().unwrap(), ["a-first", "b-deny"], "as {name}");

        let mut q = query("CountOrders", &key(1));
        q.listener = name.into();
        let answered = ask(&env, q).await.unwrap();
        assert!(
            matches!(answered.outcome, Some(query_result::Outcome::Rejected(r)) if r.code == "no_queries"),
            "as {name}"
        );
    }
    // A listener that is not there is refused, not ignored.
    let unknown = submit_from(
        &env.addr,
        signed(command("c3", "PlaceOrder", "o3"), &key(1)),
        Wait::Committed,
        "nobody",
    )
    .await
    .unwrap_err();
    assert_eq!(unknown.code(), Code::InvalidArgument);
}

/// A listener that, once connected, sends passing verdicts for every call ID it can guess
/// (`<request ID>#<n>`) for `request_id`, until stopped.
fn verdict_spammer(env: &Env, request_id: &'static str) -> JoinHandle<()> {
    let addr = env.addr.clone();
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(256);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name: "mallory".into(),
            key_id: id(&key(5)),
            event_types: vec!["orders.OrderPlaced".into()],
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = ListenerServiceClient::new(channel)
            .listen(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(msg)) = inbound.message().await {
            match msg.msg.unwrap() {
                Out::Challenge(c) => {
                    let sig = signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig;
                    tx.send(send(In::ChallengeResponse(ChallengeResponse { sig })))
                        .await
                        .unwrap();
                }
                Out::Ready(_) => {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        loop {
                            for n in 0..64 {
                                let verdict = Verdict {
                                    call_id: format!("{request_id}#{n}"),
                                    rejected: None,
                                };
                                if tx.send(send(In::Verdict(verdict))).await.is_err() {
                                    return;
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    });
                }
                _ => {}
            }
        }
    })
}

/// X8: verdicts are matched to calls by an unsigned, guessable call ID. Only the middleware a
/// call went to decides it: another listener's verdicts for it count for nothing.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_asked_middleware_decides() {
    let env = start("mw-spoofed-verdicts").await;
    spawn_worker(&env).await;
    let log = Log::default();
    fn deny(r: &Request, _: usize) -> Option<Rejected> {
        match r {
            Request::Command(c) if c.aggregate_id == "o2" => refuse("forbidden"),
            _ => None,
        }
    }
    let slow_guard = Spec {
        deliberates: Duration::from_millis(300),
        ..spec("guard", 0, deny)
    };
    let _guard = middleware(&env, slow_guard, &log);
    let mallory = verdict_spammer(&env, "c2");
    settle_in().await;

    let refused = submit(
        &env.addr,
        signed(command("c2", "PlaceOrder", "o2"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(rejected_code(&refused), Some("forbidden"));
    mallory.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_cannot_send_what_it_checks_itself() {
    let env = start("mw-self").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let mut orders_only = spec("guard", 0, pass);
    orders_only.types = vec!["orders.PlaceOrder".into()];
    let _guard = middleware(&env, orders_only, &log);
    settle_in().await;

    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let status = submit_from(&env.addr, place.clone(), Wait::Committed, "guard")
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("checks orders.PlaceOrder itself"),
        "{status:?}"
    );
    assert!(log.lock().unwrap().is_empty(), "it was never asked");

    // What it does not check, it may send; others may send what it checks.
    let placed = submit(&env.addr, place).await.unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    let add = signed(command("c2", "AddItem", "o1"), &key(1));
    let added = submit_from(&env.addr, add, Wait::Committed, "guard").await;
    assert_eq!(committed_versions(&added.unwrap()), [2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_middleware_refuses_everything_it_covers() {
    let env = start("mw-closed").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let mut only_orders = spec("orders-only", 0, pass);
    only_orders.types = vec!["orders.PlaceOrder".into()];
    let (task, stop) = middleware(&env, only_orders, &log);
    settle_in().await;
    let ok = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&ok), [1]);

    drop(stop);
    task.await.unwrap();
    settle_in().await;
    let refused = submit(
        &env.addr,
        signed(command("c2", "PlaceOrder", "o2"), &key(1)),
    )
    .await;
    assert_eq!(refused.unwrap_err().code(), Code::Unavailable);
    // A bypass does not skip the chain: authorization is the middleware's business.
    let mut bypass = command("c3", "PlaceOrder", "o3");
    bypass.context = Some(RequestContext {
        bypass_authorization: true,
        ..Default::default()
    });
    let refused = submit(&env.addr, signed(bypass, &key(1))).await;
    assert_eq!(refused.unwrap_err().code(), Code::Unavailable);
    // Types it does not cover are not affected.
    let other = submit(&env.addr, signed(command("c4", "AddItem", "o1"), &key(1)))
        .await
        .unwrap();
    assert!(other.outcome.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_registered_middleware_survives_a_kernel_restart() {
    // A middleware registered before the kernel started (it was running before a restart).
    let env = start_custom("mw-restart", |config, _| {
        let store = kernable_store::open(&config.store).unwrap();
        let record = ListenerRecord {
            name: "rbac".into(),
            key_id: id(&key(5)),
            middlewares: vec![Middleware {
                order: 0,
                types: vec!["orders.*".into()],
                when: When::Before.into(),
            }],
            ..Default::default()
        };
        store.save_listeners(&[record]).unwrap();
    })
    .await;
    spawn_worker(&env).await;
    let refused = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await;
    assert_eq!(refused.unwrap_err().code(), Code::Unavailable);

    let log = Log::default();
    let (_rbac, _stop) = middleware(&env, spec("rbac", 0, pass), &log);
    settle_in().await;
    let ok = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&ok), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnected_middleware_checks_only_after_catching_up() {
    let env = start_custom("mw-catch-up", |config, _| {
        // Registered before, for items only: orders can be placed while it is away.
        let store = kernable_store::open(&config.store).unwrap();
        let record = ListenerRecord {
            name: "counter".into(),
            key_id: id(&key(5)),
            middlewares: vec![Middleware {
                order: 0,
                types: vec!["orders.AddItem".into()],
                when: When::Before.into(),
            }],
            ..Default::default()
        };
        store.save_listeners(&[record]).unwrap();
    })
    .await;
    spawn_worker(&env).await;
    let log = Log::default();
    // A listener of a lower order that never acknowledges: the middleware gets no events.
    let mut stuck = spec("stuck", 0, pass);
    (stuck.types, stuck.acks) = (vec!["orders.Never".into()], false);
    let _stuck = middleware(&env, stuck, &log);
    settle_in().await;
    for (c, o) in [("c1", "o1"), ("c2", "o2"), ("c3", "o3")] {
        let cmd = signed(command(c, "PlaceOrder", o), &key(1));
        let done = submit_with(&env.addr, cmd, Wait::Committed).await.unwrap();
        assert_eq!(committed_versions(&done), [1]);
    }

    // Rebuilds from 0 and refuses unless it has seen all three orders.
    fn three_seen(_: &Request, seen: usize) -> Option<Rejected> {
        if seen < 3 {
            refuse("not_caught_up")
        } else {
            None
        }
    }
    let mut counter = spec("counter", 0, three_seen);
    (counter.types, counter.listener_order) = (vec!["orders.AddItem".into()], 1);
    let (_m, _stop) = middleware(&env, counter, &log);
    settle_in().await;
    // It cannot catch up, so it is never asked: the request fails, it is not checked
    // against a state that misses events.
    let cmd = signed(command("c4", "AddItem", "o1"), &key(1));
    let status = submit(&env.addr, cmd).await.unwrap_err();
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert!(log.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn middleware_registration_is_allowed_per_key() {
    let env = start("mw-allowed").await;
    let log = Log::default();
    let mut stranger = spec("stranger", 0, pass);
    stranger.key = key(6); // may listen, but not check
    let (task, _stop) = middleware(&env, stranger, &log);
    assert_eq!(task.await.unwrap().code(), Code::PermissionDenied);

    // A negative order needs a grant of reserved_order, which key 5 lacks.
    let (task, _stop) = middleware(&env, spec("reserved", -10, pass), &log);
    assert_eq!(task.await.unwrap().code(), Code::PermissionDenied);

    let mut bad = spec("bad", 0, pass);
    bad.types = vec!["orders".into()];
    let (task, _stop) = middleware(&env, bad, &log);
    assert_eq!(task.await.unwrap().code(), Code::InvalidArgument);
}

/// Negative orders are kernable's kits', for keys granted `reserved_order`; such a
/// middleware runs before every app middleware, and loses its place with a grant without it.
#[tokio::test(flavor = "multi_thread")]
async fn negative_orders_need_a_grant_of_reserved_order() {
    let env = start("mw-reserved").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let mut listener = spec("early-listener", 0, pass);
    listener.listener_order = -20;
    listener.when = None;
    let (task, _stop) = middleware(&env, listener, &log);
    assert_eq!(task.await.unwrap().code(), Code::PermissionDenied);

    let kit_grant = |reserved: bool| {
        let mut permissions: Vec<(&str, &[&str])> = vec![("listen", &["*"]), ("check", &["*"])];
        if reserved {
            permissions.push(("reserved_order", &["*"]));
        }
        grant(&key(8), &permissions)
    };
    env.kernel.grant(kit_grant(true)).unwrap();
    let kit = Spec {
        key: key(8),
        listener_order: -30,
        ..spec("z-kit", -30, pass)
    };
    let (kit_task, _kit_stop) = middleware(&env, kit, &log);
    let _app = middleware(&env, spec("a-app", 0, pass), &log);
    settle_in().await;
    let ok = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&ok), [1]);
    // By order, not by name: the kit's middleware comes first.
    assert_eq!(*log.lock().unwrap(), ["z-kit", "a-app"]);

    // A newer grant without reserved_order ends its stream.
    env.kernel.grant(kit_grant(false)).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(10), kit_task)
        .await
        .expect("the kit's stream ends")
        .unwrap();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_after_commands_sees_every_outcome_and_holds_nothing_up() {
    let env = start("mw-after-commands").await;
    spawn_worker(&env).await;
    let log = Log::default();
    fn no_o2(r: &Request, _: usize) -> Option<Rejected> {
        match r {
            Request::Command(c) if c.aggregate_id == "o2" => refuse("forbidden"),
            _ => None,
        }
    }
    let _guard = middleware(&env, spec("guard", 0, no_o2), &log);
    // Its verdict does not count: the command has happened.
    let _audit = middleware(&env, after("audit", 0, |_, _| refuse("ignored")), &log);
    settle_in().await;

    let placed = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let placed = submit(&env.addr, placed).await.unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    assert_eq!(
        logged(&log, 2).await,
        ["guard", "audit after c1 committed 1"]
    );

    // Refused before: the middleware after still hears of it.
    log.lock().unwrap().clear();
    let refused = signed(command("c2", "PlaceOrder", "o2"), &key(1));
    let refused = submit(&env.addr, refused).await.unwrap();
    assert_eq!(rejected_code(&refused), Some("forbidden"));
    assert_eq!(
        logged(&log, 2).await,
        ["guard", "audit after c2 rejected forbidden"]
    );

    // Rejected by the worker, and accepted without waiting: told once it completes.
    log.lock().unwrap().clear();
    let twice = signed(command("c3", "PlaceOrder", "o1"), &key(1));
    let twice = submit(&env.addr, twice).await.unwrap();
    assert!(rejected_code(&twice).is_some());
    let item = signed(command("c4", "AddItem", "o1"), &key(1));
    submit_with(&env.addr, item, Wait::Accepted).await.unwrap();
    let mut entries = logged(&log, 4).await;
    entries.sort();
    assert_eq!(
        entries,
        [
            "audit after c3 rejected invalid_state".to_owned(),
            "audit after c4 committed 1".into(),
            "guard".into(),
            "guard".into(),
        ]
    );

    // A resubmitted command only gets its stored result: nothing new to observe.
    log.lock().unwrap().clear();
    let again = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, again).await.unwrap();
    settle_in().await;
    assert_eq!(*log.lock().unwrap(), ["guard"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_middleware_after_commands_holds_nothing_up() {
    let env = start("mw-after-missing").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let (task, stop) = middleware(&env, after("audit", 0, pass), &log);
    settle_in().await;
    drop(stop);
    task.await.unwrap();
    settle_in().await;
    let placed = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let placed = submit(&env.addr, placed).await.unwrap();
    assert_eq!(committed_versions(&placed), [1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_after_queries_may_refuse_a_response_and_fails_closed() {
    let env = start("mw-after-queries").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let _counter = middleware(&env, counter(), &log);
    fn no_count(r: &Request, _: usize) -> Option<Rejected> {
        match r {
            Request::Query(q) if q.query_type == "CountOrders" => refuse("hidden"),
            _ => None,
        }
    }
    let mut filter = after("filter", 0, no_count);
    filter.types = vec!["orders.CountOrders".into()];
    let (task, stop) = middleware(&env, filter, &log);
    settle_in().await;

    let refused = ask(&env, query("CountOrders", &key(1))).await.unwrap();
    assert!(
        matches!(refused.outcome, Some(query_result::Outcome::Rejected(r)) if r.code == "hidden")
    );
    assert_eq!(*log.lock().unwrap(), ["filter after q1 answered 0"]);

    // Away, it refuses what it covers: the response is never sent unchecked.
    drop(stop);
    task.await.unwrap();
    settle_in().await;
    let status = ask(&env, query("CountOrders", &key(1))).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_after_queries_observes_refused_ones() {
    let env = start("mw-after-refused").await;
    spawn_worker(&env).await;
    let log = Log::default();
    let _counter = middleware(&env, counter(), &log);
    let _guard = middleware(&env, spec("guard", 0, |_, _| refuse("forbidden")), &log);
    let _audit = middleware(&env, after("audit", 0, pass), &log);
    settle_in().await;
    let refused = ask(&env, query("CountOrders", &key(1))).await.unwrap();
    assert!(matches!(
        refused.outcome,
        Some(query_result::Outcome::Rejected(_))
    ));
    assert_eq!(
        logged(&log, 2).await,
        ["guard", "audit after q1 rejected forbidden"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_has_at_most_one_middleware_before_and_one_after() {
    let env = start("mw-two").await;
    let channel = transport::connect(&env.addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    let both = |when: When| Middleware {
        order: 0,
        types: vec!["*".into()],
        when: when.into(),
    };
    tx.send(ListenerMessage {
        msg: Some(In::Hello(Hello {
            protocol: kernable_core::PROTOCOL,
            name: "double".into(),
            key_id: id(&key(5)),
            middlewares: vec![both(When::Before), both(When::Before)],
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let status = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await;
    let status = match status {
        Ok(r) => r.into_inner().message().await.unwrap_err(),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Lets file and cache requests of alice through, refuses everyone else's, and checks that
/// the file or value itself never reaches the middleware.
fn only_alice(request: &Request, _: usize) -> Option<Rejected> {
    let (context, content) = match request {
        Request::Data(d) => (d.context.as_ref(), d.data.len()),
        Request::Cache(c) => (c.context.as_ref(), c.value.len()),
        _ => return None,
    };
    assert_eq!(content, 0, "the middleware gets no file or value");
    let alice = context.is_some_and(|c| c.sender_identity == "alice");
    if alice { None } else { refuse("forbidden") }
}

#[tokio::test(flavor = "multi_thread")]
async fn files_and_cache_values_pass_the_middleware_chain() {
    let env = start("middleware-data").await;
    env.kernel
        .grant(grant(
            &key(7),
            &[
                ("data_read", &["shop.*"]),
                ("data_write", &["shop.*"]),
                ("cache", &["shop.*"]),
            ],
        ))
        .unwrap();
    let log = Log::default();
    let guard = Spec {
        types: vec!["shop.*".into()],
        ..spec("guard", 0, only_alice)
    };
    let (_guard, _stop) = middleware(&env, guard, &log);
    settle_in().await;

    let as_ = |who: &str| RequestContext {
        sender_identity: who.into(),
        ..Default::default()
    };
    let data = |op: &str, who: &str| {
        let mut r = DataRequest {
            tenant_id: "t".into(),
            domain: "shop".into(),
            key: "invoice.pdf".into(),
            data: if op == "put" {
                b"%PDF".to_vec()
            } else {
                Vec::new()
            },
            content_type: if op == "put" {
                "application/pdf".into()
            } else {
                String::new()
            },
            issued_at: now(),
            context: Some(as_(who)),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        };
        r.client_sig = Some(signing::sign(&key(7), &signing::data_message(op, &r)));
        r
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut files = DataServiceClient::new(channel.clone());
    let refused = files.put_data(data("put", "mallory")).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert!(refused.message().starts_with("forbidden"), "{refused:?}");
    files.put_data(data("put", "alice")).await.unwrap();
    let got = files
        .get_data(data("get", "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.data, b"%PDF");
    let refused = files.get_data(data("get", "mallory")).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);

    let cache = |op: &str, who: &str| {
        let mut r = CacheRequest {
            tenant_id: "t".into(),
            domain: "shop".into(),
            key: "rate".into(),
            value: if op == "set" {
                b"1.08".to_vec()
            } else {
                Vec::new()
            },
            issued_at: now(),
            context: Some(as_(who)),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        };
        r.client_sig = Some(signing::sign(&key(7), &signing::cache_message(op, &r)));
        r
    };
    let mut values = CacheServiceClient::new(channel);
    let refused = values.set_cache(cache("set", "mallory")).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);
    values.set_cache(cache("set", "alice")).await.unwrap();
    assert_eq!(
        values
            .get_cache(cache("get", "alice"))
            .await
            .unwrap()
            .into_inner()
            .value,
        b"1.08"
    );
}

fn refuse_all(_: &Request, _: usize) -> Option<Rejected> {
    refuse("closed")
}

/// Each file and cache operation reaches the middlewares of its own name only.
#[tokio::test(flavor = "multi_thread")]
async fn file_and_cache_operations_reach_the_middleware_of_their_name() {
    let env = start("middleware-ops").await;
    let all: &[&str] = &["shop.*"];
    env.kernel
        .grant(grant(
            &key(7),
            &[("data_read", all), ("data_write", all), ("cache", all)],
        ))
        .unwrap();
    env.kernel
        .grant(grant(&key(6), &[("listen", &["*"]), ("check", &["*"])]))
        .unwrap();
    let log = Log::default();
    let reads = Spec {
        types: vec!["shop.GetData".into(), "shop.GetCache".into()],
        ..spec("reads", 0, refuse_all)
    };
    let puts = Spec {
        types: vec!["shop.PutData".into()],
        key: key(6),
        ..spec("puts", 0, refuse_all)
    };
    let (_reads, _stop_reads) = middleware(&env, reads, &log);
    let (_puts, _stop_puts) = middleware(&env, puts, &log);
    settle_in().await;

    let data = |op: &str| {
        let mut r = DataRequest {
            tenant_id: "t".into(),
            domain: "shop".into(),
            key: "a.txt".into(),
            issued_at: now(),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        };
        r.client_sig = Some(signing::sign(&key(7), &signing::data_message(op, &r)));
        r
    };
    let cache = |op: &str| {
        let mut r = CacheRequest {
            tenant_id: "t".into(),
            domain: "shop".into(),
            key: "rate".into(),
            issued_at: now(),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        };
        r.client_sig = Some(signing::sign(&key(7), &signing::cache_message(op, &r)));
        r
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut files = DataServiceClient::new(channel.clone());
    let mut values = CacheServiceClient::new(channel);
    let code = |r: Result<(), tonic::Status>| r.map_err(|e| e.code());
    assert_eq!(
        code(files.put_data(data("put")).await.map(|_| ())),
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        code(files.get_data(data("get")).await.map(|_| ())),
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        code(files.delete_data(data("delete")).await.map(|_| ())),
        Ok(())
    );
    assert_eq!(
        code(values.set_cache(cache("set")).await.map(|_| ())),
        Ok(())
    );
    assert_eq!(
        code(values.get_cache(cache("get")).await.map(|_| ())),
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        code(values.delete_cache(cache("delete")).await.map(|_| ())),
        Ok(())
    );
    assert_eq!(*log.lock().unwrap(), ["puts", "reads", "reads"]);
}

/// Before a command, a middleware gets its vault values only if its key may read them.
#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_gets_vault_values_only_if_its_key_may_read_them() {
    let env = start_custom("middleware-vault", with_vault).await;
    env.kernel
        .grant(grant(&key(6), &[("listen", &["*"]), ("check", &["*"])]))
        .unwrap();
    let log = Log::default();
    let reader = spec("reader", 0, pass); // key 5 reads every vault value
    let blind = Spec {
        key: key(6), // may check, but read no vault values
        ..spec("blind", 1, pass)
    };
    let (_reader, _stop_reader) = middleware(&env, reader, &log);
    let (_blind, _stop_blind) = middleware(&env, blind, &log);
    settle_in().await;
    start_worker(env.addr.clone(), TestWorker::honest()).await;

    let value = vault_value("k-1", "max@example.com");
    let mut cmd = command("c1", "PlaceOrder", "o1");
    cmd.payload = format!(r#"{{"email":{{"$vault":"{}"}}}}"#, value.r#ref).into_bytes();
    cmd.vault_refs = vec![value.r#ref.clone()];
    let request = kernable_core::proto::SubmitRequest {
        command: Some(signed(cmd, &key(1))),
        wait: Wait::Accepted.into(),
        protocol: kernable_core::PROTOCOL,
        vault: vec![value],
        ..Default::default()
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    kernable_core::proto::command_service_client::CommandServiceClient::new(channel)
        .submit(request)
        .await
        .unwrap();
    assert_eq!(
        logged(&log, 2).await,
        ["reader with 1 vault values", "blind"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_that_goes_away_mid_check_refuses_at_once() {
    let env = start_custom("middleware-gone-mid-check", |config, _| {
        config.middleware_timeout_ms = 10_000;
    })
    .await;
    spawn_worker(&env).await;
    let log = Log::default();
    let slow = Spec {
        deliberates: Duration::from_secs(30),
        ..spec("guard", 0, pass)
    };
    let (guard, _stop) = middleware(&env, slow, &log);
    settle_in().await;
    let started = std::time::Instant::now();
    let submitted = tokio::spawn({
        let addr = env.addr.clone();
        let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
        async move { submit(&addr, place).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    guard.abort(); // the process dies while it decides
    let refused = submitted.await.unwrap();
    assert_eq!(refused.unwrap_err().code(), Code::Unavailable);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "refused at once, not after the middleware timeout"
    );
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(store.head_position().unwrap(), 0, "nothing stored");
}
