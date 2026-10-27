//! Middleware: listeners that check commands and queries before the kernel accepts or routes
//! them, in chain order, failing closed.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Hello, ListenerMessage, ListenerRecord, Middleware, QueryRequest,
        Rejected, RequestContext, Verdict, Wait, command_result::Outcome,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient, middleware_call::Request, query_result,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

/// Names of the middlewares that checked a request, in the order they did.
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
    }
}

fn pass(_: &Request, _: usize) -> Option<Rejected> {
    None
}

fn refuse(code: &str) -> Option<Rejected> {
    Some(Rejected {
        code: code.into(),
        message: "refused by a test middleware".into(),
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
            query_types: Vec::new(),
            middleware: Some(Middleware {
                order: spec.order,
                types: spec.types,
            }),
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
                Out::Ready(_) => continue,
                Out::Batch(b) => {
                    seen += b.events.len();
                    if !spec.acks {
                        continue;
                    }
                    In::Ack(Ack { position: b.up_to })
                }
                Out::Check(call) => {
                    log.lock().unwrap().push(spec.name.into());
                    In::Verdict(Verdict {
                        call_id: call.call_id,
                        rejected: (spec.decide)(&call.request.unwrap(), seen),
                    })
                }
                Out::Query(_) => unreachable!("answers no queries"),
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
    spawn_worker(&env);
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

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_middleware_refuses_everything_it_covers() {
    let env = start("mw-closed").await;
    spawn_worker(&env);
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
            middleware: Some(Middleware {
                order: 0,
                types: vec!["orders.*".into()],
            }),
            ..Default::default()
        };
        store.save_listeners(&[record]).unwrap();
    })
    .await;
    spawn_worker(&env);
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
            middleware: Some(Middleware {
                order: 0,
                types: vec!["orders.AddItem".into()],
            }),
            ..Default::default()
        };
        store.save_listeners(&[record]).unwrap();
    })
    .await;
    spawn_worker(&env);
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

    let (task, _stop) = middleware(&env, spec("reserved", -10, pass), &log);
    assert_eq!(task.await.unwrap().code(), Code::InvalidArgument);

    let mut bad = spec("bad", 0, pass);
    bad.types = vec!["orders".into()];
    let (task, _stop) = middleware(&env, bad, &log);
    assert_eq!(task.await.unwrap().code(), Code::InvalidArgument);
}
