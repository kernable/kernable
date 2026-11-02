//! Queries go through the kernel to the read model that answers them: signed, allowed per key,
//! answered in order with the read model's events.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Hello, ListenerMessage, QueryAnswer, QueryRequest, QueryResult,
        Rejected, listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient, query_answer, query_result,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

/// A read model that counts `orders.OrderPlaced` and answers `orders.CountOrders` with the
/// count; `orders.Fail` is rejected. With `ack: false` it never acknowledges events.
fn read_model(env: &Env, name: &str, key: SigningKey, ack: bool) -> JoinHandle<tonic::Status> {
    let addr = env.addr.clone();
    let name = name.to_owned();
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name,
            key_id: id(&key),
            order: 0,
            event_types: vec!["orders.OrderPlaced".into()],
            from_position: Some(0),
            query_types: vec!["orders.CountOrders".into(), "orders.Fail".into()],
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
        let mut count = 0;
        loop {
            let msg = match inbound.message().await {
                Ok(Some(m)) => m.msg.unwrap(),
                Ok(None) => return tonic::Status::ok("closed"),
                Err(status) => return status,
            };
            let reply = match msg {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Ready(_) => continue,
                Out::Batch(b) => {
                    count += b.events.len();
                    if !ack {
                        continue;
                    }
                    In::Ack(Ack {
                        position: b.up_to,
                        ..Default::default()
                    })
                }
                Out::Query(call) => {
                    let query = call.query.unwrap();
                    let outcome = match query.query_type.as_str() {
                        "CountOrders" => query_answer::Outcome::Response(count.to_string().into()),
                        _ => query_answer::Outcome::Rejected(Rejected {
                            code: "nope".into(),
                            message: "always fails".into(),
                            ..Default::default()
                        }),
                    };
                    In::Answer(QueryAnswer {
                        call_id: call.call_id,
                        outcome: Some(outcome),
                    })
                }
                Out::Check(_) => unreachable!("not a middleware"),
            };
            if tx.send(send(reply)).await.is_err() {
                return tonic::Status::ok("closed");
            }
        }
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn signed_query(query_type: &str, min_position: u64, key: &SigningKey) -> QueryRequest {
    let mut q = QueryRequest {
        listener: String::new(),
        protocol: kernable_core::PROTOCOL,
        query_id: "q1".into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        query_type: query_type.into(),
        payload: b"{}".to_vec(),
        context: None,
        min_position,
        issued_at: now_ms(),
        client_sig: None,
    };
    q.client_sig = Some(signing::sign(key, &signing::query_message(&q)));
    q
}

async fn ask(env: &Env, q: QueryRequest) -> Result<QueryResult, tonic::Status> {
    let channel = transport::connect(&env.addr).await.unwrap();
    Ok(QueryServiceClient::new(channel)
        .query(q)
        .await?
        .into_inner())
}

fn response(result: &QueryResult) -> String {
    match &result.outcome {
        Some(query_result::Outcome::Response(r)) => String::from_utf8_lossy(r).into_owned(),
        other => panic!("expected a response, got {other:?}"),
    }
}

async fn place(env: &Env, id: &str, order: &str) {
    let cmd = signed(command(id, "PlaceOrder", order), &key(1));
    committed_versions(&submit(env, cmd).await);
}

async fn submit(
    env: &Env,
    cmd: kernable_core::proto::Command,
) -> kernable_core::proto::CommandResult {
    common::submit(&env.addr, cmd).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_reaches_the_read_model_and_sees_its_own_writes() {
    let env = start("query-answer").await;
    spawn_worker(&env).await;
    let _model = read_model(&env, "orders", key(5), true);
    tokio::time::sleep(Duration::from_millis(100)).await;

    place(&env, "c1", "o1").await;
    place(&env, "c2", "o2").await;
    // The submits settled, so the read model has both events before the query.
    let counted = ask(&env, signed_query("CountOrders", 0, &key(1)))
        .await
        .unwrap();
    assert_eq!(response(&counted), "2");

    let failed = ask(&env, signed_query("Fail", 0, &key(1))).await.unwrap();
    assert!(matches!(failed.outcome, Some(query_result::Outcome::Rejected(r)) if r.code == "nope"));
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_are_signed_allowed_per_key_and_need_a_read_model() {
    let env = start("query-refused").await;
    env.kernel
        .grant(grant(&key(8), &[("query", &["orders.Fail"])]))
        .unwrap();
    let code = |r: Result<QueryResult, tonic::Status>| r.unwrap_err().code();

    // Nobody answers yet.
    assert_eq!(
        code(ask(&env, signed_query("CountOrders", 0, &key(1))).await),
        Code::Unavailable
    );

    let _model = read_model(&env, "orders", key(5), true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Key 8 may only ask orders.Fail.
    assert_eq!(
        code(ask(&env, signed_query("CountOrders", 0, &key(8))).await),
        Code::PermissionDenied
    );
    let mut tampered = signed_query("CountOrders", 0, &key(1));
    tampered.payload = b"{\"all\":true}".to_vec();
    assert_eq!(code(ask(&env, tampered).await), Code::Unauthenticated);
    let mut stale = signed_query("CountOrders", 0, &key(1));
    stale.issued_at -= 120_000;
    stale.client_sig = Some(signing::sign(&key(1), &signing::query_message(&stale)));
    assert_eq!(code(ask(&env, stale).await), Code::Unauthenticated);

    // Key 8 may ask what it is allowed to, but not past authorization (no `privileged`).
    assert!(ask(&env, signed_query("Fail", 0, &key(8))).await.is_ok());
    let mut bypass = signed_query("Fail", 0, &key(8));
    bypass.context = Some(kernable_core::proto::RequestContext {
        bypass_authorization: true,
        ..Default::default()
    });
    bypass.client_sig = Some(signing::sign(&key(8), &signing::query_message(&bypass)));
    assert_eq!(code(ask(&env, bypass).await), Code::PermissionDenied);
    // A query type is a name without dots.
    assert_eq!(
        code(ask(&env, signed_query("Count.Orders", 0, &key(1))).await),
        Code::InvalidArgument
    );

    // A listener key may only answer the queries it is allowed to (key 6 may not).
    let status = read_model(&env, "impostor", key(6), true).await.unwrap();
    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn min_position_waits_for_the_read_model_to_catch_up() {
    let env = start("query-position").await;
    spawn_worker(&env).await;
    // This read model never acknowledges, so its position stays 0.
    let _model = read_model(&env, "orders", key(5), false);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let cmd = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    common::submit_with(&env.addr, cmd, kernable_core::proto::Wait::Committed)
        .await
        .unwrap();

    assert_eq!(
        response(
            &ask(&env, signed_query("CountOrders", 0, &key(1)))
                .await
                .unwrap()
        ),
        "1"
    );
    let behind = ask(&env, signed_query("CountOrders", 1, &key(1))).await;
    assert_eq!(behind.unwrap_err().code(), Code::DeadlineExceeded);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_model_is_never_asked_from_its_own_handler() {
    let env = start("query-self").await;
    spawn_worker(&env).await;
    let _counts = read_model(&env, "counts", key(5), true);
    place(&env, "c1", "o1").await;

    let mut q = signed_query("CountOrders", 0, &key(1));
    q.listener = "counts".into(); // not signed
    let status = ask(&env, q.clone()).await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("wait behind itself"),
        "{status:?}"
    );

    // Another read model answers instead.
    let _other = read_model(&env, "other", key(5), true);
    place(&env, "c2", "o2").await;
    for _ in 0..4 {
        assert_eq!(response(&ask(&env, q.clone()).await.unwrap()), "2");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_model_that_goes_away_fails_the_query_at_once() {
    let env = start("query-gone").await;
    let (tx, rx) = mpsc::channel(16);
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: "silent".into(),
        key_id: id(&key(5)),
        order: 0,
        event_types: Vec::new(),
        from_position: None,
        query_types: vec!["orders.CountOrders".into()],
        ..Default::default()
    };
    tx.send(ListenerMessage {
        msg: Some(In::Hello(hello)),
    })
    .await
    .unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    // Handshake, then disconnect as soon as a query arrives, without answering.
    let silent = tokio::spawn(async move {
        while let Ok(Some(m)) = inbound.message().await {
            match m.msg.unwrap() {
                Out::Challenge(c) => {
                    let sig = signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig;
                    let msg = In::ChallengeResponse(ChallengeResponse { sig });
                    tx.send(ListenerMessage { msg: Some(msg) }).await.unwrap();
                }
                Out::Query(_) => return,
                _ => {}
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    let gone = ask(&env, signed_query("CountOrders", 0, &key(1))).await;
    silent.await.unwrap();
    assert_eq!(gone.unwrap_err().code(), Code::Unavailable);
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "should not wait for the timeout"
    );
}
