//! Subscriptions: backlog then live, filtered by type, only for signed and allowed keys.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{Event, SubscribeRequest, event_service_client::EventServiceClient},
    signing, transport,
};
use tonic::{Code, Status, Streaming};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn request(key: &SigningKey, after: u64, types: &[&str], issued_at: i64) -> SubscribeRequest {
    let event_types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    let msg = signing::subscribe_message(after, &event_types, issued_at);
    SubscribeRequest {
        protocol: kernable_core::PROTOCOL,
        after_position: after,
        event_types,
        issued_at,
        client_sig: Some(signing::sign(key, &msg)),
    }
}

async fn subscribe(env: &Env, req: SubscribeRequest) -> Result<Streaming<Event>, Status> {
    let channel = transport::connect(&env.addr).await.unwrap();
    Ok(EventServiceClient::new(channel)
        .subscribe(req)
        .await?
        .into_inner())
}

async fn next(stream: &mut Streaming<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("no event within 2 s")
        .unwrap()
        .unwrap()
}

async fn place_and_add(env: &Env) {
    let client = key(1);
    submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &client),
    )
    .await
    .unwrap();
    submit(&env.addr, signed(command("c2", "AddItem", "o1"), &client))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn backlog_then_live_events_in_order() {
    let env = start("sub-live").await;
    spawn_worker(&env).await;
    place_and_add(&env).await;

    let mut all = subscribe(&env, request(&key(1), 0, &[], now_ms()))
        .await
        .unwrap();
    assert_eq!(next(&mut all).await.global_position, 1);
    assert_eq!(next(&mut all).await.global_position, 2);

    let mut tail = subscribe(&env, request(&key(1), 2, &[], now_ms()))
        .await
        .unwrap();
    submit(
        &env.addr,
        signed(command("c3", "PlaceOrder", "o2"), &key(1)),
    )
    .await
    .unwrap();
    let live = next(&mut all).await;
    assert_eq!(
        (live.global_position, live.aggregate_id.as_str()),
        (3, "o2")
    );
    assert_eq!(next(&mut tail).await.global_position, 3);
    // Events carry the worker's signature, so subscribers can check them too.
    assert!(live.producer_sig.is_some() && !live.hash.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn event_types_are_filtered_and_restricted_per_key() {
    let env = start("sub-filter").await;
    spawn_worker(&env).await;
    place_and_add(&env).await;
    submit(
        &env.addr,
        signed(command("c3", "PlaceOrder", "o2"), &key(1)),
    )
    .await
    .unwrap();

    let mut placed = subscribe(&env, request(&key(1), 0, &["orders.OrderPlaced"], now_ms()))
        .await
        .unwrap();
    assert_eq!(next(&mut placed).await.global_position, 1);
    assert_eq!(next(&mut placed).await.global_position, 3);

    // Key 4 may only read OrderPlaced: an empty filter means exactly that.
    let mut restricted = subscribe(&env, request(&key(4), 0, &[], now_ms()))
        .await
        .unwrap();
    assert_eq!(next(&mut restricted).await.event_type, "OrderPlaced");
    assert_eq!(next(&mut restricted).await.global_position, 3);

    let denied = subscribe(&env, request(&key(4), 0, &["orders.ItemAdded"], now_ms())).await;
    assert_eq!(denied.unwrap_err().code(), Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn unsigned_stale_and_unknown_subscribers_are_refused() {
    let env = start("sub-auth").await;

    let mut unsigned = request(&key(1), 0, &[], now_ms());
    unsigned.client_sig = None;
    assert_eq!(
        subscribe(&env, unsigned).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let mut tampered = request(&key(1), 0, &["orders.OrderPlaced"], now_ms());
    tampered.event_types.clear();
    assert_eq!(
        subscribe(&env, tampered).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let stale = request(&key(1), 0, &[], now_ms() - 120_000);
    assert_eq!(
        subscribe(&env, stale).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let stranger = request(&key(9), 0, &[], now_ms());
    assert_eq!(
        subscribe(&env, stranger).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}
