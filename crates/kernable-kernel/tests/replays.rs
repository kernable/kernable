//! Replays and reordering: a signed request that was valid once, sent again or late, must
//! never undo a newer one.

mod common;

use common::*;
use kernable_core::{
    proto::{Membership, SubmitRequest, Wait, command_service_client::CommandServiceClient},
    transport,
};
use kernable_kernel::Kernel;
use tonic::Code;

/// The middlewares of `service` as the store holds them.
fn stored_middlewares(env: &Env, service: &str) -> Vec<String> {
    kernable_store::open_read_only(&env.store)
        .unwrap()
        .memberships()
        .unwrap()
        .into_iter()
        .filter_map(|r| r.membership)
        .find(|m| m.service == service)
        .map(|m| m.middlewares)
        .unwrap_or_default()
}

fn issued(mut m: Membership, issued_at: i64) -> Membership {
    m.issued_at = issued_at;
    m
}

/// F1: a deploy added a middleware; the membership before it, replayed within its 60 s, would
/// remove the middleware's check again.
#[tokio::test]
async fn an_older_membership_never_undoes_a_newer_one() {
    let env = start("replay-membership").await;
    let before = issued(membership("shop", &[], &[], &[]), issued_now());
    let after = issued(membership("shop", &[], &[], &["guard"]), issued_now());
    apply_issued(&env.addr, before.clone(), &key(1))
        .await
        .unwrap();
    apply_issued(&env.addr, after.clone(), &key(1))
        .await
        .unwrap();

    let replayed = apply_issued(&env.addr, before, &key(1)).await.unwrap_err();
    assert_eq!(replayed.code(), Code::FailedPrecondition, "{replayed:?}");
    assert_eq!(stored_middlewares(&env, "shop"), ["guard"]);

    // The same membership again is a retried deploy, not an attack.
    apply_issued(&env.addr, after.clone(), &key(1))
        .await
        .unwrap();
    // Another membership issued at the same moment cannot be ordered: refused.
    let twin = issued(membership("shop", &[], &["carts"], &[]), after.issued_at);
    let refused = apply_issued(&env.addr, twin, &key(1)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    assert_eq!(stored_middlewares(&env, "shop"), ["guard"]);
}

/// F1: two deploys race; whatever order their memberships arrive in, the newer one holds.
#[tokio::test(flavor = "multi_thread")]
async fn racing_deploys_end_with_the_newer_membership() {
    let env = start("replay-race").await;
    let signer = key(1);
    for round in 0..20 {
        let service = format!("svc{round}");
        let older = issued(membership(&service, &[], &[], &["old"]), issued_now());
        let newer = issued(membership(&service, &[], &[], &["new"]), issued_now());
        let (a, b) = tokio::join!(
            apply_issued(&env.addr, older, &signer),
            apply_issued(&env.addr, newer, &signer),
        );
        assert!(b.is_ok(), "round {round}: the newer one is refused: {b:?}");
        if let Err(e) = a {
            assert_eq!(e.code(), Code::FailedPrecondition, "round {round}: {e:?}");
        }
        assert_eq!(stored_middlewares(&env, &service), ["new"], "round {round}");
    }
}

/// F7: a narrower grant replaced a broader one. After a restart, the broader one, presented
/// again by a key that kept it, stays refused.
#[tokio::test]
async fn an_older_grant_stays_refused_after_a_restart() {
    let thief = key(8);
    let broad = grant(&thief, &[("submit", &["*"])]);
    let narrow = grant(&thief, &[("query", &["*"])]);
    let env = start_custom("replay-grant", |config, _| {
        // The kernel before the restart.
        let store = kernable_store::open_with(&config.store, config.durability).unwrap();
        let before = Kernel::open_on(config.clone(), store).unwrap();
        before.grant(broad.clone()).unwrap();
        before.grant(narrow.clone()).unwrap();
    })
    .await;

    let request = SubmitRequest {
        command: Some(signed(command("c1", "PlaceOrder", "o1"), &thief)),
        wait: Wait::Committed.into(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    let channel = transport::connect(&env.addr).await.unwrap();
    let refused = CommandServiceClient::new(channel)
        .submit(presenting(request, &broad))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
}
