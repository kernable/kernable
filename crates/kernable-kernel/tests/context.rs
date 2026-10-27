//! The request context: signed with the command, stored with its events, checked by the kernel.

mod common;

use common::*;
use kernable_core::proto::{Attribute, RequestContext, command_result::Outcome};

fn with_context(
    id: &str,
    command_type: &str,
    context: RequestContext,
) -> kernable_core::proto::Command {
    let mut cmd = command(id, command_type, "o1");
    cmd.context = Some(context);
    signed(cmd, &key(1))
}

fn sender() -> RequestContext {
    RequestContext {
        sender_identity: "i-1".into(),
        correlation_id: "r-1".into(),
        attributes: vec![Attribute {
            key: "channel".into(),
            value: "web".into(),
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_context_is_stored_with_the_events_and_covered_by_the_chain() {
    let env = start("ctx-stored").await;
    spawn_worker(&env);
    let result = submit(&env.addr, with_context("c1", "PlaceOrder", sender()))
        .await
        .unwrap();
    let Some(Outcome::Committed(c)) = result.outcome else {
        panic!("not committed");
    };
    assert_eq!(c.events[0].context, Some(sender()));

    let store = kernable_store::open_read_only(&env.store).unwrap();
    let stored = store.read_stream("t1", "orders", "Order", "o1").unwrap();
    assert_eq!(stored[0].context, Some(sender()));
    let report = store.verify(&[id(&env.kernel_key).to_string()]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);

    // The client signature covers the context: changing it is detected.
    let mut tampered = with_context("c2", "AddItem", sender());
    tampered.context.as_mut().unwrap().sender_identity = "i-admin".into();
    let refused = submit(&env.addr, tampered).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expected_version_that_does_not_match_rejects_the_command() {
    let env = start("ctx-version").await;
    spawn_worker(&env);
    let at = |v| RequestContext {
        expected_version: Some(v),
        ..Default::default()
    };
    assert_eq!(
        committed_versions(
            &submit(&env.addr, with_context("c1", "PlaceOrder", at(0)))
                .await
                .unwrap()
        ),
        [1]
    );
    let stale = submit(&env.addr, with_context("c2", "AddItem", at(0)))
        .await
        .unwrap();
    let Some(Outcome::Rejected(r)) = stale.outcome else {
        panic!("expected a rejection");
    };
    assert_eq!(r.code, "version_mismatch");
    assert_eq!(
        committed_versions(
            &submit(&env.addr, with_context("c3", "AddItem", at(1)))
                .await
                .unwrap()
        ),
        [2]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn only_privileged_keys_may_bypass_authorization() {
    let bypass = || RequestContext {
        bypass_authorization: true,
        ..Default::default()
    };
    let privileged = start("ctx-privileged").await;
    spawn_worker(&privileged);
    let ok = submit(&privileged.addr, with_context("c1", "PlaceOrder", bypass())).await;
    assert_eq!(committed_versions(&ok.unwrap()), [1]);

    let plain = start_custom("ctx-plain", |config, _| config.privileged.clear()).await;
    let refused = submit(&plain.addr, with_context("c1", "PlaceOrder", bypass())).await;
    assert_eq!(refused.unwrap_err().code(), tonic::Code::PermissionDenied);
}
