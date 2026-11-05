//! Replays and reordering: a signed request that was valid once, sent again or late, must
//! never undo a newer one.

mod common;

use common::*;
use kernable_core::{
    proto::{
        CacheRequest, DataRequest, Membership, SubmitRequest, Wait,
        cache_service_client::CacheServiceClient, command_service_client::CommandServiceClient,
        data_service_client::DataServiceClient,
    },
    signing, transport,
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

/// A data request of key 7, signed at `issued_at`.
fn data(op: &str, bytes: &[u8], issued_at: i64) -> DataRequest {
    let mut r = DataRequest {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: "terms.txt".into(),
        data: bytes.to_vec(),
        content_type: if op == "put" {
            "text/plain".into()
        } else {
            String::new()
        },
        issued_at,
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    r.client_sig = Some(signing::sign(&key(7), &signing::data_message(op, &r)));
    r
}

/// A cache request of key 7, signed at `issued_at`.
fn cache(op: &str, value: &[u8], issued_at: i64) -> CacheRequest {
    let mut r = CacheRequest {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: "price".into(),
        value: value.to_vec(),
        ttl_ms: 60_000,
        issued_at,
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    r.client_sig = Some(signing::sign(&key(7), &signing::cache_message(op, &r)));
    r
}

async fn start_with_writer(name: &str) -> Env {
    let env = start(name).await;
    let shop: &[&str] = &["shop.*"];
    env.kernel
        .grant(grant(
            &key(7),
            &[("data_read", shop), ("data_write", shop), ("cache", shop)],
        ))
        .unwrap();
    env
}

/// F2: a captured write, sent again within its 60 s, would undo the newer write after it.
#[tokio::test]
async fn a_replayed_data_write_never_undoes_a_newer_one() {
    let env = start_with_writer("replay-data").await;
    let mut files = DataServiceClient::new(transport::connect(&env.addr).await.unwrap());
    let old = data("put", b"old terms", issued_now());
    files.put_data(old.clone()).await.unwrap();
    files
        .put_data(data("put", b"new terms", issued_now()))
        .await
        .unwrap();

    let replayed = files.put_data(old).await.unwrap_err();
    assert_eq!(replayed.code(), Code::AlreadyExists, "{replayed:?}");
    let read = files
        .get_data(data("get", b"", issued_now()))
        .await
        .unwrap();
    assert_eq!(read.into_inner().data, b"new terms");

    // A replayed delete neither.
    let delete = data("delete", b"", issued_now());
    files.delete_data(delete.clone()).await.unwrap();
    files
        .put_data(data("put", b"newest terms", issued_now()))
        .await
        .unwrap();
    let replayed = files.delete_data(delete).await.unwrap_err();
    assert_eq!(replayed.code(), Code::AlreadyExists, "{replayed:?}");
    let read = files
        .get_data(data("get", b"", issued_now()))
        .await
        .unwrap();
    assert_eq!(read.into_inner().data, b"newest terms");

    // Reads are not writes: the same read twice is fine.
    let get = data("get", b"", issued_now());
    files.get_data(get.clone()).await.unwrap();
    files.get_data(get).await.unwrap();
}

/// F2, for the cache.
#[tokio::test]
async fn a_replayed_cache_write_never_undoes_a_newer_one() {
    let env = start_with_writer("replay-cache").await;
    let mut cached = CacheServiceClient::new(transport::connect(&env.addr).await.unwrap());
    let old = cache("set", b"10", issued_now());
    cached.set_cache(old.clone()).await.unwrap();
    cached
        .set_cache(cache("set", b"12", issued_now()))
        .await
        .unwrap();
    let replayed = cached.set_cache(old).await.unwrap_err();
    assert_eq!(replayed.code(), Code::AlreadyExists, "{replayed:?}");
    let read = cached
        .get_cache(cache("get", b"", issued_now()))
        .await
        .unwrap();
    assert_eq!(read.into_inner().value, b"12");
}

/// F2 across a restart: the kernel cannot know which writes ran before it started, so it
/// refuses writes signed before then, though they are still within their 60 s.
#[tokio::test]
async fn a_write_signed_before_the_kernel_started_is_refused() {
    let signed_before = issued_now() - 1;
    let env = start_with_writer("replay-restart").await;
    let mut files = DataServiceClient::new(transport::connect(&env.addr).await.unwrap());
    let refused = files
        .put_data(data("put", b"old terms", signed_before))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    files
        .put_data(data("put", b"old terms", issued_now()))
        .await
        .unwrap();
}

/// A write the kernel tried keeps its signature used even if it failed: it may have taken
/// effect, and a captured copy must not undo a newer write. A resend is signed anew.
#[cfg(unix)]
#[tokio::test]
async fn a_failed_data_write_cannot_be_sent_again_under_the_same_signature() {
    use std::os::unix::fs::PermissionsExt;
    // The write fails because the directory refuses it, which root's never does.
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: as root no directory refuses a write");
        return;
    }
    let env = start_with_writer("replay-failed-write").await;
    let mut files = DataServiceClient::new(transport::connect(&env.addr).await.unwrap());
    let files_dir = env.dir.join("data");
    std::fs::create_dir_all(&files_dir).unwrap();
    let mode = |m: u32| {
        std::fs::set_permissions(&files_dir, std::fs::Permissions::from_mode(m)).unwrap();
    };
    mode(0o500); // the disk refuses the write
    let first = data("put", b"terms", issued_now());
    let failed = files.put_data(first.clone()).await.unwrap_err();
    assert_eq!(failed.code(), Code::Internal, "{failed:?}");
    mode(0o700);
    let again = files.put_data(first).await.unwrap_err();
    assert_eq!(again.code(), Code::AlreadyExists, "{again:?}");
    files
        .put_data(data("put", b"terms", issued_now()))
        .await
        .unwrap();
}
