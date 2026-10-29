//! Files and the cache: what is put comes back, deletes report whether there was something, and
//! a grant covers its domain only.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use common::TestKernel;
use kernable::{
    Check, CheckKind, Data, Error, Kernable, MiddlewareOptions, Rejected, RequestContext, domain,
};
use tonic::Code;

fn denied(result: Result<impl std::fmt::Debug, Error>) {
    match result {
        Err(Error::Status(s)) => assert_eq!(s.code(), Code::PermissionDenied, "{s:?}"),
        other => panic!("expected permission denied, got {other:?}"),
    }
}

#[tokio::test]
async fn files_are_put_read_and_deleted() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let files = kernel.client().data();

    assert_eq!(files.get("shop", "a.txt").await.unwrap(), None);
    files
        .put("shop", "a.txt", "hello", "text/plain")
        .await
        .unwrap();
    assert_eq!(
        files.get("shop", "a.txt").await.unwrap(),
        Some(Data {
            data: b"hello".to_vec(),
            content_type: "text/plain".into(),
        })
    );

    // Larger than gRPC's default message size of 4 MiB.
    let big = vec![7u8; 5 << 20];
    files
        .put("shop", "big.bin", big.clone(), "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(
        files.get("shop", "big.bin").await.unwrap().unwrap().data,
        big
    );

    assert!(files.delete("shop", "a.txt").await.unwrap());
    assert!(!files.delete("shop", "a.txt").await.unwrap());
    assert_eq!(files.get("shop", "a.txt").await.unwrap(), None);
}

#[tokio::test]
async fn cache_values_are_set_read_expired_and_deleted() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let cache = kernel.client().cache();

    assert_eq!(cache.get("shop", "k").await.unwrap(), None);
    cache.set("shop", "k", "v", None).await.unwrap();
    assert_eq!(cache.get("shop", "k").await.unwrap(), Some(b"v".to_vec()));
    assert!(cache.delete("shop", "k").await.unwrap());
    assert!(!cache.delete("shop", "k").await.unwrap());
    assert_eq!(cache.get("shop", "k").await.unwrap(), None);

    cache
        .set("shop", "short", "lived", Some(Duration::from_millis(200)))
        .await
        .unwrap();
    assert_eq!(
        cache.get("shop", "short").await.unwrap(),
        Some(b"lived".to_vec())
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.get("shop", "short").await.unwrap(), None);
}

#[tokio::test]
async fn domains_without_a_grant_are_refused() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let client = kernel.client();
    let (files, cache) = (client.data(), client.cache());

    denied(files.put("billing", "a.txt", "x", "text/plain").await);
    denied(files.get("billing", "a.txt").await);
    denied(files.delete("billing", "a.txt").await);
    denied(cache.set("billing", "k", "v", None).await);
    denied(cache.get("billing", "k").await);
    denied(cache.delete("billing", "k").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_middleware_checks_file_and_cache_requests_on_their_context() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let anonymous = kernel.client();
    let alice = RequestContext {
        sender_identity: "alice".into(),
        ..Default::default()
    };
    // Alice's by default, from the client's options.
    let as_alice = Kernable::new(kernel.options().context(alice.clone())).unwrap();

    let checked = Arc::new(Mutex::new(Vec::<Check>::new()));
    let c = Arc::clone(&checked);
    let guard = domain("shop").read_model("guard");
    guard.middleware(MiddlewareOptions::new(&["shop.*"]), move |check| {
        let allowed = check.request.sender_identity == "alice";
        c.lock().unwrap().push(check);
        async move {
            if !allowed {
                return Err(Rejected::new("who are you").code("forbidden").into());
            }
            Ok(())
        }
    });
    let guard = anonymous.listener(&guard);
    let running = {
        let guard = guard.clone();
        tokio::spawn(async move { guard.run().await })
    };
    guard.ready().await;

    let refused = |result: Result<(), Error>| match result {
        Err(Error::Status(s)) => {
            assert_eq!(s.code(), Code::PermissionDenied, "{s:?}");
            assert_eq!(s.message(), "forbidden: who are you");
        }
        other => panic!("expected a refusal, got {other:?}"),
    };
    refused(
        anonymous
            .data()
            .put("shop", "a.txt", "x", "text/plain")
            .await,
    );
    refused(anonymous.cache().set("shop", "k", "v", None).await);
    as_alice
        .data()
        .put("shop", "a.txt", "hello", "text/plain")
        .await
        .unwrap();
    // A context per handle replaces the client's default.
    anonymous
        .cache()
        .context(alice)
        .set("shop", "k", "v", None)
        .await
        .unwrap();
    assert_eq!(
        as_alice.cache().get("shop", "k").await.unwrap(),
        Some(b"v".to_vec())
    );

    let seen: Vec<_> = checked
        .lock()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c.kind,
                c.tenant.is_empty(),
                c.domain.clone(),
                c.type_name.clone(),
                c.key.clone(),
                c.data.is_null(),
                c.request.sender_identity.clone(),
            )
        })
        .collect();
    let key = |k: &str| Some(k.to_owned());
    assert_eq!(
        seen,
        [
            (
                CheckKind::Data,
                false,
                "shop".into(),
                "PutData".into(),
                key("a.txt"),
                true,
                String::new()
            ),
            (
                CheckKind::Cache,
                false,
                "shop".into(),
                "SetCache".into(),
                key("k"),
                true,
                String::new()
            ),
            (
                CheckKind::Data,
                false,
                "shop".into(),
                "PutData".into(),
                key("a.txt"),
                true,
                "alice".into()
            ),
            (
                CheckKind::Cache,
                false,
                "shop".into(),
                "SetCache".into(),
                key("k"),
                true,
                "alice".into()
            ),
            (
                CheckKind::Cache,
                false,
                "shop".into(),
                "GetCache".into(),
                key("k"),
                true,
                "alice".into()
            ),
        ]
    );
    guard.stop();
    running.await.unwrap();
}
