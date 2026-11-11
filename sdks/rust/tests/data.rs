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
    // The SHA-256 of "hello", for an event to name.
    let hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    let put = files
        .put("shop", "a.txt", "hello", "text/plain")
        .await
        .unwrap();
    assert_eq!(put, hello);
    assert_eq!(
        files.get("shop", "a.txt").await.unwrap(),
        Some(Data {
            data: b"hello".to_vec(),
            content_type: "text/plain".into(),
            sha256: hello.into(),
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

    // Listed by prefix, in key order, with their sizes; a key too long for a file name too.
    let long = format!("docs/{}", "x".repeat(300));
    files.put("shop", &long, "l", "text/plain").await.unwrap();
    files
        .put("shop", "docs/b", "bb", "text/plain")
        .await
        .unwrap();
    let all = files.list("shop", "", "").await.unwrap();
    let keys: Vec<&str> = all.files.iter().map(|f| f.key.as_str()).collect();
    assert_eq!(keys, ["a.txt", "big.bin", "docs/b", long.as_str()]);
    assert_eq!(all.files[1].size, 5 << 20);
    assert_eq!(all.next, None);
    let docs = files.list("shop", "docs/", "docs/b").await.unwrap();
    assert_eq!(docs.files.len(), 1);
    assert_eq!(docs.files[0].key, long);
    assert_eq!(docs.files[0].content_type, "text/plain");

    // Each tenant has files of its own.
    let other = files.clone().tenant("other");
    assert_eq!(other.get("shop", "a.txt").await.unwrap(), None);
    other
        .put("shop", "a.txt", "theirs", "text/plain")
        .await
        .unwrap();
    assert_eq!(
        other.get("shop", "a.txt").await.unwrap().unwrap().data,
        b"theirs"
    );
    assert_eq!(
        files.get("shop", "a.txt").await.unwrap().unwrap().data,
        b"hello"
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

    fn refused<T: std::fmt::Debug>(result: Result<T, Error>) {
        match result {
            Err(Error::Status(s)) => {
                assert_eq!(s.code(), Code::PermissionDenied, "{s:?}");
                assert_eq!(s.message(), "forbidden: who are you");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
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

    // While it is away a write is refused, and not asked again: it may not run twice.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    match as_alice
        .data()
        .put("shop", "b.txt", "x", "text/plain")
        .await
    {
        Err(Error::Status(s)) => assert_eq!(s.code(), Code::Unavailable, "{s:?}"),
        other => panic!("expected UNAVAILABLE, got {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(2));
}
