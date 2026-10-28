//! Files and the cache: what is put comes back, deletes report whether there was something, and
//! a grant covers its domain only.

mod common;

use std::time::Duration;

use common::TestKernel;
use kernable::{Data, Error};
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
