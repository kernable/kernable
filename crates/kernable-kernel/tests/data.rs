//! The data and cache services: signed, per-domain grants, and the values come back.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        CacheRequest, DataRequest, cache_service_client::CacheServiceClient,
        data_service_client::DataServiceClient,
    },
    signing, transport,
};
use tonic::Code;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn data(key: &SigningKey, op: &str, domain: &str, name: &str, bytes: &[u8]) -> DataRequest {
    let mut r = DataRequest {
        tenant_id: "t".into(),
        domain: domain.into(),
        key: name.into(),
        data: bytes.to_vec(),
        content_type: if op == "put" {
            "text/plain".into()
        } else {
            String::new()
        },
        issued_at: now_ms(),
        client_sig: None,
        protocol: kernable_core::PROTOCOL,
        context: None,
    };
    r.client_sig = Some(signing::sign(key, &signing::data_message(op, &r)));
    r
}

fn cache(key: &SigningKey, op: &str, name: &str, value: &[u8], ttl_ms: u64) -> CacheRequest {
    let mut r = CacheRequest {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: name.into(),
        value: value.to_vec(),
        ttl_ms,
        issued_at: now_ms(),
        client_sig: None,
        protocol: kernable_core::PROTOCOL,
        context: None,
    };
    r.client_sig = Some(signing::sign(key, &signing::cache_message(op, &r)));
    r
}

/// Key 7 may read, write and cache in `shop`; key 8 may only read its files.
async fn start(name: &str) -> Env {
    let env = common::start(name).await;
    let all: &[&str] = &["shop.*"];
    env.kernel
        .grant(grant(
            &key(7),
            &[("data_read", all), ("data_write", all), ("cache", all)],
        ))
        .unwrap();
    env.kernel
        .grant(grant(&key(8), &[("data_read", all)]))
        .unwrap();
    env
}

#[tokio::test]
async fn files_are_put_read_and_deleted_by_keys_granted_for_their_domain() {
    let env = start("data").await;
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut client = DataServiceClient::new(channel).max_decoding_message_size(32 << 20);
    let (writer, reader) = (key(7), key(8));

    let missing = client
        .get_data(data(&reader, "get", "shop", "a.txt", b""))
        .await
        .unwrap();
    assert!(!missing.into_inner().found);
    client
        .put_data(data(&writer, "put", "shop", "a.txt", b"hello"))
        .await
        .unwrap();
    let got = client
        .get_data(data(&reader, "get", "shop", "a.txt", b""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (got.found, got.data.as_slice(), got.content_type.as_str()),
        (true, &b"hello"[..], "text/plain")
    );

    // Files larger than gRPC's default message size go through.
    let big = vec![7u8; 5 << 20];
    client
        .put_data(data(&writer, "put", "shop", "big.bin", &big))
        .await
        .unwrap();
    let got = client
        .get_data(data(&writer, "get", "shop", "big.bin", b""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.data.len(), big.len());

    // Reading is not writing, and a grant covers its domain only.
    let err = client
        .put_data(data(&reader, "put", "shop", "a.txt", b"x"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    let err = client
        .get_data(data(&writer, "get", "billing", "a.txt", b""))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    // A signed get cannot be sent as a delete.
    let err = client
        .delete_data(data(&writer, "get", "shop", "a.txt", b""))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");

    assert!(
        client
            .delete_data(data(&writer, "delete", "shop", "a.txt", b""))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(
        !client
            .delete_data(data(&writer, "delete", "shop", "a.txt", b""))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(
        !client
            .get_data(data(&reader, "get", "shop", "a.txt", b""))
            .await
            .unwrap()
            .into_inner()
            .found
    );
}

#[tokio::test]
async fn cache_values_expire_and_need_a_grant() {
    let env = start("cache").await;
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut client = CacheServiceClient::new(channel);
    let k = key(7);

    client
        .set_cache(cache(&k, "set", "kept", b"1", 0))
        .await
        .unwrap();
    client
        .set_cache(cache(&k, "set", "brief", b"2", 50))
        .await
        .unwrap();
    let got = client
        .get_cache(cache(&k, "get", "brief", b"", 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((got.found, got.value.as_slice()), (true, &b"2"[..]));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !client
            .get_cache(cache(&k, "get", "brief", b"", 0))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(
        client
            .get_cache(cache(&k, "get", "kept", b"", 0))
            .await
            .unwrap()
            .into_inner()
            .found
    );

    assert!(
        client
            .delete_cache(cache(&k, "delete", "kept", b"", 0))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(
        !client
            .get_cache(cache(&k, "get", "kept", b"", 0))
            .await
            .unwrap()
            .into_inner()
            .found
    );

    let err = client
        .get_cache(cache(&key(8), "get", "kept", b"", 0))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
}
