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

/// The outcome of a call, by its code.
fn code<T>(r: Result<tonic::Response<T>, tonic::Status>) -> Result<(), Code> {
    r.map(|_| ()).map_err(|e| e.code())
}

/// Signs `r` again, as issued at `issued_at`.
fn data_at(key: &SigningKey, op: &str, mut r: DataRequest, issued_at: i64) -> DataRequest {
    r.issued_at = issued_at;
    r.client_sig = Some(signing::sign(key, &signing::data_message(op, &r)));
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_are_held_to_their_window_key_length_and_size() {
    let env = start("data-limits").await;
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut files = DataServiceClient::new(channel.clone()).max_decoding_message_size(32 << 20);
    let mut cache_client = CacheServiceClient::new(channel);
    let writer = key(7);

    // A put says it found its place.
    let put = files
        .put_data(data(&writer, "put", "shop", "a.txt", b"x"))
        .await
        .unwrap();
    assert!(put.into_inner().found);

    // Signed too long ago or too far ahead.
    for skew in [-120_000, 120_000] {
        let r = data(&writer, "put", "shop", "late.txt", b"x");
        let r = data_at(&writer, "put", r, now_ms() + skew);
        assert_eq!(
            code(files.put_data(r).await),
            Err(Code::Unauthenticated),
            "{skew}"
        );
        let mut c = cache(&writer, "set", "late", b"x", 0);
        c.issued_at = now_ms() + skew;
        c.client_sig = Some(signing::sign(&writer, &signing::cache_message("set", &c)));
        assert_eq!(
            code(cache_client.set_cache(c).await),
            Err(Code::Unauthenticated),
            "{skew}"
        );
    }

    // A key has 1 to 512 bytes.
    let longest = "k".repeat(512);
    for (name, expected) in [
        ("", Err(Code::InvalidArgument)),
        (&"k".repeat(513), Err(Code::InvalidArgument)),
        (&longest, Ok(())),
    ] {
        let r = data(&writer, "put", "shop", name, b"x");
        assert_eq!(
            code(files.put_data(r).await),
            expected,
            "{} bytes",
            name.len()
        );
    }

    // A file of 16 MiB and a cache value of 1 MiB fit, a byte more does not.
    let r = data(&writer, "put", "shop", "full.bin", &vec![1; 16 << 20]);
    assert_eq!(code(files.put_data(r).await), Ok(()));
    let r = data(&writer, "put", "shop", "over.bin", &vec![1; (16 << 20) + 1]);
    assert_eq!(code(files.put_data(r).await), Err(Code::InvalidArgument));
    let c = cache(&writer, "set", "full", &vec![1; 1 << 20], 0);
    assert_eq!(code(cache_client.set_cache(c).await), Ok(()));
    let c = cache(&writer, "set", "over", &vec![1; (1 << 20) + 1], 0);
    assert_eq!(
        code(cache_client.set_cache(c).await),
        Err(Code::InvalidArgument)
    );
}
