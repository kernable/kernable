//! Partitions: two kernels share the tenants by a signed map; each refuses the
//! tenants of the other with `wrong_partition`, and the second takes the map, grants,
//! revocations and memberships from the control partition.

mod common;

use std::time::Duration;

use common::*;
use kernable_core::{
    proto::{
        Command, MapRequest, Partition, PartitionMap, TenantPartition,
        partition_service_client::PartitionServiceClient,
    },
    signing, transport,
};
use kernable_kernel::config::PartitionConfig;
use prost::Message;
use tonic::Code;

async fn until(what: &str, done: impl AsyncFn() -> bool) {
    for _ in 0..500 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting: {what}");
}

fn map(epoch: u64, p1: &str, p2: &str) -> PartitionMap {
    let mut map = PartitionMap {
        epoch,
        partitions: vec![
            Partition {
                name: "p1".into(),
                addresses: vec![p1.into()],
            },
            Partition {
                name: "p2".into(),
                addresses: vec![p2.into()],
            },
        ],
        tenants: vec![TenantPartition {
            tenant: "acme".into(),
            partition: "p2".into(),
        }],
        default: "p1".into(),
        control: "p1".into(),
        issued_at: issued_now(),
        sig: None,
        protocol: kernable_core::PROTOCOL,
    };
    map.sig = Some(signing::sign(&key(1), &signing::partitions_message(&map)));
    map
}

async fn apply_map(addr: &str, map: PartitionMap) -> Result<(), tonic::Status> {
    PartitionServiceClient::new(transport::connect(addr).await.unwrap())
        .apply(with_grant(map, &key(1)))
        .await
        .map(|_| ())
}

async fn map_of(addr: &str) -> Option<PartitionMap> {
    PartitionServiceClient::new(transport::connect(addr).await.unwrap())
        .map(with_grant(
            MapRequest {
                protocol: kernable_core::PROTOCOL,
            },
            &key(1),
        ))
        .await
        .ok()
        .map(tonic::Response::into_inner)
}

/// An order of `tenant`, signed by the client key.
fn place(id: &str, tenant: &str) -> Command {
    let mut cmd = command(id, "PlaceOrder", id);
    cmd.tenant_id = tenant.into();
    signed(cmd, &key(1))
}

#[tokio::test(flavor = "multi_thread")]
async fn two_partitions_share_the_tenants_by_the_map_of_the_control_partition() {
    let p1 = start_custom("part-p1", |config, _| {
        config.partition = Some(PartitionConfig {
            name: "p1".into(),
            control: None,
            node_key: "unused.key".into(),
        });
    })
    .await;
    let p1_addr = p1.addr.clone();
    let p2 = start_custom("part-p2", |config, dir| {
        let node_key = dir.join("node.key");
        std::fs::write(&node_key, hex(key(13).as_bytes())).unwrap();
        let grant = grant(&key(13), &[("replicate", &["*"])]);
        std::fs::write(dir.join("node.grant"), hex(&grant.encode_to_vec())).unwrap();
        config.partition = Some(PartitionConfig {
            name: "p2".into(),
            control: Some(p1_addr.clone()),
            node_key,
        });
    })
    .await;
    spawn_worker(&p1).await;
    spawn_worker(&p2).await;

    // No map yet: no partition takes a tenant, so none can take one twice.
    let refused = submit(&p1.addr, place("c0", "t1")).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().starts_with("wrong_partition"),
        "{refused:?}"
    );

    // The map is applied on the control partition, and p2 takes it from there.
    let refused = apply_map(&p2.addr, map(1, &p1.addr, &p2.addr))
        .await
        .unwrap_err();
    assert_eq!(
        refused.code(),
        Code::FailedPrecondition,
        "only on the control partition"
    );
    apply_map(&p1.addr, map(1, &p1.addr, &p2.addr))
        .await
        .unwrap();
    until("p2 holds the map", async || {
        map_of(&p2.addr).await.is_some_and(|m| m.epoch == 1)
    })
    .await;
    let older = apply_map(&p1.addr, map(1, &p1.addr, &p2.addr))
        .await
        .unwrap_err();
    assert_eq!(
        older.code(),
        Code::FailedPrecondition,
        "the epoch must grow"
    );

    // Each partition serves its tenants and points to the other for the rest.
    assert_eq!(
        committed_versions(&submit(&p1.addr, place("c1", "t1")).await.unwrap()),
        [1]
    );
    assert_eq!(
        committed_versions(&submit(&p2.addr, place("c2", "acme")).await.unwrap()),
        [1]
    );
    let elsewhere = submit(&p2.addr, place("c3", "t1")).await.unwrap_err();
    assert_eq!(elsewhere.code(), Code::FailedPrecondition);
    assert!(
        elsewhere.message().contains("is on partition p1"),
        "{elsewhere:?}"
    );
    let elsewhere = submit(&p1.addr, place("c4", "acme")).await.unwrap_err();
    assert!(
        elsewhere.message().contains("is on partition p2"),
        "{elsewhere:?}"
    );

    // An SDK client given only p1 finds acme's partition, and subscribes to both.
    let sdk_key = p1.dir.join("sdk.key");
    std::fs::write(&sdk_key, hex(key(1).as_bytes())).unwrap();
    std::fs::write(
        p1.dir.join("sdk.grant"),
        hex(&default_grant(&key(1)).unwrap().encode_to_vec()),
    )
    .unwrap();
    let sdk = kernable::Kernable::new(kernable::Options::new(&sdk_key).url(&p1.addr)).unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let subscription = {
        let seen = std::sync::Arc::clone(&seen);
        sdk.subscribe(
            kernable::SubscribeOptions {
                types: vec!["orders.*".into()],
                ..Default::default()
            },
            move |e| {
                seen.lock().unwrap().push(e.tenant.clone());
                async { Ok(()) }
            },
        )
    };
    for tenant in ["acme", "t1"] {
        let placed = sdk
            .submit(
                kernable::Submit::new("orders", "Order", &format!("sdk-{tenant}"), "PlaceOrder")
                    .tenant(tenant)
                    .data(serde_json::json!({}))
                    .wait(kernable::Wait::Committed),
            )
            .await
            .unwrap();
        assert!(placed.is_committed(), "{tenant}: {placed:?}");
    }
    until("the subscription saw both partitions", async || {
        let seen = seen.lock().unwrap();
        seen.iter().any(|t| t == "acme") && seen.iter().any(|t| t == "t1")
    })
    .await;
    subscription.stop().await;

    // A membership applied on the control partition reaches p2.
    apply(&p1.addr, membership("shop", &["orders.Order"], &[], &[]))
        .await
        .unwrap();
    let p2_store = p2.store.clone();
    until("p2 holds the membership", async || {
        kernable_store::open_read_only(&p2_store)
            .unwrap()
            .memberships()
            .unwrap()
            .iter()
            .any(|m| m.membership.as_ref().is_some_and(|m| m.service == "shop"))
    })
    .await;

    // A key revoked on the control partition is refused on p2 within seconds.
    p1.kernel.revoke(&id(&key(1)), 0).unwrap();
    let p2_addr = p2.addr.clone();
    until("p2 refuses the revoked key", async || {
        submit(&p2_addr, place("c5", "acme"))
            .await
            .is_err_and(|e| matches!(e.code(), Code::PermissionDenied | Code::Unauthenticated))
    })
    .await;
}

/// Files a global unique key `email`, from the command's payload `{"email": ...}`.
fn by_email(
    cmd: &Command,
) -> (
    Vec<kernable_core::proto::UniqueKey>,
    Vec<String>,
    Vec<kernable_core::proto::Requirement>,
) {
    use sha2::Digest;
    let p: serde_json::Value = serde_json::from_slice(&cmd.payload).unwrap_or_default();
    let keys = p["email"]
        .as_str()
        .map(|email| kernable_core::proto::UniqueKey {
            name: "email".into(),
            value_hash: sha2::Sha256::digest(email).to_vec(),
            global: true,
            subject: String::new(),
        })
        .into_iter()
        .collect();
    (keys, Vec::new(), Vec::new())
}

fn with_email(id: &str, command_type: &str, order: &str, tenant: &str, email: &str) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.tenant_id = tenant.into();
    cmd.payload = serde_json::json!({ "email": email })
        .to_string()
        .into_bytes();
    signed(cmd, &key(1))
}

fn node_key(dir: &std::path::Path) -> std::path::PathBuf {
    let node_key = dir.join("node.key");
    std::fs::write(&node_key, hex(key(13).as_bytes())).unwrap();
    let grant = grant(&key(13), &[("replicate", &["*"])]);
    std::fs::write(dir.join("node.grant"), hex(&grant.encode_to_vec())).unwrap();
    node_key
}

#[tokio::test(flavor = "multi_thread")]
async fn a_global_key_is_unique_across_partitions() {
    let p1 = start_custom("gk-p1", |config, dir| {
        config.partition = Some(PartitionConfig {
            name: "p1".into(),
            control: None,
            node_key: node_key(dir),
        });
    })
    .await;
    let p1_addr = p1.addr.clone();
    let p2 = start_custom("gk-p2", |config, dir| {
        config.partition = Some(PartitionConfig {
            name: "p2".into(),
            control: Some(p1_addr.clone()),
            node_key: node_key(dir),
        });
    })
    .await;
    // Both node keys are known to both partitions before anything asks.
    p1.kernel
        .grant(grant(&key(13), &[("replicate", &["*"])]))
        .unwrap();
    p2.kernel
        .grant(grant(&key(13), &[("replicate", &["*"])]))
        .unwrap();
    apply_map(&p1.addr, map(1, &p1.addr, &p2.addr))
        .await
        .unwrap();
    until("p2 holds the map", async || {
        map_of(&p2.addr).await.is_some()
    })
    .await;
    for env in [&p1, &p2] {
        let worker = TestWorker {
            index: Some(std::sync::Arc::new(by_email)),
            ..TestWorker::honest()
        };
        start_worker(env.addr.clone(), worker).await;
    }
    let taken = |r: &kernable_core::proto::CommandResult| matches!(&r.outcome, Some(kernable_core::proto::command_result::Outcome::Rejected(x)) if x.code == "unique_taken");

    // t1 lives on p1 (the owner), acme on p2.
    let a1 = submit(&p1.addr, with_email("g1", "PlaceOrder", "o1", "t1", "a@x"))
        .await
        .unwrap();
    assert_eq!(committed_versions(&a1), [1]);
    let a2 = submit(
        &p2.addr,
        with_email("g2", "PlaceOrder", "o2", "acme", "a@x"),
    )
    .await
    .unwrap();
    assert!(taken(&a2), "held on p1: {a2:?}");
    let b2 = submit(
        &p2.addr,
        with_email("g3", "PlaceOrder", "o3", "acme", "b@x"),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&b2), [1]);
    let b1 = submit(&p1.addr, with_email("g4", "PlaceOrder", "o4", "t1", "b@x"))
        .await
        .unwrap();
    assert!(taken(&b1), "held on p2, the owner asked it: {b1:?}");

    // o1 changes its email: a@x is free again, and p2 gets it.
    let moved = submit(&p1.addr, with_email("g5", "AddItem", "o1", "t1", "c@x"))
        .await
        .unwrap();
    assert_eq!(committed_versions(&moved), [2]);
    let a2 = submit(
        &p2.addr,
        with_email("g6", "PlaceOrder", "o5", "acme", "a@x"),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&a2), [1], "a@x was released on p1");

    // Both partitions want the same new key at once: exactly one gets it.
    for i in 0..20 {
        let email = format!("race-{i}@x");
        let (one, two) = tokio::join!(
            submit(
                &p1.addr,
                with_email(
                    &format!("r1-{i}"),
                    "PlaceOrder",
                    &format!("r1-{i}"),
                    "t1",
                    &email
                )
            ),
            submit(
                &p2.addr,
                with_email(
                    &format!("r2-{i}"),
                    "PlaceOrder",
                    &format!("r2-{i}"),
                    "acme",
                    &email
                )
            ),
        );
        let won = [one.unwrap(), two.unwrap()]
            .iter()
            .filter(|r| !taken(r))
            .count();
        assert_eq!(won, 1, "{email}: exactly one partition holds it");
    }
}
