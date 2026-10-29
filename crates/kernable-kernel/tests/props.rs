//! Property tests for the protocol: arbitrary submits, queries and subscriptions (malformed,
//! unsigned, forged, of unknown versions) are answered or refused, never crash or hang the
//! kernel, and leave it working. `PROPTEST_CASES=<n>` sets the number of cases (default 64).

mod common;

use std::time::Duration;

use common::*;
use kernable_core::{
    proto::{
        Command, QueryRequest, RequestContext, Signature, SubmitRequest, SubscribeRequest,
        command_service_client::CommandServiceClient, event_service_client::EventServiceClient,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use proptest::prelude::*;
use tokio_stream::StreamExt;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(64);
    ProptestConfig::with_cases(cases)
}

/// Names that are valid, empty, qualified, wildcards or odd.
fn name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("orders".to_string()),
        Just("PlaceOrder".to_string()),
        Just("AddItem".to_string()),
        Just(String::new()),
        Just("orders.*".to_string()),
        Just("*".to_string()),
        "[a-zA-Z0-9._*-]{0,12}",
        any::<String>(),
    ]
}

/// A signature that is valid for `msg`, made by a key without rights, forged, or missing.
fn signature(msg: &[u8], kind: u8) -> Option<Signature> {
    match kind % 4 {
        0 => Some(signing::sign(&key(1), msg)),
        1 => Some(signing::sign(&key(9), msg)),
        2 => Some(Signature {
            key_id: id(&key(1)),
            sig: vec![7; 64],
        }),
        _ => None,
    }
}

prop_compose! {
    fn arbitrary_command()(
        id in name(), tenant in name(), domain in name(), aggregate_type in name(),
        aggregate_id in name(), command_type in name(),
        payload in proptest::collection::vec(any::<u8>(), 0..64),
        expected_version in proptest::option::of(0u64..3),
        bypass in any::<bool>(),
        sig in any::<u8>(),
    ) -> Command {
        let mut cmd = Command {
            vault_refs: Vec::new(),
            command_id: id, tenant_id: tenant, domain, aggregate_type, aggregate_id, command_type,
            payload, client_sig: None,
            context: Some(RequestContext { expected_version, bypass_authorization: bypass, ..Default::default() }),
        };
        cmd.client_sig = signature(&signing::command_message(&cmd), sig);
        cmd
    }
}

#[derive(Debug)]
enum Call {
    Submit(SubmitRequest),
    Query(QueryRequest),
    Subscribe(SubscribeRequest),
}

fn call() -> impl Strategy<Value = Call> {
    let protocol = prop_oneof![Just(kernable_core::PROTOCOL), 0u32..4];
    prop_oneof![
        (arbitrary_command(), 0i32..5, protocol.clone(), name()).prop_map(
            |(cmd, wait, protocol, listener)| {
                Call::Submit(SubmitRequest {
                    vault: Vec::new(),
                    command: Some(cmd),
                    wait,
                    protocol,
                    listener,
                })
            }
        ),
        (
            name(),
            name(),
            name(),
            0u64..4,
            any::<u8>(),
            protocol.clone(),
            name(),
            -2i64..2
        )
            .prop_map(
                |(id, domain, query_type, min_position, sig, protocol, listener, skew)| {
                    let mut q = QueryRequest {
                        query_id: id,
                        tenant_id: "t1".into(),
                        domain,
                        query_type,
                        payload: b"{}".to_vec(),
                        min_position,
                        protocol,
                        listener,
                        issued_at: now_ms() + skew * 60_000,
                        ..Default::default()
                    };
                    q.client_sig = signature(&signing::query_message(&q), sig);
                    Call::Query(q)
                }
            ),
        (
            0u64..20,
            proptest::collection::vec(name(), 0..3),
            any::<u8>(),
            protocol
        )
            .prop_map(|(after, event_types, sig, protocol)| {
                let issued_at = now_ms();
                let msg = signing::subscribe_message(after, &event_types, issued_at);
                Call::Subscribe(SubscribeRequest {
                    after_position: after,
                    event_types,
                    issued_at,
                    protocol,
                    client_sig: signature(&msg, sig),
                })
            }),
    ]
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn send(addr: &str, call: Call) {
    let channel = transport::connect(addr).await.unwrap();
    match call {
        Call::Submit(r) => {
            let _ = CommandServiceClient::new(channel).submit(r).await;
        }
        Call::Query(q) => {
            let _ = QueryServiceClient::new(channel).query(q).await;
        }
        Call::Subscribe(s) => {
            if let Ok(stream) = EventServiceClient::new(channel).subscribe(s).await {
                // Read what is there; the stream itself stays open for live events.
                let mut stream = stream.into_inner();
                let _ = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
            }
        }
    }
}

#[test]
fn arbitrary_requests_never_crash_or_hang_the_kernel() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let env = rt.block_on(start_custom("props", |config, _| {
        config.submit_timeout_ms = 300;
        config.settle_timeout_ms = 300;
        config.query_timeout_ms = 300;
    }));
    rt.block_on(start_worker(env.addr.clone(), TestWorker::honest()));

    proptest!(config(), |(calls in proptest::collection::vec(call(), 1..6))| {
        rt.block_on(async {
            for call in calls {
                let answered = tokio::time::timeout(Duration::from_secs(3), send(&env.addr, call)).await;
                prop_assert!(answered.is_ok(), "the kernel did not answer");
            }
            Ok(())
        })?;
    });

    // Still working, and everything stored is intact.
    rt.block_on(async {
        let cmd = signed(command("after-props", "PlaceOrder", "fresh"), &key(1));
        assert_eq!(
            committed_versions(&submit(&env.addr, cmd).await.unwrap()),
            [1]
        );
    });
    let report = kernable_store::open_read_only(&env.store)
        .unwrap()
        .verify(&[id(&key(3)).to_string()])
        .unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
}
