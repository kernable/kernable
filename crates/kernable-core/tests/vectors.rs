//! Checks the signing layout against `proto/test-vectors/signing.json`, which every SDK must pass too.
//! Regenerate after an intentional layout change with `KERNABLE_REGEN_VECTORS=1 cargo test -p kernable-core`.

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{Attribute, Command, Event, NewEvent, Rejected, RequestContext},
    signing::{self, GENESIS},
};
use serde_json::{Value, json};

const PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../proto/test-vectors/signing.json"
);

fn vectors() -> Value {
    let key = SigningKey::from_bytes(&[7; 32]);
    let seed = hex::encode([7u8; 32]);
    let entry = |input: Value, msg: Vec<u8>| {
        json!({
            "input": input,
            "message": hex::encode(&msg),
            "signature": hex::encode(signing::sign(&key, &msg).sig),
        })
    };

    let context = RequestContext {
        sender_tenant: "t-0".into(),
        sender_identity: "i-1".into(),
        sender_account: "a-1".into(),
        sender_session: "s-1".into(),
        on_behalf_of: "i-2".into(),
        workspace: "w-1".into(),
        expected_version: Some(3),
        correlation_id: "r-1".into(),
        causation_id: "c-0".into(),
        trace_id: "tr-1".into(),
        bypass_authorization: true,
        attributes: vec![
            Attribute {
                key: "channel".into(),
                value: "web".into(),
            },
            Attribute {
                key: "locale".into(),
                value: "de-AT".into(),
            },
        ],
    };
    let cmd = Command {
        command_id: "c-1".into(),
        tenant_id: "t-1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o-1".into(),
        command_type: "PlaceOrder".into(),
        payload: br#"{"customerId":"k-1"}"#.to_vec(),
        client_sig: None,
        context: Some(context.clone()),
    };
    let events = [
        NewEvent {
            event_type: "OrderPlaced".into(),
            payload: br#"{"customerId":"k-1"}"#.to_vec(),
            schema: 2,
        },
        NewEvent {
            event_type: "ItemAdded".into(),
            payload: br#"{"sku":"x"}"#.to_vec(),
            schema: 1,
        },
    ];
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "Order already exists".into(),
    };
    let nonce = [1u8; 16];
    let chain_hash = [2u8; 32];
    let producer_msg = signing::result_message(&cmd, 0, &events[..1]);
    let event = Event {
        global_position: 1,
        tenant_id: cmd.tenant_id.clone(),
        domain: cmd.domain.clone(),
        aggregate_type: cmd.aggregate_type.clone(),
        aggregate_id: cmd.aggregate_id.clone(),
        version: 1,
        event_type: events[0].event_type.clone(),
        schema: events[0].schema,
        payload: events[0].payload.clone(),
        command_id: cmd.command_id.clone(),
        created_at: 1_700_000_000_000,
        hash: vec![],
        producer_sig: Some(signing::sign(&key, &producer_msg)),
        context: Some(context.clone()),
    };
    let context_json = json!({
        "sender_tenant": context.sender_tenant, "sender_identity": context.sender_identity,
        "sender_account": context.sender_account, "sender_session": context.sender_session,
        "on_behalf_of": context.on_behalf_of, "workspace": context.workspace,
        "expected_version": context.expected_version, "correlation_id": context.correlation_id,
        "causation_id": context.causation_id, "trace_id": context.trace_id,
        "bypass_authorization": context.bypass_authorization,
        "attributes": context.attributes.iter().map(|a| json!({"key": a.key, "value": a.value})).collect::<Vec<_>>(),
    });
    let cmd_json = json!({
        "command_id": cmd.command_id, "tenant_id": cmd.tenant_id, "domain": cmd.domain,
        "aggregate_type": cmd.aggregate_type, "aggregate_id": cmd.aggregate_id,
        "command_type": cmd.command_type, "payload_hex": hex::encode(&cmd.payload),
        "context": context_json,
    });
    let events_json: Vec<Value> = events
        .iter()
        .map(|e| json!({"event_type": e.event_type, "schema": e.schema, "payload_hex": hex::encode(&e.payload)}))
        .collect();

    json!({
        "description": "Signing test vectors, see proto/SIGNING.md. Signatures are Ed25519 (deterministic) with `key.seed`.",
        "key": { "seed": seed, "key_id": signing::key_id(&key.verifying_key()) },
        "command": entry(cmd_json.clone(), signing::command_message(&cmd)),
        "result": entry(
            json!({"command": cmd_json, "expected_version": 0, "events": events_json}),
            signing::result_message(&cmd, 0, &events),
        ),
        "rejection": entry(
            json!({"command_id": cmd.command_id, "code": reason.code, "message": reason.message}),
            signing::rejection_message(&cmd.command_id, &reason),
        ),
        "snapshot": entry(
            json!({"tenant_id": cmd.tenant_id, "domain": cmd.domain, "aggregate_type": cmd.aggregate_type, "aggregate_id": cmd.aggregate_id,
                   "version": 100, "schema": "1", "state_hex": hex::encode(br#"{"items":3}"#)}),
            signing::snapshot_message(&cmd, 100, "1", br#"{"items":3}"#),
        ),
        "query": entry(
            json!({"query_id": "q-1", "tenant_id": cmd.tenant_id, "domain": "identity", "query_type": "GetAccount",
                   "payload_hex": hex::encode(br#"{"id":"a-1"}"#), "context": context_json, "min_position": 7,
                   "issued_at": 1_700_000_000_000i64}),
            signing::query_message(&kernable_core::proto::QueryRequest {
                protocol: kernable_core::PROTOCOL,
                query_id: "q-1".into(),
                tenant_id: cmd.tenant_id.clone(),
                domain: "identity".into(),
                query_type: "GetAccount".into(),
                payload: br#"{"id":"a-1"}"#.to_vec(),
                context: Some(context.clone()),
                min_position: 7,
                issued_at: 1_700_000_000_000,
                client_sig: None,
            }),
        ),
        "subscribe": entry(
            json!({"after_position": 7, "issued_at": 1_700_000_000_000i64, "event_types": ["OrderPlaced", "ItemAdded"]}),
            signing::subscribe_message(7, &["OrderPlaced".into(), "ItemAdded".into()], 1_700_000_000_000),
        ),
        "challenge": entry(json!({"nonce_hex": hex::encode(nonce)}), signing::challenge_message(&nonce)),
        "checkpoint": entry(
            json!({"global_position": 42, "chain_hash_hex": hex::encode(chain_hash), "created_at": 1_700_000_000_000i64}),
            signing::checkpoint_message(42, &chain_hash, 1_700_000_000_000),
        ),
        "derived_id": {
            "input": { "command_id": cmd.command_id, "n": [0, 1, 2] },
            "ids": (0..3).map(|n| signing::derived_id(&cmd.command_id, n)).collect::<Vec<_>>(),
        },
        "event_hash": {
            "input": {
                "prev_hex": hex::encode(GENESIS),
                "global_position": event.global_position, "tenant_id": event.tenant_id, "domain": event.domain,
                "aggregate_type": event.aggregate_type, "aggregate_id": event.aggregate_id,
                "version": event.version, "event_type": event.event_type, "schema": event.schema,
                "payload_hex": hex::encode(&event.payload), "command_id": event.command_id,
                "created_at": event.created_at,
                "producer_key_id": event.producer_sig.as_ref().unwrap().key_id,
                "producer_sig_hex": hex::encode(&event.producer_sig.as_ref().unwrap().sig),
                "context": context_json,
            },
            "hash": hex::encode(signing::event_hash(&GENESIS, &event)),
        },
    })
}

#[test]
fn signing_layout_matches_test_vectors() {
    let actual = vectors();
    if std::env::var_os("KERNABLE_REGEN_VECTORS").is_some() {
        std::fs::create_dir_all(std::path::Path::new(PATH).parent().unwrap()).unwrap();
        std::fs::write(PATH, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
    }
    let expected: Value = serde_json::from_str(&std::fs::read_to_string(PATH).unwrap()).unwrap();
    assert_eq!(
        actual, expected,
        "signing layout changed; see test docs to regenerate"
    );
}

#[test]
fn signatures_verify_and_reject_tampering() {
    let key = signing::generate_key();
    let msg = b"hello".to_vec();
    let mut sig = signing::sign(&key, &msg);
    assert_eq!(signing::verify(&sig, &msg), Ok(()));
    assert!(signing::verify(&sig, b"hellO").is_err());
    sig.key_id = signing::key_id(&signing::generate_key().verifying_key());
    assert!(signing::verify(&sig, &msg).is_err());
}
