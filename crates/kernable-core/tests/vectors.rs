//! Checks the signing layout against `proto/test-vectors/signing.json`, which every SDK must pass too.
//! Regenerate after an intentional layout change with `KERNABLE_REGEN_VECTORS=1 cargo test -p kernable-core`.

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Attribute, Command, Event, NewEvent, Permission, Rejected, RequestContext, Requirement,
        UniqueKey,
    },
    signing::{self, GENESIS},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

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
        vault_refs: Vec::new(),
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
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: "OrderPlaced".into(),
            payload: br#"{"customerId":"k-1"}"#.to_vec(),
            schema: 2,
            origin_partition: String::new(),
            origin_position: 0,
        },
        NewEvent {
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: "ItemAdded".into(),
            payload: br#"{"sku":"x"}"#.to_vec(),
            schema: 1,
            origin_partition: String::new(),
            origin_position: 0,
        },
    ];
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "Order already exists".into(),
        ..Default::default()
    };
    let nonce = [1u8; 16];
    let chain_hash = [2u8; 32];
    let producer_msg = signing::result_message(&cmd, 0, &events[..1]);
    let event = Event {
        erases: String::new(),
        vault_refs: Vec::new(),
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
        origin_partition: String::new(),
        origin_position: 0,
    };
    // The same command and events with vault references, and an event that erases a subject.
    let salt = [9u8; 16];
    let email = br#""max@firma.de""#;
    let vault_ref = signing::vault_ref(&salt, "k-1", false, email);
    let secret_ref = signing::vault_ref(&salt, "", true, br#""geheim""#);
    let vault_cmd = Command {
        vault_refs: vec![vault_ref.clone(), secret_ref.clone()],
        ..cmd.clone()
    };
    let mut vault_events = events.clone();
    vault_events[0].vault_refs = vec![vault_ref.clone()];
    vault_events[1].erases = "k-2".into();
    let vault_event = Event {
        vault_refs: vec![vault_ref.clone()],
        erases: "k-2".into(),
        ..event.clone()
    };
    let moved_event = Event {
        origin_partition: "p1".into(),
        origin_position: 42,
        ..event.clone()
    };

    // Unique keys (one personal, one global), facts and requirements of a result.
    let keys = [
        UniqueKey {
            name: "number".into(),
            global: false,
            value_hash: Sha256::digest(b"A-1001").to_vec(),
            subject: String::new(),
        },
        UniqueKey {
            name: "email".into(),
            global: true,
            value_hash: Sha256::digest(b"max@firma.de").to_vec(),
            subject: "k-1".into(),
        },
    ];
    let facts = ["open".to_owned(), "paid".to_owned()];
    let requires = [
        Requirement {
            domain: "identity".into(),
            aggregate_type: "Customer".into(),
            aggregate_id: "k-1".into(),
            fact: "active".into(),
            ..Default::default()
        },
        // By a unique key, and absent.
        Requirement {
            domain: "funds".into(),
            aggregate_type: "Vehicle".into(),
            fact: "woundUp".into(),
            key: "pmdxId".into(),
            value_hash: Sha256::digest(b"VEH-lyra").to_vec(),
            absent: true,
            ..Default::default()
        },
    ];
    let keys_json: Vec<Value> = keys
        .iter()
        .map(|k| json!({"name": k.name, "global": k.global, "value_hash_hex": hex::encode(&k.value_hash), "subject": k.subject}))
        .collect();
    let requires_json: Vec<Value> = requires
        .iter()
        .map(|r| json!({"domain": r.domain, "aggregate_type": r.aggregate_type, "aggregate_id": r.aggregate_id, "fact": r.fact, "key": r.key, "value_hash_hex": hex::encode(&r.value_hash), "absent": r.absent}))
        .collect();

    let context_json = json!({
        "sender_tenant": context.sender_tenant, "sender_identity": context.sender_identity,
        "sender_account": context.sender_account, "sender_session": context.sender_session,
        "on_behalf_of": context.on_behalf_of,
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
        "result_index": entry(
            json!({"command": cmd_json, "expected_version": 0, "keys": keys_json, "facts": facts, "requires": requires_json}),
            signing::index_message(&cmd, 0, &keys, &facts, &requires),
        ),
        "unique_key_id": {
            "input": {"tenant_id": cmd.tenant_id, "domain": cmd.domain, "aggregate_type": cmd.aggregate_type, "keys": keys_json},
            "ids": keys.iter().map(|k| hex::encode(signing::unique_key_id(&cmd, k))).collect::<Vec<_>>(),
        },
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
                listener: String::new(),
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
        "data_put": entry(
            json!({"op": "put", "tenant_id": "t-1", "domain": "shop", "key": "invoices/o-1.pdf", "content_type": "application/pdf",
                   "data_hex": hex::encode(b"%PDF-1.7"), "issued_at": 1_700_000_000_000i64, "context": context_json}),
            signing::data_message("put", &kernable_core::proto::DataRequest {
                tenant_id: "t-1".into(),
                domain: "shop".into(),
                key: "invoices/o-1.pdf".into(),
                data: b"%PDF-1.7".to_vec(),
                content_type: "application/pdf".into(),
                issued_at: 1_700_000_000_000,
                client_sig: None,
                protocol: kernable_core::PROTOCOL,
                context: Some(context.clone()),
            }),
        ),
        // Without a context: encoded like an empty one.
        "cache_set": entry(
            json!({"op": "set", "tenant_id": "t-1", "domain": "shop", "key": "rate/c-1", "value_hex": hex::encode(b"3"),
                   "ttl_ms": 60_000, "issued_at": 1_700_000_000_000i64}),
            signing::cache_message("set", &kernable_core::proto::CacheRequest {
                tenant_id: "t-1".into(),
                domain: "shop".into(),
                key: "rate/c-1".into(),
                value: b"3".to_vec(),
                ttl_ms: 60_000,
                issued_at: 1_700_000_000_000,
                client_sig: None,
                protocol: kernable_core::PROTOCOL,
                context: None,
            }),
        ),
        "challenge": entry(json!({"nonce_hex": hex::encode(nonce)}), signing::challenge_message(&nonce)),
        "revocation": entry(
            json!({"key_id": signing::key_id(&key.verifying_key()), "since": 1_700_000_000_000i64, "created_at": 1_700_000_100_000i64}),
            signing::revocation_message(&signing::key_id(&key.verifying_key()), 1_700_000_000_000, 1_700_000_100_000),
        ),
        "grant": entry(
            json!({"key_id": "8b84", "succeeds": "1f0a", "issued_at": 1_700_000_000_000i64,
                   "permissions": [{"action": "submit", "types": ["orders.*", "identity.Register"]}, {"action": "listen", "types": ["*"]}]}),
            signing::grant_message(&signing::grant(
                &key,
                "8b84",
                vec![
                    Permission { action: "submit".into(), types: vec!["orders.*".into(), "identity.Register".into()] },
                    Permission { action: "listen".into(), types: vec!["*".into()] },
                ],
                "1f0a",
                1_700_000_000_000,
            )),
        ),
        "membership": entry(
            json!({"service": "shop", "workers": ["shop.Cart", "shop.Order"], "listeners": ["carts"],
                   "middlewares": ["guard"], "metadata": [{"key": "version", "value": "1.0.0"}],
                   "issued_at": 1_700_000_000_000i64}),
            signing::membership_message(&kernable_core::proto::Membership {
                service: "shop".into(),
                workers: vec!["shop.Cart".into(), "shop.Order".into()],
                listeners: vec!["carts".into()],
                middlewares: vec!["guard".into()],
                metadata: vec![kernable_core::proto::Attribute { key: "version".into(), value: "1.0.0".into() }],
                issued_at: 1_700_000_000_000,
            }),
        ),
        "revoke": entry(
            json!({"key_id": "8b84", "since": 1_700_000_000_000i64, "issued_at": 1_700_000_100_000i64}),
            signing::revoke_message("8b84", 1_700_000_000_000, 1_700_000_100_000),
        ),
        "checkpoint": entry(
            json!({"global_position": 42, "chain_hash_hex": hex::encode(chain_hash), "created_at": 1_700_000_000_000i64}),
            signing::checkpoint_message(42, &chain_hash, 1_700_000_000_000),
        ),
        "derived_id": {
            "input": { "command_id": cmd.command_id, "n": [0, 1, 2] },
            "ids": (0..3).map(|n| signing::derived_id(&cmd.command_id, n)).collect::<Vec<_>>(),
        },
        "vault_ref": {
            "input": [
                {"salt_hex": hex::encode(salt), "subject": "k-1", "secret": false, "value_hex": hex::encode(email)},
                {"salt_hex": hex::encode(salt), "subject": "", "secret": true, "value_hex": hex::encode(br#""geheim""#)},
            ],
            "refs": [vault_ref, secret_ref],
        },
        "command_vault": entry(
            json!({"command": cmd_json, "vault_refs": vault_cmd.vault_refs}),
            signing::command_message(&vault_cmd),
        ),
        "result_vault": entry(
            json!({"vault_refs": [[vault_ref], []], "erases": ["", "k-2"]}),
            signing::result_message(&cmd, 0, &vault_events),
        ),
        "event_hash_vault": {
            "input": {"vault_refs": [vault_ref], "erases": "k-2"},
            "hash": hex::encode(signing::event_hash(&GENESIS, &vault_event)),
        },
        "event_hash_origin": {
            "input": {"origin_partition": "p1", "origin_position": 42},
            "hash": hex::encode(signing::event_hash(&GENESIS, &moved_event)),
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

#[test]
fn the_event_hash_covers_vault_refs_and_erasures_each_on_their_own() {
    let plain = Event::default();
    let refs = Event {
        vault_refs: vec!["r".into()],
        ..Default::default()
    };
    let erases = Event {
        erases: "k-1".into(),
        ..Default::default()
    };
    let [p, r, e] = [&plain, &refs, &erases].map(|e| signing::event_hash(&GENESIS, e));
    assert!(p != r && p != e && r != e);
}
