//! X4: the signed byte layouts are injective. Two messages that differ in a signed field never
//! give the same bytes to sign, so a signature over one never passes for the other. Fields come
//! from a tiny alphabet, so that shifted boundaries (`"ab" + "c"` against `"a" + "bc"`) and
//! empty fields come up often.

use kernable_core::{
    proto::{
        Attribute, Command, Grant, Membership, NewEvent, Permission, QueryRequest, RequestContext,
        Requirement, UniqueKey,
    },
    signing,
};
use proptest::prelude::*;

fn text() -> impl Strategy<Value = String> {
    "[ab]{0,3}"
}

fn texts() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(text(), 0..3)
}

fn context() -> impl Strategy<Value = Option<RequestContext>> {
    let attributes = prop::collection::vec(
        (text(), text()).prop_map(|(key, value)| Attribute { key, value }),
        0..2,
    );
    let context = (
        (text(), text(), text()),
        prop::option::of(0u64..2),
        (text(), any::<bool>()),
        attributes,
    )
        .prop_map(
            |((tenant, identity, trace), expected_version, (correlation, bypass), attributes)| {
                RequestContext {
                    sender_tenant: tenant,
                    sender_identity: identity,
                    trace_id: trace,
                    expected_version,
                    correlation_id: correlation,
                    bypass_authorization: bypass,
                    attributes,
                    ..Default::default()
                }
            },
        );
    prop::option::of(context)
}

fn command() -> impl Strategy<Value = Command> {
    (
        (text(), text(), text(), text()),
        (text(), text(), text()),
        context(),
        texts(),
    )
        .prop_map(
            |(
                (id, tenant, domain, aggregate_type),
                (aggregate_id, command_type, payload),
                context,
                refs,
            )| {
                Command {
                    deliver_at: 0,
                    deliver_until: 0,
                    command_id: id,
                    tenant_id: tenant,
                    domain,
                    aggregate_type,
                    aggregate_id,
                    command_type,
                    payload: payload.into_bytes(),
                    context,
                    vault_refs: refs,
                    client_sig: None,
                }
            },
        )
}

/// What `command_message` covers: every field but the signature; no context is an empty one.
fn signed_command(c: &Command) -> Command {
    Command {
        context: Some(c.context.clone().unwrap_or_default()),
        client_sig: None,
        ..c.clone()
    }
}

fn event() -> impl Strategy<Value = NewEvent> {
    (text(), 1u32..3, text(), texts(), text()).prop_map(
        |(event_type, schema, payload, vault_refs, erases)| NewEvent {
            event_type,
            schema,
            payload: payload.into_bytes(),
            vault_refs,
            erases,
            origin_partition: String::new(),
            origin_position: 0,
        },
    )
}

fn unique_key() -> impl Strategy<Value = UniqueKey> {
    (text(), any::<bool>(), text(), text()).prop_map(|(name, global, value_hash, subject)| {
        UniqueKey {
            name,
            global,
            value_hash: value_hash.into_bytes(),
            subject,
        }
    })
}

fn requirement() -> impl Strategy<Value = Requirement> {
    (
        (text(), text(), text(), text()),
        (
            text(),
            prop::collection::vec(any::<u8>(), 0..3),
            any::<bool>(),
        ),
    )
        .prop_map(
            |((domain, aggregate_type, aggregate_id, fact), (key, value_hash, absent))| {
                Requirement {
                    domain,
                    aggregate_type,
                    aggregate_id,
                    fact,
                    key,
                    value_hash,
                    absent,
                }
            },
        )
}

fn grant() -> impl Strategy<Value = Grant> {
    let permission = (text(), texts()).prop_map(|(action, types)| Permission { action, types });
    (
        text(),
        text(),
        0i64..2,
        prop::collection::vec(permission, 0..3),
    )
        .prop_map(|(key_id, succeeds, issued_at, permissions)| Grant {
            key_id,
            succeeds,
            issued_at,
            permissions,
            root_sig: None,
        })
}

fn membership() -> impl Strategy<Value = Membership> {
    let metadata = prop::collection::vec(
        (text(), text()).prop_map(|(key, value)| Attribute { key, value }),
        0..2,
    );
    (text(), texts(), texts(), texts(), metadata, 0i64..2).prop_map(
        |(service, workers, listeners, middlewares, metadata, issued_at)| Membership {
            processes: Vec::new(),
            service,
            workers,
            listeners,
            middlewares,
            metadata,
            issued_at,
        },
    )
}

fn query() -> impl Strategy<Value = QueryRequest> {
    (
        (text(), text(), text(), text()),
        text(),
        context(),
        (0u64..2, 0i64..2, proptest::bool::ANY),
    )
        .prop_map(
            |(
                (id, tenant, domain, query_type),
                payload,
                context,
                (min_position, issued_at, wait_only),
            )| {
                QueryRequest {
                    query_id: id,
                    tenant_id: tenant,
                    domain,
                    query_type,
                    payload: payload.into_bytes(),
                    context: Some(context.unwrap_or_default()),
                    min_position,
                    issued_at,
                    wait_only,
                    ..Default::default()
                }
            },
        )
}

/// `PROPTEST_CASES=<n>` cases per message type (default 4000).
fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(4_000);
    ProptestConfig::with_cases(cases)
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn commands(a in command(), b in command()) {
        let same = signing::command_message(&a) == signing::command_message(&b);
        prop_assert_eq!(same, signed_command(&a) == signed_command(&b));
    }

    #[test]
    fn results(
        a in command(), b in command(),
        va in 0u64..2, vb in 0u64..2,
        ea in prop::collection::vec(event(), 0..3),
        eb in prop::collection::vec(event(), 0..3),
    ) {
        let same = signing::result_message(&a, va, &ea) == signing::result_message(&b, vb, &eb);
        // The stream fields of the command, the version and the events are signed.
        let stream = |c: &Command| (
            c.command_id.clone(), c.tenant_id.clone(), c.domain.clone(),
            c.aggregate_type.clone(), c.aggregate_id.clone(),
        );
        prop_assert_eq!(same, (stream(&a), va, &ea) == (stream(&b), vb, &eb));
    }

    #[test]
    fn indexes(
        a in command(), b in command(),
        va in 0u64..2, vb in 0u64..2,
        ka in prop::collection::vec(unique_key(), 0..3),
        kb in prop::collection::vec(unique_key(), 0..3),
        fa in texts(), fb in texts(),
        ra in prop::collection::vec(requirement(), 0..2),
        rb in prop::collection::vec(requirement(), 0..2),
    ) {
        let same = signing::index_message(&a, va, &ka, &fa, &ra)
            == signing::index_message(&b, vb, &kb, &fb, &rb);
        let stream = |c: &Command| (
            c.command_id.clone(), c.tenant_id.clone(), c.domain.clone(),
            c.aggregate_type.clone(), c.aggregate_id.clone(),
        );
        prop_assert_eq!(same, (stream(&a), va, &ka, &fa, &ra) == (stream(&b), vb, &kb, &fb, &rb));
    }

    #[test]
    fn grants(a in grant(), b in grant()) {
        let same = signing::grant_message(&a) == signing::grant_message(&b);
        prop_assert_eq!(same, a == b);
    }

    #[test]
    fn memberships(a in membership(), b in membership()) {
        let same = signing::membership_message(&a) == signing::membership_message(&b);
        prop_assert_eq!(same, a == b);
    }

    #[test]
    fn queries(a in query(), b in query()) {
        let same = signing::query_message(&a) == signing::query_message(&b);
        prop_assert_eq!(same, a == b);
    }
}
