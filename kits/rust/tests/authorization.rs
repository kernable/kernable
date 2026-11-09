//! The shared authorization fixtures (kits/fixtures/authorization.json): the middleware
//! learns a world of events, then decides on a check: allowed, or refused with a code. Every
//! language's kit decides the same way.

use std::path::PathBuf;

use kernable::{Check, CheckKind, RequestContext, Store, Target};
use kit::authorization::middleware::{Seen, check, learn};
use kit::authorization::policy::Knowledge;
use kit::authorization::{ids, relation};
use serde_json::Value;

/// Next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
fn fixtures() -> Value {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file = [
        here.join("fixtures/authorization.json"),
        here.join("../fixtures/authorization.json"),
    ]
    .into_iter()
    .find(|p| p.exists())
    .expect("the authorization fixtures");
    serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap()
}

fn text(v: &Value, field: &str) -> String {
    v[field].as_str().unwrap_or_default().to_owned()
}

fn learn_all(store: &Store, events: &[Value]) {
    let k = Knowledge { kv: store };
    for e in events {
        let r#type = format!("{}.{}", text(e, "domain"), text(e, "type"));
        let seen = Seen {
            r#type: &r#type,
            tenant: e["tenant"].as_str().unwrap(),
            aggregate_id: e["aggregateId"].as_str().unwrap(),
            domain: e["domain"].as_str().unwrap(),
            aggregate_type: e["aggregateType"].as_str().unwrap(),
        };
        learn(&k, &seen, e["data"].clone()).unwrap_or_else(|err| panic!("{type}: {err}"));
    }
}

fn check_of(c: &Value) -> Check {
    let r = &c["request"];
    let request = RequestContext {
        sender_tenant: text(r, "senderTenant"),
        sender_identity: text(r, "senderIdentity"),
        sender_account: text(r, "senderAccount"),
        sender_session: text(r, "senderSession"),
        on_behalf_of: text(r, "onBehalfOf"),
        bypass_authorization: r["bypassAuthorization"] == Value::Bool(true),
        ..RequestContext::default()
    };
    Check {
        kind: match c["kind"].as_str() {
            Some("command") => CheckKind::Command,
            Some("cancel") => CheckKind::Cancel,
            _ => CheckKind::Query,
        },
        id: String::new(),
        tenant: text(c, "tenant"),
        domain: text(c, "domain"),
        type_name: text(c, "type"),
        aggregate_type: c["aggregateType"].as_str().map(str::to_owned),
        aggregate_id: c["aggregateId"].as_str().map(str::to_owned),
        target: c.get("target").map(|t| Target {
            aggregate_type: text(t, "type"),
            aggregate_id: text(t, "id"),
        }),
        key: None,
        data: c["data"].clone(),
        request,
        outcome: None,
        store: None,
    }
}

#[test]
fn every_shared_authorization_case_decides_the_same() {
    let spec = fixtures();
    for r in spec["relations"].as_array().unwrap() {
        let field = text(r, "field");
        relation(r["type"].as_str().unwrap(), r["name"].as_str().unwrap())
            .set(r["event"].as_str().unwrap(), move |e| ids(&e.data[&field]));
    }
    let world = spec["world"].as_array().unwrap();
    let mut failed = vec![];
    let cases = spec["cases"].as_array().unwrap();
    for case in cases {
        let store = Store::open("memory:", "1").unwrap();
        learn_all(&store, world);
        learn_all(
            &store,
            case["extra"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default(),
        );
        let outcome = match check(&Some(store), &check_of(&case["check"])) {
            Ok(()) => "allow".to_owned(),
            Err(r) => r.code,
        };
        if outcome != text(case, "expect") {
            failed.push(format!(
                "{}: {outcome}, expected {}",
                text(case, "name"),
                text(case, "expect")
            ));
        }
    }
    eprintln!("{} authorization cases", cases.len());
    assert!(
        failed.is_empty(),
        "{} cases failed:\n{}",
        failed.len(),
        failed.join("\n")
    );
}
