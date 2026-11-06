//! The shared fixtures (kits/fixtures): given these events, when this command, then these
//! events (or this rejection), and these unique keys and facts. The TypeScript and Python kits run
//! the same cases, so the kits write the same events.

use std::collections::BTreeMap;
use std::path::PathBuf;

use kernable::{
    Aggregate, CommandContext, Error, EventMeta, FoldEvent, RequestContext, State, Timestamp,
};
use serde_json::{Value, json};

/// Next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
fn fixtures_dir() -> PathBuf {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    [here.join("fixtures"), here.join("..").join("fixtures")]
        .into_iter()
        .find(|p| p.is_dir())
        .expect("the fixtures next to the kit")
}

/// `None` if `actual` matches `expected`; placeholders `<string>`, `<number>`, `<argon2>`, `<any>`.
fn matches(expected: &Value, actual: &Value, path: &str) -> Option<String> {
    if let Some(placeholder) = expected
        .as_str()
        .filter(|s| s.starts_with('<') && s.ends_with('>') && s.len() > 2)
    {
        let ok = match placeholder {
            "<any>" => true,
            "<string>" => actual.as_str().is_some_and(|s| !s.is_empty()),
            "<number>" => actual.is_number(),
            "<argon2>" => actual.as_str().is_some_and(|s| s.starts_with("$argon2id$")),
            _ => expected == actual,
        };
        return (!ok).then(|| format!("{path}: {actual} is not {placeholder}"));
    }
    match (expected, actual) {
        (Value::Array(e), Value::Array(a)) if e.len() == a.len() => e
            .iter()
            .zip(a)
            .enumerate()
            .find_map(|(i, (e, a))| matches(e, a, &format!("{path}[{i}]"))),
        (Value::Object(e), Value::Object(a)) => {
            let (mut ek, mut ak): (Vec<&String>, Vec<&String>) =
                (e.keys().collect(), a.keys().collect());
            ek.sort();
            ak.sort();
            if ek != ak {
                return Some(format!("{path}: fields {ak:?}, expected {ek:?}"));
            }
            e.iter()
                .find_map(|(k, v)| matches(v, &a[k], &format!("{path}.{k}")))
        }
        (Value::Number(e), Value::Number(a)) if e.as_f64() == a.as_f64() => None,
        _ => (expected != actual).then(|| format!("{path}: {actual}, expected {expected}")),
    }
}

type Runner = Box<dyn Fn(&str, &Value) -> Result<(), String>>;

/// Runs a case on `aggregate`: folds `given`, handles `when`, compares `then`/`rejected`/`index`.
fn runner<S: State>(aggregate: Aggregate<S>) -> Runner {
    Box::new(move |domain, case| {
        let id = case["aggregateId"].as_str().unwrap_or_default();
        let tenant = case["tenant"].as_str().unwrap_or_default();
        let fold = |events: &[Value], from: usize| -> Vec<FoldEvent> {
            events
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    let t = e["type"].as_str().unwrap_or_default();
                    let mut f = FoldEvent::new(t, e["data"].clone());
                    f.meta = EventMeta {
                        event_type: t.to_owned(),
                        version: (from + i + 1) as u64,
                        tenant: tenant.to_owned(),
                        domain: domain.to_owned(),
                        aggregate_type: aggregate.name().to_owned(),
                        aggregate_id: id.to_owned(),
                        command_id: format!("given-{i}"),
                    };
                    f
                })
                .collect()
        };
        let given = case["given"].as_array().cloned().unwrap_or_default();
        let state = aggregate
            .fold(fold(&given, 0), None)
            .map_err(|e| format!("given: {e}"))?;
        let name = case["name"].as_str().unwrap_or_default();
        let now = case["now"].as_i64().unwrap_or_default();
        let mut ctx = CommandContext::new(
            &format!("fixture-{name}"),
            RequestContext::default(),
            Timestamp(now),
        )
        .on(id, tenant);
        let when = &case["when"];
        let data = if when["data"].is_null() {
            json!({})
        } else {
            when["data"].clone()
        };
        let produced = match aggregate.handle(
            &state,
            when["type"].as_str().unwrap_or_default(),
            data,
            &mut ctx,
        ) {
            Err(Error::Rejected(r)) => {
                return match case["rejected"].as_str() {
                    Some(code) if code == r.code => Ok(()),
                    _ => Err(format!("rejected with {}: {}", r.code, r.message)),
                };
            }
            Err(e) => return Err(format!("failed: {e}")),
            Ok(events) => events,
        };
        if let Some(code) = case["rejected"].as_str() {
            return Err(format!(
                "expected rejected {code}, got {} events",
                produced.len()
            ));
        }
        let plain: Vec<Value> = produced
            .iter()
            .map(|e| json!({ "type": e.event_type, "data": e.data }))
            .collect();
        if let Some(m) = matches(&case["then"], &Value::Array(plain.clone()), "then") {
            return Err(m);
        }
        if !case["index"].is_null() {
            let after = aggregate
                .fold(fold(&plain, given.len()), Some(state))
                .map_err(|e| format!("then: {e}"))?;
            let held = aggregate.index(&after).map_err(|e| format!("index: {e}"))?;
            let keys: Vec<Value> = held
                .keys
                .iter()
                .map(|k| {
                    let mut key = json!({ "name": k.name, "value": k.value, "scope": if k.global { "global" } else { "tenant" } });
                    if let Some(subject) = &k.subject {
                        key["subject"] = json!(subject);
                    }
                    key
                })
                .collect();
            if let Some(m) = matches(
                &case["index"],
                &json!({ "keys": keys, "facts": held.facts }),
                "index",
            ) {
                return Err(m);
            }
        }
        Ok(())
    })
}

fn aggregates() -> BTreeMap<String, Runner> {
    let mut all: BTreeMap<String, Runner> = BTreeMap::new();
    let tenancy = kit::tenancy::tenancy();
    all.insert("tenancy.tenant".into(), runner(tenancy.tenant));
    let account = kit::account::account();
    all.insert("account.account".into(), runner(account.account));
    all.insert("account.session".into(), runner(account.session));
    all.insert("account.sso".into(), runner(account.sso));
    all.insert("group.group".into(), runner(kit::group::group().group));
    all.insert(
        "identity.identity".into(),
        runner(kit::identity::identity().identity),
    );
    all.insert(
        "invitation.invitation".into(),
        runner(kit::invitation::invitation().invitation),
    );
    all
}

#[test]
fn every_shared_case_passes() {
    let aggregates = aggregates();
    let (mut passed, mut skipped, mut failed) = (0, BTreeMap::<String, usize>::new(), Vec::new());
    let mut files: Vec<PathBuf> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    for file in files.iter().filter(|f| {
        f.extension().is_some_and(|x| x == "json") && !f.ends_with("authorization.json")
    }) {
        let spec: Value = serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
        let domain = spec["domain"].as_str().unwrap();
        for case in spec["cases"].as_array().unwrap() {
            let aggregate = format!("{domain}.{}", case["aggregate"].as_str().unwrap());
            let Some(run) = aggregates.get(&aggregate) else {
                *skipped.entry(aggregate).or_default() += 1;
                continue;
            };
            match run(domain, case) {
                Ok(()) => passed += 1,
                Err(why) => failed.push(format!(
                    "{domain}: {}: {why}",
                    case["name"].as_str().unwrap_or_default()
                )),
            }
        }
    }
    eprintln!("{passed} cases passed; not yet ported: {skipped:?}");
    assert!(
        failed.is_empty(),
        "{} cases failed:\n{}",
        failed.len(),
        failed.join("\n")
    );
    assert!(passed > 0);
}
