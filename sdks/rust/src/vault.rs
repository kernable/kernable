//! Personal and secret fields: kept in the vault, with references in the signed payloads.

use std::collections::HashMap;

use kernable_core::{proto::VaultValue, signing};
use serde_json::{Map, Value};

use crate::domain::{Subject, encode};

/// Payload fields kept in the vault instead of the signed log.
#[derive(Clone, Default)]
pub(crate) struct VaultFields {
    pub personal: Vec<(Vec<String>, Subject)>,
    pub secret: Vec<String>,
}

pub(crate) struct Sealed {
    pub payload: Vec<u8>,
    pub refs: Vec<String>,
    pub values: Vec<VaultValue>,
}

/// Moves the vault fields of `data` into vault values: the payload keeps `{"$vault": ref}`.
/// Absent and null fields stay as they are.
pub(crate) fn seal(
    data: &Value,
    vault: Option<&VaultFields>,
    aggregate_id: &str,
) -> Result<Sealed, String> {
    let (Some(vault), Value::Object(fields)) = (vault, data) else {
        return Ok(Sealed {
            payload: encode(data),
            refs: Vec::new(),
            values: Vec::new(),
        });
    };
    if vault.personal.is_empty() && vault.secret.is_empty() {
        return Ok(Sealed {
            payload: encode(data),
            refs: Vec::new(),
            values: Vec::new(),
        });
    }
    let mut sealed: Map<String, Value> = fields.clone();
    let mut values = Vec::new();
    let mut keep = |field: &str, subject: &str, secret: bool| {
        let Some(v) = sealed.get(field).filter(|v| !v.is_null()) else {
            return;
        };
        let mut salt = vec![0u8; 16];
        getrandom::fill(&mut salt).expect("OS randomness unavailable");
        let value = serde_json::to_vec(v).expect("a JSON value encodes");
        let r#ref = signing::vault_ref(&salt, subject, secret, &value);
        sealed.insert(field.to_owned(), serde_json::json!({ "$vault": r#ref }));
        values.push(VaultValue {
            r#ref,
            subject: subject.to_owned(),
            secret,
            salt,
            value,
        });
    };
    for (names, subject) in &vault.personal {
        let who = subject(data, aggregate_id);
        if who.is_empty() {
            return Err(format!(
                "no subject for the personal fields {}",
                names.join(", ")
            ));
        }
        for f in names {
            keep(f, &who, false);
        }
    }
    for f in &vault.secret {
        keep(f, "", true);
    }
    Ok(Sealed {
        payload: encode(&Value::Object(sealed)),
        refs: values.iter().map(|v| v.r#ref.clone()).collect(),
        values,
    })
}

/// Puts vault values back into `data`; references without a value become `null`. Returns the
/// data and the paths of those fields.
pub(crate) fn unseal(data: Value, vault: &[VaultValue]) -> (Value, Vec<String>) {
    let values: HashMap<&str, &[u8]> = vault
        .iter()
        .map(|v| (v.r#ref.as_str(), v.value.as_slice()))
        .collect();
    let mut redacted = Vec::new();
    let data = walk(data, "", &values, &mut redacted);
    (data, redacted)
}

fn walk(v: Value, path: &str, values: &HashMap<&str, &[u8]>, redacted: &mut Vec<String>) -> Value {
    match v {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(i, x)| walk(x, &format!("{path}[{i}]"), values, redacted))
                .collect(),
        ),
        Value::Object(o) => {
            if o.len() == 1
                && let Some(Value::String(r)) = o.get("$vault")
            {
                return match values.get(r.as_str()) {
                    Some(value) => serde_json::from_slice(value).unwrap_or(Value::Null),
                    None => {
                        redacted.push(path.to_owned());
                        Value::Null
                    }
                };
            }
            Value::Object(
                o.into_iter()
                    .map(|(k, x)| {
                        let p = if path.is_empty() {
                            k.clone()
                        } else {
                            format!("{path}.{k}")
                        };
                        let x = walk(x, &p, values, redacted);
                        (k, x)
                    })
                    .collect(),
            )
        }
        other => other,
    }
}
