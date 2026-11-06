//! identity: a person or service in one tenant, holding its group memberships
//! (design/kits.md "identity"). `identity()` registers the parts.

pub mod aggregates;
pub mod commands;
pub mod events;
pub mod queries;
pub mod reactors;
pub mod readmodels;

use kernable::{Aggregate, Domain, Listener, domain};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use aggregates::Identity;
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::IdentityRow;

use crate::shared::secrets::sha256;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityStatus {
    #[default]
    None,
    Active,
    Suspended,
    Deleted,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityKind {
    #[default]
    Person,
    Service,
}

/// One aggregate a membership is limited to (or what has it as parent): `{type: "shop.order", id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub r#type: String,
    pub id: String,
}

/// The identity as the subject of its personal data.
pub fn own(_d: &Value, identity_id: &str) -> String {
    format!("identity:{identity_id}")
}

/// An API key as its holder sends it: `key_<keyId>_<secret>`.
pub fn api_key(key_id: &str, secret: &str) -> String {
    format!("key_{key_id}_{secret}")
}

/// The key ID and the whole key's hash, or `None` if it has another shape.
pub fn parse_api_key(value: &str) -> Option<(String, String)> {
    let rest = value.strip_prefix("key_")?;
    let (key_id, secret) = rest.split_once('_')?;
    let ok = (8..=32).contains(&key_id.len())
        && key_id.bytes().all(|c| c.is_ascii_alphanumeric())
        && secret.chars().count() >= 32;
    ok.then(|| (key_id.to_owned(), sha256(value)))
}

/// The identity domain with its parts.
pub struct IdentityDomain {
    pub domain: Domain,
    pub identity: Aggregate<Identity>,
    pub identities: Listener,
}

/// The identity domain, every part registered.
pub fn identity() -> IdentityDomain {
    let identity_domain = domain("identity");
    let identity = aggregates::identity(&identity_domain);
    commands::handle(&identity);
    let (identities, state) = readmodels::identities(&identity_domain);
    queries::answer(&identities, &state);
    reactors::react(&identity_domain);
    IdentityDomain {
        domain: identity_domain,
        identity,
        identities,
    }
}
