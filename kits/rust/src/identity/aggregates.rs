//! The identity aggregates: state, how each event changes it, unique keys and facts.

use kernable::{Aggregate, Domain, Unique};
use serde::{Deserialize, Serialize};

use super::{IdentityKind, IdentityStatus, Scope, events::*};
use crate::shared::constants::SNAPSHOT_SCHEMA;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipStatus {
    #[default]
    Active,
    Suspended,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Membership {
    pub membership_id: String,
    pub group: String,
    pub scope: Option<Scope>,
    pub expires_at: Option<i64>,
    pub status: MembershipStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Key {
    pub key_id: String,
    pub hash: String,
    pub label: String,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

/// A person or service in one tenant.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Identity {
    pub id: String,
    pub status: IdentityStatus,
    pub kind: IdentityKind,
    pub account_id: Option<String>,
    /// A process identity's process: `shop.fulfilment`.
    #[serde(default)]
    pub process: Option<String>,
    pub memberships: Vec<Membership>,
    pub api_keys: Vec<Key>,
}

impl Identity {
    fn membership(&mut self, id: &str) -> Option<&mut Membership> {
        self.memberships.iter_mut().find(|m| m.membership_id == id)
    }
}

pub fn identity(domain: &Domain) -> Aggregate<Identity> {
    let a = domain
        .aggregate("identity", Identity::default())
        .schema(SNAPSHOT_SCHEMA);
    a.apply(IdentityCreated, |s, d, evt, _| {
        s.id = evt.aggregate_id.clone();
        s.status = IdentityStatus::Active;
        s.kind = d.kind;
        s.account_id = d.account_id;
        if d.kind == IdentityKind::Process {
            s.process = d.display_name;
        }
    });
    a.apply(ProfileUpdated, |_, _, _, _| {});
    a.apply(MembershipGranted, |s, d, _, _| {
        s.memberships.push(Membership {
            membership_id: d.membership_id,
            group: d.group,
            scope: d.scope,
            expires_at: d.expires_at,
            status: MembershipStatus::Active,
        })
    });
    a.apply(MembershipSuspended, |s, d, _, _| {
        if let Some(m) = s.membership(&d.membership_id) {
            m.status = MembershipStatus::Suspended;
        }
    });
    a.apply(MembershipResumed, |s, d, _, _| {
        if let Some(m) = s.membership(&d.membership_id) {
            m.status = MembershipStatus::Active;
        }
    });
    a.apply(MembershipRevoked, |s, d, _, _| {
        s.memberships.retain(|m| m.membership_id != d.membership_id)
    });
    a.apply(ApiKeyCreated, |s, d, _, _| {
        s.api_keys.push(Key {
            key_id: d.key_id,
            hash: d.hash,
            label: d.label,
            expires_at: d.expires_at,
            revoked: false,
        })
    });
    a.apply(ApiKeyRevoked, |s, d, _, _| {
        for k in s.api_keys.iter_mut().filter(|k| k.key_id == d.key_id) {
            k.revoked = true;
        }
    });
    a.apply(IdentitySuspended, |s, _, _, _| {
        s.status = IdentityStatus::Suspended
    });
    a.apply(IdentityReactivated, |s, _, _, _| {
        s.status = IdentityStatus::Active
    });
    a.apply(IdentityDeleted, |s, _, _, _| {
        s.status = IdentityStatus::Deleted;
        s.memberships.clear();
        s.api_keys.clear();
    });
    // One identity per account and tenant.
    a.unique(
        "account",
        |s| {
            s.account_id
                .clone()
                .filter(|_| !matches!(s.status, IdentityStatus::None | IdentityStatus::Deleted))
        },
        Unique::tenant(),
    );
    // One identity per process and tenant.
    a.unique(
        "process",
        |s| {
            s.process
                .clone()
                .filter(|_| !matches!(s.status, IdentityStatus::None | IdentityStatus::Deleted))
        },
        Unique::tenant(),
    );
    a.fact("active", |s| s.status == IdentityStatus::Active);
    a
}
