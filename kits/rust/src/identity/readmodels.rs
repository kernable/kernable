//! The identity read models: identities with their memberships and API keys.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::aggregates::MembershipStatus;
use super::{IdentityKind, IdentityStatus, Scope, events::*};
use crate::shared::constants::orders;
use crate::tenancy::TenantDeleted;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MembershipRow {
    pub membership_id: String,
    pub group: String,
    pub scope: Option<Scope>,
    pub expires_at: Option<i64>,
    pub status: MembershipStatus,
    pub granted_by: Option<String>,
    pub granted_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyRow {
    pub key_id: String,
    pub label: String,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityRow {
    pub id: String,
    pub tenant: String,
    pub kind: IdentityKind,
    pub account_id: Option<String>,
    pub display_name: Option<String>,
    pub status: IdentityStatus,
    pub memberships: Vec<MembershipRow>,
    pub api_keys: Vec<KeyRow>,
}

/// What the API checks a key against (FindApiKey).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyRow {
    pub tenant: String,
    pub identity_id: String,
    pub hash: String,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

#[derive(Default)]
pub struct Identities {
    /// `tenant/identityId`
    pub rows: BTreeMap<String, IdentityRow>,
    /// keyId → the key
    pub keys: HashMap<String, ApiKeyRow>,
}

pub type Shared = Arc<RwLock<Identities>>;

pub fn row_key(tenant: &str, identity_id: &str) -> String {
    format!("{tenant}/{identity_id}")
}

fn row<'a>(t: &'a mut Identities, evt: &kernable::StoredEventMeta) -> Option<&'a mut IdentityRow> {
    t.rows.get_mut(&row_key(&evt.tenant, &evt.aggregate_id))
}

fn set_membership(
    t: &mut Identities,
    evt: &kernable::StoredEventMeta,
    id: &str,
    status: MembershipStatus,
) {
    if let Some(r) = row(t, evt) {
        for m in r.memberships.iter_mut().filter(|m| m.membership_id == id) {
            m.status = status;
        }
    }
}

fn set_status(t: &mut Identities, evt: &kernable::StoredEventMeta, status: IdentityStatus) {
    if let Some(r) = row(t, evt) {
        r.status = status;
    }
}

pub fn identities(domain: &Domain) -> (Listener, Shared) {
    let state: Shared = Arc::default();
    let l = domain
        .read_model("identity.identities")
        .order(orders::READ_MODELS);
    on!(l, state, IdentityCreated, |t, d, evt| {
        let r = IdentityRow {
            id: evt.aggregate_id.clone(),
            tenant: evt.tenant.clone(),
            kind: d.kind,
            account_id: d.account_id,
            display_name: d.display_name,
            status: IdentityStatus::Active,
            memberships: vec![],
            api_keys: vec![],
        };
        t.rows.insert(row_key(&evt.tenant, &evt.aggregate_id), r)
    });
    on!(l, state, ProfileUpdated, |t, d, evt| {
        if let (Some(r), Some(name)) = (row(t, &evt), d.display_name) {
            r.display_name = Some(name);
        }
    });
    // Who granted it and when come from the event's request context and time, not its payload.
    {
        let state = Arc::clone(&state);
        l.on(MembershipGranted, move |d, evt, ctx| {
            let state = Arc::clone(&state);
            async move {
                let mut t = state.write().unwrap();
                if let Some(r) = row(&mut t, &evt) {
                    r.memberships.push(MembershipRow {
                        membership_id: d.membership_id,
                        group: d.group,
                        scope: d.scope,
                        expires_at: d.expires_at,
                        status: MembershipStatus::Active,
                        granted_by: Some(ctx.request.sender_identity.clone())
                            .filter(|s| !s.is_empty()),
                        granted_at: evt.created_at.millis(),
                    });
                }
                Ok(())
            }
        });
    }
    on!(l, state, MembershipSuspended, |t, d, evt| set_membership(
        t,
        &evt,
        &d.membership_id,
        MembershipStatus::Suspended
    ));
    on!(l, state, MembershipResumed, |t, d, evt| set_membership(
        t,
        &evt,
        &d.membership_id,
        MembershipStatus::Active
    ));
    on!(l, state, MembershipRevoked, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            r.memberships.retain(|m| m.membership_id != d.membership_id);
        }
    });
    on!(l, state, ApiKeyCreated, |t, d, evt| {
        let key = ApiKeyRow {
            tenant: evt.tenant.clone(),
            identity_id: evt.aggregate_id.clone(),
            hash: d.hash,
            expires_at: d.expires_at,
            revoked: false,
        };
        t.keys.insert(d.key_id.clone(), key);
        if let Some(r) = row(t, &evt) {
            r.api_keys.push(KeyRow {
                key_id: d.key_id,
                label: d.label,
                expires_at: d.expires_at,
                revoked: false,
            });
        }
    });
    on!(l, state, ApiKeyRevoked, |t, d, evt| {
        if let Some(k) = t.keys.get_mut(&d.key_id) {
            k.revoked = true;
        }
        if let Some(r) = row(t, &evt) {
            for k in r.api_keys.iter_mut().filter(|k| k.key_id == d.key_id) {
                k.revoked = true;
            }
        }
    });
    on!(l, state, IdentitySuspended, |t, _d, evt| set_status(
        t,
        &evt,
        IdentityStatus::Suspended
    ));
    on!(l, state, IdentityReactivated, |t, _d, evt| set_status(
        t,
        &evt,
        IdentityStatus::Active
    ));
    on!(l, state, IdentityDeleted, |t, _d, evt| t
        .rows
        .remove(&row_key(&evt.tenant, &evt.aggregate_id)));
    on!(l, state, TenantDeleted, |t, _d, evt| t
        .rows
        .retain(|_, r| r.tenant != evt.aggregate_id));
    (l, state)
}
