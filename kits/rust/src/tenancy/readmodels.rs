//! The tenancy read models: one row per tenant; a deleted tenant leaves the read side.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::{SignUpPolicy, Suspension, TenantStatus, events::*};
use crate::shared::constants::orders;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantRow {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub status: TenantStatus,
    pub suspension: Option<Suspension>,
    pub mfa_required: bool,
    pub owners: Vec<String>,
}

/// What the read model holds: the rows by tenant ID, and the platform's sign-up policy.
#[derive(Default)]
pub struct Tenants {
    pub rows: BTreeMap<String, TenantRow>,
    pub sign_up: SignUpPolicy,
}

pub type Shared = Arc<RwLock<Tenants>>;

pub fn tenants(domain: &Domain) -> (Listener, Shared) {
    let state: Shared = Arc::default();
    let listener = domain
        .read_model("tenancy.tenants")
        .order(orders::READ_MODELS);
    let row = |t: &mut Tenants, id: &str, f: &dyn Fn(&mut TenantRow)| {
        if let Some(r) = t.rows.get_mut(id) {
            f(r);
        }
    };
    on!(listener, state, TenantCreated, |t, d, evt| {
        let id = evt.aggregate_id;
        let row = TenantRow {
            id: id.clone(),
            slug: d.slug,
            name: d.name,
            status: d.status,
            suspension: None,
            mfa_required: false,
            owners: vec![],
        };
        t.rows.insert(id, row)
    });
    on!(listener, state, TenantActivated, |t, _d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.status = TenantStatus::Active
    ));
    on!(listener, state, TenantRenamed, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.name = d.name.clone()
    ));
    on!(listener, state, TenantSlugChanged, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.slug = d.slug.clone()
    ));
    on!(listener, state, TenantSuspended, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| {
            r.status = TenantStatus::Suspended;
            r.suspension = Some(Suspension {
                reason: d.reason,
                at: d.at,
            });
        }
    ));
    on!(listener, state, TenantReactivated, |t, _d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| {
            r.status = TenantStatus::Active;
            r.suspension = None;
        }
    ));
    on!(listener, state, TenantDeleted, |t, _d, evt| t
        .rows
        .remove(&evt.aggregate_id));
    on!(listener, state, OwnerAdded, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.owners.push(d.identity_id.clone())
    ));
    on!(listener, state, OwnerRemoved, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.owners.retain(|o| *o != d.identity_id)
    ));
    on!(listener, state, MfaRequirementChanged, |t, d, evt| row(
        t,
        &evt.aggregate_id,
        &|r| r.mfa_required = d.required
    ));
    on!(listener, state, SignUpPolicyChanged, |t, d, _id| t
        .sign_up =
        SignUpPolicy {
            tenants: d.tenants,
            accounts: d.accounts
        });
    (listener, state)
}
