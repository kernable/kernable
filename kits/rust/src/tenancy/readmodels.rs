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

/// Registers a handler that changes the shared state; `f(state, data, aggregate_id)`.
macro_rules! on {
    ($listener:expr, $state:expr, $event:expr, |$t:ident, $d:pat_param, $id:ident| $body:expr) => {{
        let state = Arc::clone(&$state);
        $listener.on($event, move |$d, evt, _ctx| {
            let state = Arc::clone(&state);
            async move {
                let $id = evt.aggregate_id;
                let mut guard = state.write().unwrap();
                let $t = &mut *guard;
                $body;
                Ok(())
            }
        });
    }};
}

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
    on!(listener, state, TenantCreated, |t, d, id| {
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
    on!(listener, state, TenantActivated, |t, _d, id| row(
        t,
        &id,
        &|r| r.status = TenantStatus::Active
    ));
    on!(listener, state, TenantRenamed, |t, d, id| row(
        t,
        &id,
        &|r| r.name = d.name.clone()
    ));
    on!(listener, state, TenantSlugChanged, |t, d, id| row(
        t,
        &id,
        &|r| r.slug = d.slug.clone()
    ));
    on!(listener, state, TenantSuspended, |t, d, id| row(
        t,
        &id,
        &|r| {
            r.status = TenantStatus::Suspended;
            r.suspension = Some(Suspension {
                reason: d.reason,
                at: d.at,
            });
        }
    ));
    on!(listener, state, TenantReactivated, |t, _d, id| row(
        t,
        &id,
        &|r| {
            r.status = TenantStatus::Active;
            r.suspension = None;
        }
    ));
    on!(listener, state, TenantDeleted, |t, _d, id| t
        .rows
        .remove(&id));
    on!(listener, state, OwnerAdded, |t, d, id| row(t, &id, &|r| r
        .owners
        .push(d.identity_id.clone())));
    on!(listener, state, OwnerRemoved, |t, d, id| row(
        t,
        &id,
        &|r| r.owners.retain(|o| *o != d.identity_id)
    ));
    on!(listener, state, MfaRequirementChanged, |t, d, id| row(
        t,
        &id,
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
