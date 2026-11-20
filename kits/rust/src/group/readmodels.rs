//! The group read models.

use std::sync::{Arc, RwLock};

use indexmap::IndexMap;
use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::{Permission, events::*};
use crate::shared::constants::orders;
use crate::tenancy::TenantDeleted;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupRow {
    pub id: String,
    pub tenant: String,
    pub name: String,
    pub description: String,
    pub permissions: Vec<Permission>,
}

/// `tenant/groupId` → the group.
pub type Shared = Arc<RwLock<IndexMap<String, GroupRow>>>;

pub fn row_key(tenant: &str, group_id: &str) -> String {
    format!("{tenant}/{group_id}")
}

pub fn groups(domain: &Domain) -> (Listener, Shared) {
    let state: Shared = Arc::default();
    let l = domain.read_model("group.groups").order(orders::READ_MODELS);
    on!(l, state, GroupCreated, |t, d, evt| {
        let row = GroupRow {
            id: evt.aggregate_id.clone(),
            tenant: evt.tenant.clone(),
            name: d.name,
            description: d.description,
            permissions: d.permissions,
        };
        t.insert(row_key(&evt.tenant, &evt.aggregate_id), row)
    });
    on!(l, state, GroupUpdated, |t, d, evt| {
        if let Some(g) = t.get_mut(&row_key(&evt.tenant, &evt.aggregate_id)) {
            if let Some(name) = d.name {
                g.name = name;
            }
            if let Some(description) = d.description {
                g.description = description;
            }
        }
    });
    on!(l, state, GroupPermissionsSet, |t, d, evt| {
        if let Some(g) = t.get_mut(&row_key(&evt.tenant, &evt.aggregate_id)) {
            g.permissions = d.permissions;
        }
    });
    on!(l, state, GroupDeleted, |t, _d, evt| t
        .shift_remove(&row_key(&evt.tenant, &evt.aggregate_id)));
    on!(l, state, TenantDeleted, |t, _d, evt| t
        .retain(|_, g| g.tenant != evt.aggregate_id));
    (l, state)
}
