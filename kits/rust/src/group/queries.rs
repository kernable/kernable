//! The group queries: each type with its handler.

use std::sync::Arc;

use kernable::{Listener, Rejected, query};
use serde_json::{Value, json};

use super::readmodels::{GroupRow, Shared, row_key};

/// The tenant's groups, the virtual `owners` group first.
#[query("group", answer = Vec<Value>)]
pub struct ListGroups {}

#[query("group", answer = GroupRow)]
pub struct GetGroup {
    pub group_id: String,
}

pub fn answer(groups: &Listener, state: &Shared) {
    let s = Arc::clone(state);
    groups.query(ListGroups, move |_, qry, _| {
        let owners = json!({ "id": "owners", "name": "owners", "description": "The tenant's owners: every permission", "permissions": [{ "pattern": "*" }] });
        let mut all = vec![owners];
        all.extend(s.read().unwrap().values().filter(|g| g.tenant == qry.tenant).map(|g| json!(g)));
        async move { Ok(all) }
    });
    let s = Arc::clone(state);
    groups.query(GetGroup, move |d, qry, _| {
        let found = s
            .read()
            .unwrap()
            .get(&row_key(&qry.tenant, &d.group_id))
            .cloned();
        async move { found.ok_or_else(|| Rejected::new("No such group").code("not_found").into()) }
    });
}
