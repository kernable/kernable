//! The identity queries: each type with its handler.

use std::sync::Arc;

use kernable::{BoxError, Listener, Rejected, query};
use serde::{Deserialize, Serialize};

use super::IdentityStatus;
use super::readmodels::{ApiKeyRow, IdentityRow, Shared, row_key};

fn not_found(message: &str) -> BoxError {
    Rejected::new(message).code("not_found").into()
}

#[query(
    "identity",
    answer = IdentityRow,
    target(aggregate = "identity.identity", field = identity_id)
)]
pub struct GetIdentity {
    pub identity_id: String,
}

#[query("identity", answer = Vec<IdentityRow>)]
pub struct ListIdentities {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyIdentity {
    pub tenant: String,
    pub identity_id: String,
    pub status: IdentityStatus,
}

/// Every tenant the account has an identity in (ask with all partitions when tenants are partitioned).
#[query(
    "identity",
    answer = Vec<MyIdentity>,
    target(aggregate = "account.account", field = account_id)
)]
pub struct ListMyIdentities {
    pub account_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FoundIdentity {
    pub identity_id: String,
    pub status: IdentityStatus,
}

/// The API's only: it sets `sender_identity` from it.
#[query("identity", answer = FoundIdentity)]
pub struct FindIdentityByAccount {
    pub account_id: String,
}

/// The API's only: what it checks an API key against.
#[query("identity", answer = ApiKeyRow)]
pub struct FindApiKey {
    pub key_id: String,
}

pub fn answer(identities: &Listener, state: &Shared) {
    let s = Arc::clone(state);
    identities.query(GetIdentity, move |d, qry, _| {
        let found = s
            .read()
            .unwrap()
            .rows
            .get(&row_key(&qry.tenant, &d.identity_id))
            .cloned();
        async move { found.ok_or_else(|| not_found("No such identity")) }
    });
    let s = Arc::clone(state);
    identities.query(ListIdentities, move |_, qry, _| {
        let rows: Vec<IdentityRow> = s
            .read()
            .unwrap()
            .rows
            .values()
            .filter(|r| r.tenant == qry.tenant)
            .cloned()
            .collect();
        async move { Ok(rows) }
    });
    let s = Arc::clone(state);
    identities.query(ListMyIdentities, move |d, _, _| {
        let rows: Vec<MyIdentity> = s
            .read()
            .unwrap()
            .rows
            .values()
            .filter(|r| r.account_id.as_deref() == Some(&d.account_id))
            .map(|r| MyIdentity {
                tenant: r.tenant.clone(),
                identity_id: r.id.clone(),
                status: r.status,
            })
            .collect();
        async move { Ok(rows) }
    });
    let s = Arc::clone(state);
    identities.query(FindIdentityByAccount, move |d, qry, _| {
        let found = s
            .read()
            .unwrap()
            .rows
            .values()
            .find(|r| r.tenant == qry.tenant && r.account_id.as_deref() == Some(&d.account_id))
            .map(|r| FoundIdentity {
                identity_id: r.id.clone(),
                status: r.status,
            });
        async move { found.ok_or_else(|| not_found("No identity in this tenant")) }
    });
    let s = Arc::clone(state);
    identities.query(FindApiKey, move |d, _, _| {
        let found = s.read().unwrap().keys.get(&d.key_id).cloned();
        async move { found.ok_or_else(|| not_found("No such key")) }
    });
}
