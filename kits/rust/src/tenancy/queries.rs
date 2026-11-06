//! The tenancy queries: each type with its handler.

use std::sync::Arc;

use kernable::{BoxError, Listener, Rejected, query};
use serde::{Deserialize, Serialize};

use super::readmodels::{Shared, TenantRow};
use super::{SignUpPolicy, TenantStatus};

#[query("tenancy", answer = TenantRow)]
pub struct GetTenant {
    pub tenant_id: String,
}

/// For subdomains and login pages, through the API only: nothing open lists tenants.
#[query("tenancy", answer = TenantRef)]
pub struct GetTenantBySlug {
    pub slug: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantRef {
    pub id: String,
    pub name: String,
    pub status: TenantStatus,
}

#[query("tenancy", answer = TenantPage)]
pub struct ListTenants {
    pub status: Option<TenantStatus>,
    /// The last slug of the previous page.
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantPage {
    pub tenants: Vec<TenantRow>,
    pub more: bool,
}

#[query("tenancy", answer = SignUpPolicy)]
pub struct GetSignUpPolicy {}

fn not_found() -> BoxError {
    Rejected::new("No such tenant").code("not_found").into()
}

pub fn answer(tenants: &Listener, state: &Shared) {
    let s = Arc::clone(state);
    tenants.query(GetTenant, move |d, _, _| {
        let found = s.read().unwrap().rows.get(&d.tenant_id).cloned();
        async move { found.ok_or_else(not_found) }
    });
    let s = Arc::clone(state);
    tenants.query(GetTenantBySlug, move |d, _, _| {
        let found = s
            .read()
            .unwrap()
            .rows
            .values()
            .find(|t| t.slug == d.slug)
            .map(|t| TenantRef {
                id: t.id.clone(),
                name: t.name.clone(),
                status: t.status,
            });
        async move { found.ok_or_else(not_found) }
    });
    let s = Arc::clone(state);
    tenants.query(ListTenants, move |d, _, _| {
        let limit = d.limit.unwrap_or(50).clamp(1, 500);
        let state = s.read().unwrap();
        let mut rows: Vec<&TenantRow> = state
            .rows
            .values()
            .filter(|t| d.status.is_none_or(|st| t.status == st))
            .collect();
        rows.sort_by(|a, b| a.slug.cmp(&b.slug));
        let rows: Vec<TenantRow> = rows
            .into_iter()
            .filter(|t| d.after.as_ref().is_none_or(|a| t.slug > *a))
            .cloned()
            .collect();
        let page = TenantPage {
            more: rows.len() > limit,
            tenants: rows.into_iter().take(limit).collect(),
        };
        async move { Ok(page) }
    });
    let s = Arc::clone(state);
    tenants.query(GetSignUpPolicy, move |_, _, _| {
        let policy = s.read().unwrap().sign_up;
        async move { Ok(policy) }
    });
}
