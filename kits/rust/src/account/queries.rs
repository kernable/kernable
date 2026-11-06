//! The account queries: each type with its handler.

use std::sync::Arc;

use kernable::{BoxError, Listener, Rejected, query};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::AccountStatus;
use super::readmodels::{AccountRow, SessionRow, SharedAccounts, SharedSsos, sso_key};
use crate::shared::secrets::normal_email;

fn not_found(message: &str) -> BoxError {
    Rejected::new(message).code("not_found").into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Found {
    pub id: String,
    pub status: AccountStatus,
}

/// The API's only: an open lookup would reveal who has an account.
#[query("account", answer = Found)]
pub struct FindAccountByEmail {
    pub email: String,
}

#[query("account", answer = AccountRow)]
pub struct GetAccount {
    pub account_id: String,
}

/// The account's sessions, the open ones first: "my devices".
#[query("account", answer = Vec<SessionRow>)]
pub struct ListSessions {
    pub account_id: String,
}

/// The API's only: the account an external login is linked to.
#[query("account", answer = Found)]
pub struct FindAccountByLink {
    pub provider: String,
    pub subject: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SsoFound {
    pub tenant: String,
    pub sso_id: String,
    pub issuer: String,
    pub client_id: String,
    pub enforced: bool,
}

/// The API's only: the active SSO that verified the domain, to sign its people in (any tenant).
#[query("account", answer = SsoFound)]
pub struct FindSsoByDomain {
    pub domain: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SsoSecret {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
}

/// The API's only: what it signs in with, the client secret included.
#[query("account", answer = SsoSecret)]
pub struct GetSsoSecret {
    pub sso_id: String,
}

/// The tenant's owners: the configuration without the secret, with the TXT values to publish.
#[query("account", answer = Value)]
pub struct GetSso {
    pub sso_id: String,
}

pub fn answer(
    accounts: &Listener,
    account_state: &SharedAccounts,
    ssos: &Listener,
    sso_state: &SharedSsos,
) {
    let s = Arc::clone(account_state);
    accounts.query(FindAccountByEmail, move |d, _, _| {
        let t = s.read().unwrap();
        let found = t
            .by_email
            .get(&normal_email(&d.email))
            .and_then(|id| t.rows.get(id))
            .map(|a| Found {
                id: a.id.clone(),
                status: a.status,
            });
        async move { found.ok_or_else(|| not_found("No such account")) }
    });
    let s = Arc::clone(account_state);
    accounts.query(GetAccount, move |d, _, _| {
        let found = s.read().unwrap().rows.get(&d.account_id).cloned();
        async move { found.ok_or_else(|| not_found("No such account")) }
    });
    let s = Arc::clone(account_state);
    accounts.query(ListSessions, move |d, _, _| {
        let mut rows: Vec<SessionRow> = s
            .read()
            .unwrap()
            .sessions
            .get(&d.account_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        rows.sort_by_key(|r| (!r.active, -r.last_at));
        async move { Ok(rows) }
    });
    let s = Arc::clone(account_state);
    accounts.query(FindAccountByLink, move |d, _, _| {
        let t = s.read().unwrap();
        let found = t
            .by_link
            .get(&format!("{}:{}", d.provider, d.subject))
            .and_then(|id| t.rows.get(id))
            .map(|a| Found {
                id: a.id.clone(),
                status: a.status,
            });
        async move { found.ok_or_else(|| not_found("Not linked")) }
    });
    let s = Arc::clone(sso_state);
    ssos.query(FindSsoByDomain, move |d, _, _| {
        let domain = d.domain.to_lowercase();
        let found = s
            .read()
            .unwrap()
            .values()
            .find(|r| {
                r.status == "active" && r.domains.iter().any(|x| x.domain == domain && x.verified)
            })
            .map(|r| SsoFound {
                tenant: r.tenant.clone(),
                sso_id: r.id.clone(),
                issuer: r.issuer.clone(),
                client_id: r.client_id.clone(),
                enforced: r.enforced,
            });
        async move { found.ok_or_else(|| not_found("No SSO for this domain")) }
    });
    let s = Arc::clone(sso_state);
    ssos.query(GetSsoSecret, move |d, qry, _| {
        let found = s
            .read()
            .unwrap()
            .get(&sso_key(&qry.tenant, &d.sso_id))
            .map(|r| SsoSecret {
                issuer: r.issuer.clone(),
                client_id: r.client_id.clone(),
                client_secret: r.client_secret.clone(),
            });
        async move { found.ok_or_else(|| not_found("No such SSO")) }
    });
    let s = Arc::clone(sso_state);
    ssos.query(GetSso, move |d, qry, _| {
        let found = s
            .read()
            .unwrap()
            .get(&sso_key(&qry.tenant, &d.sso_id))
            .map(|r| {
                let mut v = json!(r);
                v.as_object_mut().map(|o| o.remove("clientSecret"));
                v
            });
        async move { found.ok_or_else(|| not_found("No such SSO")) }
    });
}
