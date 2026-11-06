//! The account read models: accounts with their links and sessions, and the tenants' SSOs.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::{AccountStatus, events::*};
use crate::shared::constants::orders;
use crate::tenancy::TenantDeleted;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountRow {
    pub id: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub status: AccountStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRow {
    pub id: String,
    pub account_id: String,
    pub method: String,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
    pub opened_at: i64,
    pub last_at: i64,
    pub active: bool,
}

#[derive(Default)]
pub struct Accounts {
    pub rows: HashMap<String, AccountRow>,
    pub by_email: HashMap<String, String>,
    /// `provider:subject` → account ID
    pub by_link: HashMap<String, String>,
    /// account ID → session ID → session
    pub sessions: HashMap<String, BTreeMap<String, SessionRow>>,
}

pub type SharedAccounts = Arc<RwLock<Accounts>>;

fn set_status(t: &mut Accounts, id: &str, status: AccountStatus) {
    if let Some(a) = t.rows.get_mut(id) {
        a.status = status;
    }
}

pub fn accounts(domain: &Domain) -> (Listener, SharedAccounts) {
    let state: SharedAccounts = Arc::default();
    let l = domain
        .read_model("account.accounts")
        .order(orders::READ_MODELS);
    on!(l, state, AccountRegistered, |t, d, evt| {
        let id = evt.aggregate_id;
        let status = if d.verified {
            AccountStatus::Active
        } else {
            AccountStatus::Pending
        };
        if let Some(email) = &d.email {
            t.by_email.insert(email.clone(), id.clone());
        }
        t.rows.insert(
            id.clone(),
            AccountRow {
                id,
                email: d.email,
                name: d.name,
                status,
            },
        )
    });
    on!(l, state, EmailVerified, |t, _d, evt| {
        if let Some(a) = t
            .rows
            .get_mut(&evt.aggregate_id)
            .filter(|a| a.status == AccountStatus::Pending)
        {
            a.status = AccountStatus::Active;
        }
    });
    // The new address finds the account, the old one no more.
    on!(l, state, EmailChanged, |t, d, evt| {
        if let Some(a) = t.rows.get_mut(&evt.aggregate_id) {
            if let Some(old) = a.email.take() {
                t.by_email.remove(&old);
            }
            a.email = d.email.clone();
            if let Some(email) = d.email {
                t.by_email.insert(email, evt.aggregate_id);
            }
        }
    });
    on!(l, state, ProviderLinked, |t, d, evt| {
        if let Some(subject) = d.subject {
            t.by_link
                .insert(format!("{}:{subject}", d.provider), evt.aggregate_id);
        }
    });
    on!(l, state, ProviderUnlinked, |t, d, _evt| {
        if let Some(subject) = d.subject {
            t.by_link.remove(&format!("{}:{subject}", d.provider));
        }
    });
    on!(l, state, AccountSuspended, |t, _d, evt| set_status(
        t,
        &evt.aggregate_id,
        AccountStatus::Suspended
    ));
    on!(l, state, AccountReactivated, |t, _d, evt| set_status(
        t,
        &evt.aggregate_id,
        AccountStatus::Active
    ));
    // A deleted account leaves the read side; its email is free.
    on!(l, state, AccountDeleted, |t, _d, evt| {
        if let Some(email) = t.rows.remove(&evt.aggregate_id).and_then(|a| a.email) {
            t.by_email.remove(&email);
        }
        t.sessions.remove(&evt.aggregate_id)
    });
    on!(l, state, SessionOpened, |t, d, evt| {
        let row = SessionRow {
            id: evt.aggregate_id.clone(),
            account_id: d.account_id.clone(),
            method: d.method,
            ip: d.ip,
            user_agent: d.user_agent,
            opened_at: d.at,
            last_at: d.at,
            active: true,
        };
        t.sessions
            .entry(d.account_id)
            .or_default()
            .insert(evt.aggregate_id, row)
    });
    on!(l, state, SessionRefreshed, |t, d, evt| {
        if let Some(s) = t
            .sessions
            .get_mut(&d.account_id)
            .and_then(|m| m.get_mut(&evt.aggregate_id))
        {
            s.last_at = d.at;
            s.ip = d.ip;
        }
    });
    on!(l, state, SessionEnded, |t, _d, evt| {
        for s in t
            .sessions
            .values_mut()
            .filter_map(|m| m.get_mut(&evt.aggregate_id))
        {
            s.active = false;
        }
    });
    (l, state)
}

// --- A tenant's single sign-on ----------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SsoDomainRow {
    pub domain: String,
    pub txt: String,
    pub verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SsoRow {
    pub id: String,
    pub tenant: String,
    pub status: String,
    pub issuer: String,
    pub client_id: String,
    /// Read with vault_read; only the API gets it (GetSsoSecret).
    pub client_secret: Option<String>,
    pub domains: Vec<SsoDomainRow>,
    pub enforced: bool,
}

/// `tenant/ssoId` → the SSO.
pub type SharedSsos = Arc<RwLock<HashMap<String, SsoRow>>>;

pub fn sso_key(tenant: &str, sso_id: &str) -> String {
    format!("{tenant}/{sso_id}")
}

pub fn ssos(domain: &Domain) -> (Listener, SharedSsos) {
    let state: SharedSsos = Arc::default();
    let l = domain.read_model("account.ssos").order(orders::READ_MODELS);
    fn row<'a>(
        t: &'a mut HashMap<String, SsoRow>,
        evt: &kernable::StoredEventMeta,
    ) -> Option<&'a mut SsoRow> {
        t.get_mut(&sso_key(&evt.tenant, &evt.aggregate_id))
    }
    on!(l, state, SsoCreated, |t, d, evt| {
        let r = SsoRow {
            id: evt.aggregate_id.clone(),
            tenant: evt.tenant.clone(),
            status: "active".into(),
            issuer: d.issuer,
            client_id: d.client_id,
            client_secret: d.client_secret,
            domains: vec![],
            enforced: false,
        };
        t.insert(sso_key(&evt.tenant, &evt.aggregate_id), r)
    });
    on!(l, state, SsoUpdated, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            r.issuer = d.issuer;
            r.client_id = d.client_id;
            r.client_secret = d.client_secret;
        }
    });
    on!(l, state, SsoDomainAdded, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            r.domains.push(SsoDomainRow {
                domain: d.domain,
                txt: d.txt,
                verified: false,
            });
        }
    });
    on!(l, state, SsoDomainVerified, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            for x in r.domains.iter_mut().filter(|x| x.domain == d.domain) {
                x.verified = true;
            }
        }
    });
    on!(l, state, SsoDomainRemoved, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            r.domains.retain(|x| x.domain != d.domain);
        }
    });
    on!(l, state, SsoEnforcementChanged, |t, d, evt| {
        if let Some(r) = row(t, &evt) {
            r.enforced = d.enforced;
        }
    });
    on!(l, state, SsoDisabled, |t, _d, evt| {
        if let Some(r) = row(t, &evt) {
            r.status = "disabled".into();
        }
    });
    on!(l, state, SsoEnabled, |t, _d, evt| {
        if let Some(r) = row(t, &evt) {
            r.status = "active".into();
        }
    });
    on!(l, state, SsoDeleted, |t, _d, evt| t
        .remove(&sso_key(&evt.tenant, &evt.aggregate_id)));
    on!(l, state, TenantDeleted, |t, _d, evt| t
        .retain(|_, r| r.tenant != evt.aggregate_id));
    (l, state)
}
