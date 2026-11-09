//! The authorization middleware: one listener at order -30 that learns from events and
//! checks every request before the kernel accepts or routes it. Its knowledge lives in its store.

use std::sync::{LazyLock, RwLock};

use kernable::{
    BoxError, Check, CheckKind, Domain, Listener, MiddlewareOptions, Rejected, RequestContext,
    Store, Timestamp, query,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::policy::*;
use super::relations::{RelationEvent, Target, parent_rules, relation_events, relation_rules};
use crate::account;
use crate::group;
use crate::identity;
use crate::shared::constants::{AUTHORIZATION_STORE, orders};
use crate::tenancy::{self, SignUpPolicy};

/// Where the middleware keeps its knowledge: a SQLite file, so a restart continues where it
/// stopped instead of replaying the log while every request is refused. Call before `kit_domains()`.
pub fn use_authorization_store(url: &str) {
    *STORE_URL.write().unwrap() = url.to_owned();
}

static STORE_URL: LazyLock<RwLock<String>> =
    LazyLock::new(|| RwLock::new(AUTHORIZATION_STORE.to_owned()));
/// Without a store (a unit test): memory.
static MEMORY: LazyLock<MemoryKeyValues> = LazyLock::new(MemoryKeyValues::default);

fn kv(store: &Option<Store>) -> &dyn KeyValues {
    match store {
        Some(s) => s,
        None => &*MEMORY,
    }
}

/// The event the knowledge learns from: its qualified type and where it happened.
pub struct Seen<'a> {
    pub r#type: &'a str,
    pub tenant: &'a str,
    pub aggregate_id: &'a str,
    pub domain: &'a str,
    pub aggregate_type: &'a str,
}

fn name<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn data<T: DeserializeOwned>(value: Value) -> Result<T, BoxError> {
    Ok(serde_json::from_value(value)?)
}

/// The event types the knowledge is folded from (besides the relations' events).
pub const LEARNED: &[&str] = &[
    "tenancy.TenantCreated",
    "tenancy.TenantActivated",
    "tenancy.TenantSuspended",
    "tenancy.TenantReactivated",
    "tenancy.OwnerAdded",
    "tenancy.OwnerRemoved",
    "tenancy.MfaRequirementChanged",
    "tenancy.TenantDeleted",
    "tenancy.SignUpPolicyChanged",
    "account.AccountRegistered",
    "account.EmailVerified",
    "account.AccountSuspended",
    "account.AccountReactivated",
    "account.AccountDeleted",
    "account.TotpConfirmed",
    "account.TotpDisabled",
    "account.SessionOpened",
    "account.SessionFactorAdded",
    "account.SessionEnded",
    "account.SsoEnforcementChanged",
    "account.SsoDisabled",
    "account.SsoDeleted",
    "identity.IdentityCreated",
    "identity.IdentitySuspended",
    "identity.IdentityReactivated",
    "identity.MembershipGranted",
    "identity.MembershipSuspended",
    "identity.MembershipResumed",
    "identity.MembershipRevoked",
    "identity.IdentityDeleted",
    "identity.ApiKeyCreated",
    "identity.ApiKeyRevoked",
    "group.GroupCreated",
    "group.GroupPermissionsSet",
    "group.GroupDeleted",
];

/// Folds one event into the knowledge.
pub fn learn(k: &Knowledge, e: &Seen, payload: Value) -> Result<(), BoxError> {
    let id = e.aggregate_id;
    let ikey = format!("{}/{id}", e.tenant);
    let tenant = |f: &dyn Fn(&mut TenantK)| k.update(TENANTS, id, |t: &mut TenantK| f(t));
    let account_status = |status: &str| {
        let mut a: AccountK = k.get(ACCOUNTS, id).unwrap_or_default();
        a.status = status.to_owned();
        k.set(ACCOUNTS, id, &a)
    };
    let membership = |membership_id: String, active: bool| {
        k.update(IDENTITIES, &ikey, |i: &mut IdentityK| {
            for m in i
                .memberships
                .iter_mut()
                .filter(|m| m.membership_id == membership_id)
            {
                m.active = active;
            }
        })
    };
    let sso_off = || {
        k.update(TENANTS, e.tenant, |t: &mut TenantK| {
            if t.sso_enforced.as_deref() == Some(id) {
                t.sso_enforced = None
            }
        })
    };
    match e.r#type {
        "tenancy.TenantCreated" => {
            let d: tenancy::TenantCreated = data(payload.clone())?;
            k.set(
                TENANTS,
                id,
                &TenantK {
                    status: name(&d.status),
                    ..TenantK::default()
                },
            )?;
        }
        "tenancy.TenantActivated" => tenant(&|t| t.status = "active".into())?,
        "tenancy.TenantSuspended" => {
            let d: tenancy::TenantSuspended = data(payload.clone())?;
            tenant(&|t| {
                t.status = "suspended".into();
                t.suspension = Some(name(&d.reason));
            })?
        }
        "tenancy.TenantReactivated" => tenant(&|t| {
            t.status = "active".into();
            t.suspension = None;
        })?,
        "tenancy.OwnerAdded" => {
            let d: tenancy::OwnerAdded = data(payload.clone())?;
            tenant(&|t| {
                if !t.owners.contains(&d.identity_id) {
                    t.owners.push(d.identity_id.clone());
                }
            })?
        }
        "tenancy.OwnerRemoved" => {
            let d: tenancy::OwnerRemoved = data(payload.clone())?;
            tenant(&|t| t.owners.retain(|o| *o != d.identity_id))?
        }
        "tenancy.MfaRequirementChanged" => {
            let d: tenancy::MfaRequirementChanged = data(payload.clone())?;
            tenant(&|t| t.mfa_required = d.required)?
        }
        "tenancy.TenantDeleted" => {
            tenant(&|t| {
                t.status = "deleted".into();
                t.owners.clear();
            })?;
            for ns in [IDENTITIES, GROUPS, RELATIONS, PARENTS] {
                k.delete_prefix(ns, &format!("{id}/"))?;
            }
        }
        "tenancy.SignUpPolicyChanged" => {
            let d: tenancy::SignUpPolicyChanged = data(payload.clone())?;
            k.set_sign_up(SignUpPolicy {
                tenants: d.tenants,
                accounts: d.accounts,
            })?;
        }
        "account.AccountRegistered" => {
            let d: account::AccountRegistered = data(payload.clone())?;
            account_status(if d.verified { "active" } else { "pending" })?;
        }
        "account.EmailVerified" => k.update(ACCOUNTS, id, |a: &mut AccountK| {
            if a.status == "pending" {
                a.status = "active".into();
            }
        })?,
        "account.AccountSuspended" => account_status("suspended")?,
        "account.AccountReactivated" => account_status("active")?,
        "account.AccountDeleted" => account_status("deleted")?,
        "account.TotpConfirmed" => k.update(ACCOUNTS, id, |a: &mut AccountK| a.mfa = true)?,
        "account.TotpDisabled" => k.update(ACCOUNTS, id, |a: &mut AccountK| a.mfa = false)?,
        "account.SessionOpened" => {
            let d: account::SessionOpened = data(payload.clone())?;
            k.set(
                SESSIONS,
                id,
                &SessionK {
                    account_id: d.account_id,
                    factors: d.factors,
                    active: true,
                    method: d.method,
                },
            )?;
        }
        "account.SessionFactorAdded" => {
            let d: account::SessionFactorAdded = data(payload.clone())?;
            k.update(SESSIONS, id, |s: &mut SessionK| s.factors.push(d.factor))?;
        }
        "account.SessionEnded" => k.update(SESSIONS, id, |s: &mut SessionK| s.active = false)?,
        "account.SsoEnforcementChanged" => {
            let d: account::SsoEnforcementChanged = data(payload.clone())?;
            k.update(TENANTS, e.tenant, |t: &mut TenantK| {
                if d.enforced {
                    t.sso_enforced = Some(id.to_owned());
                } else if t.sso_enforced.as_deref() == Some(id) {
                    t.sso_enforced = None;
                }
            })?
        }
        "account.SsoDisabled" | "account.SsoDeleted" => sso_off()?,
        "identity.IdentityCreated" => {
            let d: identity::IdentityCreated = data(payload.clone())?;
            k.set(
                IDENTITIES,
                &ikey,
                &IdentityK {
                    account_id: d.account_id,
                    active: true,
                    memberships: vec![],
                },
            )?;
        }
        "identity.IdentitySuspended" => {
            k.update(IDENTITIES, &ikey, |i: &mut IdentityK| i.active = false)?
        }
        "identity.IdentityReactivated" => {
            k.update(IDENTITIES, &ikey, |i: &mut IdentityK| i.active = true)?
        }
        "identity.MembershipGranted" => {
            let d: identity::MembershipGranted = data(payload.clone())?;
            k.update(IDENTITIES, &ikey, |i: &mut IdentityK| {
                i.memberships.push(MembershipK {
                    membership_id: d.membership_id,
                    group: d.group,
                    scope: d.scope,
                    expires_at: d.expires_at,
                    active: true,
                })
            })?
        }
        "identity.MembershipSuspended" => membership(
            data::<identity::MembershipSuspended>(payload.clone())?.membership_id,
            false,
        )?,
        "identity.MembershipResumed" => membership(
            data::<identity::MembershipResumed>(payload.clone())?.membership_id,
            true,
        )?,
        "identity.MembershipRevoked" => {
            let d: identity::MembershipRevoked = data(payload.clone())?;
            k.update(IDENTITIES, &ikey, |i: &mut IdentityK| {
                i.memberships.retain(|m| m.membership_id != d.membership_id)
            })?
        }
        "identity.IdentityDeleted" => k.delete(IDENTITIES, &ikey)?,
        "identity.ApiKeyCreated" => {
            let d: identity::ApiKeyCreated = data(payload.clone())?;
            let key = ApiKeyK {
                tenant: e.tenant.to_owned(),
                identity_id: id.to_owned(),
                expires_at: d.expires_at,
                revoked: false,
            };
            k.set(API_KEYS, &d.key_id, &key)?;
        }
        "identity.ApiKeyRevoked" => {
            let d: identity::ApiKeyRevoked = data(payload.clone())?;
            k.update(API_KEYS, &d.key_id, |key: &mut ApiKeyK| key.revoked = true)?
        }
        // A deleted group's memberships stop counting at once.
        "group.GroupCreated" => k.set(
            GROUPS,
            &ikey,
            &data::<group::GroupCreated>(payload.clone())?.permissions,
        )?,
        "group.GroupPermissionsSet" => k.set(
            GROUPS,
            &ikey,
            &data::<group::GroupPermissionsSet>(payload.clone())?.permissions,
        )?,
        "group.GroupDeleted" => k.delete(GROUPS, &ikey)?,
        _ => {}
    }
    // The relations and parents the app declared, from its events.
    let aggregate = format!("{}.{}", e.domain, e.aggregate_type);
    let event = RelationEvent {
        data: payload,
        aggregate_id: id.to_owned(),
        tenant: e.tenant.to_owned(),
    };
    for rule in relation_rules()
        .into_iter()
        .filter(|r| r.event == e.r#type && r.r#type == aggregate)
    {
        k.relate(
            e.tenant,
            &Target::new(&rule.r#type, id),
            &rule.name,
            &(rule.ids)(&event),
            rule.set,
        )?;
    }
    for rule in parent_rules()
        .into_iter()
        .filter(|p| p.event == e.r#type && p.r#type == aggregate)
    {
        if let Some(p) = (rule.parent)(&event) {
            k.set(PARENTS, &format!("{}/{}/{id}", e.tenant, rule.r#type), &p)?;
        }
    }
    Ok(())
}

/// Every event type the middleware learns from: the kit's and the declared relations'.
pub fn learned_events() -> Vec<String> {
    let mut all: Vec<String> = LEARNED
        .iter()
        .map(|s| (*s).to_owned())
        .chain(relation_events())
        .collect();
    all.sort();
    all.dedup();
    all
}

/// Why a request would be allowed or refused: the middleware's own steps. For oneself, with
/// one's real session; for another identity of the tenant (needs `group.Explain`), sessions aside.
#[query(
    "group",
    answer = Explained,
    target(aggregate = "identity.identity", field = identity_id)
)]
pub struct Explain {
    pub identity_id: String,
    /// The request type, qualified: `shop.CancelOrder`.
    pub r#type: String,
    /// The aggregate the request is about: a command's, or a query's target. Unqualified: of the
    /// type's domain.
    pub aggregate_type: Option<String>,
    pub aggregate_id: Option<String>,
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Explained {
    pub allowed: bool,
    pub code: Option<String>,
    pub steps: Vec<String>,
}

/// The middleware's check of a request: `Err` with the refusal's code to refuse.
pub fn check(store: &Option<Store>, c: &Check) -> Result<(), Rejected> {
    let k = Knowledge { kv: kv(store) };
    match decide(&k, c, Timestamp::now().millis(), None, false) {
        None => Ok(()),
        Some(refusal) => Err(Rejected::new(refusal.message).code(&refusal.code)),
    }
}

/// The authorization listener of the group domain, at order -30, learning from every event type
/// in [`learned_events`] (declare relations before building the kit's domains).
pub fn authorization(group_domain: &Domain) -> Listener {
    let url = STORE_URL.read().unwrap().clone();
    let listener = group_domain
        .read_model("group.authorization")
        .order(orders::AUTHORIZATION)
        .store(&url, "1");
    for r#type in learned_events() {
        let t = r#type.clone();
        listener.on(r#type.as_str(), move |payload: Value, evt, ctx| {
            let t = t.clone();
            async move {
                let k = Knowledge { kv: kv(&ctx.store) };
                let seen = Seen {
                    r#type: &t,
                    tenant: &evt.tenant,
                    aggregate_id: &evt.aggregate_id,
                    domain: &evt.domain,
                    aggregate_type: &evt.aggregate_type,
                };
                learn(&k, &seen, payload)
            }
        });
    }
    listener.middleware(
        MiddlewareOptions {
            order: orders::AUTHORIZATION,
            ..MiddlewareOptions::new(&["*"])
        },
        |c| async move { check(&c.store, &c).map_err(BoxError::from) },
    );
    listener.query(Explain, |d, qry, ctx| async move {
        let (domain, name) = d
            .r#type
            .split_once('.')
            .filter(|(a, b)| !a.is_empty() && !b.is_empty())
            .ok_or_else(|| {
                BoxError::from(Rejected::new("A qualified type: domain.Type").code("invalid_type"))
            })?;
        let asker = ctx.request.clone();
        let oneself = asker.sender_identity == d.identity_id && asker.sender_tenant == qry.tenant;
        let request = if oneself {
            asker
        } else {
            RequestContext {
                sender_tenant: qry.tenant.clone(),
                sender_identity: d.identity_id.clone(),
                ..RequestContext::default()
            }
        };
        let target = match (&d.aggregate_type, &d.aggregate_id) {
            (Some(t), Some(id)) if !t.is_empty() && !id.is_empty() => Some(kernable::Target {
                aggregate_type: if t.contains('.') {
                    t.clone()
                } else {
                    format!("{domain}.{t}")
                },
                aggregate_id: id.clone(),
            }),
            _ => None,
        };
        let c = Check {
            kind: if d.aggregate_id.is_some() {
                CheckKind::Command
            } else {
                CheckKind::Query
            },
            id: String::new(),
            tenant: qry.tenant.clone(),
            domain: domain.to_owned(),
            type_name: name.to_owned(),
            aggregate_type: d.aggregate_type,
            aggregate_id: d.aggregate_id,
            target,
            key: None,
            data: d.data.unwrap_or_else(|| Value::Object(Default::default())),
            request,
            outcome: None,
            store: None,
        };
        let k = Knowledge { kv: kv(&ctx.store) };
        let mut steps = vec![];
        let refusal = decide(
            &k,
            &c,
            Timestamp::now().millis(),
            Some(&mut steps),
            !oneself,
        );
        Ok(Explained {
            allowed: refusal.is_none(),
            code: refusal.map(|r| r.code),
            steps,
        })
    });
    listener
}
