//! What the authorization middleware knows (folded from events) and how it decides
//! (design/kits.md "The authorization middleware"). Pure: no kernel needed, so it is tested alone.

use std::collections::BTreeMap;
use std::sync::Mutex;

use kernable::{Check, CheckKind, RequestContext, Store};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::levels::{ESCALATION, Level, rule_of};
use super::relations::Target;
use crate::group::Permission;
use crate::identity::Scope;
use crate::shared::constants::PLATFORM_TENANT;
use crate::tenancy::{AccountSignUp, SignUpPolicy, TenantSignUp};

/// Why a request is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// `unauthenticated`, `forbidden`, `tenant_suspended`, `mfa_required`, `sso_required`, `sign_up_closed`.
    pub code: String,
    pub message: String,
}

/// Where the knowledge lives: the listener's store, or memory (tests). A value that cannot
/// be read counts as unknown, so the middleware refuses rather than allows.
pub trait KeyValues: Send + Sync {
    fn get(&self, ns: &str, key: &str) -> Option<Value>;
    fn set(&self, ns: &str, key: &str, value: Value) -> Result<(), String>;
    fn remove(&self, ns: &str, key: &str) -> Result<(), String>;
    fn keys(&self, ns: &str) -> Vec<String>;
}

impl KeyValues for Store {
    fn get(&self, ns: &str, key: &str) -> Option<Value> {
        Store::get(self, ns, key).ok().flatten()
    }
    fn set(&self, ns: &str, key: &str, value: Value) -> Result<(), String> {
        Store::set(self, ns, key, &value).map_err(|e| e.to_string())
    }
    fn remove(&self, ns: &str, key: &str) -> Result<(), String> {
        Store::remove(self, ns, key).map_err(|e| e.to_string())
    }
    fn keys(&self, ns: &str) -> Vec<String> {
        Store::all::<Value>(self, ns)
            .map(|all| all.into_iter().map(|(k, _)| k).collect())
            .unwrap_or_default()
    }
}

/// Key-values in memory, the same as a store holds them (as JSON).
#[derive(Default)]
pub struct MemoryKeyValues(Mutex<BTreeMap<String, BTreeMap<String, Value>>>);

impl KeyValues for MemoryKeyValues {
    fn get(&self, ns: &str, key: &str) -> Option<Value> {
        self.0
            .lock()
            .unwrap()
            .get(ns)
            .and_then(|m| m.get(key))
            .cloned()
    }
    fn set(&self, ns: &str, key: &str, value: Value) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .entry(ns.to_owned())
            .or_default()
            .insert(key.to_owned(), value);
        Ok(())
    }
    fn remove(&self, ns: &str, key: &str) -> Result<(), String> {
        if let Some(m) = self.0.lock().unwrap().get_mut(ns) {
            m.remove(key);
        }
        Ok(())
    }
    fn keys(&self, ns: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .get(ns)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }
}

// --- What it knows (design/kits.md, "What it knows") ----------------------------------------------------

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TenantK {
    pub status: String,
    pub owners: Vec<String>,
    pub mfa_required: bool,
    pub suspension: Option<String>,
    pub sso_enforced: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountK {
    pub status: String,
    /// The account turned a second factor on: every session needs it.
    pub mfa: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionK {
    pub account_id: String,
    pub factors: Vec<String>,
    pub active: bool,
    pub method: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MembershipK {
    pub membership_id: String,
    pub group: String,
    pub scope: Option<Scope>,
    pub expires_at: Option<i64>,
    pub active: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IdentityK {
    pub account_id: Option<String>,
    pub active: bool,
    pub memberships: Vec<MembershipK>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ApiKeyK {
    pub tenant: String,
    pub identity_id: String,
    pub expires_at: Option<i64>,
    pub revoked: bool,
}

pub const TENANTS: &str = "tenants";
pub const ACCOUNTS: &str = "accounts";
pub const SESSIONS: &str = "sessions";
/// `tenant/identityId`
pub const IDENTITIES: &str = "identities";
/// keyId
pub const API_KEYS: &str = "apiKeys";
/// `tenant/groupId` → permissions
pub const GROUPS: &str = "groups";
/// `tenant/type/id` → relation → identity IDs
pub const RELATIONS: &str = "relations";
/// `tenant/type/id` → the parent
pub const PARENTS: &str = "parents";
const SETTINGS: &str = "settings";

/// The knowledge, read and written through `kv`. Values are copies: write back with `set` or `update`.
pub struct Knowledge<'a> {
    pub kv: &'a dyn KeyValues,
}

impl Knowledge<'_> {
    pub fn get<T: DeserializeOwned>(&self, ns: &str, key: &str) -> Option<T> {
        self.kv
            .get(ns, key)
            .and_then(|v| serde_json::from_value(v).ok())
    }

    pub fn set<T: Serialize>(&self, ns: &str, key: &str, value: &T) -> Result<(), String> {
        self.kv.set(
            ns,
            key,
            serde_json::to_value(value).map_err(|e| e.to_string())?,
        )
    }

    /// Changes the value under `key`, if there is one.
    pub fn update<T: Serialize + DeserializeOwned>(
        &self,
        ns: &str,
        key: &str,
        change: impl FnOnce(&mut T),
    ) -> Result<(), String> {
        match self.get::<T>(ns, key) {
            Some(mut value) => {
                change(&mut value);
                self.set(ns, key, &value)
            }
            None => Ok(()),
        }
    }

    pub fn delete(&self, ns: &str, key: &str) -> Result<(), String> {
        self.kv.remove(ns, key)
    }

    /// Removes every key starting with `prefix` (a deleted tenant's).
    pub fn delete_prefix(&self, ns: &str, prefix: &str) -> Result<(), String> {
        self.kv
            .keys(ns)
            .into_iter()
            .filter(|k| k.starts_with(prefix))
            .try_for_each(|k| self.kv.remove(ns, &k))
    }

    pub fn sign_up(&self) -> SignUpPolicy {
        self.get(SETTINGS, "signUp").unwrap_or_default()
    }

    pub fn set_sign_up(&self, policy: SignUpPolicy) -> Result<(), String> {
        self.set(SETTINGS, "signUp", &policy)
    }

    pub fn identity(&self, tenant: &str, identity_id: &str) -> Option<IdentityK> {
        if identity_id.is_empty() {
            None
        } else {
            self.get(IDENTITIES, &format!("{tenant}/{identity_id}"))
        }
    }

    pub fn holds(&self, tenant: &str, target: &Target, relation: &str, identity_id: &str) -> bool {
        let rels: BTreeMap<String, Vec<String>> = self
            .get(
                RELATIONS,
                &format!("{tenant}/{}/{}", target.r#type, target.id),
            )
            .unwrap_or_default();
        rels.get(relation)
            .is_some_and(|ids| ids.iter().any(|i| i == identity_id))
    }

    pub fn relate(
        &self,
        tenant: &str,
        target: &Target,
        relation: &str,
        ids: &[String],
        on: bool,
    ) -> Result<(), String> {
        let key = format!("{tenant}/{}/{}", target.r#type, target.id);
        let mut rels: BTreeMap<String, Vec<String>> = self.get(RELATIONS, &key).unwrap_or_default();
        let current = rels.entry(relation.to_owned()).or_default();
        for id in ids {
            if on && !current.contains(id) {
                current.push(id.clone());
            } else if !on {
                current.retain(|c| c != id);
            }
        }
        current.sort();
        self.set(RELATIONS, &key, &rels)
    }

    fn parent(&self, tenant: &str, target: &Target) -> Option<Target> {
        self.get(
            PARENTS,
            &format!("{tenant}/{}/{}", target.r#type, target.id),
        )
    }
}

/// Whether `pattern` covers `name`: `*`, `domain.*` or the qualified type; `name` may be `domain.*` itself.
pub fn covers(pattern: &str, name: &str) -> bool {
    if pattern == "*" || pattern == name {
        return true;
    }
    pattern
        .strip_suffix(".*")
        .is_some_and(|domain| name != "*" && name.split('.').next() == Some(domain))
}

/// The aggregate the request is about: a command's, or the one a query declares (its
/// `target(...)`).
pub fn target_of(check: &Check) -> Option<Target> {
    if let Some(t) = &check.target {
        return Some(Target::new(&t.aggregate_type, &t.aggregate_id));
    }
    if check.kind != CheckKind::Command {
        return None;
    }
    let (t, id) = (
        check.aggregate_type.as_deref()?,
        check.aggregate_id.as_deref()?,
    );
    (!t.is_empty() && !id.is_empty()).then(|| Target::new(&format!("{}.{t}", check.domain), id))
}

/// Whether the target is the sender itself: its identity, account or session (built-in `self`).
fn is_self(k: &Knowledge, check: &Check, r: &RequestContext, target: Option<&Target>) -> bool {
    let Some(target) = target else { return false };
    match target.r#type.as_str() {
        "identity.identity" => target.id == r.sender_identity && check.tenant == r.sender_tenant,
        "account.account" => target.id == r.sender_account,
        "account.session" => {
            target.id == r.sender_session
                || k.get::<SessionK>(SESSIONS, &target.id)
                    .is_some_and(|s| s.account_id == r.sender_account)
        }
        _ => false,
    }
}

/// Who sends a request.
struct Sender {
    account_id: Option<String>,
    factors: Vec<String>,
    method: String,
    api_key: bool,
}

fn refuse(code: &str, message: impl Into<String>) -> Refusal {
    Refusal {
        code: code.to_owned(),
        message: message.into(),
    }
}

/// An active session of an active account, or a valid API key (step 3).
fn authenticate(k: &Knowledge, r: &RequestContext, now: i64) -> Result<Sender, Refusal> {
    let sid = &r.sender_session;
    if sid.is_empty() {
        return Err(refuse("unauthenticated", "Sign in first"));
    }
    if let Some(session) = k.get::<SessionK>(SESSIONS, sid) {
        if !session.active {
            return Err(refuse("unauthenticated", "The session has ended"));
        }
        if session.account_id != r.sender_account {
            return Err(refuse(
                "unauthenticated",
                "The session is another account's",
            ));
        }
        if k.get::<AccountK>(ACCOUNTS, &session.account_id)
            .is_none_or(|a| a.status != "active")
        {
            return Err(refuse("unauthenticated", "The account is not active"));
        }
        return Ok(Sender {
            account_id: Some(session.account_id),
            factors: session.factors,
            method: session.method,
            api_key: false,
        });
    }
    if let Some(key) = k.get::<ApiKeyK>(API_KEYS, sid) {
        if key.revoked || key.expires_at.is_some_and(|e| now > e) {
            return Err(refuse(
                "unauthenticated",
                "The API key is revoked or expired",
            ));
        }
        if key.identity_id != r.sender_identity || key.tenant != r.sender_tenant {
            return Err(refuse(
                "unauthenticated",
                "The API key is another identity's",
            ));
        }
        return Ok(Sender {
            account_id: None,
            factors: vec!["api_key".into()],
            method: String::new(),
            api_key: true,
        });
    }
    Err(refuse("unauthenticated", "Unknown session"))
}

/// The permissions an identity holds through its memberships, at `now`.
fn permissions(
    k: &Knowledge,
    tenant: &str,
    identity_id: &str,
    now: i64,
) -> Vec<(Permission, MembershipK)> {
    let Some(identity) = k.identity(tenant, identity_id).filter(|i| i.active) else {
        return vec![];
    };
    identity
        .memberships
        .into_iter()
        .filter(|m| m.active && m.expires_at.is_none_or(|e| now < e))
        .flat_map(|m| {
            let held: Vec<Permission> = k
                .get(GROUPS, &format!("{tenant}/{}", m.group))
                .unwrap_or_default();
            held.into_iter().map(move |p| (p, m.clone()))
        })
        .collect()
}

struct Asked<'a> {
    check: &'a Check,
    r#type: &'a str,
    target: Option<&'a Target>,
}

fn matches(
    k: &Knowledge,
    tenant: &str,
    identity_id: &str,
    p: &Permission,
    m: &MembershipK,
    a: &Asked,
) -> bool {
    if !covers(&p.pattern, a.r#type) {
        return false;
    }
    if let Some(scope) = &m.scope {
        let at = |t: &Target| scope.r#type == t.r#type && scope.id == t.id;
        let parent = a.target.and_then(|t| k.parent(tenant, t));
        if !(a.target.is_some_and(at) || parent.as_ref().is_some_and(at)) {
            return false;
        }
    }
    let Some(when) = p.when.as_deref() else {
        return true;
    };
    // A relation needs a target.
    let Some(target) = a.target else { return false };
    if when == "self" {
        return is_self(k, a.check, &a.check.request, Some(target));
    }
    if k.holds(&a.check.tenant, target, when, identity_id) {
        return true;
    }
    p.via_parent == Some(true)
        && k.parent(&a.check.tenant, target)
            .is_some_and(|parent| k.holds(&a.check.tenant, &parent, when, identity_id))
}

/// The patterns concerned by a request that hands out permissions (no escalation).
fn concerned(k: &Knowledge, r#type: &str, tenant: &str, data: &Value) -> Vec<String> {
    let of_group = |g: &Value| -> Vec<String> {
        let Some(g) = g.as_str() else { return vec![] };
        let held: Vec<Permission> = k.get(GROUPS, &format!("{tenant}/{g}")).unwrap_or_default();
        held.into_iter().map(|p| p.pattern).collect()
    };
    match r#type {
        "identity.GrantMembership" => of_group(&data["group"]),
        "invitation.CreateInvitation" | "invitation.CreateInvitationLink" => data["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|g| of_group(&g["group"]))
            .collect(),
        _ => data["permissions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| p["pattern"].as_str().unwrap_or("null").to_owned())
            .collect(),
    }
}

/// Decides on a request (design/kits.md, steps 1–9): `None` allows it. `now` is the middleware's
/// clock (ms). With `trace`, each deciding step is noted (`group.Explain`); `assume_session`
/// explains what another identity may do, its sessions aside.
pub fn decide(
    k: &Knowledge,
    check: &Check,
    now: i64,
    mut trace: Option<&mut Vec<String>>,
    assume_session: bool,
) -> Option<Refusal> {
    let r#type = format!("{}.{}", check.domain, check.type_name);
    let r = &check.request;
    let rule = rule_of(&r#type);
    let data = if check.data.is_object() {
        check.data.clone()
    } else {
        Value::Object(Default::default())
    };
    let mut say = |line: String| {
        if let Some(t) = trace.as_deref_mut() {
            t.push(line);
        }
    };
    macro_rules! allow {
        ($($why:tt)*) => {{
            say(format!("allowed: {}", format!($($why)*)));
            return None;
        }};
    }
    macro_rules! no {
        ($code:expr, $($message:tt)*) => {{
            let message = format!($($message)*);
            say(format!("refused ({}): {message}", $code));
            return Some(refuse($code, message));
        }};
    }

    // 1. Privileged keys (the kernel checked the grant): the kit's reactors and the bootstrap.
    if r.bypass_authorization {
        allow!("a privileged key (bypass_authorization)");
    }
    if rule.level == Level::Internal {
        no!("forbidden", "{type} is sent by the kit only");
    }

    // 2. Public: the command carries its proof, the aggregate checks it.
    let registering = r#type == "account.RegisterAccount";
    if rule.level == Level::Public && !(registering && data["verified"] == Value::Bool(true)) {
        if registering && k.sign_up().accounts != AccountSignUp::Open {
            no!("sign_up_closed", "Registration is by invitation only");
        }
        allow!("{type} is public; the command carries its proof");
    }

    // 3. Who is asking.
    let sender = if assume_session {
        let who = k
            .identity(&r.sender_tenant, &r.sender_identity)
            .unwrap_or_default();
        say("sessions not checked (explaining for another identity)".into());
        Sender {
            api_key: who.account_id.is_none(),
            account_id: who.account_id,
            factors: vec!["password".into(), "assumed".into()],
            method: String::new(),
        }
    } else {
        match authenticate(k, r, now) {
            Ok(s) => s,
            Err(refusal) => no!(&refusal.code, "{}", refusal.message),
        }
    };
    if !assume_session {
        if sender.api_key {
            say(format!("signed in with the API key {}", r.sender_session));
        } else {
            say(format!(
                "signed in as account {} (session {}, {})",
                sender.account_id.as_deref().unwrap_or_default(),
                r.sender_session,
                sender.factors.join(" + ")
            ));
        }
    }

    // An account with a second factor on: nothing but signing out until the session proved it.
    let account = sender
        .account_id
        .as_deref()
        .and_then(|a| k.get::<AccountK>(ACCOUNTS, a));
    let two = sender.factors.len() >= 2 || sender.factors.iter().any(|f| f == "passkey");
    if account.is_some_and(|a| a.mfa) && !two && r#type != "account.EndSession" {
        no!("mfa_required", "Prove the second factor first");
    }
    let target = target_of(check);
    // Oneself: the account and its sessions at once; an identity only after the tenant's checks.
    let itself = rule.itself && is_self(k, check, r, target.as_ref());
    if let Some(t) = target
        .as_ref()
        .filter(|t| itself && t.r#type != "identity.identity")
    {
        // Oneself in another tenant than the platform's (the API checks a person against a tenant
        // they join, before they hold an identity there): that tenant's checks too.
        if check.domain == "account" && check.tenant != PLATFORM_TENANT {
            match k.get::<TenantK>(TENANTS, &check.tenant) {
                None => no!("forbidden", "No such tenant"),
                Some(t) if t.status == "deleted" => no!("forbidden", "No such tenant"),
                Some(t) if t.status != "active" => {
                    no!("tenant_suspended", "The tenant is {}", t.status)
                }
                Some(t) if t.mfa_required && !two => {
                    no!("mfa_required", "This tenant requires a second factor")
                }
                Some(t) => {
                    if let Some(sso) = t.sso_enforced
                        && !assume_session
                        && sender.method != format!("sso:{sso}")
                    {
                        no!(
                            "sso_required",
                            "Sign in through the tenant's single sign-on"
                        );
                    }
                }
            }
        }
        allow!("oneself ({})", t.r#type);
    }
    if r#type == "account.SuspendAccount"
        && data["reason"] == "owner_requested"
        && is_self(k, check, r, target.as_ref())
    {
        allow!("one's own account, for oneself");
    }

    // 4. Authenticated: a session is enough.
    if rule.level == Level::Authenticated {
        if r#type == "tenancy.CreateTenant" {
            let own = data["owner"]["accountId"]
                .as_str()
                .is_some_and(|a| Some(a) == sender.account_id.as_deref());
            let wanted = match data["status"].as_str().unwrap_or("active") {
                "pending" => TenantSignUp::Pending,
                "active" => TenantSignUp::Active,
                _ => TenantSignUp::Off,
            };
            let policy = k.sign_up().tenants;
            if own && policy != TenantSignUp::Off && wanted == policy {
                allow!("self-service sign-up of a tenant");
            }
        } else {
            allow!("{type} needs a session only");
        }
    }

    // 5. The tenant, and the sender's tenant.
    let platform = r.sender_tenant == PLATFORM_TENANT;
    if r.sender_tenant.is_empty() || (!platform && r.sender_tenant != check.tenant) {
        no!("forbidden", "Not a member of this tenant");
    }
    let tenant = k.get::<TenantK>(TENANTS, &check.tenant);
    if !platform {
        match &tenant {
            None => no!("forbidden", "No such tenant"),
            Some(t) if t.status == "deleted" => no!("forbidden", "No such tenant"),
            Some(t) if t.status != "active" && r#type != "tenancy.ReactivateTenant" => {
                no!("tenant_suspended", "The tenant is {}", t.status)
            }
            _ => {}
        }
    }

    // 6. The identity, and the tenant's MFA requirement.
    if !r.on_behalf_of.is_empty() {
        no!(
            "forbidden",
            "Acting on behalf of others is not available yet"
        );
    }
    let identity_id = r.sender_identity.as_str();
    let Some(identity) = k
        .identity(&r.sender_tenant, identity_id)
        .filter(|i| i.active)
    else {
        no!("forbidden", "No active identity in this tenant");
    };
    if !sender.api_key && identity.account_id != sender.account_id {
        no!("forbidden", "The identity is another account's");
    }
    let home = k.get::<TenantK>(TENANTS, &r.sender_tenant);
    if [&tenant, &home]
        .iter()
        .any(|t| t.as_ref().is_some_and(|t| t.mfa_required))
        && !sender.api_key
        && !two
    {
        no!("mfa_required", "This tenant requires a second factor");
    }

    // 7. Owners hold every permission in their tenant, except the platform's; platform owners hold all.
    let owner = home
        .as_ref()
        .is_some_and(|h| h.owners.iter().any(|o| o == identity_id));
    let sso = tenant.as_ref().and_then(|t| t.sso_enforced.clone());
    if let Some(sso) = sso
        && !platform
        && !owner
        && !sender.api_key
        && !assume_session
        && sender.method != format!("sso:{sso}")
    {
        no!(
            "sso_required",
            "Sign in through the tenant's single sign-on"
        );
    }
    if itself {
        allow!("oneself (identity.identity)");
    }
    if rule.level == Level::Platform && !platform {
        let suspension = tenant.as_ref().and_then(|t| t.suspension.as_deref());
        let own = owner
            && ((r#type == "tenancy.SuspendTenant" && data["reason"] == "owner_requested")
                || (r#type == "tenancy.ReactivateTenant" && suspension == Some("owner_requested")));
        if own {
            allow!("an owner, for their own suspension");
        }
        no!("forbidden", "{type} is for the platform");
    }
    if owner {
        allow!(
            "{}",
            if platform {
                "an owner of the platform"
            } else {
                "an owner of the tenant (every permission)"
            }
        );
    }
    if r#type == "invitation.CreateInvitation" && data["owner"] == Value::Bool(true) && !platform {
        no!("forbidden", "Owner invitations are the owners'");
    }
    let status = tenant
        .as_ref()
        .map(|t| t.status.clone())
        .unwrap_or_default();
    if !platform && status != "active" {
        no!("tenant_suspended", "The tenant is {status}");
    }
    if rule.level == Level::Owners {
        no!("forbidden", "{type} is for the tenant's owners");
    }

    // 8. Memberships: pattern, scope, relation, expiry.
    let held = permissions(k, &r.sender_tenant, identity_id, now);
    let asked = Asked {
        check,
        r#type: &r#type,
        target: target.as_ref(),
    };
    let Some((p, m)) = held
        .iter()
        .find(|(p, m)| matches(k, &r.sender_tenant, identity_id, p, m, &asked))
    else {
        no!("forbidden", "No permission for {type}");
    };

    // 9. No escalation: only permissions held without scope and relation count.
    if ESCALATION.contains(&r#type.as_str()) {
        let plain: Vec<&str> = held
            .iter()
            .filter(|(p, m)| p.when.is_none() && m.scope.is_none())
            .map(|(p, _)| p.pattern.as_str())
            .collect();
        if let Some(missing) = concerned(k, &r#type, &check.tenant, &data)
            .into_iter()
            .find(|x| !plain.iter().any(|h| covers(h, x)))
        {
            no!("forbidden", "Granting {missing} needs holding it");
        }
    }
    let when = p.when.as_ref().map(|w| {
        format!(
            " when {w}{}",
            if p.via_parent == Some(true) {
                " (via parent)"
            } else {
                ""
            }
        )
    });
    let scope = m
        .scope
        .as_ref()
        .map(|s| format!(", scoped to {} {}", s.r#type, s.id));
    let until = m.expires_at.map(|e| format!(", until {e}"));
    allow!(
        "membership {} of group {}{}: {}{}{}{}",
        m.membership_id,
        m.group,
        if platform { " (platform)" } else { "" },
        p.pattern,
        when.unwrap_or_default(),
        scope.unwrap_or_default(),
        until.unwrap_or_default()
    );
}
