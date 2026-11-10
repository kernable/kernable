//! The identity commands: each type with its handler.

use kernable::{Aggregate, Rejected, Requirement, command};

use super::aggregates::MembershipStatus;
use super::{Identity, IdentityKind, IdentityStatus, Scope, api_key, events::*, own};
use crate::shared::secrets::{sha256, trim_space};

fn rejected(message: &str, code: &str) -> Rejected {
    Rejected::new(message).code(code)
}

fn active(s: &Identity) -> Result<(), Rejected> {
    if s.status == IdentityStatus::Active {
        Ok(())
    } else {
        Err(rejected("The identity is not active", "invalid_state"))
    }
}

/// `{type: "domain.aggregate", id}`.
pub fn scope(value: Option<Scope>) -> Result<Option<Scope>, Rejected> {
    let Some(v) = value else { return Ok(None) };
    let part = |p: &str| {
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    };
    let typed = v.r#type.split_once('.').is_some_and(|(d, a)| {
        d.starts_with(|c: char| c.is_ascii_lowercase()) && part(d) && part(a)
    });
    if typed && !v.id.is_empty() {
        Ok(Some(v))
    } else {
        Err(rejected(
            "A scope is {type: 'domain.aggregate', id}",
            "invalid_scope",
        ))
    }
}

/// Internal: from an accepted invitation or a self-service tenant.
#[command("identity", aggregate = "identity", personal(fields = [display_name, title, email], subject = own))]
pub struct CreateIdentity {
    pub account_id: String,
    pub display_name: Option<String>,
    pub title: Option<String>,
    pub email: Option<String>,
}

#[command("identity", aggregate = "identity", personal(fields = [display_name], subject = own))]
pub struct CreateServiceIdentity {
    pub display_name: String,
}

/// Internal: the identity of a business process (`shop.fulfilment`), made by the kit's
/// `identity.processes` reactor when an app registers the process; one per process.
#[command("identity", aggregate = "identity")]
pub struct CreateProcessIdentity {
    pub process: String,
}

#[command("identity", aggregate = "identity", personal(fields = [display_name, title, email], subject = own))]
pub struct UpdateProfile {
    pub display_name: Option<String>,
    pub title: Option<String>,
    pub email: Option<String>,
}

/// The middleware lets the sender grant only groups whose permissions it holds.
#[command("identity", aggregate = "identity")]
pub struct GrantMembership {
    pub membership_id: String,
    pub group: String,
    pub scope: Option<Scope>,
    pub expires_at: Option<i64>,
}

#[command("identity", aggregate = "identity")]
pub struct SuspendMembership {
    pub membership_id: String,
}

#[command("identity", aggregate = "identity")]
pub struct ResumeMembership {
    pub membership_id: String,
}

#[command("identity", aggregate = "identity")]
pub struct RevokeMembership {
    pub membership_id: String,
}

/// The API generates the key and shows it once; only its hash stays.
#[command("identity", aggregate = "identity", secret = [secret])]
pub struct CreateApiKey {
    pub key_id: String,
    pub secret: String,
    pub label: Option<String>,
    pub expires_at: Option<i64>,
}

#[command("identity", aggregate = "identity")]
pub struct RevokeApiKey {
    pub key_id: String,
}

#[command("identity", aggregate = "identity")]
pub struct SuspendIdentity {
    pub reason: Option<String>,
}

#[command("identity", aggregate = "identity")]
pub struct ReactivateIdentity {}

/// Erases the identity's personal data. Never while a tenant holds it as owner: atomic,
/// through the tenant's `owner` key (an absent requirement).
#[command("identity", aggregate = "identity")]
pub struct DeleteIdentity {}

fn membership(
    s: &Identity,
    id: &str,
    status: Option<MembershipStatus>,
    message: &str,
) -> Result<(), Rejected> {
    let found = s
        .memberships
        .iter()
        .find(|m| m.membership_id == id)
        .is_some_and(|m| status.is_none_or(|st| m.status == st));
    if found {
        Ok(())
    } else {
        Err(rejected(message, "not_found"))
    }
}

pub fn handle(identity: &Aggregate<Identity>) {
    identity.command(CreateIdentity, |s, d, _, _| {
        if s.status != IdentityStatus::None {
            return Err(rejected("The identity exists", "exists"));
        }
        if d.account_id.is_empty() {
            return Err(rejected("Which account?", "invalid_account"));
        }
        Ok(vec![
            IdentityCreated {
                kind: IdentityKind::Person,
                account_id: Some(d.account_id),
                display_name: Some(d.display_name.unwrap_or_default()),
                title: d.title,
                email: d.email,
            }
            .into(),
        ])
    });
    identity.command(CreateServiceIdentity, |s, d, _, _| {
        if s.status != IdentityStatus::None {
            return Err(rejected("The identity exists", "exists"));
        }
        if trim_space(&d.display_name).is_empty() {
            return Err(rejected("A service needs a name", "invalid_name"));
        }
        Ok(vec![
            IdentityCreated {
                kind: IdentityKind::Service,
                account_id: None,
                display_name: Some(trim_space(&d.display_name).to_owned()),
                title: None,
                email: None,
            }
            .into(),
        ])
    });
    identity.command(CreateProcessIdentity, |s, d, _, _| {
        if s.status != IdentityStatus::None {
            return Err(rejected("The identity exists", "exists"));
        }
        let qualified = d.process.split_once('.').is_some_and(|(domain, name)| {
            let part = |p: &str| {
                !p.is_empty()
                    && p.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            };
            part(domain) && part(name)
        });
        if !qualified {
            return Err(rejected(
                "A qualified process name: domain.process",
                "invalid_process",
            ));
        }
        Ok(vec![
            IdentityCreated {
                kind: IdentityKind::Process,
                account_id: None,
                display_name: Some(d.process),
                title: None,
                email: None,
            }
            .into(),
        ])
    });
    identity.command(UpdateProfile, |s, d, _, _| {
        active(s)?;
        Ok(vec![
            ProfileUpdated {
                display_name: d.display_name,
                title: d.title,
                email: d.email,
            }
            .into(),
        ])
    });
    identity.command(GrantMembership, |s, d, _, _| {
        active(s)?;
        if d.membership_id.is_empty() || d.group.is_empty() {
            return Err(rejected(
                "A membership needs an ID and a group",
                "invalid_membership",
            ));
        }
        let scope = scope(d.scope)?;
        if s.memberships
            .iter()
            .any(|m| m.membership_id == d.membership_id || (m.group == d.group && m.scope == scope))
        {
            return Err(rejected("Already a member", "exists"));
        }
        Ok(vec![
            MembershipGranted {
                membership_id: d.membership_id,
                group: d.group,
                scope,
                expires_at: d.expires_at,
            }
            .into(),
        ])
    });
    identity.requires(GrantMembership, |_, d| {
        [Requirement::new("group", "group", &d.group, "active")]
    });
    identity.command(SuspendMembership, |s, d, _, _| {
        membership(
            s,
            &d.membership_id,
            Some(MembershipStatus::Active),
            "No such active membership",
        )?;
        Ok(vec![
            MembershipSuspended {
                membership_id: d.membership_id,
            }
            .into(),
        ])
    });
    identity.command(ResumeMembership, |s, d, _, _| {
        membership(
            s,
            &d.membership_id,
            Some(MembershipStatus::Suspended),
            "No such suspended membership",
        )?;
        Ok(vec![
            MembershipResumed {
                membership_id: d.membership_id,
            }
            .into(),
        ])
    });
    identity.command(RevokeMembership, |s, d, _, _| {
        membership(s, &d.membership_id, None, "No such membership")?;
        Ok(vec![
            MembershipRevoked {
                membership_id: d.membership_id,
            }
            .into(),
        ])
    });
    identity.command(CreateApiKey, |s, d, _, _| {
        active(s)?;
        if !matches!(s.kind, IdentityKind::Service | IdentityKind::Process) {
            return Err(rejected(
                "Only a service or a process has API keys",
                "not_a_service",
            ));
        }
        let id_ok = (8..=32).contains(&d.key_id.len())
            && d.key_id.bytes().all(|c| c.is_ascii_alphanumeric());
        if !id_ok || d.secret.chars().count() < 32 {
            return Err(rejected(
                "A key ID of 8 to 32 letters and digits, and a secret of at least 32 characters",
                "invalid_key",
            ));
        }
        if s.api_keys.iter().any(|k| k.key_id == d.key_id) {
            return Err(rejected("The key exists", "exists"));
        }
        let hash = sha256(&api_key(&d.key_id, &d.secret));
        Ok(vec![
            ApiKeyCreated {
                key_id: d.key_id,
                hash,
                label: d.label.unwrap_or_default(),
                expires_at: d.expires_at,
            }
            .into(),
        ])
    });
    identity.command(RevokeApiKey, |s, d, _, _| {
        if !s
            .api_keys
            .iter()
            .any(|k| k.key_id == d.key_id && !k.revoked)
        {
            return Err(rejected("No such key", "not_found"));
        }
        Ok(vec![ApiKeyRevoked { key_id: d.key_id }.into()])
    });
    identity.command(SuspendIdentity, |s, d, _, _| {
        active(s)?;
        Ok(vec![
            IdentitySuspended {
                reason: d.reason.unwrap_or_else(|| "by_group".into()),
            }
            .into(),
        ])
    });
    identity.command(ReactivateIdentity, |s, _, _, _| {
        if s.status != IdentityStatus::Suspended {
            return Err(rejected("Only a suspended identity", "invalid_state"));
        }
        Ok(vec![IdentityReactivated {}.into()])
    });
    identity.command(DeleteIdentity, |s, _, _, _| {
        if matches!(s.status, IdentityStatus::None | IdentityStatus::Deleted) {
            return Err(rejected("No such identity", "not_found"));
        }
        Ok(vec![IdentityDeleted {}.into()])
    });
    identity.requires(DeleteIdentity, |s, _| {
        (!s.id.is_empty())
            .then(|| Requirement::by_key("tenancy", "tenant", "owner", &s.id, "exists").absent())
    });
}
