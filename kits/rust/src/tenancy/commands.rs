//! The tenancy commands: each type with its handler.

use kernable::{Aggregate, CommandMeta, Rejected, Requirement, command};

use super::{
    AccountSignUp, Owner, SuspensionReason, Tenant, TenantSignUp, TenantStatus, events::*,
};
use crate::shared::constants::{DAY, DELETE_AFTER_DAYS, PLATFORM_TENANT, RESERVED_SLUGS};
use crate::shared::secrets::{normal_email, valid_email};

fn rejected(message: &str, code: &str) -> Rejected {
    Rejected::new(message).code(code)
}

fn slug(value: &str, tenant_id: &str) -> Result<String, Rejected> {
    let b = value.as_bytes();
    let shaped = (3..=63).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
        && !value.contains("--");
    if !shaped || (tenant_id != PLATFORM_TENANT && RESERVED_SLUGS.contains(&value)) {
        return Err(rejected(
            "A slug is 3 to 63 lowercase letters, digits and single dashes, and not reserved",
            "invalid_slug",
        ));
    }
    Ok(value.to_owned())
}

fn name(value: &str) -> Result<String, Rejected> {
    let trimmed = value.trim();
    if trimmed.is_empty() || value.chars().count() > 200 {
        return Err(rejected("A tenant needs a name", "invalid_name"));
    }
    Ok(trimmed.to_owned())
}

fn exists(s: &Tenant) -> Result<(), Rejected> {
    if s.exists() {
        Ok(())
    } else {
        Err(rejected("No such tenant", "not_found"))
    }
}

fn is_platform(cmd: &CommandMeta) -> bool {
    cmd.aggregate_id == PLATFORM_TENANT
}

#[command("tenancy", aggregate = "tenant")]
pub struct CreateTenant {
    pub slug: String,
    pub name: String,
    /// `email` when the platform creates it (an owner invitation follows), `account_id` by self-service.
    pub owner: Owner,
    /// `active`, or `pending` for a self-service tenant waiting for approval (the middleware
    /// holds it to the sign-up policy).
    pub status: Option<TenantStatus>,
}

#[command("tenancy", aggregate = "tenant")]
pub struct ActivateTenant {}

#[command("tenancy", aggregate = "tenant")]
pub struct RenameTenant {
    pub name: String,
}

#[command("tenancy", aggregate = "tenant")]
pub struct ChangeTenantSlug {
    pub slug: String,
}

#[command("tenancy", aggregate = "tenant")]
pub struct SuspendTenant {
    pub reason: SuspensionReason,
}

#[command("tenancy", aggregate = "tenant")]
pub struct ReactivateTenant {}

#[command("tenancy", aggregate = "tenant")]
pub struct DeleteTenant {}

#[command("tenancy", aggregate = "tenant")]
pub struct AddOwner {
    pub identity_id: String,
}

#[command("tenancy", aggregate = "tenant")]
pub struct RemoveOwner {
    pub identity_id: String,
}

#[command("tenancy", aggregate = "tenant")]
pub struct RequireMfa {
    pub required: bool,
}

#[command("tenancy", aggregate = "tenant")]
pub struct ChangeSignUpPolicy {
    pub tenants: TenantSignUp,
    pub accounts: AccountSignUp,
}

pub fn handle(tenant: &Aggregate<Tenant>) {
    tenant.command(CreateTenant, |s, d, cmd, _| {
        // A tenant's aggregate ID is its tenant ID.
        if cmd.aggregate_id != cmd.tenant {
            return Err(rejected(
                "A tenant lives under its own tenant ID",
                "invalid_tenant",
            ));
        }
        if s.status != TenantStatus::None {
            return Err(rejected("The tenant exists", "exists"));
        }
        let email = normal_email(d.owner.email.as_deref().unwrap_or_default());
        let first = match d.owner.account_id.filter(|a| !a.is_empty()) {
            Some(account_id) => Owner {
                account_id: Some(account_id),
                email: None,
            },
            None if valid_email(&email) => Owner {
                email: Some(email),
                account_id: None,
            },
            // The platform's owners come from the bootstrap.
            None if is_platform(cmd) => Owner {
                email: Some(String::new()),
                account_id: None,
            },
            None => {
                return Err(rejected(
                    "A tenant needs an owner: an email or an account",
                    "invalid_owner",
                ));
            }
        };
        let status = if d.status == Some(TenantStatus::Pending) {
            TenantStatus::Pending
        } else {
            TenantStatus::Active
        };
        Ok(vec![
            TenantCreated {
                slug: slug(&d.slug, &cmd.aggregate_id)?,
                name: name(&d.name)?,
                owner: first,
                status,
            }
            .into(),
        ])
    });

    tenant.command(ActivateTenant, |s, _, _, _| {
        if s.status != TenantStatus::Pending {
            return Err(rejected(
                "Only a pending tenant can be activated",
                "invalid_state",
            ));
        }
        Ok(vec![TenantActivated {}.into()])
    });

    tenant.command(RenameTenant, |s, d, _, _| {
        if !matches!(s.status, TenantStatus::Pending | TenantStatus::Active) {
            return Err(rejected("Cannot rename this tenant", "invalid_state"));
        }
        Ok(vec![
            TenantRenamed {
                name: name(&d.name)?,
            }
            .into(),
        ])
    });

    tenant.command(ChangeTenantSlug, |s, d, cmd, _| {
        if s.status != TenantStatus::Active {
            return Err(rejected(
                "Only an active tenant changes its slug",
                "invalid_state",
            ));
        }
        Ok(vec![
            TenantSlugChanged {
                slug: slug(&d.slug, &cmd.aggregate_id)?,
            }
            .into(),
        ])
    });

    tenant.command(SuspendTenant, |s, d, cmd, ctx| {
        if is_platform(cmd) {
            return Err(rejected(
                "The platform tenant cannot be suspended",
                "platform",
            ));
        }
        if !matches!(s.status, TenantStatus::Pending | TenantStatus::Active) {
            return Err(rejected("Cannot suspend this tenant", "invalid_state"));
        }
        if (d.reason == SuspensionReason::Rejected) != (s.status == TenantStatus::Pending) {
            return Err(rejected(
                "A pending tenant is rejected; an active one suspended for another reason",
                "invalid_reason",
            ));
        }
        Ok(vec![
            TenantSuspended {
                reason: d.reason,
                at: ctx.now().millis(),
            }
            .into(),
        ])
    });

    tenant.command(ReactivateTenant, |s, _, _, _| {
        let rejected_tenant = s
            .suspension
            .as_ref()
            .is_some_and(|x| x.reason == SuspensionReason::Rejected);
        if s.status != TenantStatus::Suspended || rejected_tenant {
            return Err(rejected(
                "Only a suspended tenant is reactivated",
                "invalid_state",
            ));
        }
        Ok(vec![TenantReactivated {}.into()])
    });

    // Final: the read models drop the tenant, reactors erase its identities, the slug becomes
    // free; the events stay.
    tenant.command(DeleteTenant, |s, _, cmd, ctx| {
        if is_platform(cmd) {
            return Err(rejected(
                "The platform tenant cannot be deleted",
                "platform",
            ));
        }
        let Some(suspension) = s
            .suspension
            .as_ref()
            .filter(|_| s.status == TenantStatus::Suspended)
        else {
            return Err(rejected(
                "Only a suspended tenant is deleted",
                "invalid_state",
            ));
        };
        if ctx.now().millis() < suspension.at + DELETE_AFTER_DAYS * DAY {
            return Err(rejected(
                &format!("A tenant is deleted {DELETE_AFTER_DAYS} days after its suspension"),
                "too_early",
            ));
        }
        Ok(vec![TenantDeleted {}.into()])
    });

    tenant.command(AddOwner, |s, d, _, _| {
        exists(s)?;
        if d.identity_id.is_empty() {
            return Err(rejected("Which identity?", "invalid_identity"));
        }
        if s.owners.contains(&d.identity_id) {
            return Err(rejected("Already an owner", "exists"));
        }
        Ok(vec![
            OwnerAdded {
                identity_id: d.identity_id,
            }
            .into(),
        ])
    });
    tenant.requires(AddOwner, |_, d| {
        [Requirement::new(
            "identity",
            "identity",
            &d.identity_id,
            "active",
        )]
    });

    tenant.command(RemoveOwner, |s, d, _, _| {
        exists(s)?;
        if !s.owners.contains(&d.identity_id) {
            return Err(rejected("Not an owner", "not_found"));
        }
        if s.owners.len() == 1 {
            return Err(rejected("The last owner cannot be removed", "last_owner"));
        }
        Ok(vec![
            OwnerRemoved {
                identity_id: d.identity_id,
            }
            .into(),
        ])
    });

    tenant.command(RequireMfa, |s, d, _, _| {
        if s.status != TenantStatus::Active {
            return Err(rejected("Only an active tenant", "invalid_state"));
        }
        Ok(vec![
            MfaRequirementChanged {
                required: d.required,
            }
            .into(),
        ])
    });

    tenant.command(ChangeSignUpPolicy, |s, d, cmd, _| {
        if !is_platform(cmd) {
            return Err(rejected(
                "Only the platform tenant has a sign-up policy",
                "platform",
            ));
        }
        exists(s)?;
        Ok(vec![
            SignUpPolicyChanged {
                tenants: d.tenants,
                accounts: d.accounts,
            }
            .into(),
        ])
    });
}
