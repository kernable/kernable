//! The tenancy aggregates: state, how each event changes it, unique keys and facts.

use kernable::{Aggregate, Domain, Unique};
use serde::{Deserialize, Serialize};

use super::{SignUpPolicy, Suspension, TenantStatus, events::*};
use crate::shared::constants::SNAPSHOT_SCHEMA;

/// The tenant's state, and the aggregate `tenancy.tenant`; its ID is the tenant's.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub status: TenantStatus,
    pub slug: Option<String>,
    pub name: Option<String>,
    /// Identity IDs: the only place owners are kept; `owners` is a virtual group.
    pub owners: Vec<String>,
    pub suspension: Option<Suspension>,
    pub mfa_required: bool,
    /// Only the platform tenant's matters.
    pub sign_up: SignUpPolicy,
}

impl Tenant {
    /// Created and not deleted.
    pub fn exists(&self) -> bool {
        !matches!(self.status, TenantStatus::None | TenantStatus::Deleted)
    }
}

pub fn tenant(domain: &Domain) -> Aggregate<Tenant> {
    let tenant = domain
        .aggregate("tenant", Tenant::default())
        .schema(SNAPSHOT_SCHEMA);
    tenant.apply(TenantCreated, |s, d, evt, _| {
        s.id = evt.aggregate_id.clone();
        s.status = d.status;
        s.slug = Some(d.slug);
        s.name = Some(d.name);
    });
    tenant.apply(TenantActivated, |s, _, _, _| {
        s.status = TenantStatus::Active
    });
    tenant.apply(TenantRenamed, |s, d, _, _| s.name = Some(d.name));
    tenant.apply(TenantSlugChanged, |s, d, _, _| s.slug = Some(d.slug));
    tenant.apply(TenantSuspended, |s, d, _, _| {
        s.status = TenantStatus::Suspended;
        s.suspension = Some(Suspension {
            reason: d.reason,
            at: d.at,
        });
    });
    tenant.apply(TenantReactivated, |s, _, _, _| {
        s.status = TenantStatus::Active;
        s.suspension = None;
    });
    tenant.apply(TenantDeleted, |s, _, _, _| {
        s.status = TenantStatus::Deleted;
        s.owners.clear();
    });
    tenant.apply(OwnerAdded, |s, d, _, _| s.owners.push(d.identity_id));
    tenant.apply(OwnerRemoved, |s, d, _, _| {
        s.owners.retain(|o| *o != d.identity_id)
    });
    tenant.apply(MfaRequirementChanged, |s, d, _, _| {
        s.mfa_required = d.required
    });
    tenant.apply(SignUpPolicyChanged, |s, d, _, _| {
        s.sign_up = SignUpPolicy {
            tenants: d.tenants,
            accounts: d.accounts,
        }
    });

    // A slug is unique across tenants while the tenant is not deleted: deleting frees it.
    tenant.unique(
        "slug",
        |s| s.slug.clone().filter(|_| s.exists()),
        Unique::global(),
    );
    // Lets identity.DeleteIdentity require that no tenant holds the identity as owner.
    tenant.unique("owner", |s| s.owners.clone(), Unique::tenant());
    tenant.fact("active", |s| s.status == TenantStatus::Active);
    tenant.fact("exists", Tenant::exists);
    tenant
}
