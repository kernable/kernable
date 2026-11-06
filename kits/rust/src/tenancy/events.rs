//! The tenancy events: what happened to tenants, the vocabulary other domains read.

use kernable::event;

use super::{AccountSignUp, Owner, SuspensionReason, TenantSignUp, TenantStatus};

#[event("tenancy")]
#[derive(Clone, Debug)]
pub struct TenantCreated {
    pub slug: String,
    pub name: String,
    pub owner: Owner,
    /// `active` or `pending`.
    pub status: TenantStatus,
}

#[event("tenancy")]
pub struct TenantActivated {}

#[event("tenancy")]
pub struct TenantRenamed {
    pub name: String,
}

#[event("tenancy")]
pub struct TenantSlugChanged {
    pub slug: String,
}

#[event("tenancy")]
pub struct TenantSuspended {
    pub reason: SuspensionReason,
    /// In the payload: the retention before deletion is computed from it.
    pub at: i64,
}

#[event("tenancy")]
pub struct TenantReactivated {}

#[event("tenancy")]
pub struct TenantDeleted {}

#[event("tenancy")]
pub struct OwnerAdded {
    pub identity_id: String,
}

#[event("tenancy")]
pub struct OwnerRemoved {
    pub identity_id: String,
}

#[event("tenancy")]
pub struct MfaRequirementChanged {
    pub required: bool,
}

#[event("tenancy")]
pub struct SignUpPolicyChanged {
    pub tenants: TenantSignUp,
    pub accounts: AccountSignUp,
}
