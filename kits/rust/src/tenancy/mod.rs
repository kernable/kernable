//! tenancy: one Tenant per tenant, living under its own tenant ID (design/kits.md
//! "tenancy"). The domain, the types its parts share, and `tenancy()`, which registers the parts.

pub mod aggregates;
pub mod commands;
pub mod events;
pub mod queries;
pub mod readmodels;

use kernable::{Aggregate, Domain, Listener, domain};
use serde::{Deserialize, Serialize};

pub use aggregates::Tenant;
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::TenantRow;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    #[default]
    None,
    Pending,
    Active,
    Suspended,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuspensionReason {
    OwnerRequested,
    Unpaid,
    Abuse,
    ByPlatform,
    Rejected,
}

/// Whether people may open tenants themselves, and in which status they start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantSignUp {
    #[default]
    Off,
    Pending,
    Active,
}

/// Whether people may register accounts themselves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountSignUp {
    #[default]
    Off,
    Open,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignUpPolicy {
    pub tenants: TenantSignUp,
    pub accounts: AccountSignUp,
}

/// `email` when the platform created the tenant (an owner invitation follows), `account_id` by
/// self-service.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Owner {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suspension {
    pub reason: SuspensionReason,
    pub at: i64,
}

/// The tenancy domain with its parts.
pub struct Tenancy {
    pub domain: Domain,
    pub tenant: Aggregate<Tenant>,
    pub tenants: Listener,
}

/// The tenancy domain, every part registered (a part not registered here does not exist).
pub fn tenancy() -> Tenancy {
    let tenancy_domain = domain("tenancy");
    let tenant = aggregates::tenant(&tenancy_domain);
    commands::handle(&tenant);
    let (tenants, rows) = readmodels::tenants(&tenancy_domain);
    queries::answer(&tenants, &rows);
    Tenancy {
        domain: tenancy_domain,
        tenant,
        tenants,
    }
}
