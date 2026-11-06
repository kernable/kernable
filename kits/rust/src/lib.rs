//! The kernable kit: tenancy, accounts, identities, groups, invitations and
//! authorization as source code in your project, yours to change. See README.md.

pub mod shared;
pub mod tenancy;

use kernable::Domain;

/// Every domain of the kit, built anew, ready for an app: `kernable.app(App::new("kit").domains(kit_domains()))`.
pub fn kit_domains() -> Vec<Domain> {
    vec![tenancy::tenancy().domain]
}
