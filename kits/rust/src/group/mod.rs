//! group: a named set of permissions per tenant (design/kits.md "group"). The `owners`
//! group is virtual: owners live on the Tenant. The authorization middleware is a listener of
//! this domain too.

pub mod aggregates;
pub mod commands;
pub mod events;
pub mod queries;
pub mod readmodels;

use kernable::{Aggregate, Domain, Listener, domain};
use serde::{Deserialize, Serialize};

pub use aggregates::Group;
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::GroupRow;

/// A type pattern in the grant syntax (`shop.PlaceOrder`, `shop.*`, `*`), optionally tied to a
/// relation of the target aggregate (`when`), which may hold on its parent (`via_parent`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Permission {
    pub pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_parent: Option<bool>,
}

/// The group domain with its parts.
pub struct GroupDomain {
    pub domain: Domain,
    pub group: Aggregate<Group>,
    pub groups: Listener,
}

/// The group domain, every part registered.
pub fn group() -> GroupDomain {
    let group_domain = domain("group");
    let group = aggregates::group(&group_domain);
    commands::handle(&group);
    let (groups, rows) = readmodels::groups(&group_domain);
    queries::answer(&groups, &rows);
    GroupDomain {
        domain: group_domain,
        group,
        groups,
    }
}
