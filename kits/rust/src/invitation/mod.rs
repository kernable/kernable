//! invitation: one Invitation per invitation, under its tenant's ID (A77, design/kits.md
//! "invitation"). `invitation()` registers the parts.

pub mod aggregates;
pub mod commands;
pub mod events;
pub mod queries;
pub mod reactors;
pub mod readmodels;

use kernable::{Aggregate, Domain, Listener, domain};
use serde::{Deserialize, Serialize};

pub use aggregates::Invitation;
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::InvitationRow;

use crate::identity::Scope;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvitationStatus {
    #[default]
    None,
    Open,
    Accepted,
    Declined,
    Revoked,
}

/// A group the invited person joins, for the whole tenant or one aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvitedGroup {
    pub group: String,
    #[serde(default)]
    pub scope: Option<Scope>,
}

/// The invitation domain with its parts.
pub struct InvitationDomain {
    pub domain: Domain,
    pub invitation: Aggregate<Invitation>,
    pub invitations: Listener,
}

/// The invitation domain, every part registered.
pub fn invitation() -> InvitationDomain {
    let invitation_domain = domain("invitation");
    let invitation = aggregates::invitation(&invitation_domain);
    commands::handle(&invitation);
    let (invitations, state) = readmodels::invitations(&invitation_domain);
    queries::answer(&invitations, &state);
    reactors::react(&invitation_domain);
    InvitationDomain {
        domain: invitation_domain,
        invitation,
        invitations,
    }
}
