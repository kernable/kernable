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

pub use aggregates::{Invitation, InvitationLink};
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::{InvitationRow, LinkRow};

use crate::identity::Scope;
use crate::shared::secrets::sha256;

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkStatus {
    #[default]
    None,
    Open,
    Revoked,
}

/// An invitation link as people get it: `link_<tenantId>.<linkId>.<secret>`.
pub fn link_token(tenant: &str, link_id: &str, secret: &str) -> String {
    format!("link_{tenant}.{link_id}.{secret}")
}

/// The parts of an invitation link token: the tenant, the link and the whole token's hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkToken {
    pub tenant: String,
    pub link_id: String,
    pub hash: String,
}

/// The tenant, the link and the whole token's hash, or `None` if it has another shape.
pub fn parse_link_token(value: &str) -> Option<LinkToken> {
    let p: Vec<&str> = value.strip_prefix("link_")?.split('.').collect();
    let part = |s: &str| !s.is_empty() && !s.chars().any(char::is_whitespace);
    let secret = |s: &str| {
        s.len() >= 32
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    (p.len() == 3 && part(p[0]) && part(p[1]) && secret(p[2])).then(|| LinkToken {
        tenant: p[0].to_owned(),
        link_id: p[1].to_owned(),
        hash: sha256(value),
    })
}

/// The invitation domain with its parts.
pub struct InvitationDomain {
    pub domain: Domain,
    pub invitation: Aggregate<Invitation>,
    pub link: Aggregate<InvitationLink>,
    pub invitations: Listener,
}

/// The invitation domain, every part registered.
pub fn invitation() -> InvitationDomain {
    let invitation_domain = domain("invitation");
    let invitation = aggregates::invitation(&invitation_domain);
    let link = aggregates::link(&invitation_domain);
    commands::handle(&invitation);
    commands::handle_links(&link);
    let (invitations, state, links) = readmodels::invitations(&invitation_domain);
    queries::answer(&invitations, &state, &links);
    reactors::react(&invitation_domain);
    InvitationDomain {
        domain: invitation_domain,
        invitation,
        link,
        invitations,
    }
}
