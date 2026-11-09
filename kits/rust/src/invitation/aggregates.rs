//! The invitation aggregates: state, how each event changes it, unique keys.

use kernable::{Aggregate, Domain, Unique};
use serde::{Deserialize, Serialize};

use super::{InvitationStatus, InvitedGroup, LinkStatus, events::*};
use crate::shared::constants::SNAPSHOT_SCHEMA;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Invitation {
    pub status: InvitationStatus,
    pub erased: bool,
    pub email: Option<String>,
    pub groups: Vec<InvitedGroup>,
    pub owner: bool,
    pub token_id: String,
    /// The token while open; in the vault, so `None` once used or erased.
    pub token: Option<String>,
    pub expires_at: i64,
    pub account_id: Option<String>,
}

pub fn invitation(domain: &Domain) -> Aggregate<Invitation> {
    let a = domain
        .aggregate("invitation", Invitation::default())
        .schema(SNAPSHOT_SCHEMA);
    a.apply(InvitationCreated, |s, d, _, _| {
        s.status = InvitationStatus::Open;
        s.email = d.email;
        s.groups = d.groups;
        s.owner = d.owner;
        s.token_id = d.token_id;
        s.token = d.token;
        s.expires_at = d.expires_at;
    });
    a.apply(InvitationResent, |s, d, _, _| {
        s.token_id = d.token_id;
        s.token = d.token;
        s.expires_at = d.expires_at;
    });
    a.apply(InvitationRevoked, |s, _, _, _| {
        s.status = InvitationStatus::Revoked;
        s.token = None;
    });
    a.apply(InvitationDeclined, |s, _, _, _| {
        s.status = InvitationStatus::Declined;
        s.token = None;
    });
    a.apply(InvitationAccepted, |s, d, _, _| {
        s.status = InvitationStatus::Accepted;
        s.token = None;
        s.account_id = Some(d.account_id);
    });
    a.apply(InvitationMailSent, |_, _, _, _| {});
    a.apply(InvitationErased, |s, _, _, _| {
        s.erased = true;
        s.email = None;
    });
    // One open invitation per email and tenant.
    a.unique(
        "email",
        |s| {
            s.email
                .clone()
                .filter(|e| s.status == InvitationStatus::Open && !e.is_empty())
        },
        Unique::tenant(),
    );
    a
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InvitationLink {
    pub status: LinkStatus,
    pub groups: Vec<InvitedGroup>,
    /// The token's hash: the token itself is shown once, by the API.
    pub hash: Option<String>,
    pub label: String,
    pub expires_at: Option<i64>,
    pub max_uses: Option<i64>,
    /// The accounts that joined through it: each once.
    pub accounts: Vec<String>,
}

pub fn link(domain: &Domain) -> Aggregate<InvitationLink> {
    let a = domain
        .aggregate("link", InvitationLink::default())
        .schema(SNAPSHOT_SCHEMA);
    a.apply(InvitationLinkCreated, |s, d, _, _| {
        s.status = LinkStatus::Open;
        s.groups = d.groups;
        s.hash = Some(d.hash);
        s.label = d.label;
        s.expires_at = d.expires_at;
        s.max_uses = d.max_uses;
    });
    a.apply(InvitationLinkRevoked, |s, _, _, _| {
        s.status = LinkStatus::Revoked
    });
    a.apply(InvitationLinkUsed, |s, d, _, _| {
        s.accounts.push(d.account_id)
    });
    a
}
