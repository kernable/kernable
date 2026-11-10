//! The invitation queries: each type with its handler.

use std::sync::Arc;

use kernable::{Listener, Rejected, Timestamp, query};
use serde::{Deserialize, Serialize};

use super::readmodels::{InvitationRow, LinkRow, Links, Shared, row_key};
use super::{InvitationStatus, LinkStatus, parse_link_token};
use crate::shared::secrets::{equal_secrets, parse_invitation_token, sha256};

/// The tenant's invitations; the open ones appear in user lists.
#[query("invitation", answer = Vec<InvitationRow>)]
pub struct ListInvitations {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Peeked {
    pub tenant: String,
    pub invitation_id: String,
    pub email: Option<String>,
    pub owner: bool,
    pub expired: bool,
}

/// The API's only: what the acceptance page shows for a token, only for the right token (its secret checked).
#[query("invitation", answer = Peeked)]
pub struct PeekInvitation {
    pub token: String,
}

/// The tenant's invitation links, with how often each was used.
#[query("invitation", answer = Vec<LinkRow>)]
pub struct ListInvitationLinks {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeekedLink {
    pub tenant: String,
    pub link_id: String,
    pub label: String,
    pub usable: bool,
}

/// The API's only: whether a link token can be used, for the join page, only for the right token
/// (its hash checked).
#[query("invitation", answer = PeekedLink)]
pub struct PeekInvitationLink {
    pub token: String,
}

pub fn answer(invitations: &Listener, state: &Shared, links: &Links) {
    let s = Arc::clone(state);
    invitations.query(ListInvitations, move |_, qry, _| {
        let rows: Vec<InvitationRow> = s
            .read()
            .unwrap()
            .values()
            .filter(|r| r.tenant == qry.tenant)
            .cloned()
            .collect();
        async move { Ok(rows) }
    });
    let s = Arc::clone(state);
    invitations.query(PeekInvitation, move |d, _, _| {
        let found = parse_invitation_token(&d.token).and_then(|t| {
            s.read()
                .unwrap()
                .get(&row_key(&t.tenant, &t.invitation_id))
                .filter(|r| r.status == InvitationStatus::Open)
                .filter(|r| {
                    r.token.as_ref().is_some_and(|(id, hash)| {
                        *id == t.token_id && equal_secrets(hash, &sha256(&t.secret))
                    })
                })
                .map(|r| Peeked {
                    tenant: r.tenant.clone(),
                    invitation_id: r.id.clone(),
                    email: r.email.clone(),
                    owner: r.owner,
                    expired: Timestamp::now().millis() > r.expires_at,
                })
        });
        async move {
            found.ok_or_else(|| {
                Rejected::new("The invitation is not valid")
                    .code("invalid_token")
                    .into()
            })
        }
    });
    let l = Arc::clone(links);
    invitations.query(ListInvitationLinks, move |_, qry, _| {
        let rows: Vec<LinkRow> = l
            .read()
            .unwrap()
            .values()
            .filter(|r| r.tenant == qry.tenant)
            .cloned()
            .collect();
        async move { Ok(rows) }
    });
    let l = Arc::clone(links);
    invitations.query(PeekInvitationLink, move |d, _, _| {
        let found = parse_link_token(&d.token).and_then(|t| {
            l.read()
                .unwrap()
                .get(&row_key(&t.tenant, &t.link_id))
                .filter(|r| r.status == LinkStatus::Open && equal_secrets(&r.hash, &t.hash))
                .map(|r| {
                    let now = Timestamp::now().millis();
                    PeekedLink {
                        tenant: r.tenant.clone(),
                        link_id: r.id.clone(),
                        label: r.label.clone(),
                        usable: r.expires_at.is_none_or(|e| now <= e)
                            && r.max_uses.is_none_or(|m| r.uses < m),
                    }
                })
        });
        async move {
            found.ok_or_else(|| {
                Rejected::new("The link is not valid")
                    .code("invalid_token")
                    .into()
            })
        }
    });
}
