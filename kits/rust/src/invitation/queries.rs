//! The invitation queries: each type with its handler.

use std::sync::Arc;

use kernable::{Listener, Rejected, Timestamp, query};
use serde::{Deserialize, Serialize};

use super::InvitationStatus;
use super::readmodels::{InvitationRow, Shared, row_key};
use crate::shared::secrets::parse_invitation_token;

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

/// The API's only: what the acceptance page shows for a token (the token itself is checked on accepting).
#[query("invitation", answer = Peeked)]
pub struct PeekInvitation {
    pub token: String,
}

pub fn answer(invitations: &Listener, state: &Shared) {
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
}
