//! The invitation read models.

use std::sync::{Arc, RwLock};

use indexmap::IndexMap;
use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::{InvitationStatus, InvitedGroup, LinkStatus, events::*};
use crate::shared::constants::orders;
use crate::shared::secrets::sha256;
use crate::tenancy::TenantDeleted;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationRow {
    pub id: String,
    pub tenant: String,
    pub email: Option<String>,
    pub groups: Vec<InvitedGroup>,
    pub owner: bool,
    pub status: InvitationStatus,
    pub expires_at: i64,
    pub invited_by: Option<String>,
    pub invited_at: i64,
    /// The open token's ID and the SHA-256 of its secret: `PeekInvitation` answers only for the
    /// right token. Never shown in lists.
    #[serde(skip)]
    pub token: Option<(String, String)>,
}

/// `tenant/invitationId` → the invitation.
pub type Shared = Arc<RwLock<IndexMap<String, InvitationRow>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkRow {
    pub id: String,
    pub tenant: String,
    pub groups: Vec<InvitedGroup>,
    pub label: String,
    pub status: LinkStatus,
    pub expires_at: Option<i64>,
    pub max_uses: Option<i64>,
    /// How many joined through it.
    pub uses: i64,
    pub created_by: Option<String>,
    pub created_at: i64,
    /// The link token's hash: `PeekInvitationLink` answers only for the right token. Never shown
    /// in lists.
    #[serde(skip)]
    pub hash: String,
}

/// `tenant/linkId` → the invitation link.
pub type Links = Arc<RwLock<IndexMap<String, LinkRow>>>;

pub fn row_key(tenant: &str, invitation_id: &str) -> String {
    format!("{tenant}/{invitation_id}")
}

fn update<R>(t: &mut IndexMap<String, R>, evt: &kernable::StoredEventMeta, f: impl FnOnce(&mut R)) {
    if let Some(r) = t.get_mut(&row_key(&evt.tenant, &evt.aggregate_id)) {
        f(r);
    }
}

pub fn invitations(domain: &Domain) -> (Listener, Shared, Links) {
    let state: Shared = Arc::default();
    let links: Links = Arc::default();
    let l = domain
        .read_model("invitation.invitations")
        .order(orders::READ_MODELS);
    {
        let state = Arc::clone(&state);
        l.on(InvitationCreated, move |d, evt, ctx| {
            let state = Arc::clone(&state);
            async move {
                let row = InvitationRow {
                    id: evt.aggregate_id.clone(),
                    tenant: evt.tenant.clone(),
                    email: d.email,
                    groups: d.groups,
                    owner: d.owner,
                    status: InvitationStatus::Open,
                    expires_at: d.expires_at,
                    invited_by: Some(ctx.request.sender_identity.clone()).filter(|s| !s.is_empty()),
                    invited_at: evt.created_at.millis(),
                    token: d.token.map(|t| (d.token_id, sha256(&t))),
                };
                state
                    .write()
                    .unwrap()
                    .insert(row_key(&evt.tenant, &evt.aggregate_id), row);
                Ok(())
            }
        });
    }
    on!(l, state, InvitationResent, |t, d, evt| update(
        t,
        &evt,
        |r| {
            r.expires_at = d.expires_at;
            r.token = d.token.map(|token| (d.token_id, sha256(&token)));
        }
    ));
    on!(l, state, InvitationRevoked, |t, _d, evt| update(
        t,
        &evt,
        |r| {
            r.status = InvitationStatus::Revoked;
            r.token = None;
        }
    ));
    on!(l, state, InvitationDeclined, |t, _d, evt| update(
        t,
        &evt,
        |r| {
            r.status = InvitationStatus::Declined;
            r.token = None;
        }
    ));
    on!(l, state, InvitationAccepted, |t, _d, evt| update(
        t,
        &evt,
        |r| {
            r.status = InvitationStatus::Accepted;
            r.token = None;
        }
    ));
    on!(l, state, InvitationErased, |t, _d, evt| update(
        t,
        &evt,
        |r| r.email = None
    ));
    {
        let links = Arc::clone(&links);
        l.on(InvitationLinkCreated, move |d, evt, ctx| {
            let links = Arc::clone(&links);
            async move {
                let row = LinkRow {
                    id: evt.aggregate_id.clone(),
                    tenant: evt.tenant.clone(),
                    groups: d.groups,
                    label: d.label,
                    status: LinkStatus::Open,
                    expires_at: d.expires_at,
                    max_uses: d.max_uses,
                    uses: 0,
                    created_by: Some(ctx.request.sender_identity.clone()).filter(|s| !s.is_empty()),
                    created_at: evt.created_at.millis(),
                    hash: d.hash,
                };
                links
                    .write()
                    .unwrap()
                    .insert(row_key(&evt.tenant, &evt.aggregate_id), row);
                Ok(())
            }
        });
    }
    on!(l, links, InvitationLinkRevoked, |t, _d, evt| update(
        t,
        &evt,
        |r| r.status = LinkStatus::Revoked
    ));
    on!(l, links, InvitationLinkUsed, |t, _d, evt| update(
        t,
        &evt,
        |r| r.uses += 1
    ));
    {
        let (state, links) = (Arc::clone(&state), Arc::clone(&links));
        l.on(TenantDeleted, move |_d, evt, _ctx| {
            let (state, links) = (Arc::clone(&state), Arc::clone(&links));
            async move {
                let tenant = &evt.aggregate_id;
                state.write().unwrap().retain(|_, r| &r.tenant != tenant);
                links.write().unwrap().retain(|_, r| &r.tenant != tenant);
                Ok(())
            }
        });
    }
    (l, state, links)
}
