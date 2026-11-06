//! The invitation read models.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kernable::{Domain, Listener};
use serde::{Deserialize, Serialize};

use super::{InvitationStatus, InvitedGroup, events::*};
use crate::shared::constants::orders;
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
}

/// `tenant/invitationId` → the invitation.
pub type Shared = Arc<RwLock<BTreeMap<String, InvitationRow>>>;

pub fn row_key(tenant: &str, invitation_id: &str) -> String {
    format!("{tenant}/{invitation_id}")
}

fn update(
    t: &mut BTreeMap<String, InvitationRow>,
    evt: &kernable::StoredEventMeta,
    f: impl FnOnce(&mut InvitationRow),
) {
    if let Some(r) = t.get_mut(&row_key(&evt.tenant, &evt.aggregate_id)) {
        f(r);
    }
}

pub fn invitations(domain: &Domain) -> (Listener, Shared) {
    let state: Shared = Arc::default();
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
        |r| r.expires_at = d.expires_at
    ));
    on!(l, state, InvitationRevoked, |t, _d, evt| update(
        t,
        &evt,
        |r| r.status = InvitationStatus::Revoked
    ));
    on!(l, state, InvitationDeclined, |t, _d, evt| update(
        t,
        &evt,
        |r| r.status = InvitationStatus::Declined
    ));
    on!(l, state, InvitationAccepted, |t, _d, evt| update(
        t,
        &evt,
        |r| r.status = InvitationStatus::Accepted
    ));
    on!(l, state, InvitationErased, |t, _d, evt| update(
        t,
        &evt,
        |r| r.email = None
    ));
    on!(l, state, TenantDeleted, |t, _d, evt| t
        .retain(|_, r| r.tenant != evt.aggregate_id));
    (l, state)
}
