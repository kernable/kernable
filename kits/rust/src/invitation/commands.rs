//! The invitation commands: each type with its handler.

use kernable::{Aggregate, Rejected, Requirement, command};

use super::{Invitation, InvitationStatus, InvitedGroup, events::*};
use crate::shared::constants::{DAY, INVITATION_DAYS};
use crate::shared::secrets::{
    equal_secrets, normal_email, parse_invitation_token, random_secret, valid_email,
};

fn rejected(message: &str, code: &str) -> Rejected {
    Rejected::new(message).code(code)
}

fn groups(value: Option<Vec<InvitedGroup>>) -> Result<Vec<InvitedGroup>, Rejected> {
    let groups = value.unwrap_or_default();
    if groups.iter().any(|g| g.group.is_empty()) {
        return Err(rejected("Each group needs its ID", "invalid_groups"));
    }
    Ok(groups)
}

/// The token, checked against the invitation: open, this tenant's, not expired, the secret right.
fn proven(
    s: &Invitation,
    value: &str,
    tenant: &str,
    aggregate_id: &str,
    now: i64,
) -> Result<String, Rejected> {
    let ok = parse_invitation_token(value).filter(|t| {
        s.status == InvitationStatus::Open
            && t.tenant == tenant
            && t.invitation_id == aggregate_id
            && t.token_id == s.token_id
            && s.token
                .as_deref()
                .is_some_and(|secret| equal_secrets(secret, &t.secret))
    });
    let Some(t) = ok else {
        return Err(rejected("The invitation is not valid", "invalid_token"));
    };
    if now > s.expires_at {
        return Err(rejected("The invitation has expired", "expired_token"));
    }
    Ok(t.token_id)
}

/// Invites `email` into `groups`. The middleware lets the sender attach only groups it could
/// grant, and owner invitations only to owners and the platform.
#[command("invitation", aggregate = "invitation")]
pub struct CreateInvitation {
    pub email: String,
    pub groups: Option<Vec<InvitedGroup>>,
    pub owner: Option<bool>,
}

#[command("invitation", aggregate = "invitation")]
pub struct ResendInvitation {}

#[command("invitation", aggregate = "invitation")]
pub struct RevokeInvitation {}

/// Public: the token is the proof.
#[command("invitation", aggregate = "invitation", secret = [token])]
pub struct DeclineInvitation {
    pub token: String,
}

/// The API's: it says which account accepts, and its email, which must be the invited one.
#[command("invitation", aggregate = "invitation", secret = [token])]
pub struct AcceptInvitation {
    pub token: String,
    pub account_id: String,
    pub email: String,
    pub new_account: bool,
}

/// Internal: the mail reactor, once the mail is out.
#[command("invitation", aggregate = "invitation")]
pub struct RecordInvitationMailSent {
    pub token_id: String,
}

/// Erases the invited email; an open invitation is revoked first.
#[command("invitation", aggregate = "invitation")]
pub struct EraseInvitation {}

pub fn handle(invitation: &Aggregate<Invitation>) {
    invitation.command(CreateInvitation, |s, d, _, ctx| {
        if s.status != InvitationStatus::None {
            return Err(rejected("The invitation exists", "exists"));
        }
        let email = normal_email(&d.email);
        if !valid_email(&email) {
            return Err(rejected("Not an email address", "invalid_email"));
        }
        Ok(vec![
            InvitationCreated {
                email: Some(email),
                groups: groups(d.groups)?,
                owner: d.owner == Some(true),
                token_id: random_secret(9),
                token: Some(random_secret(32)),
                expires_at: ctx.now().millis() + INVITATION_DAYS * DAY,
            }
            .into(),
        ])
    });
    invitation.requires(CreateInvitation, |_, d| {
        d.groups
            .unwrap_or_default()
            .into_iter()
            .map(|g| Requirement::new("group", "group", &g.group, "active"))
            .collect::<Vec<_>>()
    });
    invitation.command(ResendInvitation, |s, _, _, ctx| {
        if s.status != InvitationStatus::Open {
            return Err(rejected(
                "Only an open invitation is resent",
                "invalid_state",
            ));
        }
        Ok(vec![
            InvitationResent {
                previous_token_id: s.token_id.clone(),
                token_id: random_secret(9),
                token: Some(random_secret(32)),
                expires_at: ctx.now().millis() + INVITATION_DAYS * DAY,
            }
            .into(),
        ])
    });
    invitation.command(RevokeInvitation, |s, _, _, _| {
        if s.status != InvitationStatus::Open {
            return Err(rejected(
                "Only an open invitation is revoked",
                "invalid_state",
            ));
        }
        Ok(vec![
            InvitationRevoked {
                token_id: s.token_id.clone(),
            }
            .into(),
        ])
    });
    invitation.command(DeclineInvitation, |s, d, cmd, ctx| {
        Ok(vec![
            InvitationDeclined {
                token_id: proven(
                    s,
                    &d.token,
                    &cmd.tenant,
                    &cmd.aggregate_id,
                    ctx.now().millis(),
                )?,
            }
            .into(),
        ])
    });
    invitation.command(AcceptInvitation, |s, d, cmd, ctx| {
        let token_id = proven(
            s,
            &d.token,
            &cmd.tenant,
            &cmd.aggregate_id,
            ctx.now().millis(),
        )?;
        if Some(normal_email(&d.email)) != s.email {
            return Err(rejected(
                "The invitation is for another email address",
                "wrong_email",
            ));
        }
        if d.account_id.is_empty() {
            return Err(rejected("Which account?", "invalid_account"));
        }
        Ok(vec![
            InvitationAccepted {
                token_id,
                account_id: d.account_id,
                new_account: d.new_account,
                groups: s.groups.clone(),
                owner: s.owner,
            }
            .into(),
        ])
    });
    invitation.command(RecordInvitationMailSent, |_, d, _, _| {
        Ok(vec![
            InvitationMailSent {
                token_id: d.token_id,
            }
            .into(),
        ])
    });
    invitation.command(EraseInvitation, |s, _, _, _| {
        if s.status == InvitationStatus::None || s.erased {
            return Err(rejected("Nothing to erase", "not_found"));
        }
        Ok(if s.status == InvitationStatus::Open {
            vec![
                InvitationRevoked {
                    token_id: s.token_id.clone(),
                }
                .into(),
                InvitationErased {}.into(),
            ]
        } else {
            vec![InvitationErased {}.into()]
        })
    });
}
