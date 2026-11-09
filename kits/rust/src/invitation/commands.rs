//! The invitation commands: each type with its handler.

use kernable::{Aggregate, Rejected, Requirement, command};

use super::{
    Invitation, InvitationLink, InvitationStatus, InvitedGroup, LinkStatus, events::*, link_token,
    parse_link_token,
};
use crate::shared::constants::{DAY, INVITATION_DAYS};
use crate::shared::secrets::{
    equal_secrets, normal_email, parse_invitation_token, random_secret, sha256, valid_email,
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

/// A reusable invitation link into `groups` (never as owner). The API generates the secret and
/// shows the token once; the event keeps its hash. The middleware lets the sender attach only
/// groups it could grant.
#[command("invitation", aggregate = "link", secret = [secret])]
pub struct CreateInvitationLink {
    pub secret: String,
    pub groups: Option<Vec<InvitedGroup>>,
    pub label: Option<String>,
    /// Until when it works (ms since epoch); `None`: until revoked.
    pub expires_at: Option<i64>,
    /// How many people may join through it; `None`: no limit.
    pub max_uses: Option<i64>,
}

#[command("invitation", aggregate = "link")]
pub struct RevokeInvitationLink {}

/// The API's: the signed-in account `account_id` joins the tenant through the link. Someone
/// without an account registers and verifies first: a link, unlike an emailed invitation, proves
/// no email.
#[command("invitation", aggregate = "link", secret = [token])]
pub struct JoinWithInvitationLink {
    pub token: String,
    pub account_id: String,
}

/// A link's secret: at least 32 URL-safe characters.
fn valid_link_secret(secret: &str) -> bool {
    secret.len() >= 32
        && secret
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

pub fn handle_links(link: &Aggregate<InvitationLink>) {
    link.command(CreateInvitationLink, |s, d, cmd, _| {
        if s.status != LinkStatus::None {
            return Err(rejected("The link exists", "exists"));
        }
        if !valid_link_secret(&d.secret) {
            return Err(rejected(
                "A secret of at least 32 URL-safe characters",
                "invalid_secret",
            ));
        }
        if d.max_uses.is_some_and(|m| m <= 0) {
            return Err(rejected(
                "maxUses is a positive whole number",
                "invalid_max_uses",
            ));
        }
        Ok(vec![
            InvitationLinkCreated {
                groups: groups(d.groups)?,
                hash: sha256(&link_token(&cmd.tenant, &cmd.aggregate_id, &d.secret)),
                label: d.label.unwrap_or_default(),
                expires_at: d.expires_at,
                max_uses: d.max_uses,
            }
            .into(),
        ])
    });
    link.requires(CreateInvitationLink, |_, d| {
        d.groups
            .unwrap_or_default()
            .into_iter()
            .map(|g| Requirement::new("group", "group", &g.group, "active"))
            .collect::<Vec<_>>()
    });
    link.command(RevokeInvitationLink, |s, _, _, _| {
        if s.status != LinkStatus::Open {
            return Err(rejected("Only an open link is revoked", "invalid_state"));
        }
        Ok(vec![InvitationLinkRevoked {}.into()])
    });
    link.command(JoinWithInvitationLink, |s, d, cmd, ctx| {
        let valid = parse_link_token(&d.token).is_some_and(|t| {
            s.status == LinkStatus::Open
                && t.tenant == cmd.tenant
                && t.link_id == cmd.aggregate_id
                && s.hash.as_deref().is_some_and(|h| equal_secrets(h, &t.hash))
        });
        if !valid {
            return Err(rejected("The link is not valid", "invalid_token"));
        }
        if s.expires_at.is_some_and(|e| ctx.now().millis() > e) {
            return Err(rejected("The link has expired", "expired_token"));
        }
        if s.max_uses.is_some_and(|m| s.accounts.len() as i64 >= m) {
            return Err(rejected("The link is used up", "used_up"));
        }
        if d.account_id.is_empty() {
            return Err(rejected("Which account?", "invalid_account"));
        }
        if s.accounts.contains(&d.account_id) {
            return Err(rejected("Joined through this link already", "exists"));
        }
        Ok(vec![
            InvitationLinkUsed {
                account_id: d.account_id,
                groups: s.groups.clone(),
            }
            .into(),
        ])
    });
}
