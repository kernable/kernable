//! The invitation reactors: side effects and follow-up commands.

use kernable::{BoxError, Domain, ListenerContext, Query, Submit};

use super::commands::{CreateInvitation, EraseInvitation, RecordInvitationMailSent};
use super::events::{InvitationCreated, InvitationResent};
use super::queries::ListInvitations;
use super::readmodels::InvitationRow;
use crate::shared::constants::orders;
use crate::shared::internal;
use crate::shared::mail::{Mail, Purpose, current_mailer};
use crate::shared::secrets::{invitation_token, new_id};
use crate::tenancy::{TenantCreated, TenantDeleted};

struct Invited<'a> {
    tenant: &'a str,
    invitation_id: &'a str,
    token_id: &'a str,
    secret: Option<&'a str>,
    email: Option<&'a str>,
    owner: bool,
}

async fn send(i: Invited<'_>, ctx: &ListenerContext) -> Result<(), BoxError> {
    // Used or erased meanwhile: nothing to send.
    let (Some(secret), Some(email)) = (i.secret, i.email.filter(|e| !e.is_empty())) else {
        return Ok(());
    };
    let link = invitation_token(i.tenant, i.invitation_id, i.token_id, secret);
    current_mailer().send(Mail {
        to: email.to_owned(),
        subject: if i.owner {
            "You are invited to own a tenant"
        } else {
            "You are invited"
        }
        .into(),
        text: format!("Accept the invitation with this code: {link}"),
        purpose: Purpose::Invitation,
        token: link,
    });
    let record = Submit::command(
        RecordInvitationMailSent {
            token_id: i.token_id.to_owned(),
        },
        i.invitation_id,
    )
    .id(format!("mail-{}", i.token_id))
    .tenant(i.tenant)
    .context(internal(Some(ctx)));
    ctx.submit(record).await?;
    Ok(())
}

pub fn react(domain: &Domain) {
    let mails = domain.reactor("invitation.mail").order(orders::REACTORS);
    mails.on(InvitationCreated, |d, evt, ctx| async move {
        let invited = Invited {
            tenant: &evt.tenant,
            invitation_id: &evt.aggregate_id,
            token_id: &d.token_id,
            secret: d.token.as_deref(),
            email: d.email.as_deref(),
            owner: d.owner,
        };
        send(invited, &ctx).await
    });
    mails.on(InvitationResent, |d, evt, ctx| async move {
        let rows: Vec<InvitationRow> = ctx
            .query(
                Query::of(ListInvitations {})
                    .tenant(&evt.tenant)
                    .context(internal(None)),
            )
            .await?;
        let row = rows.into_iter().find(|r| r.id == evt.aggregate_id);
        let invited = Invited {
            tenant: &evt.tenant,
            invitation_id: &evt.aggregate_id,
            token_id: &d.token_id,
            secret: d.token.as_deref(),
            email: row.as_ref().and_then(|r| r.email.as_deref()),
            owner: row.as_ref().is_some_and(|r| r.owner),
        };
        send(invited, &ctx).await
    });

    let owners = domain.reactor("invitation.owners").order(orders::REACTORS);
    // A tenant the platform creates gets its first owner by an owner invitation.
    owners.on(TenantCreated, |d, evt, ctx| async move {
        let Some(email) = d.owner.email.filter(|e| !e.is_empty()) else {
            return Ok(());
        };
        let invite = Submit::command(
            CreateInvitation {
                email,
                groups: None,
                owner: Some(true),
            },
            &new_id(),
        )
        .id(format!("owner-invitation-{}", evt.aggregate_id))
        .tenant(&evt.aggregate_id)
        .context(internal(Some(&ctx)));
        ctx.submit(invite).await?;
        Ok(())
    });

    let cleanup = domain.reactor("invitation.cleanup").order(orders::CLEANUP);
    // A deleted tenant's invitations are revoked and their emails erased, before the read models drop them.
    cleanup.on(TenantDeleted, |_d, evt, ctx| async move {
        let tenant = evt.aggregate_id;
        let rows: Vec<InvitationRow> = ctx
            .query(
                Query::of(ListInvitations {})
                    .tenant(&tenant)
                    .context(internal(None)),
            )
            .await?;
        for r in rows.into_iter().filter(|r| r.email.is_some()) {
            let erase = Submit::command(EraseInvitation {}, &r.id)
                .id(format!("erase-{}", r.id))
                .tenant(&tenant)
                .context(internal(Some(&ctx)));
            ctx.submit(erase).await?;
        }
        Ok(())
    });
}
