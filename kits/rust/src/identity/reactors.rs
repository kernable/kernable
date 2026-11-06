//! The identity reactors: side effects and follow-up commands.

use kernable::{BoxError, Domain, Error, ListenerContext, Query, Submit};

use super::commands::{CreateIdentity, DeleteIdentity, GrantMembership, RevokeMembership};
use super::queries::{
    FindIdentityByAccount, FoundIdentity, ListIdentities, ListMyIdentities, MyIdentity,
};
use super::readmodels::IdentityRow;
use crate::account::AccountDeleted;
use crate::group::GroupDeleted;
use crate::invitation::InvitationAccepted;
use crate::shared::constants::orders;
use crate::shared::internal;
use crate::shared::secrets::new_id;
use crate::tenancy::{AddOwner, TenantCreated, TenantDeleted};

/// A follow-up command of this event, in `tenant`, with a fixed ID.
fn follow(ctx: &ListenerContext, submit: Submit, id: String, tenant: &str) -> Submit {
    submit
        .id(id)
        .tenant(tenant.to_owned())
        .context(internal(Some(ctx)))
}

/// Submits, accepting a rejection with `code` (done before).
async fn tolerating(ctx: &ListenerContext, code: &str, submit: Submit) -> Result<(), BoxError> {
    match ctx.submit(submit).await {
        Err(Error::Rejected(r)) if r.code == code => Ok(()),
        r => r.map(|_| ()).map_err(Into::into),
    }
}

pub fn react(domain: &Domain) {
    let owners = domain.reactor("identity.owners").order(orders::REACTORS);
    // Creates the first owner of a self-service tenant, then makes it the owner.
    owners.on(TenantCreated, |d, evt, ctx| async move {
        let Some(account_id) = d.owner.account_id else {
            return Ok(());
        };
        let tenant = evt.aggregate_id;
        // A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
        let create = Submit::command(
            CreateIdentity {
                account_id,
                display_name: None,
                title: None,
                email: None,
            },
            &new_id(),
        );
        let created = ctx
            .submit(follow(
                &ctx,
                create,
                format!("owner-identity-{tenant}"),
                &tenant,
            ))
            .await?;
        let identity_id = created
            .first()
            .map(|e| e.aggregate_id.clone())
            .ok_or("no identity created")?;
        ctx.submit(follow(
            &ctx,
            Submit::command(AddOwner { identity_id }, &tenant),
            format!("owner-{tenant}"),
            &tenant,
        ))
        .await?;
        Ok(())
    });

    let invitations = domain
        .reactor("identity.invitations")
        .order(orders::REACTORS);
    // An accepted invitation becomes an identity with its groups, and an owner for an owner invitation.
    invitations.on(InvitationAccepted, |d, evt, ctx| async move {
        let (tenant, invitation) = (evt.tenant.clone(), evt.aggregate_id.clone());
        // An account with an identity here already keeps it and gains the groups.
        let ask = Query::of(FindIdentityByAccount {
            account_id: d.account_id.clone(),
        })
        .tenant(&tenant)
        .context(internal(None));
        let identity_id = match ctx.query::<FoundIdentity>(ask).await {
            Ok(found) => found.identity_id,
            Err(Error::Rejected(_)) => {
                let create = Submit::command(
                    CreateIdentity {
                        account_id: d.account_id,
                        display_name: None,
                        title: None,
                        email: None,
                    },
                    &new_id(),
                );
                let created = ctx
                    .submit(follow(
                        &ctx,
                        create,
                        format!("invitation-identity-{invitation}"),
                        &tenant,
                    ))
                    .await?;
                created
                    .first()
                    .map(|e| e.aggregate_id.clone())
                    .ok_or("no identity created")?
            }
            Err(e) => return Err(e.into()),
        };
        for (i, g) in d.groups.into_iter().enumerate() {
            let grant = GrantMembership {
                membership_id: format!("{invitation}-{i}"),
                group: g.group,
                scope: g.scope,
                expires_at: None,
            };
            let submit = follow(
                &ctx,
                Submit::command(grant, &identity_id),
                format!("invitation-membership-{invitation}-{i}"),
                &tenant,
            );
            tolerating(&ctx, "exists", submit).await?;
        }
        if d.owner {
            let submit = follow(
                &ctx,
                Submit::command(AddOwner { identity_id }, &tenant),
                format!("invitation-owner-{invitation}"),
                &tenant,
            );
            tolerating(&ctx, "exists", submit).await?;
        }
        Ok(())
    });

    let cleanup = domain.reactor("identity.cleanup").order(orders::CLEANUP);
    // A deleted tenant's identities are erased, before the read models drop the tenant.
    cleanup.on(TenantDeleted, |_d, evt, ctx| async move {
        let tenant = evt.aggregate_id;
        let rows: Vec<IdentityRow> = ctx
            .query(
                Query::of(ListIdentities {})
                    .tenant(&tenant)
                    .context(internal(None)),
            )
            .await?;
        for r in rows {
            ctx.submit(follow(
                &ctx,
                Submit::command(DeleteIdentity {}, &r.id),
                format!("delete-{}", r.id),
                &tenant,
            ))
            .await?;
        }
        Ok(())
    });

    let follow_up = domain.reactor("identity.follow").order(orders::REACTORS);
    // A deleted account's identities go in every tenant. One that still owns a tenant is refused
    // (an owner must be replaced first) and parks here for an operator.
    follow_up.on(AccountDeleted, |_d, evt, ctx| async move {
        let ask = Query::of(ListMyIdentities {
            account_id: evt.aggregate_id.clone(),
        })
        .tenant(&evt.tenant)
        .context(internal(None))
        .all_partitions();
        let rows: Vec<MyIdentity> = ctx.query(ask).await?;
        for r in rows {
            let submit = Submit::command(DeleteIdentity {}, &r.identity_id);
            ctx.submit(follow(
                &ctx,
                submit,
                format!("delete-{}", r.identity_id),
                &r.tenant,
            ))
            .await?;
        }
        Ok(())
    });
    // A deleted group's memberships stopped counting at once; here they leave the identities.
    follow_up.on(GroupDeleted, |_d, evt, ctx| async move {
        let rows: Vec<IdentityRow> = ctx
            .query(
                Query::of(ListIdentities {})
                    .tenant(&evt.tenant)
                    .context(internal(None)),
            )
            .await?;
        for r in rows {
            for m in r.memberships.iter().filter(|m| m.group == evt.aggregate_id) {
                let revoke = Submit::command(
                    RevokeMembership {
                        membership_id: m.membership_id.clone(),
                    },
                    &r.id,
                );
                ctx.submit(follow(
                    &ctx,
                    revoke,
                    format!("revoke-{}-{}", r.id, m.membership_id),
                    &evt.tenant,
                ))
                .await?;
            }
        }
        Ok(())
    });
}
