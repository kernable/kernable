"""The identity reactors: side effects and follow-up commands."""

import uuid

from kernable import Rejected

from ..account.events import AccountDeleted
from ..group.events import GroupDeleted
from ..invitation.events import InvitationAccepted
from ..shared.constants import ORDERS, internal
from ..tenancy.commands import AddOwner
from ..tenancy.events import TenantCreated, TenantDeleted
from .commands import CreateIdentity, DeleteIdentity, GrantMembership, RevokeMembership
from .domain import identity_domain
from .queries import FindIdentityByAccount, ListIdentities, ListMyIdentities

owners_reactor = identity_domain.reactor("identity.owners", order=ORDERS.reactors)


@owners_reactor.on(TenantCreated)
def _(d: TenantCreated, evt, ctx):
    """Creates the first owner of a self-service tenant, then makes it the owner."""
    account_id = d.owner.get("account_id") if isinstance(d.owner, dict) else None
    if not account_id:
        return
    tenant = evt.aggregate_id
    # A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
    created = ctx.submit(CreateIdentity(account_id=account_id), aggregate_id=str(uuid.uuid4()), id=f"owner-identity-{tenant}",
                         tenant=tenant, context=internal(ctx))
    ctx.submit(AddOwner(identity_id=created[0].aggregate_id), aggregate_id=tenant, id=f"owner-{tenant}", tenant=tenant, context=internal(ctx))


def _tolerating(code: str, submit) -> None:
    try:
        submit()
    except Rejected as err:
        if err.code != code:
            raise


invitations_reactor = identity_domain.reactor("identity.invitations", order=ORDERS.reactors)


@invitations_reactor.on(InvitationAccepted)
def _(d: InvitationAccepted, evt, ctx):
    """An accepted invitation becomes an identity with its groups, and an owner for an owner invitation."""
    tenant, invitation = evt.tenant, evt.aggregate_id
    try:
        # An account with an identity here already keeps it and gains the groups.
        identity_id = ctx.query(FindIdentityByAccount(account_id=d.account_id), tenant=tenant, context=internal())["identity_id"]
    except Rejected:
        created = ctx.submit(CreateIdentity(account_id=d.account_id), aggregate_id=str(uuid.uuid4()), id=f"invitation-identity-{invitation}",
                             tenant=tenant, context=internal(ctx))
        identity_id = created[0].aggregate_id
    for i, g in enumerate(d.groups):
        _tolerating("exists", lambda g=g, i=i: ctx.submit(
            GrantMembership(membership_id=f"{invitation}-{i}", group=g["group"], scope=g.get("scope")), aggregate_id=identity_id,
            id=f"invitation-membership-{invitation}-{i}", tenant=tenant, context=internal(ctx)))
    if d.owner:
        _tolerating("exists", lambda: ctx.submit(AddOwner(identity_id=identity_id), aggregate_id=tenant, id=f"invitation-owner-{invitation}",
                                                 tenant=tenant, context=internal(ctx)))


cleanup_reactor = identity_domain.reactor("identity.cleanup", order=ORDERS.cleanup)


@cleanup_reactor.on(TenantDeleted)
def _(d, evt, ctx):
    """A deleted tenant's identities are erased, before the read models drop the tenant."""
    tenant = evt.aggregate_id
    for r in ctx.query(ListIdentities(), tenant=tenant, context=internal()):
        ctx.submit(DeleteIdentity(), aggregate_id=r["id"], id=f"delete-{r['id']}", tenant=tenant, context=internal(ctx))


follow_reactor = identity_domain.reactor("identity.follow", order=ORDERS.reactors)


@follow_reactor.on(AccountDeleted)
def _(d, evt, ctx):
    """A deleted account's identities go in every tenant. One that still owns a tenant is refused
    (an owner must be replaced first) and parks here for an operator."""
    for r in ctx.query(ListMyIdentities(account_id=evt.aggregate_id), tenant=evt.tenant, context=internal(), all_partitions=True):
        ctx.submit(DeleteIdentity(), aggregate_id=r["identity_id"], id=f"delete-{r['identity_id']}", tenant=r["tenant"], context=internal(ctx))


@follow_reactor.on(GroupDeleted)
def _(d, evt, ctx):
    """A deleted group's memberships stopped counting at once; here they leave the identities."""
    for r in ctx.query(ListIdentities(), tenant=evt.tenant, context=internal()):
        for m in r["memberships"]:
            if m["group"] == evt.aggregate_id:
                ctx.submit(RevokeMembership(membership_id=m["membership_id"]), aggregate_id=r["id"],
                           id=f"revoke-{r['id']}-{m['membership_id']}", tenant=evt.tenant, context=internal(ctx))
