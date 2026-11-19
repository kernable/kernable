"""The identity reactors: side effects and follow-up commands."""

import uuid

from kernable import Rejected

from ..account.events import AccountDeleted
from ..group.events import GroupDeleted
from ..invitation.events import InvitationAccepted, InvitationLinkUsed
from ..shared.constants import ORDERS, PLATFORM_TENANT, internal
from ..shared.secrets import random_secret
from ..tenancy.commands import AddOwner
from ..tenancy.events import TenantCreated, TenantDeleted
from .commands import CreateApiKey, CreateIdentity, CreateProcessIdentity, DeleteIdentity, GrantMembership, RevokeMembership
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


def _join(ctx, tenant: str, source: str, account_id: str, groups: list, owner: bool) -> None:
    """An account joins a tenant with groups (and as owner): its identity there is created, or kept if
    it has one, and gains the groups. `source` makes the follow-up command IDs unique per join."""
    try:
        identity_id = ctx.query(FindIdentityByAccount(account_id=account_id), tenant=tenant, context=internal())["identity_id"]
    except Rejected:
        created = ctx.submit(CreateIdentity(account_id=account_id), aggregate_id=str(uuid.uuid4()), id=f"invitation-identity-{source}",
                             tenant=tenant, context=internal(ctx))
        identity_id = created[0].aggregate_id
    for i, g in enumerate(groups):
        _tolerating("exists", lambda g=g, i=i: ctx.submit(
            GrantMembership(membership_id=f"{source}-{i}", group=g["group"], scope=g.get("scope")), aggregate_id=identity_id,
            id=f"invitation-membership-{source}-{i}", tenant=tenant, context=internal(ctx)))
    if owner:
        _tolerating("exists", lambda: ctx.submit(AddOwner(identity_id=identity_id), aggregate_id=tenant, id=f"invitation-owner-{source}",
                                                 tenant=tenant, context=internal(ctx)))


@invitations_reactor.on(InvitationAccepted)
def _(d: InvitationAccepted, evt, ctx):
    """An accepted invitation becomes an identity with its groups, and an owner for an owner invitation."""
    _join(ctx, evt.tenant, evt.aggregate_id, d.account_id, d.groups, d.owner)


@invitations_reactor.on(InvitationLinkUsed)
def _(d: InvitationLinkUsed, evt, ctx):
    """A join through an invitation link becomes an identity with its groups, never as owner."""
    _join(ctx, evt.tenant, f"{evt.aggregate_id}-{d.account_id}", d.account_id, d.groups, False)


# From the first event when new: an app registers its processes as it starts, maybe before this
# reactor's first connect; starting at the newest event would miss them (each step is safe to repeat).
processes_reactor = identity_domain.reactor("identity.processes", order=ORDERS.reactors, start=0)


@processes_reactor.on("process.ProcessRegistered")
def _(d, evt, ctx):
    """A registered process acts as an identity of its own in the platform tenant (kind `process`,
    no permissions: a platform admin grants it groups), with an API key whose secret nobody keeps;
    the kernel learns it by `Act`. Fixed IDs make every step safe to repeat, also for a new version."""
    from kernable import RequestContext

    name = d["name"]
    identity_id = f"process-{name}"
    _tolerating("exists", lambda: ctx.submit(CreateProcessIdentity(process=name), aggregate_id=identity_id,
                                             id=f"process-identity-{name}", tenant=PLATFORM_TENANT, context=internal(ctx)))
    key_id = "".join(c if c.isalnum() else "x" for c in random_secret(12))
    # A redelivered event gets the first outcome back, with the key ID it created.
    created = ctx.submit(CreateApiKey(key_id=key_id, secret=random_secret(), label=f"process {name}"), aggregate_id=identity_id,
                         id=f"process-key-{name}", tenant=PLATFORM_TENANT, context=internal(ctx))
    key_id = next(e.data["keyId"] for e in created if e.type == "ApiKeyCreated")
    ctx.process_act(name, RequestContext(sender_tenant=PLATFORM_TENANT, sender_identity=identity_id, sender_session=key_id))


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
