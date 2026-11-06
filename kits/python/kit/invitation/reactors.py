"""The invitation reactors: side effects and follow-up commands."""

import uuid

from ..shared.constants import ORDERS, internal
from ..shared.mail import Mail, current_mailer
from ..shared.secrets import invitation_token
from ..tenancy.events import TenantCreated, TenantDeleted
from .commands import CreateInvitation, EraseInvitation, RecordInvitationMailSent
from .domain import invitation_domain
from .events import InvitationCreated, InvitationResent
from .queries import ListInvitations

mail_reactor = invitation_domain.reactor("invitation.mail", order=ORDERS.reactors)


def _send(tenant, invitation_id, token_id, secret, email, owner, ctx) -> None:
    if not secret or not email:
        return  # used or erased meanwhile
    link = invitation_token(tenant, invitation_id, token_id, secret)
    current_mailer().send(Mail(to=email, purpose="invitation", token=link, subject="You are invited to own a tenant" if owner else "You are invited",
                               text=f"Accept the invitation with this code: {link}"))
    ctx.submit(RecordInvitationMailSent(token_id=token_id), aggregate_id=invitation_id, id=f"mail-{token_id}", tenant=tenant, context=internal(ctx))


@mail_reactor.on(InvitationCreated)
def _(d: InvitationCreated, evt, ctx):
    _send(evt.tenant, evt.aggregate_id, d.token_id, d.token, d.email, d.owner, ctx)


@mail_reactor.on(InvitationResent)
def _(d: InvitationResent, evt, ctx):
    row = next((r for r in ctx.query(ListInvitations(), tenant=evt.tenant, context=internal()) if r["id"] == evt.aggregate_id), None)
    _send(evt.tenant, evt.aggregate_id, d.token_id, d.token, row and row["email"], bool(row and row["owner"]), ctx)


owners_reactor = invitation_domain.reactor("invitation.owners", order=ORDERS.reactors)


@owners_reactor.on(TenantCreated)
def _(d: TenantCreated, evt, ctx):
    """A tenant the platform creates gets its first owner by an owner invitation."""
    email = d.owner.get("email") if isinstance(d.owner, dict) else None
    if email:
        ctx.submit(CreateInvitation(email=email, owner=True), aggregate_id=str(uuid.uuid4()), id=f"owner-invitation-{evt.aggregate_id}",
                   tenant=evt.aggregate_id, context=internal(ctx))


cleanup_reactor = invitation_domain.reactor("invitation.cleanup", order=ORDERS.cleanup)


@cleanup_reactor.on(TenantDeleted)
def _(d, evt, ctx):
    """A deleted tenant's invitations are revoked and their emails erased, before the read models drop them."""
    for r in ctx.query(ListInvitations(), tenant=evt.aggregate_id, context=internal()):
        if r["email"] is not None:
            ctx.submit(EraseInvitation(), aggregate_id=r["id"], id=f"erase-{r['id']}", tenant=evt.aggregate_id, context=internal(ctx))
