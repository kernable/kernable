"""The invitation read models."""

from typing import TypedDict

from ..shared.constants import ORDERS
from ..tenancy.events import TenantDeleted
from .domain import InvitationStatus, InvitedGroup, LinkStatus, invitation_domain
from .events import (InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationLinkCreated, InvitationLinkRevoked,
                     InvitationLinkUsed, InvitationResent, InvitationRevoked)


class InvitationRow(TypedDict):
    id: str
    tenant: str
    email: str | None
    groups: list[InvitedGroup]
    owner: bool
    status: InvitationStatus
    expires_at: int
    invited_by: str | None
    invited_at: int


#: `tenant/invitationId`
invitation_rows: dict[str, InvitationRow] = {}


class LinkRow(TypedDict):
    id: str
    tenant: str
    groups: list[InvitedGroup]
    label: str
    status: LinkStatus
    expires_at: int | None
    max_uses: int | None
    #: How many joined through it.
    uses: int
    created_by: str | None
    created_at: int


#: `tenant/linkId`
link_rows: dict[str, LinkRow] = {}


def row_key(tenant: str, invitation_id: str) -> str:
    return f"{tenant}/{invitation_id}"


invitations_readmodel = invitation_domain.read_model("invitation.invitations", order=ORDERS.read_models)


@invitations_readmodel.on(InvitationCreated)
def _(d: InvitationCreated, evt, ctx):
    invitation_rows[row_key(evt.tenant, evt.aggregate_id)] = InvitationRow(
        id=evt.aggregate_id, tenant=evt.tenant, email=d.email, groups=d.groups, owner=d.owner, status="open", expires_at=d.expires_at,
        invited_by=ctx.request.sender_identity or None, invited_at=int(evt.created_at.timestamp() * 1000))


@invitations_readmodel.on(InvitationResent)
def _(d: InvitationResent, evt):
    if r := invitation_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        r["expires_at"] = d.expires_at


def _status(status: InvitationStatus):
    def handler(d, evt):
        if r := invitation_rows.get(row_key(evt.tenant, evt.aggregate_id)):
            r["status"] = status
    return handler


invitations_readmodel.on(InvitationRevoked, _status("revoked"))
invitations_readmodel.on(InvitationDeclined, _status("declined"))
invitations_readmodel.on(InvitationAccepted, _status("accepted"))


@invitations_readmodel.on(InvitationErased)
def _(d, evt):
    if r := invitation_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        r["email"] = None


@invitations_readmodel.on(InvitationLinkCreated)
def _(d: InvitationLinkCreated, evt, ctx):
    link_rows[row_key(evt.tenant, evt.aggregate_id)] = LinkRow(
        id=evt.aggregate_id, tenant=evt.tenant, groups=d.groups, label=d.label, status="open", expires_at=d.expires_at, max_uses=d.max_uses,
        uses=0, created_by=ctx.request.sender_identity or None, created_at=int(evt.created_at.timestamp() * 1000))


@invitations_readmodel.on(InvitationLinkRevoked)
def _(d, evt):
    if r := link_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        r["status"] = "revoked"


@invitations_readmodel.on(InvitationLinkUsed)
def _(d, evt):
    if r := link_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        r["uses"] += 1


@invitations_readmodel.on(TenantDeleted)
def _(d, evt):
    for k in [k for k, r in invitation_rows.items() if r["tenant"] == evt.aggregate_id]:
        del invitation_rows[k]
    for k in [k for k, r in link_rows.items() if r["tenant"] == evt.aggregate_id]:
        del link_rows[k]
