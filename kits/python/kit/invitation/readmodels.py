"""The invitation read models."""

from typing import TypedDict

from ..shared.constants import ORDERS
from ..tenancy.events import TenantDeleted
from .domain import InvitationStatus, InvitedGroup, invitation_domain
from .events import InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationResent, InvitationRevoked


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


@invitations_readmodel.on(TenantDeleted)
def _(d, evt):
    for k in [k for k, r in invitation_rows.items() if r["tenant"] == evt.aggregate_id]:
        del invitation_rows[k]
