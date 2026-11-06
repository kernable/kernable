"""The invitation aggregates: state, how each event changes it, unique keys."""

from kernable import Aggregate

from .domain import InvitationStatus, invitation_domain
from .events import InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationResent, InvitationRevoked


class Invitation(Aggregate, domain=invitation_domain, schema="py-1"):
    status: InvitationStatus = "none"
    erased: bool = False
    email: str | None = None
    groups: list = []
    owner: bool = False
    token_id: str | None = None
    #: The token while open; in the vault, so None once used or erased.
    token: str | None = None
    expires_at: int = 0
    account_id: str | None = None


@Invitation.apply(InvitationCreated)
def _(s: Invitation, d: InvitationCreated):
    s.update(status="open", email=d.email, groups=list(d.groups), owner=d.owner, token_id=d.token_id, token=d.token, expires_at=d.expires_at)


@Invitation.apply(InvitationResent)
def _(s: Invitation, d: InvitationResent):
    s.update(token_id=d.token_id, token=d.token, expires_at=d.expires_at)


@Invitation.apply(InvitationRevoked)
def _(s: Invitation):
    s.update(status="revoked", token=None)


@Invitation.apply(InvitationDeclined)
def _(s: Invitation):
    s.update(status="declined", token=None)


@Invitation.apply(InvitationAccepted)
def _(s: Invitation, d: InvitationAccepted):
    s.update(status="accepted", token=None, account_id=d.account_id)


@Invitation.apply(InvitationErased)
def _(s: Invitation):
    s.update(erased=True, email=None)


# One open invitation per email and tenant.
Invitation.unique("email", lambda s: s.email if s.status == "open" and s.email else None)

#: The aggregate type, for workers.
invitation_aggregate = Invitation.__aggregate__
