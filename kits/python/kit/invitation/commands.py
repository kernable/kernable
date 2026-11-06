"""The invitation commands: each class with its handler."""

from kernable import Command, Rejected, Requirement

from ..shared.constants import DAY, INVITATION_DAYS, now_ms
from ..shared.secrets import equal_secrets, normal_email, parse_invitation_token, random_secret, valid_email
from .aggregates import Invitation
from .domain import InvitedGroup
from .events import (InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationMailSent, InvitationResent,
                     InvitationRevoked)


def _groups(value) -> list[InvitedGroup]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise Rejected("groups is a list", "invalid_groups")
    out = []
    for g in value:
        if not isinstance(g, dict) or not isinstance(g.get("group"), str) or not g["group"]:
            raise Rejected("Each group needs its ID", "invalid_groups")
        scope = g.get("scope")
        scope = {"type": scope["type"], "id": scope["id"]} if isinstance(scope, dict) and isinstance(scope.get("type"), str) and isinstance(scope.get("id"), str) else None
        out.append({"group": g["group"], "scope": scope})
    return out


def _proven(s: Invitation, value, tenant: str, aggregate_id: str, now: int) -> str:
    """The token, checked against the invitation: open, this tenant's, not expired, the secret right."""
    t = parse_invitation_token(value)
    if s.status != "open" or not t or t["tenant"] != tenant or t["invitation_id"] != aggregate_id or t["token_id"] != s.token_id \
            or not s.token or not equal_secrets(s.token, t["secret"]):
        raise Rejected("The invitation is not valid", "invalid_token")
    if now > s.expires_at:
        raise Rejected("The invitation has expired", "expired_token")
    return t["token_id"]


class CreateInvitation(Command):
    """Invites `email` into `groups`. The middleware lets the sender attach only groups it could
    grant, and owner invitations only to owners and the platform."""
    email: str
    groups: list[InvitedGroup] | None = None
    owner: bool | None = None


@Invitation.command(CreateInvitation, requires=lambda d, s: [Requirement("group", "group", g["group"], "active") for g in _groups(d.groups)])
def _(s: Invitation, d: CreateInvitation, cmd, ctx):
    if s.status != "none":
        raise Rejected("The invitation exists", "exists")
    email = normal_email(d.email)
    if not valid_email(email):
        raise Rejected("Not an email address", "invalid_email")
    return [InvitationCreated(email=email, groups=_groups(d.groups), owner=d.owner is True, token_id=random_secret(9), token=random_secret(),
                              expires_at=now_ms(ctx) + INVITATION_DAYS * DAY)]


class ResendInvitation(Command):
    pass


@Invitation.command(ResendInvitation)
def _(s: Invitation, d, cmd, ctx):
    if s.status != "open":
        raise Rejected("Only an open invitation is resent", "invalid_state")
    return [InvitationResent(previous_token_id=s.token_id, token_id=random_secret(9), token=random_secret(), expires_at=now_ms(ctx) + INVITATION_DAYS * DAY)]


class RevokeInvitation(Command):
    pass


@Invitation.command(RevokeInvitation)
def _(s: Invitation):
    if s.status != "open":
        raise Rejected("Only an open invitation is revoked", "invalid_state")
    return [InvitationRevoked(token_id=s.token_id)]


class DeclineInvitation(Command):
    """Public: the token is the proof."""
    vault = {"secret": ["token"]}
    token: str


@Invitation.command(DeclineInvitation)
def _(s: Invitation, d: DeclineInvitation, cmd, ctx):
    return [InvitationDeclined(token_id=_proven(s, d.token, cmd.tenant, cmd.aggregate_id, now_ms(ctx)))]


class AcceptInvitation(Command):
    """The API's: it says which account accepts, and its email, which must be the invited one."""
    vault = {"secret": ["token"]}
    token: str
    account_id: str
    email: str
    new_account: bool


@Invitation.command(AcceptInvitation)
def _(s: Invitation, d: AcceptInvitation, cmd, ctx):
    token_id = _proven(s, d.token, cmd.tenant, cmd.aggregate_id, now_ms(ctx))
    if normal_email(d.email) != s.email:
        raise Rejected("The invitation is for another email address", "wrong_email")
    if not isinstance(d.account_id, str) or not d.account_id:
        raise Rejected("Which account?", "invalid_account")
    return [InvitationAccepted(token_id=token_id, account_id=d.account_id, new_account=d.new_account is True, groups=list(s.groups), owner=s.owner)]


class RecordInvitationMailSent(Command):
    """Internal: the mail reactor, once the mail is out."""
    token_id: str


@Invitation.command(RecordInvitationMailSent)
def _(s: Invitation, d: RecordInvitationMailSent):
    return [InvitationMailSent(token_id=d.token_id)]


class EraseInvitation(Command):
    """Erases the invited email; an open invitation is revoked first."""


@Invitation.command(EraseInvitation)
def _(s: Invitation):
    if s.status == "none" or s.erased:
        raise Rejected("Nothing to erase", "not_found")
    return [InvitationRevoked(token_id=s.token_id), InvitationErased()] if s.status == "open" else [InvitationErased()]
