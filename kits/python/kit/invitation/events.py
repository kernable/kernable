"""The invitation events: what happened to invitations, the vocabulary other domains read."""

from kernable import Event

from .domain import InvitedGroup, invitation_domain


def _own(_d, invitation_id):
    return f"invitation:{invitation_id}"


def _token(d, _id):
    return f"token:{d['token_id']}"


class InvitationCreated(Event):
    """The email belongs to the invitation (its own subject); the token sits in the vault until used."""
    vault = {"personal": [{"fields": ["email"], "subject": _own}, {"fields": ["token"], "subject": _token}]}
    email: str | None
    groups: list[InvitedGroup]
    #: An owner invitation: accepting makes the person an owner.
    owner: bool
    token_id: str
    token: str | None
    expires_at: int


class InvitationResent(Event):
    """A new token; erases the old one."""
    vault = {"personal": [{"fields": ["token"], "subject": _token}], "erases": lambda d, _id: f"token:{d['previous_token_id']}"}
    previous_token_id: str
    token_id: str
    token: str | None
    expires_at: int


class InvitationRevoked(Event):
    vault = {"erases": _token}
    token_id: str


class InvitationDeclined(Event):
    vault = {"erases": _token}
    token_id: str


class InvitationAccepted(Event):
    """The identity reactor creates the identity with these groups (and the owner) from it."""
    vault = {"erases": _token}
    token_id: str
    account_id: str
    new_account: bool
    groups: list[InvitedGroup]
    owner: bool


class InvitationMailSent(Event):
    token_id: str


class InvitationErased(Event):
    """Erases the invited email: when the tenant is deleted, or the person asks."""
    vault = {"erases": _own}


class InvitationLinkCreated(Event):
    """A reusable invitation: anyone signed in who has its token may join the tenant with these groups,
    until it is revoked, expires or is used up. Only the token's hash is kept: the API shows the
    token once."""
    groups: list[InvitedGroup]
    hash: str
    label: str
    expires_at: int | None
    max_uses: int | None


class InvitationLinkRevoked(Event):
    pass


class InvitationLinkUsed(Event):
    """Someone joined through the link: the identity reactor creates their identity with these groups."""
    account_id: str
    groups: list[InvitedGroup]


# Declared on the domain, so a process that only reads them knows them too.
invitation_domain.event(InvitationCreated, InvitationResent, InvitationRevoked, InvitationDeclined, InvitationAccepted,
                        InvitationMailSent, InvitationErased, InvitationLinkCreated, InvitationLinkRevoked, InvitationLinkUsed)
