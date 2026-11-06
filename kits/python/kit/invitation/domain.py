"""invitation: one Invitation per invitation, under its tenant's ID (design/kits.md "invitation")."""

from typing import Literal, TypedDict

from kernable import domain

invitation_domain = domain("invitation")

InvitationStatus = Literal["none", "open", "accepted", "declined", "revoked"]


class InvitedGroup(TypedDict, total=False):
    """A group the invited person joins, for the whole tenant or one aggregate."""
    group: str
    scope: dict | None
