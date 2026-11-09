"""invitation: one Invitation per invitation, under its tenant's ID (design/kits.md "invitation")."""

import re
from typing import Literal, TypedDict

from kernable import domain

from ..shared.secrets import sha256

invitation_domain = domain("invitation")

InvitationStatus = Literal["none", "open", "accepted", "declined", "revoked"]


class InvitedGroup(TypedDict, total=False):
    """A group the invited person joins, for the whole tenant or one aggregate."""
    group: str
    scope: dict | None


LinkStatus = Literal["none", "open", "revoked"]

_LINK_TOKEN = re.compile(r"^link_([^.\s]+)\.([^.\s]+)\.([A-Za-z0-9_-]{32,})$")


def link_token(tenant: str, link_id: str, secret: str) -> str:
    """An invitation link as people get it: `link_<tenantId>.<linkId>.<secret>`."""
    return f"link_{tenant}.{link_id}.{secret}"


def parse_link_token(value: object) -> dict | None:
    """The tenant, the link and the whole token's hash, or None if it has another shape."""
    if not isinstance(value, str):
        return None
    m = _LINK_TOKEN.match(value)
    return {"tenant": m.group(1), "link_id": m.group(2), "hash": sha256(value)} if m else None
