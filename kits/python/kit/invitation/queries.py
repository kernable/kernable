"""The invitation queries: each class with its handler."""

import time

from kernable import Query, Rejected

from ..shared.secrets import parse_invitation_token
from .readmodels import InvitationRow, invitation_rows, invitations_readmodel, row_key


class ListInvitations(Query[list[InvitationRow]]):
    """The tenant's invitations; the open ones appear in user lists."""


@invitations_readmodel.query(ListInvitations)
def _(d, qry):
    return [r for r in invitation_rows.values() if r["tenant"] == qry.tenant]


class PeekInvitation(Query[dict]):
    """The API's only: what the acceptance page shows for a token (the token itself is checked on accepting)."""
    token: str


@invitations_readmodel.query(PeekInvitation)
def _(d: PeekInvitation):
    t = parse_invitation_token(d.token)
    r = invitation_rows.get(row_key(t["tenant"], t["invitation_id"])) if t else None
    if not t or not r or r["status"] != "open":
        raise Rejected("The invitation is not valid", "invalid_token")
    return {"tenant": r["tenant"], "invitation_id": r["id"], "email": r["email"], "owner": r["owner"], "expired": time.time() * 1000 > r["expires_at"]}
