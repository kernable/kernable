"""The invitation queries: each class with its handler."""

import time

from kernable import Query, Rejected

from ..shared.secrets import parse_invitation_token
from .domain import parse_link_token
from .readmodels import InvitationRow, LinkRow, invitation_rows, invitations_readmodel, link_rows, row_key


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


class ListInvitationLinks(Query[list[LinkRow]]):
    """The tenant's invitation links, with how often each was used."""


@invitations_readmodel.query(ListInvitationLinks)
def _(d, qry):
    return [r for r in link_rows.values() if r["tenant"] == qry.tenant]


class PeekInvitationLink(Query[dict]):
    """The API's only: whether a link token can be used, for the join page (the token is checked on joining)."""
    token: str


@invitations_readmodel.query(PeekInvitationLink)
def _(d: PeekInvitationLink):
    t = parse_link_token(d.token)
    r = link_rows.get(row_key(t["tenant"], t["link_id"])) if t else None
    if not t or not r or r["status"] != "open":
        raise Rejected("The link is not valid", "invalid_token")
    usable = (r["expires_at"] is None or time.time() * 1000 <= r["expires_at"]) and (r["max_uses"] is None or r["uses"] < r["max_uses"])
    return {"tenant": r["tenant"], "link_id": r["id"], "label": r["label"], "usable": usable}
