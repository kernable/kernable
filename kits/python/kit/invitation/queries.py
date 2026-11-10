"""The invitation queries: each class with its handler."""

import time

from kernable import Query, Rejected

from ..shared.secrets import equal_secrets, parse_invitation_token, sha256
from .domain import parse_link_token
from .readmodels import (InvitationRow, LinkRow, invitation_rows, invitation_tokens, invitations_readmodel, link_hashes, link_rows,
                         row_key)


class ListInvitations(Query[list[InvitationRow]]):
    """The tenant's invitations; the open ones appear in user lists."""


@invitations_readmodel.query(ListInvitations)
def _(d, qry):
    return [r for r in invitation_rows.values() if r["tenant"] == qry.tenant]


class PeekInvitation(Query[dict]):
    """The API's only: what the acceptance page shows for a token, only for the right token (its secret checked)."""
    token: str


@invitations_readmodel.query(PeekInvitation)
def _(d: PeekInvitation):
    t = parse_invitation_token(d.token)
    r = invitation_rows.get(row_key(t["tenant"], t["invitation_id"])) if t else None
    kept = invitation_tokens.get(row_key(t["tenant"], t["invitation_id"])) if t else None
    if (not t or not r or r["status"] != "open" or not kept or kept["token_id"] != t["token_id"]
            or not equal_secrets(kept["hash"], sha256(t["secret"]))):
        raise Rejected("The invitation is not valid", "invalid_token")
    return {"tenant": r["tenant"], "invitation_id": r["id"], "email": r["email"], "owner": r["owner"], "expired": time.time() * 1000 > r["expires_at"]}


class ListInvitationLinks(Query[list[LinkRow]]):
    """The tenant's invitation links, with how often each was used."""


@invitations_readmodel.query(ListInvitationLinks)
def _(d, qry):
    return [r for r in link_rows.values() if r["tenant"] == qry.tenant]


class PeekInvitationLink(Query[dict]):
    """The API's only: whether a link token can be used, for the join page, only for the right token (its hash checked)."""
    token: str


@invitations_readmodel.query(PeekInvitationLink)
def _(d: PeekInvitationLink):
    t = parse_link_token(d.token)
    r = link_rows.get(row_key(t["tenant"], t["link_id"])) if t else None
    if not t or not r or r["status"] != "open" or not equal_secrets(link_hashes.get(row_key(t["tenant"], t["link_id"])) or "", t["hash"]):
        raise Rejected("The link is not valid", "invalid_token")
    usable = (r["expires_at"] is None or time.time() * 1000 <= r["expires_at"]) and (r["max_uses"] is None or r["uses"] < r["max_uses"])
    return {"tenant": r["tenant"], "link_id": r["id"], "label": r["label"], "usable": usable}
