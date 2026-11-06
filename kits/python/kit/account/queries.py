"""The account queries: each class with its handler."""

from kernable import Query, Rejected

from ..shared.secrets import normal_email
from .readmodels import (AccountRow, SessionRow, account_rows, accounts_by_email, accounts_by_link, accounts_readmodel, session_rows,
                         sso_key, sso_rows, ssos_readmodel)


class FindAccountByEmail(Query[dict]):
    """The API's only: an open lookup would reveal who has an account."""
    email: str


@accounts_readmodel.query(FindAccountByEmail)
def _(d: FindAccountByEmail):
    a = account_rows.get(accounts_by_email.get(normal_email(d.email), ""))
    if not a:
        raise Rejected("No such account", "not_found")
    return {"id": a["id"], "status": a["status"]}


class GetAccount(Query[AccountRow]):
    account_id: str


@accounts_readmodel.query(GetAccount)
def _(d: GetAccount):
    if d.account_id not in account_rows:
        raise Rejected("No such account", "not_found")
    return account_rows[d.account_id]


class ListSessions(Query[list[SessionRow]]):
    """The account's sessions, the open ones first: "my devices"."""
    account_id: str


@accounts_readmodel.query(ListSessions)
def _(d: ListSessions):
    return sorted(session_rows.get(d.account_id, {}).values(), key=lambda s: (not s["active"], -s["last_at"]))


class FindAccountByLink(Query[dict]):
    """The API's only: the account an external login is linked to."""
    provider: str
    subject: str


@accounts_readmodel.query(FindAccountByLink)
def _(d: FindAccountByLink):
    a = account_rows.get(accounts_by_link.get(f"{d.provider}:{d.subject}", ""))
    if not a:
        raise Rejected("Not linked", "not_found")
    return {"id": a["id"], "status": a["status"]}


class FindSsoByDomain(Query[dict]):
    """The API's only: the active SSO that verified the domain, to sign its people in (any tenant)."""
    domain: str


@ssos_readmodel.query(FindSsoByDomain)
def _(d: FindSsoByDomain):
    domain = (d.domain or "").lower()
    r = next((x for x in sso_rows.values() if x["status"] == "active" and any(y["domain"] == domain and y["verified"] for y in x["domains"])), None)
    if not r:
        raise Rejected("No SSO for this domain", "not_found")
    return {"tenant": r["tenant"], "sso_id": r["id"], "issuer": r["issuer"], "client_id": r["client_id"], "enforced": r["enforced"]}


class GetSsoSecret(Query[dict]):
    """The API's only: what it signs in with, the client secret included."""
    sso_id: str


@ssos_readmodel.query(GetSsoSecret)
def _(d: GetSsoSecret, qry):
    r = sso_rows.get(sso_key(qry.tenant, d.sso_id))
    if not r:
        raise Rejected("No such SSO", "not_found")
    return {"issuer": r["issuer"], "client_id": r["client_id"], "client_secret": r["client_secret"]}


class GetSso(Query[dict]):
    """The tenant's owners: the configuration without the secret, with the TXT values to publish."""
    sso_id: str


@ssos_readmodel.query(GetSso)
def _(d: GetSso, qry):
    r = sso_rows.get(sso_key(qry.tenant, d.sso_id))
    if not r:
        raise Rejected("No such SSO", "not_found")
    return {k: v for k, v in r.items() if k != "client_secret"}
