"""The account read models: accounts with their links and sessions, and the tenants' SSOs."""

from typing import TypedDict

from ..shared.constants import ORDERS
from ..tenancy.events import TenantDeleted
from .domain import AccountStatus, account_domain
from .events import (AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailChanged, EmailVerified,
                     ProviderLinked, ProviderUnlinked, SessionEnded, SessionOpened, SessionRefreshed, SsoCreated, SsoDeleted,
                     SsoDisabled, SsoDomainAdded, SsoDomainRemoved, SsoDomainVerified, SsoEnabled, SsoEnforcementChanged, SsoUpdated)


class AccountRow(TypedDict):
    id: str
    email: str | None
    name: str | None
    status: AccountStatus


class SessionRow(TypedDict):
    id: str
    account_id: str
    method: str
    ip: str | None
    user_agent: str | None
    opened_at: int
    last_at: int
    active: bool


account_rows: dict[str, AccountRow] = {}
accounts_by_email: dict[str, str] = {}
#: `provider:subject` → account ID
accounts_by_link: dict[str, str] = {}
#: account ID → session ID → session
session_rows: dict[str, dict[str, SessionRow]] = {}

accounts_readmodel = account_domain.read_model("account.accounts", order=ORDERS.read_models)


@accounts_readmodel.on(AccountRegistered)
def _(d: AccountRegistered, evt):
    account_rows[evt.aggregate_id] = AccountRow(id=evt.aggregate_id, email=d.email, name=d.name, status="active" if d.verified else "pending")
    if d.email:
        accounts_by_email[d.email] = evt.aggregate_id


@accounts_readmodel.on(EmailVerified)
def _(d, evt):
    if (a := account_rows.get(evt.aggregate_id)) and a["status"] == "pending":
        a["status"] = "active"


@accounts_readmodel.on(EmailChanged)
def _(d: EmailChanged, evt):
    """The new address finds the account, the old one no more."""
    if not (a := account_rows.get(evt.aggregate_id)):
        return
    if a["email"]:
        accounts_by_email.pop(a["email"], None)
    a["email"] = d.email
    if d.email:
        accounts_by_email[d.email] = evt.aggregate_id


@accounts_readmodel.on(ProviderLinked)
def _(d: ProviderLinked, evt):
    if d.subject:
        accounts_by_link[f"{d.provider}:{d.subject}"] = evt.aggregate_id


@accounts_readmodel.on(ProviderUnlinked)
def _(d: ProviderUnlinked):
    if d.subject:
        accounts_by_link.pop(f"{d.provider}:{d.subject}", None)


def _status(status: AccountStatus):
    def handler(d, evt):
        if a := account_rows.get(evt.aggregate_id):
            a["status"] = status
    return handler


accounts_readmodel.on(AccountSuspended, _status("suspended"))
accounts_readmodel.on(AccountReactivated, _status("active"))


@accounts_readmodel.on(AccountDeleted)
def _(d, evt):
    """A deleted account leaves the read side; its email is free."""
    if (a := account_rows.pop(evt.aggregate_id, None)) and a["email"]:
        accounts_by_email.pop(a["email"], None)
    session_rows.pop(evt.aggregate_id, None)


@accounts_readmodel.on(SessionOpened)
def _(d: SessionOpened, evt):
    session_rows.setdefault(d.account_id, {})[evt.aggregate_id] = SessionRow(
        id=evt.aggregate_id, account_id=d.account_id, method=d.method, ip=d.ip, user_agent=d.user_agent, opened_at=d.at, last_at=d.at, active=True)


@accounts_readmodel.on(SessionRefreshed)
def _(d: SessionRefreshed, evt):
    if s := session_rows.get(d.account_id, {}).get(evt.aggregate_id):
        s["last_at"] = d.at
        s["ip"] = d.ip


@accounts_readmodel.on(SessionEnded)
def _(d, evt):
    for mine in session_rows.values():
        if s := mine.get(evt.aggregate_id):
            s["active"] = False


# --- A tenant's single sign-on --------------------------------------------------------------------------


class SsoRow(TypedDict):
    id: str
    tenant: str
    status: str
    issuer: str
    client_id: str
    #: Read with vault_read; only the API gets it (GetSsoSecret).
    client_secret: str | None
    domains: list[dict]
    enforced: bool


#: `tenant/ssoId`
sso_rows: dict[str, SsoRow] = {}


def sso_key(tenant: str, sso_id: str) -> str:
    return f"{tenant}/{sso_id}"


ssos_readmodel = account_domain.read_model("account.ssos", order=ORDERS.read_models)


@ssos_readmodel.on(SsoCreated)
def _(d: SsoCreated, evt):
    sso_rows[sso_key(evt.tenant, evt.aggregate_id)] = SsoRow(id=evt.aggregate_id, tenant=evt.tenant, status="active", issuer=d.issuer,
                                                            client_id=d.client_id, client_secret=d.client_secret, domains=[], enforced=False)


def _sso(evt) -> SsoRow | None:
    return sso_rows.get(sso_key(evt.tenant, evt.aggregate_id))


@ssos_readmodel.on(SsoUpdated)
def _(d: SsoUpdated, evt):
    if r := _sso(evt):
        r.update(issuer=d.issuer, client_id=d.client_id, client_secret=d.client_secret)


@ssos_readmodel.on(SsoDomainAdded)
def _(d: SsoDomainAdded, evt):
    if r := _sso(evt):
        r["domains"].append({"domain": d.domain, "txt": d.txt, "verified": False})


@ssos_readmodel.on(SsoDomainVerified)
def _(d: SsoDomainVerified, evt):
    for x in (_sso(evt) or {"domains": []})["domains"]:
        if x["domain"] == d.domain:
            x["verified"] = True


@ssos_readmodel.on(SsoDomainRemoved)
def _(d: SsoDomainRemoved, evt):
    if r := _sso(evt):
        r["domains"] = [x for x in r["domains"] if x["domain"] != d.domain]


@ssos_readmodel.on(SsoEnforcementChanged)
def _(d: SsoEnforcementChanged, evt):
    if r := _sso(evt):
        r["enforced"] = d.enforced


@ssos_readmodel.on(SsoDisabled)
def _(d, evt):
    if r := _sso(evt):
        r["status"] = "disabled"


@ssos_readmodel.on(SsoEnabled)
def _(d, evt):
    if r := _sso(evt):
        r["status"] = "active"


@ssos_readmodel.on(SsoDeleted)
def _(d, evt):
    sso_rows.pop(sso_key(evt.tenant, evt.aggregate_id), None)


@ssos_readmodel.on(TenantDeleted)
def _(d, evt):
    for k in [k for k, r in sso_rows.items() if r["tenant"] == evt.aggregate_id]:
        del sso_rows[k]
