"""The identity queries: each class with its handler."""

from kernable import Query, Rejected

from .readmodels import IdentityRow, api_key_rows, identities_readmodel, identity_rows, row_key


class GetIdentity(Query[IdentityRow]):
    identity_id: str


@identities_readmodel.query(GetIdentity)
def _(d: GetIdentity, qry):
    r = identity_rows.get(row_key(qry.tenant, d.identity_id))
    if not r:
        raise Rejected("No such identity", "not_found")
    return r


class ListIdentities(Query[list[IdentityRow]]):
    pass


@identities_readmodel.query(ListIdentities)
def _(d, qry):
    return [r for r in identity_rows.values() if r["tenant"] == qry.tenant]


class ListMyIdentities(Query[list[dict]]):
    """Every tenant the account has an identity in (ask with all_partitions when tenants are partitioned)."""
    account_id: str


@identities_readmodel.query(ListMyIdentities)
def _(d: ListMyIdentities):
    return [{"tenant": r["tenant"], "identity_id": r["id"], "status": r["status"]} for r in identity_rows.values() if r["account_id"] == d.account_id]


class FindIdentityByAccount(Query[dict]):
    """The API's only: it sets `sender_identity` from it."""
    account_id: str


@identities_readmodel.query(FindIdentityByAccount)
def _(d: FindIdentityByAccount, qry):
    r = next((x for x in identity_rows.values() if x["tenant"] == qry.tenant and x["account_id"] == d.account_id), None)
    if not r:
        raise Rejected("No identity in this tenant", "not_found")
    return {"identity_id": r["id"], "status": r["status"]}


class FindApiKey(Query[dict]):
    """The API's only: what it checks an API key against."""
    key_id: str


@identities_readmodel.query(FindApiKey)
def _(d: FindApiKey):
    if d.key_id not in api_key_rows:
        raise Rejected("No such key", "not_found")
    return api_key_rows[d.key_id]
