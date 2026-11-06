"""The tenancy queries: each class with its handler."""

from kernable import Query, Rejected

from .domain import SignUpPolicy, TenantStatus
from .readmodels import TenantRow, policy, tenant_rows, tenants_readmodel


class GetTenant(Query[TenantRow]):
    tenant_id: str


@tenants_readmodel.query(GetTenant)
def _(d: GetTenant):
    if d.tenant_id not in tenant_rows:
        raise Rejected("No such tenant", "not_found")
    return tenant_rows[d.tenant_id]


class GetTenantBySlug(Query[dict]):
    """For subdomains and login pages, through the API only: nothing open lists tenants."""
    slug: str


@tenants_readmodel.query(GetTenantBySlug)
def _(d: GetTenantBySlug):
    row = next((t for t in tenant_rows.values() if t["slug"] == d.slug), None)
    if row is None:
        raise Rejected("No such tenant", "not_found")
    return {"id": row["id"], "name": row["name"], "status": row["status"]}


class ListTenants(Query[dict]):
    status: TenantStatus | None = None
    #: The last slug of the previous page.
    after: str | None = None
    limit: int | None = None


@tenants_readmodel.query(ListTenants)
def _(d: ListTenants):
    limit = min(max(int(d.limit or 50), 1), 500)
    rows = sorted((t for t in tenant_rows.values() if not d.status or t["status"] == d.status), key=lambda t: t["slug"])
    rows = [t for t in rows if not d.after or t["slug"] > d.after]
    return {"tenants": rows[:limit], "more": len(rows) > limit}


class GetSignUpPolicy(Query[SignUpPolicy]):
    pass


@tenants_readmodel.query(GetSignUpPolicy)
def _():
    return policy["sign_up"]
