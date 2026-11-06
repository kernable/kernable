"""The tenancy aggregates: state, how each event changes it, unique keys and facts."""

from kernable import Aggregate

from .domain import TenantStatus, tenancy_domain
from .events import (MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated,
                     TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended)


class Tenant(Aggregate, domain=tenancy_domain):
    """The tenant's state, and the aggregate `tenancy.tenant`; its ID is the tenant's."""
    id: str | None = None
    status: TenantStatus = "none"
    slug: str | None = None
    name: str | None = None
    #: Identity IDs: the only place owners are kept; `owners` is a virtual group.
    owners: list = []
    suspension: dict | None = None
    mfa_required: bool = False
    #: Only the platform tenant's matters.
    sign_up: dict = {"tenants": "off", "accounts": "off"}


@Tenant.apply(TenantCreated)
def _(s: Tenant, d: TenantCreated, evt):
    s.id = evt.aggregate_id
    s.status = d.status
    s.slug = d.slug
    s.name = d.name


@Tenant.apply(TenantActivated)
def _(s: Tenant):
    s.status = "active"


@Tenant.apply(TenantRenamed)
def _(s: Tenant, d: TenantRenamed):
    s.name = d.name


@Tenant.apply(TenantSlugChanged)
def _(s: Tenant, d: TenantSlugChanged):
    s.slug = d.slug


@Tenant.apply(TenantSuspended)
def _(s: Tenant, d: TenantSuspended):
    s.status = "suspended"
    s.suspension = {"reason": d.reason, "at": d.at}


@Tenant.apply(TenantReactivated)
def _(s: Tenant):
    s.status = "active"
    s.suspension = None


@Tenant.apply(TenantDeleted)
def _(s: Tenant):
    s.status = "deleted"
    s.owners = []


@Tenant.apply(OwnerAdded)
def _(s: Tenant, d: OwnerAdded):
    s.owners.append(d.identity_id)


@Tenant.apply(OwnerRemoved)
def _(s: Tenant, d: OwnerRemoved):
    s.owners = [o for o in s.owners if o != d.identity_id]


@Tenant.apply(MfaRequirementChanged)
def _(s: Tenant, d: MfaRequirementChanged):
    s.mfa_required = d.required


@Tenant.apply(SignUpPolicyChanged)
def _(s: Tenant, d: SignUpPolicyChanged):
    s.sign_up = {"tenants": d.tenants, "accounts": d.accounts}


# A slug is unique across tenants while the tenant is not deleted: deleting frees it.
Tenant.unique("slug", lambda s: s.slug if s.status not in ("none", "deleted") else None, scope="global")
# Lets identity.DeleteIdentity require that no tenant holds the identity as owner.
Tenant.unique("owner", lambda s: s.owners)
Tenant.fact("active", lambda s: s.status == "active")
Tenant.fact("exists", lambda s: s.status not in ("none", "deleted"))

#: The aggregate type, for workers and `requires`.
tenant_aggregate = Tenant.__aggregate__
