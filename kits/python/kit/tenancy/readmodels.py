"""The tenancy read models: one row per tenant; a deleted tenant leaves the read side."""

from typing import TypedDict

from ..shared.constants import ORDERS
from .domain import SignUpPolicy, TenantStatus, tenancy_domain
from .events import (MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated,
                     TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended)


class TenantRow(TypedDict):
    id: str
    slug: str
    name: str
    status: TenantStatus
    suspension: dict | None
    mfa_required: bool
    owners: list[str]


tenant_rows: dict[str, TenantRow] = {}
policy: dict[str, SignUpPolicy] = {"sign_up": {"tenants": "off", "accounts": "off"}}

tenants_readmodel = tenancy_domain.read_model("tenancy.tenants", order=ORDERS.read_models)


@tenants_readmodel.on(TenantCreated)
def _(d: TenantCreated, evt):
    tenant_rows[evt.aggregate_id] = TenantRow(id=evt.aggregate_id, slug=d.slug, name=d.name, status=d.status,
                                              suspension=None, mfa_required=False, owners=[])


def _update(evt, **values) -> None:
    if row := tenant_rows.get(evt.aggregate_id):
        row.update(values)  # type: ignore[typeddict-item]


@tenants_readmodel.on(TenantActivated)
def _(d, evt):
    _update(evt, status="active")


@tenants_readmodel.on(TenantRenamed)
def _(d: TenantRenamed, evt):
    _update(evt, name=d.name)


@tenants_readmodel.on(TenantSlugChanged)
def _(d: TenantSlugChanged, evt):
    _update(evt, slug=d.slug)


@tenants_readmodel.on(TenantSuspended)
def _(d: TenantSuspended, evt):
    _update(evt, status="suspended", suspension={"reason": d.reason, "at": d.at})


@tenants_readmodel.on(TenantReactivated)
def _(d, evt):
    _update(evt, status="active", suspension=None)


@tenants_readmodel.on(TenantDeleted)
def _(d, evt):
    tenant_rows.pop(evt.aggregate_id, None)


@tenants_readmodel.on(OwnerAdded)
def _(d: OwnerAdded, evt):
    if row := tenant_rows.get(evt.aggregate_id):
        row["owners"].append(d.identity_id)


@tenants_readmodel.on(OwnerRemoved)
def _(d: OwnerRemoved, evt):
    if row := tenant_rows.get(evt.aggregate_id):
        row["owners"] = [o for o in row["owners"] if o != d.identity_id]


@tenants_readmodel.on(MfaRequirementChanged)
def _(d: MfaRequirementChanged, evt):
    _update(evt, mfa_required=d.required)


@tenants_readmodel.on(SignUpPolicyChanged)
def _(d: SignUpPolicyChanged):
    policy["sign_up"] = {"tenants": d.tenants, "accounts": d.accounts}
