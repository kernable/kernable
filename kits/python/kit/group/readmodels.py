"""The group read models."""

from typing import TypedDict

from ..shared.constants import ORDERS
from ..tenancy.events import TenantDeleted
from .domain import Permission, group_domain
from .events import GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated


class GroupRow(TypedDict):
    id: str
    tenant: str
    name: str
    description: str
    permissions: list[Permission]


#: `tenant/groupId`
group_rows: dict[str, GroupRow] = {}


def row_key(tenant: str, group_id: str) -> str:
    return f"{tenant}/{group_id}"


groups_readmodel = group_domain.read_model("group.groups", order=ORDERS.read_models)


@groups_readmodel.on(GroupCreated)
def _(d: GroupCreated, evt):
    group_rows[row_key(evt.tenant, evt.aggregate_id)] = GroupRow(id=evt.aggregate_id, tenant=evt.tenant, name=d.name,
                                                                 description=d.description, permissions=d.permissions)


@groups_readmodel.on(GroupUpdated)
def _(d: GroupUpdated, evt):
    if g := group_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        if d.name is not None:
            g["name"] = d.name
        if d.description is not None:
            g["description"] = d.description


@groups_readmodel.on(GroupPermissionsSet)
def _(d: GroupPermissionsSet, evt):
    if g := group_rows.get(row_key(evt.tenant, evt.aggregate_id)):
        g["permissions"] = d.permissions


@groups_readmodel.on(GroupDeleted)
def _(d, evt):
    group_rows.pop(row_key(evt.tenant, evt.aggregate_id), None)


@groups_readmodel.on(TenantDeleted)
def _(d, evt):
    for k in [k for k, g in group_rows.items() if g["tenant"] == evt.aggregate_id]:
        del group_rows[k]
