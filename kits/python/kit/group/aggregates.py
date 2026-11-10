"""The group aggregates: state, how each event changes it, unique keys and facts."""

from kernable import Aggregate

from ..shared.secrets import trim_space
from .domain import group_domain
from .events import GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated


class Group(Aggregate, domain=group_domain, schema="py-1"):
    status: str = "none"
    name: str | None = None
    description: str | None = None
    permissions: list = []


@Group.apply(GroupCreated)
def _(s: Group, d: GroupCreated):
    s.update(status="active", name=d.name, description=d.description, permissions=list(d.permissions))


@Group.apply(GroupUpdated)
def _(s: Group, d: GroupUpdated):
    if d.name is not None:
        s.name = d.name
    if d.description is not None:
        s.description = d.description


@Group.apply(GroupPermissionsSet)
def _(s: Group, d: GroupPermissionsSet):
    s.permissions = list(d.permissions)


@Group.apply(GroupDeleted)
def _(s: Group):
    s.status = "deleted"


Group.unique("name", lambda s: trim_space(s.name).lower() if s.status == "active" and s.name else None)
Group.fact("active", lambda s: s.status == "active")

#: The aggregate type, for workers and `requires`.
group_aggregate = Group.__aggregate__
