"""The group commands: each class with its handler."""

import re

from kernable import Command, Rejected

from ..shared.secrets import trim_space
from .aggregates import Group
from .domain import Permission
from .events import GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated


def valid_pattern(p) -> bool:
    return isinstance(p, str) and (p == "*" or bool(re.fullmatch(r"[a-z][\w-]*\.(\*|[A-Za-z]\w*)", p)))


def permissions(value) -> list[Permission]:
    """The permissions, checked and normalised."""
    if not isinstance(value, list):
        raise Rejected("permissions is a list", "invalid_permissions")
    out: list[Permission] = []
    for p in value:
        if not isinstance(p, dict) or not valid_pattern(p.get("pattern")):
            raise Rejected(f"Not a type pattern: {(p or {}).get('pattern')}", "invalid_permissions")
        when = p.get("when")
        if when is not None and (not isinstance(when, str) or not re.fullmatch(r"[a-z]\w*", when)):
            raise Rejected(f"Not a relation: {when}", "invalid_permissions")
        item: Permission = {"pattern": p["pattern"]}
        if when:
            item["when"] = when
            if p.get("via_parent"):
                item["via_parent"] = True
        out.append(item)
    return out


def name(value) -> str:
    if not isinstance(value, str) or not trim_space(value) or len(value) > 100 or trim_space(value).lower() == "owners":
        raise Rejected("A group needs a name other than owners", "invalid_name")
    return trim_space(value)


class CreateGroup(Command):
    """The middleware lets the sender hand out only permissions it holds."""
    name: str
    description: str | None = None
    permissions: list[Permission]


@Group.command(CreateGroup)
def _(s: Group, d: CreateGroup):
    if s.status != "none":
        raise Rejected("The group exists", "exists")
    return [GroupCreated(name=name(d.name), description=d.description or "", permissions=permissions(d.permissions))]


class UpdateGroup(Command):
    name: str | None = None
    description: str | None = None


@Group.command(UpdateGroup)
def _(s: Group, d: UpdateGroup):
    if s.status != "active":
        raise Rejected("No such group", "not_found")
    return [GroupUpdated(name=name(d.name) if d.name is not None else None, description=d.description)]


class SetPermissions(Command):
    permissions: list[Permission]


@Group.command(SetPermissions)
def _(s: Group, d: SetPermissions):
    if s.status != "active":
        raise Rejected("No such group", "not_found")
    return [GroupPermissionsSet(permissions=permissions(d.permissions))]


class DeleteGroup(Command):
    """Its memberships stop counting at once (the middleware); a reactor removes them later."""


@Group.command(DeleteGroup)
def _(s: Group):
    if s.status != "active":
        raise Rejected("No such group", "not_found")
    return [GroupDeleted()]
