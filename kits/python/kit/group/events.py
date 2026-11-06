"""The group events: what happened to groups, the vocabulary other domains read."""

from kernable import Event

from .domain import Permission, group_domain


class GroupCreated(Event):
    name: str
    description: str
    permissions: list[Permission]


class GroupUpdated(Event):
    name: str | None = None
    description: str | None = None


class GroupPermissionsSet(Event):
    """The whole list in one event: simpler to read in the audit."""
    permissions: list[Permission]


class GroupDeleted(Event):
    pass


# Declared on the domain, so a process that only reads them knows them too.
group_domain.event(GroupCreated, GroupUpdated, GroupPermissionsSet, GroupDeleted)
