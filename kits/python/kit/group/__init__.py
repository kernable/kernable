"""Registers every part of the group domain and exports what other domains and clients use."""

from .domain import *  # noqa: F401,F403
from .events import *  # noqa: F401,F403
from .aggregates import Group, group_aggregate  # noqa: F401
from .commands import CreateGroup, DeleteGroup, SetPermissions, UpdateGroup  # noqa: F401
from .readmodels import GroupRow, groups_readmodel  # noqa: F401
from .queries import GetGroup, ListGroups  # noqa: F401
