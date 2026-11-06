"""The group queries: each class with its handler."""

from kernable import Query, Rejected

from .readmodels import GroupRow, group_rows, groups_readmodel, row_key


class ListGroups(Query[list[dict]]):
    pass


@groups_readmodel.query(ListGroups)
def _(d, qry):
    """The virtual `owners` group comes first."""
    owners = {"id": "owners", "name": "owners", "description": "The tenant's owners: every permission", "permissions": [{"pattern": "*"}]}
    return [owners, *(g for g in group_rows.values() if g["tenant"] == qry.tenant)]


class GetGroup(Query[GroupRow]):
    group_id: str


@groups_readmodel.query(GetGroup)
def _(d: GetGroup, qry):
    g = group_rows.get(row_key(qry.tenant, d.group_id))
    if not g:
        raise Rejected("No such group", "not_found")
    return g
