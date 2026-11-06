// The group queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import { type GroupRow, groupRows, groupsReadModel, rowKey } from "./readmodels.ts";

export class GetGroup extends Query<GroupRow> {
  groupId!: string;
}

groupsReadModel.query(GetGroup, (d, qry) => {
  const g = groupRows.get(rowKey(qry.tenant, d?.groupId));
  if (!g) throw new Rejected("No such group", "not_found");
  return g;
});

export class ListGroups extends Query<(Omit<GroupRow, "tenant"> & { tenant?: string })[]> {}

// The virtual `owners` group comes first.
groupsReadModel.query(ListGroups, (_d, qry) => [
  { id: "owners", name: "owners", description: "The tenant's owners: every permission", permissions: [{ pattern: "*" }] },
  ...[...groupRows.values()].filter((g) => g.tenant === qry.tenant),
]);
