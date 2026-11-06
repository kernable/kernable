import { groupRows, groupsReadModel } from "../readmodel/groups.ts";

// The virtual `owners` group comes first.
groupsReadModel.query("ListGroups", (_d, qry) => [
  { id: "owners", name: "owners", description: "The tenant's owners: every permission", permissions: [{ pattern: "*" }] },
  ...[...groupRows.values()].filter((g) => g.tenant === qry.tenant),
]);
