import { Rejected } from "@kernable/sdk";

import { groupRows, groupsReadModel, rowKey } from "../readmodel/groups.ts";

groupsReadModel.query("GetGroup", (d: { groupId: string }, qry) => {
  const g = groupRows.get(rowKey(qry.tenant, d?.groupId));
  if (!g) throw new Rejected("No such group", "not_found");
  return g;
});
