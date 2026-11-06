import { Rejected } from "@kernable/sdk";

import { identitiesReadModel, identityRows, rowKey } from "../readmodel/identities.ts";

identitiesReadModel.query("GetIdentity", (d: { identityId: string }, qry) => {
  const r = identityRows.get(rowKey(qry.tenant, d?.identityId));
  if (!r) throw new Rejected("No such identity", "not_found");
  return r;
});
