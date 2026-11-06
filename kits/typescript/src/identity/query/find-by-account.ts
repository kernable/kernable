import { Rejected } from "@kernable/sdk";

import { identitiesReadModel, identityRows } from "../readmodel/identities.ts";

// The API's only: it sets `senderIdentity` from it.
identitiesReadModel.query("FindIdentityByAccount", (d: { accountId: string }, qry) => {
  const r = [...identityRows.values()].find((x) => x.tenant === qry.tenant && x.accountId === d?.accountId);
  if (!r) throw new Rejected("No identity in this tenant", "not_found");
  return { identityId: r.id, status: r.status };
});
