import { Rejected } from "@kernable/sdk";

import { tenantRows, tenantsReadModel } from "../readmodel/tenants.ts";

tenantsReadModel.query("GetTenant", (d: { tenantId: string }) => {
  const r = tenantRows.get(d?.tenantId);
  if (!r) throw new Rejected("No such tenant", "not_found");
  return r;
});
