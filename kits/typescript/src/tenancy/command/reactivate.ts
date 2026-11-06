import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";

tenantAggregate.command("ReactivateTenant", (s) => {
  if (s.status !== "suspended" || s.suspension?.reason === "rejected") {
    throw new Rejected("Only a suspended tenant is reactivated", "invalid_state");
  }
  return [{ type: "TenantReactivated" }];
});
