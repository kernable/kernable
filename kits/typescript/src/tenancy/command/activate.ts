import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";

tenantAggregate.command("ActivateTenant", (s) => {
  if (s.status !== "pending") throw new Rejected("Only a pending tenant can be activated", "invalid_state");
  return [{ type: "TenantActivated" }];
});
