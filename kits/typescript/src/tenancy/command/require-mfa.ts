import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";

tenantAggregate.command("RequireMfa", (s, d: { required: boolean }) => {
  if (s.status !== "active") throw new Rejected("Only an active tenant", "invalid_state");
  return [{ type: "MfaRequirementChanged", data: { required: d.required === true } }];
});
