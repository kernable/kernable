import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";
import { name } from "./checks.ts";

tenantAggregate.command("RenameTenant", (s, d: { name: string }) => {
  if (s.status !== "pending" && s.status !== "active") throw new Rejected("Cannot rename this tenant", "invalid_state");
  return [{ type: "TenantRenamed", data: { name: name(d.name) } }];
});
