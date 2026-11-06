import { Rejected } from "@kernable/sdk";

import { DAY, DELETE_AFTER_DAYS, PLATFORM_TENANT } from "../../shared/constants.ts";
import { tenantAggregate } from "../aggregate/tenant.ts";

// Final: the read models drop the tenant, reactors erase its identities, the slug becomes free;
// the events stay.
tenantAggregate.command("DeleteTenant", (s, _d, cmd, ctx) => {
  if (cmd.aggregateId === PLATFORM_TENANT) throw new Rejected("The platform tenant cannot be deleted", "platform");
  if (s.status !== "suspended" || !s.suspension) throw new Rejected("Only a suspended tenant is deleted", "invalid_state");
  if (ctx.now().getTime() < s.suspension.at + DELETE_AFTER_DAYS * DAY) {
    throw new Rejected(`A tenant is deleted ${DELETE_AFTER_DAYS} days after its suspension`, "too_early");
  }
  return [{ type: "TenantDeleted" }];
});
