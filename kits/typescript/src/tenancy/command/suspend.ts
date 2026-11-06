import { Rejected } from "@kernable/sdk";

import { PLATFORM_TENANT } from "../../shared/constants.ts";
import { tenantAggregate } from "../aggregate/tenant.ts";
import { SUSPENSION_REASONS, type SuspensionReason } from "../domain.ts";

tenantAggregate.command("SuspendTenant", (s, d: { reason: SuspensionReason }, cmd, ctx) => {
  if (cmd.aggregateId === PLATFORM_TENANT) throw new Rejected("The platform tenant cannot be suspended", "platform");
  if (s.status !== "pending" && s.status !== "active") throw new Rejected("Cannot suspend this tenant", "invalid_state");
  if (!SUSPENSION_REASONS.includes(d.reason)) throw new Rejected("Unknown reason", "invalid_reason");
  if ((d.reason === "rejected") !== (s.status === "pending")) {
    throw new Rejected("A pending tenant is rejected; an active one suspended for another reason", "invalid_reason");
  }
  // The time is in the payload: the retention before deletion is computed from it.
  return [{ type: "TenantSuspended", data: { reason: d.reason, at: ctx.now().getTime() } }];
});
