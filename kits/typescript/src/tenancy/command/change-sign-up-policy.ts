import { Rejected } from "@kernable/sdk";

import { PLATFORM_TENANT } from "../../shared/constants.ts";
import { tenantAggregate } from "../aggregate/tenant.ts";
import type { SignUpPolicy } from "../domain.ts";
import { exists } from "./checks.ts";

tenantAggregate.command("ChangeSignUpPolicy", (s, d: SignUpPolicy, cmd) => {
  if (cmd.aggregateId !== PLATFORM_TENANT) throw new Rejected("Only the platform tenant has a sign-up policy", "platform");
  exists(s);
  if (!["off", "pending", "active"].includes(d.tenants) || !["off", "open"].includes(d.accounts)) {
    throw new Rejected("tenants: off, pending or active; accounts: off or open", "invalid_policy");
  }
  return [{ type: "SignUpPolicyChanged", data: { tenants: d.tenants, accounts: d.accounts } }];
});
