import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("ReactivateIdentity", (s) => {
  if (s.status !== "suspended") throw new Rejected("Only a suspended identity", "invalid_state");
  return [{ type: "IdentityReactivated" }];
});
