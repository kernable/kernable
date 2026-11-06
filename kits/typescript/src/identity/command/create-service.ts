import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("CreateServiceIdentity", (s, d: { displayName: string }) => {
  if (s.status !== "none") throw new Rejected("The identity exists", "exists");
  if (typeof d.displayName !== "string" || !d.displayName.trim()) throw new Rejected("A service needs a name", "invalid_name");
  return [{ type: "IdentityCreated", data: { kind: "service", accountId: null, displayName: d.displayName.trim(), title: null, email: null } }];
});
