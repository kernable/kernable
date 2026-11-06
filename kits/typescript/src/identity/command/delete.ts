import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

// Erases the identity's personal data. Never while a tenant holds it as owner: atomic,
// through the tenant's `owner` key (an absent requirement).
identityAggregate.command("DeleteIdentity", (s) => {
  if (s.status === "none" || s.status === "deleted") throw new Rejected("No such identity", "not_found");
  return [{ type: "IdentityDeleted" }];
}, {
  requires: (_d, s) =>
    s.id ? [{ domain: "tenancy", aggregateType: "tenant", key: "owner", value: s.id, fact: "exists", absent: true }] : [],
});
