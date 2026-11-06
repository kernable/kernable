import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";
import { exists } from "./checks.ts";

tenantAggregate.command("AddOwner", (s, d: { identityId: string }) => {
  exists(s);
  if (typeof d.identityId !== "string" || !d.identityId) throw new Rejected("Which identity?", "invalid_identity");
  if (s.owners.includes(d.identityId)) throw new Rejected("Already an owner", "exists");
  return [{ type: "OwnerAdded", data: { identityId: d.identityId } }];
}, {
  // The identity must be active in this tenant, checked by the kernel at commit.
  requires: (d) => [{ domain: "identity", aggregateType: "identity", aggregateId: d.identityId, fact: "active" }],
});
