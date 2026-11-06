import { Rejected } from "@kernable/sdk";

import { tenantAggregate } from "../aggregate/tenant.ts";
import { exists } from "./checks.ts";

tenantAggregate.command("RemoveOwner", (s, d: { identityId: string }) => {
  exists(s);
  if (!s.owners.includes(d.identityId)) throw new Rejected("Not an owner", "not_found");
  if (s.owners.length === 1) throw new Rejected("The last owner cannot be removed", "last_owner");
  return [{ type: "OwnerRemoved", data: { identityId: d.identityId } }];
});
