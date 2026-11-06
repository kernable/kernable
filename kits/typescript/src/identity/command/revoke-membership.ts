import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("RevokeMembership", (s, d: { membershipId: string }) => {
  if (!s.memberships.some((x) => x.membershipId === d.membershipId)) throw new Rejected("No such membership", "not_found");
  return [{ type: "MembershipRevoked", data: { membershipId: d.membershipId } }];
});
