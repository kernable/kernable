import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("SuspendMembership", (s, d: { membershipId: string }) => {
  const m = s.memberships.find((x) => x.membershipId === d.membershipId);
  if (!m || m.status !== "active") throw new Rejected("No such active membership", "not_found");
  return [{ type: "MembershipSuspended", data: { membershipId: d.membershipId } }];
});
