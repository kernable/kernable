import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("ResumeMembership", (s, d: { membershipId: string }) => {
  const m = s.memberships.find((x) => x.membershipId === d.membershipId);
  if (!m || m.status !== "suspended") throw new Rejected("No such suspended membership", "not_found");
  return [{ type: "MembershipResumed", data: { membershipId: d.membershipId } }];
});
