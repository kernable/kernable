import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";
import type { Membership } from "../domain.ts";
import { active, scope } from "./checks.ts";

identityAggregate.command("GrantMembership", (s, d: { membershipId: string; group: string; scope?: unknown; expiresAt?: number | null }) => {
  active(s);
  if (typeof d.membershipId !== "string" || !d.membershipId || typeof d.group !== "string" || !d.group) {
    throw new Rejected("A membership needs an ID and a group", "invalid_membership");
  }
  const sc = scope(d.scope);
  const same = (m: Membership) => m.group === d.group && JSON.stringify(m.scope) === JSON.stringify(sc);
  if (s.memberships.some((m) => m.membershipId === d.membershipId || same(m))) throw new Rejected("Already a member", "exists");
  return [{ type: "MembershipGranted", data: { membershipId: d.membershipId, group: d.group, scope: sc, expiresAt: d.expiresAt ?? null } }];
}, {
  // The group must be active, in this tenant, at commit.
  requires: (d) => [{ domain: "group", aggregateType: "group", aggregateId: d.group, fact: "active" }],
});
