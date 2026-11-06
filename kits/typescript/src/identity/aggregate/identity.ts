// The Identity aggregate: state, how events change it, its unique key and fact. Commands are in
// ../command/.

import { identityDomain, type ApiKey, type Identity, type IdentityCreated, type MembershipGranted } from "../domain.ts";

const membership = (s: Identity, id: string) => s.memberships.find((m) => m.membershipId === id);

export const identityAggregate = identityDomain
  .aggregate<Identity>("identity", { status: "none", memberships: [], apiKeys: [] })
  .apply("IdentityCreated", (s, d: IdentityCreated, evt) => {
    s.id = evt.aggregateId;
    s.status = "active";
    s.kind = d.kind;
    if (d.accountId) s.accountId = d.accountId;
  })
  .apply("ProfileUpdated", () => {})
  .apply("MembershipGranted", (s, d: MembershipGranted) => void s.memberships.push({ ...d, status: "active" }))
  .apply("MembershipSuspended", (s, d: { membershipId: string }) => {
    const m = membership(s, d.membershipId);
    if (m) m.status = "suspended";
  })
  .apply("MembershipResumed", (s, d: { membershipId: string }) => {
    const m = membership(s, d.membershipId);
    if (m) m.status = "active";
  })
  .apply("MembershipRevoked", (s, d: { membershipId: string }) => void (s.memberships = s.memberships.filter((m) => m.membershipId !== d.membershipId)))
  .apply("ApiKeyCreated", (s, d: Omit<ApiKey, "revoked">) => void s.apiKeys.push({ ...d, revoked: false }))
  .apply("ApiKeyRevoked", (s, d: { keyId: string }) => {
    const k = s.apiKeys.find((x) => x.keyId === d.keyId);
    if (k) k.revoked = true;
  })
  .apply("IdentitySuspended", (s) => void (s.status = "suspended"))
  .apply("IdentityReactivated", (s) => void (s.status = "active"))
  .apply("IdentityDeleted", (s) => {
    s.status = "deleted";
    s.memberships = [];
    s.apiKeys = [];
  })
  // One identity per account and tenant.
  .unique("account", (s) => (s.status !== "none" && s.status !== "deleted" ? s.accountId : null))
  .fact("active", (s) => s.status === "active");
