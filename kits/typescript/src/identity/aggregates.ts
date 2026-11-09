// The identity aggregates: state, how each event changes it, unique keys and facts.

import { type ApiKey, identityDomain, type IdentityKind, type IdentityStatus, type Membership } from "./domain.ts";
import { ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended, MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated } from "./events.ts";

export interface Identity {
  id?: string;
  status: IdentityStatus;
  kind?: IdentityKind;
  accountId?: string;
  /** A process identity's process: `shop.fulfilment`. */
  process?: string;
  memberships: Membership[];
  apiKeys: ApiKey[];
}

const membership = (s: Identity, id: string) => s.memberships.find((m) => m.membershipId === id);

export const identityAggregate = identityDomain
  .aggregate<Identity>("identity", { status: "none", memberships: [], apiKeys: [] })
  .apply(IdentityCreated, (s, d, evt) => {
    s.id = evt.aggregateId;
    s.status = "active";
    s.kind = d.kind;
    if (d.accountId) s.accountId = d.accountId;
    if (d.kind === "process" && d.displayName) s.process = d.displayName;
  })
  .apply(ProfileUpdated, () => {})
  .apply(MembershipGranted, (s, d) => void s.memberships.push({ ...d, status: "active" }))
  .apply(MembershipSuspended, (s, d) => {
    const m = membership(s, d.membershipId);
    if (m) m.status = "suspended";
  })
  .apply(MembershipResumed, (s, d) => {
    const m = membership(s, d.membershipId);
    if (m) m.status = "active";
  })
  .apply(MembershipRevoked, (s, d) => void (s.memberships = s.memberships.filter((m) => m.membershipId !== d.membershipId)))
  .apply(ApiKeyCreated, (s, d) => void s.apiKeys.push({ ...d, revoked: false }))
  .apply(ApiKeyRevoked, (s, d) => {
    const k = s.apiKeys.find((x) => x.keyId === d.keyId);
    if (k) k.revoked = true;
  })
  .apply(IdentitySuspended, (s) => void (s.status = "suspended"))
  .apply(IdentityReactivated, (s) => void (s.status = "active"))
  .apply(IdentityDeleted, (s) => {
    s.status = "deleted";
    s.memberships = [];
    s.apiKeys = [];
  })
  // One identity per account and tenant.
  .unique("account", (s) => (s.status !== "none" && s.status !== "deleted" ? s.accountId : null))
  // One identity per process (in the platform tenant).
  .unique("process", (s) => (s.status !== "none" && s.status !== "deleted" ? s.process : null))
  .fact("active", (s) => s.status === "active");
