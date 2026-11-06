// The identity read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { TenantDeleted } from "../tenancy/index.ts";
import { type ApiKey, identityDomain, type IdentityStatus, type Membership } from "./domain.ts";
import { ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended, MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated } from "./events.ts";

export interface IdentityRow {
  id: string;
  tenant: string;
  kind: "person" | "service";
  accountId: string | null;
  displayName: string | null;
  status: IdentityStatus;
  memberships: (Membership & { grantedBy: string | null; grantedAt: number })[];
  apiKeys: Omit<ApiKey, "hash">[];
}

/** `tenant/identityId` */
export const identityRows = new Map<string, IdentityRow>();
export const rowKey = (tenant: string, id: string) => `${tenant}/${id}`;
const update = (tenant: string, id: string, f: (r: IdentityRow) => void) => {
  const r = identityRows.get(rowKey(tenant, id));
  if (r) f(r);
};
const setMembership = (status: "active" | "suspended") => (d: { membershipId: string }, evt: { tenant: string; aggregateId: string }) =>
  update(evt.tenant, evt.aggregateId, (r) => {
    const m = r.memberships.find((x) => x.membershipId === d.membershipId);
    if (m) m.status = status;
  });

export const identitiesReadModel = identityDomain
  .readModel("identity.identities", { order: ORDERS.readModels })
  .on(IdentityCreated, (d, evt) => {
    identityRows.set(rowKey(evt.tenant, evt.aggregateId), {
      id: evt.aggregateId, tenant: evt.tenant, kind: d.kind, accountId: d.accountId, displayName: d.displayName,
      status: "active", memberships: [], apiKeys: [],
    });
  })
  .on(ProfileUpdated, (d, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    if (d.displayName !== null) r.displayName = d.displayName;
  }))
  // Who granted it and when come from the event's request context and time, not its payload.
  .on(MembershipGranted, (d, evt, ctx) => update(evt.tenant, evt.aggregateId, (r) => {
    r.memberships.push({ ...d, status: "active", grantedBy: ctx.request.senderIdentity ?? null, grantedAt: evt.createdAt.getTime() });
  }))
  .on(MembershipSuspended, setMembership("suspended"))
  .on(MembershipResumed, setMembership("active"))
  .on(MembershipRevoked, (d, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    r.memberships = r.memberships.filter((m) => m.membershipId !== d.membershipId);
  }))
  .on(ApiKeyCreated, (d, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    r.apiKeys.push({ keyId: d.keyId, label: d.label, expiresAt: d.expiresAt, revoked: false });
  }))
  .on(ApiKeyRevoked, (d, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    const k = r.apiKeys.find((x) => x.keyId === d.keyId);
    if (k) k.revoked = true;
  }))
  .on(IdentitySuspended, (_d, evt) => update(evt.tenant, evt.aggregateId, (r) => void (r.status = "suspended")))
  .on(IdentityReactivated, (_d, evt) => update(evt.tenant, evt.aggregateId, (r) => void (r.status = "active")))
  .on(IdentityDeleted, (_d, evt) => void identityRows.delete(rowKey(evt.tenant, evt.aggregateId)))
  .on(TenantDeleted, (_d, evt) => {
    for (const [k, r] of identityRows) if (r.tenant === evt.aggregateId) identityRows.delete(k);
  });
