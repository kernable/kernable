// The identities read model. Its queries are in ../query/.

import { ORDERS } from "../../shared/constants.ts";
import { identityDomain, type ApiKey, type IdentityCreated, type IdentityStatus, type Membership, type MembershipGranted } from "../domain.ts";

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

export const identitiesReadModel = identityDomain
  .readModel("identities", { order: ORDERS.readModels })
  .on("IdentityCreated", (d: IdentityCreated, evt) => {
    identityRows.set(rowKey(evt.tenant, evt.aggregateId), {
      id: evt.aggregateId, tenant: evt.tenant, kind: d.kind, accountId: d.accountId, displayName: d.displayName,
      status: "active", memberships: [], apiKeys: [],
    });
  })
  .on("ProfileUpdated", (d: { displayName: string | null }, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    if (d.displayName !== null) r.displayName = d.displayName;
  }))
  // Who granted it and when come from the event's request context and time, not its payload.
  .on("MembershipGranted", (d: MembershipGranted, evt, ctx) => update(evt.tenant, evt.aggregateId, (r) => {
    r.memberships.push({ ...d, status: "active", grantedBy: ctx.request.senderIdentity ?? null, grantedAt: evt.createdAt.getTime() });
  }))
  .on("MembershipSuspended", (d: { membershipId: string }, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    const m = r.memberships.find((x) => x.membershipId === d.membershipId);
    if (m) m.status = "suspended";
  }))
  .on("MembershipResumed", (d: { membershipId: string }, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    const m = r.memberships.find((x) => x.membershipId === d.membershipId);
    if (m) m.status = "active";
  }))
  .on("MembershipRevoked", (d: { membershipId: string }, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    r.memberships = r.memberships.filter((m) => m.membershipId !== d.membershipId);
  }))
  .on("ApiKeyCreated", (d: ApiKey, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    r.apiKeys.push({ keyId: d.keyId, label: d.label, expiresAt: d.expiresAt, revoked: false });
  }))
  .on("ApiKeyRevoked", (d: { keyId: string }, evt) => update(evt.tenant, evt.aggregateId, (r) => {
    const k = r.apiKeys.find((x) => x.keyId === d.keyId);
    if (k) k.revoked = true;
  }))
  .on("IdentitySuspended", (_d, evt) => update(evt.tenant, evt.aggregateId, (r) => void (r.status = "suspended")))
  .on("IdentityReactivated", (_d, evt) => update(evt.tenant, evt.aggregateId, (r) => void (r.status = "active")))
  .on("IdentityDeleted", (_d, evt) => void identityRows.delete(rowKey(evt.tenant, evt.aggregateId)))
  .on("tenancy.TenantDeleted", (_d, evt) => {
    for (const [k, r] of identityRows) if (r.tenant === evt.aggregateId) identityRows.delete(k);
  });
