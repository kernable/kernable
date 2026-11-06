// The authorization middleware: one listener at order -30 that learns from events and
// checks every request before the kernel accepts or routes it.

import { Rejected, type Listener } from "@kernable/sdk";

import { groupDomain, type Permission } from "../group/domain.ts";
import type { Membership } from "../identity/domain.ts";
import { ORDERS } from "../shared/constants.ts";
import type { SignUpPolicy, TenantStatus } from "../tenancy/domain.ts";
import { Knowledge, decide } from "./policy.ts";
import { parentRules, relationEvents, relationRules } from "./relations.ts";

export const knowledge = new Knowledge();
const k = knowledge;

const tenant = (id: string) => k.tenants.get(id);
const setTenant = (id: string, f: (t: NonNullable<ReturnType<typeof tenant>>) => void) => {
  const t = tenant(id);
  if (t) f(t);
};
const identity = (t: string, id: string) => k.identities.get(`${t}/${id}`);
const membership = (t: string, id: string, membershipId: string): Membership | undefined =>
  identity(t, id)?.memberships.find((m) => m.membershipId === membershipId);

export const authorizationListener: Listener = groupDomain
  .readModel("authorization", { order: ORDERS.authorization })
  // tenancy
  .on("tenancy.TenantCreated", (d: { status: TenantStatus }, evt) => {
    k.tenants.set(evt.aggregateId, { status: d.status, owners: new Set(), mfaRequired: false, suspension: null });
  })
  .on("tenancy.TenantActivated", (_d, evt) => setTenant(evt.aggregateId, (t) => void (t.status = "active")))
  .on("tenancy.TenantSuspended", (d: { reason: string }, evt) => setTenant(evt.aggregateId, (t) => {
    t.status = "suspended";
    t.suspension = d.reason;
  }))
  .on("tenancy.TenantReactivated", (_d, evt) => setTenant(evt.aggregateId, (t) => {
    t.status = "active";
    t.suspension = null;
  }))
  .on("tenancy.TenantDeleted", (_d, evt) => {
    setTenant(evt.aggregateId, (t) => {
      t.status = "deleted";
      t.owners.clear();
    });
    const prefix = `${evt.aggregateId}/`;
    for (const map of [k.identities, k.groups, k.relations, k.parents] as Map<string, unknown>[]) {
      for (const key of map.keys()) if (key.startsWith(prefix)) map.delete(key);
    }
  })
  .on("tenancy.OwnerAdded", (d: { identityId: string }, evt) => setTenant(evt.aggregateId, (t) => void t.owners.add(d.identityId)))
  .on("tenancy.OwnerRemoved", (d: { identityId: string }, evt) => setTenant(evt.aggregateId, (t) => void t.owners.delete(d.identityId)))
  .on("tenancy.MfaRequirementChanged", (d: { required: boolean }, evt) => setTenant(evt.aggregateId, (t) => void (t.mfaRequired = d.required)))
  .on("tenancy.SignUpPolicyChanged", (d: SignUpPolicy) => void (k.signUp = { tenants: d.tenants, accounts: d.accounts }))
  // account
  .on("account.AccountRegistered", (d: { verified: boolean }, evt) => void k.accounts.set(evt.aggregateId, { status: d.verified ? "active" : "pending" }))
  .on("account.EmailVerified", (_d, evt) => {
    const a = k.accounts.get(evt.aggregateId);
    if (a?.status === "pending") a.status = "active";
  })
  .on("account.AccountSuspended", (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "suspended" }))
  .on("account.AccountReactivated", (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "active" }))
  .on("account.AccountDeleted", (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "deleted" }))
  .on("account.SessionOpened", (d: { accountId: string; factors: string[] }, evt) => {
    k.sessions.set(evt.aggregateId, { accountId: d.accountId, factors: [...d.factors], active: true });
  })
  .on("account.SessionFactorAdded", (d: { factor: string }, evt) => void k.sessions.get(evt.aggregateId)?.factors.push(d.factor))
  .on("account.SessionEnded", (_d, evt) => {
    const s = k.sessions.get(evt.aggregateId);
    if (s) s.active = false;
  })
  // identity
  .on("identity.IdentityCreated", (d: { accountId: string | null }, evt) => {
    k.identities.set(`${evt.tenant}/${evt.aggregateId}`, { accountId: d.accountId, active: true, memberships: [] });
  })
  .on("identity.IdentitySuspended", (_d, evt) => {
    const i = identity(evt.tenant, evt.aggregateId);
    if (i) i.active = false;
  })
  .on("identity.IdentityReactivated", (_d, evt) => {
    const i = identity(evt.tenant, evt.aggregateId);
    if (i) i.active = true;
  })
  .on("identity.IdentityDeleted", (_d, evt) => void k.identities.delete(`${evt.tenant}/${evt.aggregateId}`))
  .on("identity.MembershipGranted", (d: Omit<Membership, "status">, evt) => {
    identity(evt.tenant, evt.aggregateId)?.memberships.push({ ...d, status: "active" });
  })
  .on("identity.MembershipSuspended", (d: { membershipId: string }, evt) => {
    const m = membership(evt.tenant, evt.aggregateId, d.membershipId);
    if (m) m.status = "suspended";
  })
  .on("identity.MembershipResumed", (d: { membershipId: string }, evt) => {
    const m = membership(evt.tenant, evt.aggregateId, d.membershipId);
    if (m) m.status = "active";
  })
  .on("identity.MembershipRevoked", (d: { membershipId: string }, evt) => {
    const i = identity(evt.tenant, evt.aggregateId);
    if (i) i.memberships = i.memberships.filter((m) => m.membershipId !== d.membershipId);
  })
  .on("identity.ApiKeyCreated", (d: { keyId: string; expiresAt: number | null }, evt) => {
    k.apiKeys.set(d.keyId, { tenant: evt.tenant, identityId: evt.aggregateId, expiresAt: d.expiresAt, revoked: false });
  })
  .on("identity.ApiKeyRevoked", (d: { keyId: string }) => {
    const key = k.apiKeys.get(d.keyId);
    if (key) key.revoked = true;
  })
  // group: a deleted group's memberships stop counting at once.
  .on("GroupCreated", (d: { permissions: Permission[] }, evt) => void k.groups.set(`${evt.tenant}/${evt.aggregateId}`, d.permissions))
  .on("GroupPermissionsSet", (d: { permissions: Permission[] }, evt) => void k.groups.set(`${evt.tenant}/${evt.aggregateId}`, d.permissions))
  .on("GroupDeleted", (_d, evt) => void k.groups.delete(`${evt.tenant}/${evt.aggregateId}`))
  .middleware({ types: ["*"], order: ORDERS.authorization }, (check) => {
    const refusal = decide(k, check, Date.now());
    if (refusal) throw new Rejected(refusal.message, refusal.code);
  });

let relationsInstalled = false;

/**
 * Teaches the listener the relations declared with `relation()` and `parent()`. `kitDomains()`
 * calls it; declare your relations before that.
 */
export function installRelations(): void {
  if (relationsInstalled) return;
  relationsInstalled = true;
  for (const event of relationEvents()) {
    authorizationListener.on(event, (data, evt) => {
      const e = { data, aggregateId: evt.aggregateId, tenant: evt.tenant };
      const aggregate = `${evt.domain}.${evt.aggregateType}`;
      for (const rule of relationRules.filter((r) => r.event === event && r.type === aggregate)) {
        const ids = rule.ids(e);
        const list = (Array.isArray(ids) ? ids : ids ? [ids] : []).filter((x): x is string => typeof x === "string");
        k.relate(evt.tenant, { type: rule.type, id: evt.aggregateId }, rule.name, list, rule.op === "set");
      }
      for (const rule of parentRules.filter((r) => r.event === event && r.type === aggregate)) {
        const p = rule.parent(e);
        if (p?.type && p.id) k.parents.set(`${evt.tenant}/${rule.type}/${evt.aggregateId}`, { type: p.type, id: p.id });
      }
    });
  }
}
