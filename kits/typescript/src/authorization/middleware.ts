// The authorization middleware: one listener at order -30 that learns from events and
// checks every request before the kernel accepts or routes it.

import { Rejected, type Listener } from "@kernable/sdk";

import {
  AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailVerified, SessionEnded, SessionFactorAdded, SessionOpened,
} from "../account/index.ts";
import { GroupCreated, GroupDeleted, GroupPermissionsSet, groupDomain } from "../group/index.ts";
import {
  ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended, MembershipGranted,
  MembershipResumed, MembershipRevoked, MembershipSuspended, type Membership,
} from "../identity/index.ts";
import { ORDERS } from "../shared/constants.ts";
import {
  MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated, TenantDeleted,
  TenantReactivated, TenantSuspended,
} from "../tenancy/index.ts";
import { Knowledge, decide } from "./policy.ts";
import { parentRules, relationEvents, relationRules } from "./relations.ts";

export const knowledge = new Knowledge();
const k = knowledge;

type Evt = { tenant: string; aggregateId: string };
const setTenant = (id: string, f: (t: NonNullable<ReturnType<typeof k.tenants.get>>) => void) => {
  const t = k.tenants.get(id);
  if (t) f(t);
};
const identity = (evt: Evt) => k.identities.get(`${evt.tenant}/${evt.aggregateId}`);
const setMembership = (status: Membership["status"]) => (d: { membershipId: string }, evt: Evt) => {
  const m = identity(evt)?.memberships.find((x) => x.membershipId === d.membershipId);
  if (m) m.status = status;
};
const setIdentity = (active: boolean) => (_d: unknown, evt: Evt) => {
  const i = identity(evt);
  if (i) i.active = active;
};

export const authorizationListener: Listener = groupDomain
  .readModel("group.authorization", { order: ORDERS.authorization })
  // tenancy
  .on(TenantCreated, (d, evt) => void k.tenants.set(evt.aggregateId, { status: d.status, owners: new Set(), mfaRequired: false, suspension: null }))
  .on(TenantActivated, (_d, evt) => setTenant(evt.aggregateId, (t) => void (t.status = "active")))
  .on(TenantSuspended, (d, evt) => setTenant(evt.aggregateId, (t) => {
    t.status = "suspended";
    t.suspension = d.reason;
  }))
  .on(TenantReactivated, (_d, evt) => setTenant(evt.aggregateId, (t) => {
    t.status = "active";
    t.suspension = null;
  }))
  .on(TenantDeleted, (_d, evt) => {
    setTenant(evt.aggregateId, (t) => {
      t.status = "deleted";
      t.owners.clear();
    });
    const prefix = `${evt.aggregateId}/`;
    for (const map of [k.identities, k.groups, k.relations, k.parents] as Map<string, unknown>[]) {
      for (const key of map.keys()) if (key.startsWith(prefix)) map.delete(key);
    }
  })
  .on(OwnerAdded, (d, evt) => setTenant(evt.aggregateId, (t) => void t.owners.add(d.identityId)))
  .on(OwnerRemoved, (d, evt) => setTenant(evt.aggregateId, (t) => void t.owners.delete(d.identityId)))
  .on(MfaRequirementChanged, (d, evt) => setTenant(evt.aggregateId, (t) => void (t.mfaRequired = d.required)))
  .on(SignUpPolicyChanged, (d) => void (k.signUp = { tenants: d.tenants, accounts: d.accounts }))
  // account
  .on(AccountRegistered, (d, evt) => void k.accounts.set(evt.aggregateId, { status: d.verified ? "active" : "pending" }))
  .on(EmailVerified, (_d, evt) => {
    const a = k.accounts.get(evt.aggregateId);
    if (a?.status === "pending") a.status = "active";
  })
  .on(AccountSuspended, (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "suspended" }))
  .on(AccountReactivated, (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "active" }))
  .on(AccountDeleted, (_d, evt) => void k.accounts.set(evt.aggregateId, { status: "deleted" }))
  .on(SessionOpened, (d, evt) => void k.sessions.set(evt.aggregateId, { accountId: d.accountId, factors: [...d.factors], active: true }))
  .on(SessionFactorAdded, (d, evt) => void k.sessions.get(evt.aggregateId)?.factors.push(d.factor))
  .on(SessionEnded, (_d, evt) => {
    const s = k.sessions.get(evt.aggregateId);
    if (s) s.active = false;
  })
  // identity
  .on(IdentityCreated, (d, evt) => void k.identities.set(`${evt.tenant}/${evt.aggregateId}`, { accountId: d.accountId, active: true, memberships: [] }))
  .on(IdentitySuspended, setIdentity(false))
  .on(IdentityReactivated, setIdentity(true))
  .on(IdentityDeleted, (_d, evt) => void k.identities.delete(`${evt.tenant}/${evt.aggregateId}`))
  .on(MembershipGranted, (d, evt) => void identity(evt)?.memberships.push({ ...d, status: "active" }))
  .on(MembershipSuspended, setMembership("suspended"))
  .on(MembershipResumed, setMembership("active"))
  .on(MembershipRevoked, (d, evt) => {
    const i = identity(evt);
    if (i) i.memberships = i.memberships.filter((m) => m.membershipId !== d.membershipId);
  })
  .on(ApiKeyCreated, (d, evt) => void k.apiKeys.set(d.keyId, { tenant: evt.tenant, identityId: evt.aggregateId, expiresAt: d.expiresAt, revoked: false }))
  .on(ApiKeyRevoked, (d) => {
    const key = k.apiKeys.get(d.keyId);
    if (key) key.revoked = true;
  })
  // group: a deleted group's memberships stop counting at once.
  .on(GroupCreated, (d, evt) => void k.groups.set(`${evt.tenant}/${evt.aggregateId}`, d.permissions))
  .on(GroupPermissionsSet, (d, evt) => void k.groups.set(`${evt.tenant}/${evt.aggregateId}`, d.permissions))
  .on(GroupDeleted, (_d, evt) => void k.groups.delete(`${evt.tenant}/${evt.aggregateId}`))
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
