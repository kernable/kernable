// The authorization middleware: one listener at order -30 that learns from events and
// checks every request before the kernel accepts or routes it.

import { Query, Rejected, type Check, type Listener } from "@kernable/sdk";

import {
  AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailVerified, SessionEnded, SessionFactorAdded, SessionOpened, TotpConfirmed,
  TotpDisabled, SsoDeleted, SsoDisabled, SsoEnforcementChanged,
} from "../account/index.ts";
import { GroupCreated, GroupDeleted, GroupPermissionsSet, groupDomain } from "../group/index.ts";
import {
  ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended, MembershipGranted,
  MembershipResumed, MembershipRevoked, MembershipSuspended, type Membership,
} from "../identity/index.ts";
import { AUTHORIZATION_STORE, ORDERS } from "../shared/constants.ts";
import {
  MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated, TenantDeleted,
  TenantReactivated, TenantSuspended,
} from "../tenancy/index.ts";
import { Knowledge, MemoryKeyValues, decide, type KeyValues } from "./policy.ts";
import { parentRules, relationEvents, relationRules } from "./relations.ts";

/** The knowledge, in the listener's store when it has one. */
export const knowledge = new Knowledge();
const k = knowledge;
const memory = new MemoryKeyValues();
/** Points the knowledge at the store the SDK hands this listener's handlers. */
const use = (store: KeyValues | undefined) => void (k.kv = store ?? memory);

type Evt = { tenant: string; aggregateId: string };
const identityKey = (evt: Evt) => `${evt.tenant}/${evt.aggregateId}`;
const setMembership = (status: Membership["status"]) => (d: { membershipId: string }, evt: Evt, ctx: { store?: KeyValues }) => {
  use(ctx.store);
  k.identities.update(identityKey(evt), (i) => {
    const m = i.memberships.find((x) => x.membershipId === d.membershipId);
    if (m) m.status = status;
  });
};
const setIdentity = (active: boolean) => (_d: unknown, evt: Evt, ctx: { store?: KeyValues }) => {
  use(ctx.store);
  k.identities.update(identityKey(evt), (i) => void (i.active = active));
};
const setAccount = (status: string) => (_d: unknown, evt: Evt, ctx: { store?: KeyValues }) => {
  use(ctx.store);
  k.accounts.set(evt.aggregateId, { ...k.accounts.get(evt.aggregateId), status });
};
const setMfa = (mfa: boolean) => (_d: unknown, evt: Evt, ctx: { store?: KeyValues }) => {
  use(ctx.store);
  k.accounts.update(evt.aggregateId, (a) => void (a.mfa = mfa));
};

export const authorizationListener: Listener = groupDomain
  .readModel("group.authorization", { order: ORDERS.authorization, store: AUTHORIZATION_STORE })
  // tenancy
  .on(TenantCreated, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.set(evt.aggregateId, { status: d.status, owners: [], mfaRequired: false, suspension: null });
  })
  .on(TenantActivated, (_d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => void (t.status = "active"));
  })
  .on(TenantSuspended, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => {
      t.status = "suspended";
      t.suspension = d.reason;
    });
  })
  .on(TenantReactivated, (_d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => {
      t.status = "active";
      t.suspension = null;
    });
  })
  .on(TenantDeleted, (_d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => {
      t.status = "deleted";
      t.owners = [];
    });
    for (const table of [k.identities, k.groups, k.relations, k.parents]) table.deletePrefix(`${evt.aggregateId}/`);
  })
  .on(OwnerAdded, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => void (t.owners.includes(d.identityId) || t.owners.push(d.identityId)));
  })
  .on(OwnerRemoved, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => void (t.owners = t.owners.filter((o) => o !== d.identityId)));
  })
  .on(MfaRequirementChanged, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.aggregateId, (t) => void (t.mfaRequired = d.required));
  })
  .on(SignUpPolicyChanged, (d, _evt, ctx) => {
    use(ctx.store);
    k.signUp = { tenants: d.tenants, accounts: d.accounts };
  })
  // account
  .on(AccountRegistered, (d, evt, ctx) => setAccount(d.verified ? "active" : "pending")(d, evt, ctx))
  .on(EmailVerified, (_d, evt, ctx) => {
    use(ctx.store);
    k.accounts.update(evt.aggregateId, (a) => void (a.status === "pending" && (a.status = "active")));
  })
  .on(TotpConfirmed, setMfa(true))
  .on(TotpDisabled, setMfa(false))
  .on(AccountSuspended, setAccount("suspended"))
  .on(AccountReactivated, setAccount("active"))
  .on(AccountDeleted, setAccount("deleted"))
  .on(SessionOpened, (d, evt, ctx) => {
    use(ctx.store);
    k.sessions.set(evt.aggregateId, { accountId: d.accountId, factors: [...d.factors], active: true, method: d.method });
  })
  .on(SessionFactorAdded, (d, evt, ctx) => {
    use(ctx.store);
    k.sessions.update(evt.aggregateId, (s) => void s.factors.push(d.factor));
  })
  .on(SessionEnded, (_d, evt, ctx) => {
    use(ctx.store);
    k.sessions.update(evt.aggregateId, (s) => void (s.active = false));
  })
  .on(SsoEnforcementChanged, (d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.tenant, (t) => void (t.ssoEnforced = d.enforced ? evt.aggregateId : t.ssoEnforced === evt.aggregateId ? null : t.ssoEnforced));
  })
  .on(SsoDisabled, (_d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.tenant, (t) => void (t.ssoEnforced === evt.aggregateId && (t.ssoEnforced = null)));
  })
  .on(SsoDeleted, (_d, evt, ctx) => {
    use(ctx.store);
    k.tenants.update(evt.tenant, (t) => void (t.ssoEnforced === evt.aggregateId && (t.ssoEnforced = null)));
  })
  // identity
  .on(IdentityCreated, (d, evt, ctx) => {
    use(ctx.store);
    k.identities.set(identityKey(evt), { accountId: d.accountId, active: true, memberships: [] });
  })
  .on(IdentitySuspended, setIdentity(false))
  .on(IdentityReactivated, setIdentity(true))
  .on(IdentityDeleted, (_d, evt, ctx) => {
    use(ctx.store);
    k.identities.delete(identityKey(evt));
  })
  .on(MembershipGranted, (d, evt, ctx) => {
    use(ctx.store);
    k.identities.update(identityKey(evt), (i) => void i.memberships.push({ ...d, status: "active" }));
  })
  .on(MembershipSuspended, setMembership("suspended"))
  .on(MembershipResumed, setMembership("active"))
  .on(MembershipRevoked, (d, evt, ctx) => {
    use(ctx.store);
    k.identities.update(identityKey(evt), (i) => void (i.memberships = i.memberships.filter((m) => m.membershipId !== d.membershipId)));
  })
  .on(ApiKeyCreated, (d, evt, ctx) => {
    use(ctx.store);
    k.apiKeys.set(d.keyId, { tenant: evt.tenant, identityId: evt.aggregateId, expiresAt: d.expiresAt, revoked: false });
  })
  .on(ApiKeyRevoked, (d, _evt, ctx) => {
    use(ctx.store);
    k.apiKeys.update(d.keyId, (key) => void (key.revoked = true));
  })
  // group: a deleted group's memberships stop counting at once.
  .on(GroupCreated, (d, evt, ctx) => {
    use(ctx.store);
    k.groups.set(identityKey(evt), d.permissions);
  })
  .on(GroupPermissionsSet, (d, evt, ctx) => {
    use(ctx.store);
    k.groups.set(identityKey(evt), d.permissions);
  })
  .on(GroupDeleted, (_d, evt, ctx) => {
    use(ctx.store);
    k.groups.delete(identityKey(evt));
  })
  .middleware({ types: ["*"], order: ORDERS.authorization }, (check, _outcome, ctx) => {
    use(ctx.store);
    const refusal = decide(k, check, Date.now());
    if (refusal) throw new Rejected(refusal.message, refusal.code);
  });

/**
 * Why a request would be allowed or refused: the middleware's own steps. For oneself, with
 * one's real session; for another identity of the tenant (needs `group.Explain`), sessions aside.
 */
export class Explain extends Query<{ allowed: boolean; code: string | null; steps: string[] }> {
  static target = { aggregateType: "identity.identity", field: "identityId" };
  identityId!: string;
  /** The request type, qualified: `shop.CancelOrder`. */
  type!: string;
  /** The aggregate the request is about: a command's, or a query's target. Unqualified: of the type's domain. */
  aggregateType?: string;
  aggregateId?: string;
  data?: unknown;
}

authorizationListener.query(Explain, (d, qry, ctx) => {
  use(ctx.store);
  const [domain, type] = String(d?.type ?? "").split(".");
  if (!domain || !type) throw new Rejected("A qualified type: domain.Type", "invalid_type");
  const asker = ctx.request;
  const oneself = asker.senderIdentity === d.identityId && asker.senderTenant === qry.tenant;
  const request = oneself ? asker : { senderTenant: qry.tenant, senderIdentity: d.identityId };
  const check: Check = {
    kind: d.aggregateId ? "command" : "query", id: "", tenant: qry.tenant, domain, type, data: d.data ?? {}, request,
    ...(d.aggregateId && d.aggregateType
      ? {
          aggregateType: d.aggregateType,
          aggregateId: d.aggregateId,
          target: { type: d.aggregateType.includes(".") ? d.aggregateType : `${domain}.${d.aggregateType}`, id: d.aggregateId },
        }
      : {}),
  };
  const steps: string[] = [];
  const refusal = decide(k, check, Date.now(), { trace: steps, assumeSession: !oneself });
  return { allowed: refusal === null, code: refusal?.code ?? null, steps };
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
    authorizationListener.on(event, (data, evt, ctx) => {
      use(ctx.store);
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

/**
 * Where the middleware keeps its knowledge: a SQLite file, so a restart continues where it
 * stopped instead of replaying the log while every request is refused. Call before the app runs.
 */
export function useAuthorizationStore(url: string): void {
  authorizationListener.options.store = url;
}
