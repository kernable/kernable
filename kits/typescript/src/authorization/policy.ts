// What the authorization middleware knows (folded from events) and how it decides
// (design/kits.md "The authorization middleware"). Pure: no kernel needed, so it is tested alone.

import type { Check, RequestContext } from "@kernable/sdk";

import type { Permission } from "../group/domain.ts";
import type { Membership } from "../identity/domain.ts";
import type { SignUpPolicy, TenantStatus } from "../tenancy/domain.ts";
import { PLATFORM_TENANT } from "../shared/constants.ts";
import { ESCALATION, QUERY_TARGETS, ruleOf } from "./levels.ts";

export interface Refusal {
  code: "unauthenticated" | "forbidden" | "tenant_suspended" | "mfa_required" | "sign_up_closed";
  message: string;
}

export interface Target { type: string; id: string }

export class Knowledge {
  tenants = new Map<string, { status: TenantStatus; owners: Set<string>; mfaRequired: boolean; suspension: string | null }>();
  signUp: SignUpPolicy = { tenants: "off", accounts: "off" };
  accounts = new Map<string, { status: string }>();
  sessions = new Map<string, { accountId: string; factors: string[]; active: boolean }>();
  /** `tenant/identityId` */
  identities = new Map<string, { accountId: string | null; active: boolean; memberships: Membership[] }>();
  /** keyId */
  apiKeys = new Map<string, { tenant: string; identityId: string; expiresAt: number | null; revoked: boolean }>();
  /** `tenant/groupId` */
  groups = new Map<string, Permission[]>();
  /** `tenant/type/id` → relation → identity IDs */
  relations = new Map<string, Map<string, Set<string>>>();
  /** `tenant/type/id` → the parent */
  parents = new Map<string, Target>();

  identity(tenant: string, id: string | undefined) {
    return id ? this.identities.get(`${tenant}/${id}`) : undefined;
  }

  holds(tenant: string, target: Target, relation: string, identityId: string): boolean {
    return this.relations.get(`${tenant}/${target.type}/${target.id}`)?.get(relation)?.has(identityId) ?? false;
  }

  relate(tenant: string, target: Target, relation: string, ids: string[], on: boolean): void {
    const key = `${tenant}/${target.type}/${target.id}`;
    let rels = this.relations.get(key);
    if (!rels) this.relations.set(key, (rels = new Map()));
    let set = rels.get(relation);
    if (!set) rels.set(relation, (set = new Set()));
    for (const id of ids) on ? set.add(id) : set.delete(id);
  }
}

/** Whether `pattern` covers `name`: `*`, `domain.*` or the qualified type; `name` may be `domain.*` itself. */
export function covers(pattern: string, name: string): boolean {
  if (pattern === "*" || pattern === name) return true;
  if (!pattern.endsWith(".*")) return false;
  return name.split(".")[0] === pattern.slice(0, -2) && name !== "*";
}

const refuse = (code: Refusal["code"], message: string): Refusal => ({ code, message });

/** The command's aggregate, or the field a query names as its target. */
export function targetOf(check: Check): Target | null {
  if (check.kind === "command" && check.aggregateType && check.aggregateId) {
    return { type: `${check.domain}.${check.aggregateType}`, id: check.aggregateId };
  }
  if (check.kind === "query") {
    const t = QUERY_TARGETS[`${check.domain}.${check.type}`];
    const id = t && (check.data as Record<string, unknown> | undefined)?.[t.field];
    if (t && typeof id === "string" && id) return { type: t.type, id };
  }
  return null;
}

/** Whether the target is the sender itself: its identity, account or session (built-in `self`). */
function isSelf(k: Knowledge, check: Check, r: RequestContext, target: Target | null): boolean {
  if (!target) return false;
  switch (target.type) {
    case "identity.identity":
      return target.id === r.senderIdentity && check.tenant === r.senderTenant;
    case "account.account":
      return target.id === r.senderAccount;
    case "account.session":
      return target.id === r.senderSession || k.sessions.get(target.id)?.accountId === r.senderAccount;
    default:
      return false;
  }
}

interface Sender {
  accountId?: string;
  factors: string[];
  /** Signed in with an API key rather than a session. */
  apiKey: boolean;
}

/** Who sends the request: an active session of an active account, or a valid API key (step 3). */
function authenticate(k: Knowledge, r: RequestContext, now: number): Sender | Refusal {
  const sid = r.senderSession;
  if (!sid) return refuse("unauthenticated", "Sign in first");
  const session = k.sessions.get(sid);
  if (session) {
    if (!session.active) return refuse("unauthenticated", "The session has ended");
    if (session.accountId !== r.senderAccount) return refuse("unauthenticated", "The session is another account's");
    if (k.accounts.get(session.accountId)?.status !== "active") return refuse("unauthenticated", "The account is not active");
    return { accountId: session.accountId, factors: session.factors, apiKey: false };
  }
  const key = k.apiKeys.get(sid);
  if (key) {
    if (key.revoked || (key.expiresAt !== null && now > key.expiresAt)) return refuse("unauthenticated", "The API key is revoked or expired");
    if (key.identityId !== r.senderIdentity || key.tenant !== r.senderTenant) return refuse("unauthenticated", "The API key is another identity's");
    return { factors: ["api_key"], apiKey: true };
  }
  return refuse("unauthenticated", "Unknown session");
}

/** The permissions an identity holds through its memberships, at `now`. */
function permissions(k: Knowledge, tenant: string, identityId: string, now: number): { p: Permission; m: Membership }[] {
  const identity = k.identity(tenant, identityId);
  if (!identity?.active) return [];
  return identity.memberships
    .filter((m) => m.status === "active" && (m.expiresAt === null || now < m.expiresAt))
    .flatMap((m) => (k.groups.get(`${tenant}/${m.group}`) ?? []).map((p) => ({ p, m })));
}

function matches(k: Knowledge, tenant: string, identityId: string, p: Permission, m: Membership, type: string, target: Target | null, check: Check, r: RequestContext): boolean {
  if (!covers(p.pattern, type)) return false;
  if (m.scope && !(target && m.scope.type === target.type && m.scope.id === target.id)) {
    // A scope may also name the target's parent.
    const parent = target ? k.parents.get(`${tenant}/${target.type}/${target.id}`) : undefined;
    if (!(parent && m.scope.type === parent.type && m.scope.id === parent.id)) return false;
  }
  if (!p.when) return true;
  if (!target) return false; // a relation needs a target
  if (p.when === "self") return isSelf(k, check, r, target);
  if (k.holds(check.tenant, target, p.when, identityId)) return true;
  if (!p.viaParent) return false;
  const parent = k.parents.get(`${check.tenant}/${target.type}/${target.id}`);
  return parent ? k.holds(check.tenant, parent, p.when, identityId) : false;
}

/** The patterns concerned by a request that hands out permissions (no escalation). */
function concerned(k: Knowledge, type: string, check: Check): string[] {
  const d = (check.data ?? {}) as Record<string, any>;
  const ofGroup = (g: unknown) => (typeof g === "string" ? (k.groups.get(`${check.tenant}/${g}`) ?? []).map((p) => p.pattern) : []);
  switch (type) {
    case "identity.GrantMembership":
      return ofGroup(d.group);
    case "invitation.CreateInvitation":
      return Array.isArray(d.groups) ? d.groups.flatMap((g: { group: string }) => ofGroup(g?.group)) : [];
    default:
      return Array.isArray(d.permissions) ? d.permissions.map((p: Permission) => String(p?.pattern)) : [];
  }
}

/**
 * Decides on a request (design/kits.md, steps 1–9): null allows it. `now` is the middleware's
 * clock; the request carries no time of its own at this point.
 */
export function decide(k: Knowledge, check: Check, now: number): Refusal | null {
  const type = `${check.domain}.${check.type}`;
  const r = check.request ?? {};
  const rule = ruleOf(type);
  const data = (check.data ?? {}) as Record<string, any>;

  // 1. Privileged keys (the kernel checked the grant): the kit's reactors and the bootstrap.
  if (r.bypassAuthorization) return null;
  if (rule.level === "internal") return refuse("forbidden", `${type} is sent by the kit only`);

  // 2. Public: the command carries its proof, the aggregate checks it.
  if (rule.level === "public") {
    if (type === "account.RegisterAccount" && data.verified === true) {
      // Only the API's identity, after an invitation proved the email: handled as a group permission below.
    } else {
      if (type === "account.RegisterAccount" && k.signUp.accounts !== "open") return refuse("sign_up_closed", "Registration is by invitation only");
      return null;
    }
  }

  // 3. Who is asking.
  const sender = authenticate(k, r, now);
  if ("code" in sender) return sender;
  const target = targetOf(check);
  if (rule.self && isSelf(k, check, r, target)) return null;

  // 4. Authenticated: a session is enough.
  if (rule.level === "authenticated") {
    if (type === "tenancy.CreateTenant") {
      const own = data.owner?.accountId && data.owner.accountId === sender.accountId;
      if (own && k.signUp.tenants !== "off" && (data.status ?? "active") === k.signUp.tenants) return null;
      // Else only the platform may create tenants (below).
    } else {
      return null;
    }
  }

  // 5. The tenant, and the sender's tenant.
  const platform = r.senderTenant === PLATFORM_TENANT;
  if (!r.senderTenant || (!platform && r.senderTenant !== check.tenant)) return refuse("forbidden", "Not a member of this tenant");
  const tenant = k.tenants.get(check.tenant);
  if (!platform) {
    if (!tenant || tenant.status === "deleted") return refuse("forbidden", "No such tenant");
    // An owner may undo their own suspension (owner_requested), decided in step 7.
    if (tenant.status !== "active" && type !== "tenancy.ReactivateTenant") return refuse("tenant_suspended", `The tenant is ${tenant.status}`);
  }

  // 6. The identity, and the tenant's MFA requirement.
  if (r.onBehalfOf) return refuse("forbidden", "Acting on behalf of others is not available yet");
  const identityId = r.senderIdentity;
  const identity = k.identity(r.senderTenant, identityId);
  if (!identityId || !identity?.active) return refuse("forbidden", "No active identity in this tenant");
  if (!sender.apiKey && identity.accountId !== sender.accountId) return refuse("forbidden", "The identity is another account's");
  const homeTenant = k.tenants.get(r.senderTenant);
  for (const t of [tenant, homeTenant]) {
    if (t?.mfaRequired && !sender.apiKey && sender.factors.length < 2 && !sender.factors.includes("passkey")) {
      return refuse("mfa_required", "This tenant requires a second factor");
    }
  }

  // 7. Owners hold every permission in their tenant, except what is the platform's: they suspend
  // and reactivate their tenant only for themselves (owner_requested). Platform owners hold all.
  const owner = homeTenant?.owners.has(identityId) ?? false;
  if (rule.level === "platform" && !platform) {
    const own = owner && (
      (type === "tenancy.SuspendTenant" && data.reason === "owner_requested") ||
      (type === "tenancy.ReactivateTenant" && tenant?.suspension === "owner_requested"));
    return own ? null : refuse("forbidden", `${type} is for the platform`);
  }
  if (owner) return null;
  if (!platform && tenant?.status !== "active") return refuse("tenant_suspended", `The tenant is ${tenant?.status}`);
  if (rule.level === "owners") return refuse("forbidden", `${type} is for the tenant's owners`);

  // 8. Memberships: pattern, scope, relation, expiry.
  const held = permissions(k, r.senderTenant, identityId, now);
  if (!held.some(({ p, m }) => matches(k, r.senderTenant!, identityId, p, m, type, target, check, r))) {
    return refuse("forbidden", `No permission for ${type}`);
  }

  // 9. No escalation: only permissions held without scope and relation count.
  if (ESCALATION.includes(type)) {
    const plain = held.filter(({ p, m }) => !p.when && !m.scope).map(({ p }) => p.pattern);
    const missing = concerned(k, type, check).find((pattern) => !plain.some((h) => covers(h, pattern)));
    if (missing) return refuse("forbidden", `Granting ${missing} needs holding it`);
  }
  return null;
}
