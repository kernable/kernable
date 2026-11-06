// What the authorization middleware knows (folded from events) and how it decides
// (design/kits.md "The authorization middleware"). Pure: no kernel needed, so it is tested alone.

import type { Check, RequestContext } from "@kernable/sdk";

import type { Permission } from "../group/domain.ts";
import type { Membership } from "../identity/domain.ts";
import type { SignUpPolicy, TenantStatus } from "../tenancy/domain.ts";
import { PLATFORM_TENANT } from "../shared/constants.ts";
import { ESCALATION, QUERY_TARGETS, ruleOf } from "./levels.ts";

export interface Refusal {
  code: "unauthenticated" | "forbidden" | "tenant_suspended" | "mfa_required" | "sso_required" | "sign_up_closed";
  message: string;
}

export interface Target { type: string; id: string }

/** Where the knowledge lives: the listener's store, or memory (unit tests). */
export interface KeyValues {
  get<T = unknown>(ns: string, key: string): T | undefined;
  set(ns: string, key: string, value: unknown): void;
  remove(ns: string, key: string): void;
  all<T = unknown>(ns: string): [string, T][];
}

/** Key-values in memory, the same as a store holds them. */
export class MemoryKeyValues implements KeyValues {
  private readonly data = new Map<string, Map<string, string>>();
  private ns(ns: string) {
    let m = this.data.get(ns);
    if (!m) this.data.set(ns, (m = new Map()));
    return m;
  }
  get<T>(ns: string, key: string): T | undefined {
    const v = this.ns(ns).get(key);
    return v === undefined ? undefined : (JSON.parse(v) as T);
  }
  set(ns: string, key: string, value: unknown): void {
    this.ns(ns).set(key, JSON.stringify(value));
  }
  remove(ns: string, key: string): void {
    this.ns(ns).delete(key);
  }
  all<T>(ns: string): [string, T][] {
    return [...this.ns(ns)].sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => [k, JSON.parse(v) as T]);
  }
}

/** One namespace of the knowledge, read and written like a map. Values are copies: write back with `set` or `update`. */
export class Table<V> {
  private readonly owner: Knowledge;
  private readonly ns: string;
  constructor(owner: Knowledge, ns: string) {
    this.owner = owner;
    this.ns = ns;
  }
  get(key: string): V | undefined {
    return this.owner.kv.get<V>(this.ns, key);
  }
  set(key: string, value: V): void {
    this.owner.kv.set(this.ns, key, value);
  }
  delete(key: string): void {
    this.owner.kv.remove(this.ns, key);
  }
  /** Changes the value under `key`, if there is one. */
  update(key: string, f: (v: V) => void): void {
    const v = this.get(key);
    if (v === undefined) return;
    f(v);
    this.set(key, v);
  }
  /** Removes every key starting with `prefix` (a deleted tenant's). */
  deletePrefix(prefix: string): void {
    for (const [key] of this.owner.kv.all(this.ns)) if (key.startsWith(prefix)) this.owner.kv.remove(this.ns, key);
  }
}

export interface TenantKnown { status: TenantStatus; owners: string[]; mfaRequired: boolean; suspension: string | null; ssoEnforced?: string | null }

/** What the middleware knows, folded from events (design/kits.md, "What it knows"). */
export class Knowledge {
  kv: KeyValues;
  constructor(kv: KeyValues = new MemoryKeyValues()) {
    this.kv = kv;
  }
  tenants = new Table<TenantKnown>(this, "tenants");
  /** `mfa`: the account turned a second factor on: every session needs it. */
  accounts = new Table<{ status: string; mfa?: boolean }>(this, "accounts");
  sessions = new Table<{ accountId: string; factors: string[]; active: boolean; method?: string }>(this, "sessions");
  /** `tenant/identityId` */
  identities = new Table<{ accountId: string | null; active: boolean; memberships: Membership[] }>(this, "identities");
  /** keyId */
  apiKeys = new Table<{ tenant: string; identityId: string; expiresAt: number | null; revoked: boolean }>(this, "apiKeys");
  /** `tenant/groupId` */
  groups = new Table<Permission[]>(this, "groups");
  /** `tenant/type/id` → relation → identity IDs */
  relations = new Table<Record<string, string[]>>(this, "relations");
  /** `tenant/type/id` → the parent */
  parents = new Table<Target>(this, "parents");
  private settings = new Table<SignUpPolicy>(this, "settings");

  get signUp(): SignUpPolicy {
    return this.settings.get("signUp") ?? { tenants: "off", accounts: "off" };
  }
  set signUp(policy: SignUpPolicy) {
    this.settings.set("signUp", policy);
  }

  identity(tenant: string, id: string | undefined) {
    return id ? this.identities.get(`${tenant}/${id}`) : undefined;
  }

  holds(tenant: string, target: Target, relation: string, identityId: string): boolean {
    return this.relations.get(`${tenant}/${target.type}/${target.id}`)?.[relation]?.includes(identityId) ?? false;
  }

  relate(tenant: string, target: Target, relation: string, ids: string[], on: boolean): void {
    const key = `${tenant}/${target.type}/${target.id}`;
    const rels = this.relations.get(key) ?? {};
    const set = new Set(rels[relation] ?? []);
    for (const id of ids) on ? set.add(id) : set.delete(id);
    rels[relation] = [...set];
    this.relations.set(key, rels);
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
  /** How the session signed in: `password`, `passkey`, a provider, `sso:<id>`. */
  method?: string;
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
    return { accountId: session.accountId, factors: session.factors, method: session.method, apiKey: false };
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

  // An account with a second factor on: nothing but signing out until the session proved it.
  const account = sender.accountId ? k.accounts.get(sender.accountId) : undefined;
  if (account?.mfa && sender.factors.length < 2 && !sender.factors.includes("passkey") && type !== "account.EndSession") {
    return refuse("mfa_required", "Prove the second factor first");
  }
  const target = targetOf(check);
  // Oneself: the account and its sessions at once; an identity lives in a tenant, so only after
  // the tenant's checks (suspended, MFA, enforced SSO) below.
  const self = rule.self === true && isSelf(k, check, r, target);
  if (self && target?.type !== "identity.identity") return null;
  // The person suspends their own account only for themselves.
  if (type === "account.SuspendAccount" && data.reason === "owner_requested" && isSelf(k, check, r, target)) return null;

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
  const owner = homeTenant?.owners.includes(identityId) ?? false;
  // A tenant with enforced SSO: only sessions signed in through it (owners excepted, so they can mend it).
  const sso = tenant?.ssoEnforced;
  if (sso && !platform && !owner && !sender.apiKey && sender.method !== `sso:${sso}`) return refuse("sso_required", "Sign in through the tenant's single sign-on");
  if (self) return null;
  if (rule.level === "platform" && !platform) {
    const own = owner && (
      (type === "tenancy.SuspendTenant" && data.reason === "owner_requested") ||
      (type === "tenancy.ReactivateTenant" && tenant?.suspension === "owner_requested"));
    return own ? null : refuse("forbidden", `${type} is for the platform`);
  }
  if (owner) return null;
  if (type === "invitation.CreateInvitation" && data.owner === true && !platform) return refuse("forbidden", "Owner invitations are the owners'");
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
