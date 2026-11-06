// The account read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { TenantDeleted } from "../tenancy/events.ts";
import { accountDomain, type AccountStatus } from "./domain.ts";
import {
  AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailChanged, EmailVerified, ProviderLinked, ProviderUnlinked, SessionEnded,
  SessionOpened, SessionRefreshed, SsoCreated, SsoDeleted, SsoDisabled, SsoDomainAdded, SsoDomainRemoved, SsoDomainVerified, SsoEnabled,
  SsoEnforcementChanged, SsoUpdated,
} from "./events.ts";

export interface AccountRow { id: string; email: string | null; name: string | null; status: AccountStatus }
export interface SessionRow { id: string; accountId: string; method: string; ip: string | null; userAgent: string | null; openedAt: number; lastAt: number; active: boolean }

export const accountRows = new Map<string, AccountRow>();
export const accountsByEmail = new Map<string, string>();
/** `provider:subject` → accountId */
export const accountsByLink = new Map<string, string>();

/** accountId → sessionId → session */
export const sessionRows = new Map<string, Map<string, SessionRow>>();

const status = (s: AccountStatus) => (_d: unknown, evt: { aggregateId: string }) => {
  const a = accountRows.get(evt.aggregateId);
  if (a) a.status = s;
};

export const accountsReadModel = accountDomain
  .readModel("account.accounts", { order: ORDERS.readModels })
  .on(AccountRegistered, (d, evt) => {
    accountRows.set(evt.aggregateId, { id: evt.aggregateId, email: d.email, name: d.name, status: d.verified ? "active" : "pending" });
    if (d.email) accountsByEmail.set(d.email, evt.aggregateId);
  })
  .on(EmailVerified, (_d, evt) => {
    const a = accountRows.get(evt.aggregateId);
    if (a?.status === "pending") a.status = "active";
  })
  // The new address finds the account, the old one no more.
  .on(EmailChanged, (d, evt) => {
    const a = accountRows.get(evt.aggregateId);
    if (!a) return;
    if (a.email) accountsByEmail.delete(a.email);
    a.email = d.email;
    if (d.email) accountsByEmail.set(d.email, evt.aggregateId);
  })
  .on(ProviderLinked, (d, evt) => void (d.subject && accountsByLink.set(`${d.provider}:${d.subject}`, evt.aggregateId)))
  .on(ProviderUnlinked, (d) => void (d.subject && accountsByLink.delete(`${d.provider}:${d.subject}`)))
  .on(AccountSuspended, status("suspended"))
  .on(AccountReactivated, status("active"))
  // A deleted account leaves the read side; its email is free.
  .on(AccountDeleted, (_d, evt) => {
    const a = accountRows.get(evt.aggregateId);
    if (a?.email) accountsByEmail.delete(a.email);
    accountRows.delete(evt.aggregateId);
    sessionRows.delete(evt.aggregateId);
  })
  .on(SessionOpened, (d, evt) => {
    let mine = sessionRows.get(d.accountId);
    if (!mine) sessionRows.set(d.accountId, (mine = new Map()));
    mine.set(evt.aggregateId, { id: evt.aggregateId, accountId: d.accountId, method: d.method, ip: d.ip, userAgent: d.userAgent, openedAt: d.at, lastAt: d.at, active: true });
  })
  .on(SessionRefreshed, (d, evt) => {
    const s = sessionRows.get(d.accountId)?.get(evt.aggregateId);
    if (s) {
      s.lastAt = d.at;
      s.ip = d.ip;
    }
  })
  .on(SessionEnded, (_d, evt) => {
    for (const mine of sessionRows.values()) {
      const s = mine.get(evt.aggregateId);
      if (s) s.active = false;
    }
  });

// --- A tenant's single sign-on ---

export interface SsoRow {
  id: string;
  tenant: string;
  status: "active" | "disabled";
  issuer: string;
  clientId: string;
  /** Read with vault_read; only the API gets it (GetSsoSecret). */
  clientSecret: string | null;
  domains: { domain: string; txt: string; verified: boolean }[];
  enforced: boolean;
}

/** `tenant/ssoId` */
export const ssoRows = new Map<string, SsoRow>();
export const ssoKey = (tenant: string, id: string) => `${tenant}/${id}`;
const sso = (evt: { tenant: string; aggregateId: string }, f: (r: SsoRow) => void) => {
  const r = ssoRows.get(ssoKey(evt.tenant, evt.aggregateId));
  if (r) f(r);
};

export const ssosReadModel = accountDomain
  .readModel("account.ssos", { order: ORDERS.readModels })
  .on(SsoCreated, (d, evt) => {
    ssoRows.set(ssoKey(evt.tenant, evt.aggregateId), {
      id: evt.aggregateId, tenant: evt.tenant, status: "active", issuer: d.issuer, clientId: d.clientId, clientSecret: d.clientSecret, domains: [], enforced: false,
    });
  })
  .on(SsoUpdated, (d, evt) => sso(evt, (r) => {
    r.issuer = d.issuer;
    r.clientId = d.clientId;
    r.clientSecret = d.clientSecret;
  }))
  .on(SsoDomainAdded, (d, evt) => sso(evt, (r) => void r.domains.push({ domain: d.domain, txt: d.txt, verified: false })))
  .on(SsoDomainVerified, (d, evt) => sso(evt, (r) => {
    const x = r.domains.find((y) => y.domain === d.domain);
    if (x) x.verified = true;
  }))
  .on(SsoDomainRemoved, (d, evt) => sso(evt, (r) => void (r.domains = r.domains.filter((x) => x.domain !== d.domain))))
  .on(SsoEnforcementChanged, (d, evt) => sso(evt, (r) => void (r.enforced = d.enforced)))
  .on(SsoDisabled, (_d, evt) => sso(evt, (r) => void (r.status = "disabled")))
  .on(SsoEnabled, (_d, evt) => sso(evt, (r) => void (r.status = "active")))
  .on(SsoDeleted, (_d, evt) => void ssoRows.delete(ssoKey(evt.tenant, evt.aggregateId)))
  .on(TenantDeleted, (_d, evt) => {
    for (const [k, r] of ssoRows) if (r.tenant === evt.aggregateId) ssoRows.delete(k);
  });
