// The account read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { accountDomain, type AccountStatus } from "./domain.ts";
import {
  AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailChanged, EmailVerified, SessionEnded, SessionOpened, SessionRefreshed,
} from "./events.ts";

export interface AccountRow { id: string; email: string | null; name: string | null; status: AccountStatus }
export interface SessionRow { id: string; accountId: string; method: string; ip: string | null; userAgent: string | null; openedAt: number; lastAt: number; active: boolean }

export const accountRows = new Map<string, AccountRow>();
export const accountsByEmail = new Map<string, string>();
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
