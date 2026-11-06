// The account read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { accountDomain, type AccountStatus } from "./domain.ts";
import { AccountRegistered, EmailVerified } from "./events.ts";

export interface AccountRow { id: string; email: string | null; name: string | null; status: AccountStatus }
export const accountRows = new Map<string, AccountRow>();
export const accountsByEmail = new Map<string, string>();

export const accountsReadModel = accountDomain
  .readModel("accounts", { order: ORDERS.readModels })
  .on(AccountRegistered, (d, evt) => {
    accountRows.set(evt.aggregateId, { id: evt.aggregateId, email: d.email, name: d.name, status: d.verified ? "active" : "pending" });
    if (d.email) accountsByEmail.set(d.email, evt.aggregateId);
  })
  .on(EmailVerified, (_d, evt) => {
    const a = accountRows.get(evt.aggregateId);
    if (a?.status === "pending") a.status = "active";
  });
