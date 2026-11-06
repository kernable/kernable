// The account queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import { normalEmail } from "../shared/secrets.ts";
import type { AccountStatus } from "./domain.ts";
import { type AccountRow, accountRows, accountsByEmail, accountsReadModel, type SessionRow, sessionRows } from "./readmodels.ts";

/** The API's only: an open lookup would reveal who has an account. */
export class FindAccountByEmail extends Query<{ id: string; status: AccountStatus }> {
  email!: string;
}

accountsReadModel.query(FindAccountByEmail, (d) => {
  const id = accountsByEmail.get(normalEmail(d?.email));
  const a = id ? accountRows.get(id) : undefined;
  if (!a) throw new Rejected("No such account", "not_found");
  return { id: a.id, status: a.status };
});

export class GetAccount extends Query<AccountRow> {
  accountId!: string;
}

accountsReadModel.query(GetAccount, (d) => {
  const a = accountRows.get(d?.accountId);
  if (!a) throw new Rejected("No such account", "not_found");
  return a;
});

/** The account's sessions, the open ones first: "my devices". */
export class ListSessions extends Query<SessionRow[]> {
  accountId!: string;
}

accountsReadModel.query(ListSessions, (d) =>
  [...(sessionRows.get(d?.accountId)?.values() ?? [])].sort((a, b) => Number(b.active) - Number(a.active) || b.lastAt - a.lastAt));
