// The account queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import { normalEmail } from "../shared/secrets.ts";
import type { AccountStatus } from "./domain.ts";
import { accountsByLink, ssoKey, ssoRows, ssosReadModel, type SsoRow, type AccountRow, accountRows, accountsByEmail, accountsReadModel, type SessionRow, sessionRows } from "./readmodels.ts";

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
  static target = { aggregateType: "account.account", field: "accountId" };
  accountId!: string;
}

accountsReadModel.query(GetAccount, (d) => {
  const a = accountRows.get(d?.accountId);
  if (!a) throw new Rejected("No such account", "not_found");
  return a;
});

/** The account's sessions, the open ones first: "my devices". */
export class ListSessions extends Query<SessionRow[]> {
  static target = { aggregateType: "account.account", field: "accountId" };
  accountId!: string;
}

accountsReadModel.query(ListSessions, (d) =>
  [...(sessionRows.get(d?.accountId)?.values() ?? [])].sort((a, b) => Number(b.active) - Number(a.active) || b.lastAt - a.lastAt));

/** The API's only: the account an external login is linked to. */
export class FindAccountByLink extends Query<{ id: string; status: AccountStatus }> {
  provider!: string;
  subject!: string;
}

accountsReadModel.query(FindAccountByLink, (d) => {
  const id = accountsByLink.get(`${d?.provider}:${d?.subject}`);
  const a = id ? accountRows.get(id) : undefined;
  if (!a) throw new Rejected("Not linked", "not_found");
  return { id: a.id, status: a.status };
});

/** The API's only: the active SSO that verified the domain, to sign its people in (any tenant). */
export class FindSsoByDomain extends Query<{ tenant: string; ssoId: string; issuer: string; clientId: string; enforced: boolean }> {
  domain!: string;
}

ssosReadModel.query(FindSsoByDomain, (d) => {
  const domain = typeof d?.domain === "string" ? d.domain.toLowerCase() : "";
  const r = [...ssoRows.values()].find((x) => x.status === "active" && x.domains.some((y) => y.domain === domain && y.verified));
  if (!r) throw new Rejected("No SSO for this domain", "not_found");
  return { tenant: r.tenant, ssoId: r.id, issuer: r.issuer, clientId: r.clientId, enforced: r.enforced };
});

/** The API's only: what it signs in with, the client secret included, and the verified domains whose people it may sign in. */
export class GetSsoSecret extends Query<{ issuer: string; clientId: string; clientSecret: string | null; domains: string[] }> {
  ssoId!: string;
}

ssosReadModel.query(GetSsoSecret, (d, qry) => {
  const r = ssoRows.get(ssoKey(qry.tenant, d?.ssoId));
  if (!r) throw new Rejected("No such SSO", "not_found");
  return { issuer: r.issuer, clientId: r.clientId, clientSecret: r.clientSecret, domains: r.domains.filter((x) => x.verified).map((x) => x.domain) };
});

/** The tenant's owners: the configuration without the secret, with the TXT values to publish. */
export class GetSso extends Query<Omit<SsoRow, "clientSecret">> {
  ssoId!: string;
}

ssosReadModel.query(GetSso, (d, qry) => {
  const r = ssoRows.get(ssoKey(qry.tenant, d?.ssoId));
  if (!r) throw new Rejected("No such SSO", "not_found");
  const { clientSecret: _, ...rest } = r;
  return rest;
});
