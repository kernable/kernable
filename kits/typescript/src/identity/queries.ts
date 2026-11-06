// The identity queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import type { IdentityStatus } from "./domain.ts";
import { identitiesReadModel, type IdentityRow, identityRows, rowKey } from "./readmodels.ts";

/** The API's only: it sets `senderIdentity` from it. */
export class FindIdentityByAccount extends Query<{ identityId: string; status: IdentityStatus }> {
  accountId!: string;
}

identitiesReadModel.query(FindIdentityByAccount, (d, qry) => {
  const r = [...identityRows.values()].find((x) => x.tenant === qry.tenant && x.accountId === d?.accountId);
  if (!r) throw new Rejected("No identity in this tenant", "not_found");
  return { identityId: r.id, status: r.status };
});

export class GetIdentity extends Query<IdentityRow> {
  identityId!: string;
}

identitiesReadModel.query(GetIdentity, (d, qry) => {
  const r = identityRows.get(rowKey(qry.tenant, d?.identityId));
  if (!r) throw new Rejected("No such identity", "not_found");
  return r;
});

/** Every tenant the account has an identity in (ask with allPartitions when tenants are partitioned). */
export class ListMyIdentities extends Query<{ tenant: string; identityId: string; status: IdentityStatus }[]> {
  accountId!: string;
}

identitiesReadModel.query(ListMyIdentities, (d) =>
  [...identityRows.values()].filter((r) => r.accountId === d?.accountId).map((r) => ({ tenant: r.tenant, identityId: r.id, status: r.status })));

export class ListIdentities extends Query<IdentityRow[]> {}

identitiesReadModel.query(ListIdentities, (_d, qry) => [...identityRows.values()].filter((r) => r.tenant === qry.tenant));
