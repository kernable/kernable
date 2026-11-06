import { identitiesReadModel, identityRows } from "../readmodel/identities.ts";

// Every tenant the account has an identity in (ask with allPartitions when tenants are partitioned).
identitiesReadModel.query("ListMyIdentities", (d: { accountId: string }) =>
  [...identityRows.values()].filter((r) => r.accountId === d?.accountId).map((r) => ({ tenant: r.tenant, identityId: r.id, status: r.status })));
