// Creates the first owner of a self-service tenant, then makes it the owner.

import { randomUUID } from "node:crypto";

import { INTERNAL, ORDERS } from "../../shared/constants.ts";
import type { TenantCreated } from "../../tenancy/domain.ts";
import { identityDomain } from "../domain.ts";

export const ownersReactor = identityDomain
  .reactor("owners", { order: ORDERS.reactors })
  .on("tenancy.TenantCreated", async (d: TenantCreated, evt, ctx) => {
    const accountId = "accountId" in d.owner ? d.owner.accountId : undefined;
    if (!accountId) return;
    const tenant = evt.aggregateId;
    // A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
    const created = await ctx.submit({
      id: `owner-identity-${tenant}`, domain: "identity", aggregateType: "identity", aggregateId: randomUUID(), tenant,
      type: "CreateIdentity", data: { accountId }, context: { ...ctx.follow(), ...INTERNAL },
    });
    await ctx.submit({
      id: `owner-${tenant}`, domain: "tenancy", aggregateType: "tenant", aggregateId: tenant, tenant,
      type: "AddOwner", data: { identityId: created[0].aggregateId }, context: { ...ctx.follow(), ...INTERNAL },
    });
  });
