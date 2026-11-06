// The identity reactors: side effects and follow-up commands.

import { randomUUID } from "node:crypto";

import { INTERNAL, ORDERS } from "../shared/constants.ts";
import { AddOwner, TenantCreated } from "../tenancy/index.ts";
import { CreateIdentity } from "./commands.ts";
import { identityDomain } from "./domain.ts";

export const ownersReactor = identityDomain
  .reactor("owners", { order: ORDERS.reactors })
  .on(TenantCreated, async (d, evt, ctx) => {
    const accountId = "accountId" in d.owner ? d.owner.accountId : undefined;
    if (!accountId) return;
    const tenant = evt.aggregateId;
    const context = { ...ctx.follow(), ...INTERNAL };
    // A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
    const created = await ctx.submit(CreateIdentity.new({ accountId }), { id: `owner-identity-${tenant}`, aggregateId: randomUUID(), tenant, context });
    await ctx.submit(AddOwner.new({ identityId: created[0].aggregateId }), { id: `owner-${tenant}`, aggregateId: tenant, tenant, context });
  });
