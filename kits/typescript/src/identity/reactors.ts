// The identity reactors: side effects and follow-up commands.

import { randomUUID } from "node:crypto";

import { INTERNAL, ORDERS } from "../shared/constants.ts";
import type { ListenerContext } from "@kernable/sdk";

import { AccountDeleted } from "../account/index.ts";
import { GroupDeleted } from "../group/index.ts";
import { InvitationAccepted } from "../invitation/index.ts";
import { AddOwner, TenantCreated, TenantDeleted } from "../tenancy/index.ts";
import { CreateIdentity, DeleteIdentity, GrantMembership, RevokeMembership } from "./commands.ts";
import { FindIdentityByAccount, ListIdentities, ListMyIdentities } from "./queries.ts";
import { identityDomain } from "./domain.ts";

export const ownersReactor = identityDomain
  .reactor("identity.owners", { order: ORDERS.reactors })
  .on(TenantCreated, async (d, evt, ctx) => {
    const accountId = "accountId" in d.owner ? d.owner.accountId : undefined;
    if (!accountId) return;
    const tenant = evt.aggregateId;
    const context = { ...ctx.follow(), ...INTERNAL };
    // A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
    const created = await ctx.submit(CreateIdentity.new({ accountId }), { id: `owner-identity-${tenant}`, aggregateId: randomUUID(), tenant, context });
    await ctx.submit(AddOwner.new({ identityId: created[0].aggregateId }), { id: `owner-${tenant}`, aggregateId: tenant, tenant, context });
  });

const internal = (ctx: ListenerContext) => ({ ...ctx.follow(), ...INTERNAL });

/** An accepted invitation becomes an identity with its groups, and an owner for an owner invitation. */
export const invitationsReactor = identityDomain
  .reactor("identity.invitations", { order: ORDERS.reactors })
  .on(InvitationAccepted, async (d, evt, ctx) => {
    const tenant = evt.tenant;
    const invitation = evt.aggregateId;
    // An account with an identity here already keeps it and gains the groups.
    let identityId: string;
    try {
      identityId = (await ctx.query(FindIdentityByAccount.new({ accountId: d.accountId }), { tenant, context: INTERNAL })).identityId;
    } catch {
      const created = await ctx.submit(CreateIdentity.new({ accountId: d.accountId }), {
        id: `invitation-identity-${invitation}`, aggregateId: randomUUID(), tenant, context: internal(ctx),
      });
      identityId = created[0].aggregateId;
    }
    for (const [i, g] of d.groups.entries()) {
      try {
        await ctx.submit(GrantMembership.new({ membershipId: `${invitation}-${i}`, group: g.group, scope: g.scope ?? null }), {
          id: `invitation-membership-${invitation}-${i}`, aggregateId: identityId, tenant, context: internal(ctx),
        });
      } catch (err) {
        if ((err as { code?: string }).code !== "exists") throw err; // a member already
      }
    }
    if (d.owner) {
      try {
        await ctx.submit(AddOwner.new({ identityId }), { id: `invitation-owner-${invitation}`, aggregateId: tenant, tenant, context: internal(ctx) });
      } catch (err) {
        if ((err as { code?: string }).code !== "exists") throw err; // an owner already
      }
    }
  });

/** A deleted tenant's identities are erased, before the read models drop the tenant. */
export const identityCleanupReactor = identityDomain
  .reactor("identity.cleanup", { order: ORDERS.cleanup })
  .on(TenantDeleted, async (_d, evt, ctx) => {
    const tenant = evt.aggregateId;
    for (const r of await ctx.query(ListIdentities.new(), { tenant, context: INTERNAL })) {
      await ctx.submit(DeleteIdentity.new(), { id: `delete-${r.id}`, aggregateId: r.id, tenant, context: internal(ctx) });
    }
  });

/** Identities follow their account and their groups. */
export const identityFollowReactor = identityDomain
  .reactor("identity.follow", { order: ORDERS.reactors })
  // A deleted account's identities go in every tenant. One that still owns a tenant is refused
  // (an owner must be replaced first) and parks here for an operator.
  .on(AccountDeleted, async (_d, evt, ctx) => {
    const mine = await ctx.query(ListMyIdentities.new({ accountId: evt.aggregateId }), { tenant: evt.tenant, context: INTERNAL, allPartitions: true });
    for (const r of mine) {
      await ctx.submit(DeleteIdentity.new(), { id: `delete-${r.identityId}`, aggregateId: r.identityId, tenant: r.tenant, context: internal(ctx) });
    }
  })
  // A deleted group's memberships stopped counting at once; here they leave the identities.
  .on(GroupDeleted, async (_d, evt, ctx) => {
    for (const r of await ctx.query(ListIdentities.new(), { tenant: evt.tenant, context: INTERNAL })) {
      for (const m of r.memberships.filter((x) => x.group === evt.aggregateId)) {
        await ctx.submit(RevokeMembership.new({ membershipId: m.membershipId }), {
          id: `revoke-${r.id}-${m.membershipId}`, aggregateId: r.id, tenant: evt.tenant, context: internal(ctx),
        });
      }
    }
  });
