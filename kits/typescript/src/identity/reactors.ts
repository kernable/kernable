// The identity reactors: side effects and follow-up commands.

import { randomUUID } from "node:crypto";

import { INTERNAL, ORDERS, PLATFORM_TENANT } from "../shared/constants.ts";
import { randomSecret } from "../shared/secrets.ts";
import type { ListenerContext } from "@kernable/sdk";

import { AccountDeleted } from "../account/index.ts";
import { GroupDeleted } from "../group/index.ts";
import { InvitationAccepted, InvitationLinkUsed, type InvitedGroup } from "../invitation/index.ts";
import { AddOwner, TenantCreated, TenantDeleted } from "../tenancy/index.ts";
import { CreateApiKey, CreateIdentity, CreateProcessIdentity, DeleteIdentity, GrantMembership, RevokeMembership } from "./commands.ts";
import { ApiKeyCreated } from "./events.ts";
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

/**
 * An account joins a tenant with groups (and as owner): its identity there is created, or kept if
 * it has one, and gains the groups. `source` makes the follow-up command IDs unique per join.
 */
async function join(ctx: ListenerContext, tenant: string, source: string, accountId: string, groups: InvitedGroup[], owner: boolean) {
  let identityId: string;
  try {
    identityId = (await ctx.query(FindIdentityByAccount.new({ accountId }), { tenant, context: INTERNAL })).identityId;
  } catch {
    const created = await ctx.submit(CreateIdentity.new({ accountId }), {
      id: `invitation-identity-${source}`, aggregateId: randomUUID(), tenant, context: internal(ctx),
    });
    identityId = created[0].aggregateId;
  }
  for (const [i, g] of groups.entries()) {
    try {
      await ctx.submit(GrantMembership.new({ membershipId: `${source}-${i}`, group: g.group, scope: g.scope ?? null }), {
        id: `invitation-membership-${source}-${i}`, aggregateId: identityId, tenant, context: internal(ctx),
      });
    } catch (err) {
      if ((err as { code?: string }).code !== "exists") throw err; // a member already
    }
  }
  if (owner) {
    try {
      await ctx.submit(AddOwner.new({ identityId }), { id: `invitation-owner-${source}`, aggregateId: tenant, tenant, context: internal(ctx) });
    } catch (err) {
      if ((err as { code?: string }).code !== "exists") throw err; // an owner already
    }
  }
}

/**
 * An accepted invitation becomes an identity with its groups, and an owner for an owner invitation;
 * so does a join through an invitation link, never as owner.
 */
export const invitationsReactor = identityDomain
  .reactor("identity.invitations", { order: ORDERS.reactors })
  .on(InvitationAccepted, (d, evt, ctx) => join(ctx, evt.tenant, evt.aggregateId, d.accountId, d.groups, d.owner))
  .on(InvitationLinkUsed, (d, evt, ctx) => join(ctx, evt.tenant, `${evt.aggregateId}-${d.accountId}`, d.accountId, d.groups, false));

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

/** The ID of a process's identity in the platform tenant, `process-<name>`: every run, and every kit, finds the same. */
export function processIdentityId(process: string): string {
  return `process-${process}`;
}

/**
 * Each process the kernel runs gets an identity (design/processes.md, "With the kit"): on
 * `process.ProcessRegistered` (a first version, a new one, a re-activation), the process's
 * identity in the platform tenant, kind `process`, without permissions; an API key for it, whose
 * secret nobody keeps (only its ID travels); and the kernel told to act as both. Fixed command and
 * aggregate IDs make every redelivery and every new version find the same identity and key.
 * Until a platform admin grants the identity a group, the process's commands are refused.
 */
export const processesReactor = identityDomain
  .reactor("identity.processes", { order: ORDERS.reactors })
  .on("process.ProcessRegistered", async (d: { name: string; version: number }, _evt, ctx) => {
    const process = d.name;
    const identityId = processIdentityId(process);
    const target = { tenant: PLATFORM_TENANT, context: internal(ctx) };
    try {
      await ctx.submit(CreateProcessIdentity.new({ process }), { ...target, id: `process-identity-${process}`, aggregateId: identityId });
    } catch (err) {
      if ((err as { code?: string }).code !== "exists") throw err; // kept
    }
    // A fixed command ID: a redelivery returns the first outcome, so the first key's ID.
    const created = await ctx.submit(
      CreateApiKey.new({ keyId: randomSecret(18).replace(/[^A-Za-z0-9]/g, "x"), secret: randomSecret(32), label: `process ${process}` }),
      { ...target, id: `process-key-${process}`, aggregateId: identityId },
    );
    const key = created.find((e) => e.type === "ApiKeyCreated");
    if (!key) throw new Error(`no API key for the process ${process}`);
    const keyId = (key.data as ApiKeyCreated).keyId;
    await ctx.processAct(process, { senderTenant: PLATFORM_TENANT, senderIdentity: identityId, senderSession: keyId });
  });
