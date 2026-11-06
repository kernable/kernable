// The invitation reactors: side effects and follow-up commands.

import { randomUUID } from "node:crypto";

import type { ListenerContext } from "@kernable/sdk";

import { INTERNAL, ORDERS } from "../shared/constants.ts";
import { currentMailer } from "../shared/mail.ts";
import { invitationToken } from "../shared/secrets.ts";
import { TenantCreated, TenantDeleted } from "../tenancy/index.ts";
import { CreateInvitation, EraseInvitation, RecordInvitationMailSent } from "./commands.ts";
import { invitationDomain } from "./domain.ts";
import { InvitationCreated, InvitationResent } from "./events.ts";
import { ListInvitations } from "./queries.ts";

/** Sends the invitation mails; it reads the tokens from the vault. */
export const invitationMailReactor = invitationDomain
  .reactor("invitation.mail", { order: ORDERS.reactors })
  .on(InvitationCreated, async (d, evt, ctx) => send(evt.tenant, evt.aggregateId, d.tokenId, d.token, d.email, d.owner, ctx))
  .on(InvitationResent, async (d, evt, ctx) => {
    const rows = await ctx.query(ListInvitations.new(), { tenant: evt.tenant, context: INTERNAL });
    const row = rows.find((r) => r.id === evt.aggregateId);
    await send(evt.tenant, evt.aggregateId, d.tokenId, d.token, row?.email ?? null, row?.owner ?? false, ctx);
  });

async function send(tenant: string, id: string, tokenId: string, secret: string | null, email: string | null, owner: boolean, ctx: ListenerContext) {
  if (!secret || !email) return; // used or erased meanwhile
  const link = invitationToken(tenant, id, tokenId, secret);
  await currentMailer().send({
    to: email, purpose: "invitation", token: link, subject: owner ? "You are invited to own a tenant" : "You are invited",
    text: `Accept the invitation with this code: ${link}`,
  });
  await ctx.submit(RecordInvitationMailSent.new({ tokenId }), { id: `mail-${tokenId}`, aggregateId: id, tenant, context: { ...ctx.follow(), ...INTERNAL } });
}

/** A tenant the platform creates gets its first owner by an owner invitation. */
export const ownerInvitationReactor = invitationDomain
  .reactor("invitation.owners", { order: ORDERS.reactors })
  .on(TenantCreated, async (d, evt, ctx) => {
    const email = "email" in d.owner ? d.owner.email : "";
    if (!email) return;
    await ctx.submit(CreateInvitation.new({ email, owner: true }), {
      id: `owner-invitation-${evt.aggregateId}`, aggregateId: randomUUID(), tenant: evt.aggregateId, context: { ...ctx.follow(), ...INTERNAL },
    });
  });

/** A deleted tenant's invitations are revoked and their emails erased, before the read models drop them. */
export const invitationCleanupReactor = invitationDomain
  .reactor("invitation.cleanup", { order: ORDERS.cleanup })
  .on(TenantDeleted, async (_d, evt, ctx) => {
    const rows = await ctx.query(ListInvitations.new(), { tenant: evt.aggregateId, context: INTERNAL });
    for (const r of rows.filter((x) => x.email !== null)) {
      await ctx.submit(EraseInvitation.new(), { id: `erase-${r.id}`, aggregateId: r.id, tenant: evt.aggregateId, context: { ...ctx.follow(), ...INTERNAL } });
    }
  });
