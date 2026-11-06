// The account reactors: side effects and follow-up commands.

import type { ListenerContext } from "@kernable/sdk";

import { INTERNAL, ORDERS, PLATFORM_TENANT } from "../shared/constants.ts";
import { currentMailer } from "../shared/mail.ts";
import { token } from "../shared/secrets.ts";
import { EndSession, OpenSession } from "./commands.ts";
import { accountDomain } from "./domain.ts";
import { AccountDeleted, AccountSuspended, SignedIn, VerificationRequested } from "./events.ts";
import { ListSessions } from "./queries.ts";

export const mailReactor = accountDomain
  .reactor("account.mail", { order: ORDERS.reactors })
  .on(VerificationRequested, async (d, evt) => {
    // Erased already (the account verified, or was deleted): nothing to send.
    if (!d.token || !d.email) return;
    const link = token(evt.aggregateId, d.tokenId, d.token);
    await currentMailer().send({
      to: d.email, purpose: "verify", token: link, subject: "Confirm your email address",
      text: `Confirm your email address with this code: ${link}`,
    });
  });

export const sessionsReactor = accountDomain
  .reactor("account.sessions", { order: ORDERS.reactors })
  .on(SignedIn, async (d, evt, ctx) => {
    await ctx.submit(
      OpenSession.new({
        accountId: evt.aggregateId, method: d.method, factors: d.factors, refreshTokenHash: d.refreshTokenHash,
        ip: d.ip, userAgent: d.userAgent, at: d.at,
      }),
      { id: `open-session-${d.sessionId}`, aggregateId: d.sessionId, tenant: PLATFORM_TENANT, context: { ...ctx.follow(), ...INTERNAL } },
    );
  });

/** A suspended or deleted account's sessions end at once. */
export const endingReactor = accountDomain
  .reactor("account.ending", { order: ORDERS.reactors })
  .on(AccountSuspended, (_d, evt, ctx) => endAll(evt.aggregateId, "account_suspended", ctx))
  .on(AccountDeleted, (_d, evt, ctx) => endAll(evt.aggregateId, "account_deleted", ctx));

async function endAll(accountId: string, reason: "account_suspended" | "account_deleted", ctx: ListenerContext): Promise<void> {
  const sessions = await ctx.query(ListSessions.new({ accountId }), { tenant: PLATFORM_TENANT, context: INTERNAL });
  for (const s of sessions.filter((x) => x.active)) {
    try {
      await ctx.submit(EndSession.new({ reason }), {
        id: `end-${s.id}-${reason}`, aggregateId: s.id, tenant: PLATFORM_TENANT, context: { ...ctx.follow(), ...INTERNAL },
      });
    } catch (err) {
      if ((err as { code?: string }).code !== "invalid_state") throw err; // ended meanwhile (signed out)
    }
  }
}
