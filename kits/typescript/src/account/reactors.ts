// The account reactors: side effects and follow-up commands.

import { INTERNAL, ORDERS, PLATFORM_TENANT } from "../shared/constants.ts";
import { currentMailer } from "../shared/mail.ts";
import { token } from "../shared/secrets.ts";
import { OpenSession } from "./commands.ts";
import { accountDomain } from "./domain.ts";
import { SignedIn, VerificationRequested } from "./events.ts";

export const mailReactor = accountDomain
  .reactor("mail", { order: ORDERS.reactors })
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
  .reactor("sessions", { order: ORDERS.reactors })
  .on(SignedIn, async (d, evt, ctx) => {
    await ctx.submit(
      OpenSession.new({
        accountId: evt.aggregateId, method: d.method, factors: d.factors, refreshTokenHash: d.refreshTokenHash,
        ip: d.ip, userAgent: d.userAgent, at: d.at,
      }),
      { id: `open-session-${d.sessionId}`, aggregateId: d.sessionId, tenant: PLATFORM_TENANT, context: { ...ctx.follow(), ...INTERNAL } },
    );
  });
