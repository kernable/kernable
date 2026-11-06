// Opens the session of every sign-in.

import { INTERNAL, ORDERS, PLATFORM_TENANT } from "../../shared/constants.ts";
import { accountDomain, type SignedIn } from "../domain.ts";

export const sessionsReactor = accountDomain
  .reactor("sessions", { order: ORDERS.reactors })
  .on("SignedIn", async (d: SignedIn, evt, ctx) => {
    await ctx.submit({
      id: `open-session-${d.sessionId}`,
      aggregateType: "session",
      aggregateId: d.sessionId,
      tenant: PLATFORM_TENANT,
      type: "OpenSession",
      data: {
        accountId: evt.aggregateId, method: d.method, factors: d.factors, refreshTokenHash: d.refreshTokenHash,
        ip: d.ip, userAgent: d.userAgent, at: d.at,
      },
      context: { ...ctx.follow(), ...INTERNAL },
    });
  });
