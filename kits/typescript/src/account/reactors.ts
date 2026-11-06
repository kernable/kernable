// The account reactors: side effects and follow-up commands.

import type { ListenerContext } from "@kernable/sdk";

import { INTERNAL, ORDERS, PLATFORM_TENANT } from "../shared/constants.ts";
import { currentMailer } from "../shared/mail.ts";
import { token } from "../shared/secrets.ts";
import { AddSessionFactor, EndSession, OpenSession } from "./commands.ts";
import { accountDomain } from "./domain.ts";
import {
  AccountDeleted, AccountSuspended, EmailChangeRequested, PasswordChanged, PasswordReset, PasswordResetRequested, RegistrationAttempted,
  SecondFactorProved, SignedIn, VerificationRequested,
} from "./events.ts";
import { ListSessions } from "./queries.ts";

const mail = (to: string | null, purpose: "verify" | "reset" | "email_change" | "account_exists", link: string, subject: string, text: string) =>
  to ? currentMailer().send({ to, purpose, token: link, subject, text }) : Promise.resolve();

/** Sends the emailed tokens and notices; it reads them from the vault. Erased meanwhile: nothing to send. */
export const mailReactor = accountDomain
  .reactor("account.mail", { order: ORDERS.reactors })
  .on(VerificationRequested, (d, evt) =>
    d.token ? mail(d.email, "verify", token(evt.aggregateId, d.tokenId, d.token), "Confirm your email address", `Confirm your email address with this code: ${token(evt.aggregateId, d.tokenId, d.token)}`) : undefined)
  .on(PasswordResetRequested, (d, evt) =>
    d.token ? mail(d.email, "reset", token(evt.aggregateId, d.tokenId, d.token), "Reset your password", `Set a new password with this code: ${token(evt.aggregateId, d.tokenId, d.token)}`) : undefined)
  .on(EmailChangeRequested, (d, evt) =>
    d.token ? mail(d.email, "email_change", token(evt.aggregateId, d.tokenId, d.token), "Confirm your new email address", `Confirm this address with this code: ${token(evt.aggregateId, d.tokenId, d.token)}`) : undefined)
  .on(RegistrationAttempted, (d) =>
    mail(d.email, "account_exists", "", "You already have an account", "Someone registered with this address, which has an account already. Sign in, or reset your password."));

export const sessionsReactor = accountDomain
  .reactor("account.sessions", { order: ORDERS.reactors })
  .on(SecondFactorProved, async (d, evt, ctx) => {
    await ctx.submit(AddSessionFactor.new({ factor: d.method, accountId: evt.aggregateId }), {
      id: `factor-${d.sessionId}-${evt.commandId}`, aggregateId: d.sessionId, tenant: PLATFORM_TENANT, context: { ...ctx.follow(), ...INTERNAL },
    });
  })
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
  .on(AccountDeleted, (_d, evt, ctx) => endAll(evt.aggregateId, "account_deleted", ctx))
  // A new password signs out everywhere else.
  .on(PasswordChanged, (d, evt, ctx) => endAll(evt.aggregateId, "password_changed", ctx, d.keepSessionId))
  .on(PasswordReset, (_d, evt, ctx) => endAll(evt.aggregateId, "password_changed", ctx));

async function endAll(accountId: string, reason: "account_suspended" | "account_deleted" | "password_changed", ctx: ListenerContext, keep: string | null = null): Promise<void> {
  const sessions = await ctx.query(ListSessions.new({ accountId }), { tenant: PLATFORM_TENANT, context: INTERNAL });
  for (const s of sessions.filter((x) => x.active && x.id !== keep)) {
    try {
      await ctx.submit(EndSession.new({ reason }), {
        id: `end-${s.id}-${reason}`, aggregateId: s.id, tenant: PLATFORM_TENANT, context: { ...ctx.follow(), ...INTERNAL },
      });
    } catch (err) {
      if ((err as { code?: string }).code !== "invalid_state") throw err; // ended meanwhile (signed out)
    }
  }
}
