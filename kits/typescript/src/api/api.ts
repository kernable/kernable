// The API part: framework-neutral functions for what only the API can do. Wire them into
// your HTTP framework; `example/server.ts` shows one with node:http.

import { randomUUID } from "node:crypto";

import type { Kernable, Outcome, RequestContext, StoredEvent } from "@kernable/sdk";

import {
  ChangePassword, ConfirmEmailChange, EndSession, FindAccountByEmail, GetAccount, NoteRegistrationAttempt, RefreshSession, RegisterAccount, RequestEmailChange,
  RequestPasswordReset, ResetPassword, SignIn, VerifyEmail, type SessionRefreshed, type SignInFailed, type SignedIn,
} from "../account/index.ts";
import { AcceptInvitation, PeekInvitation } from "../invitation/index.ts";
import { FindIdentityByAccount } from "../identity/index.ts";
import { PLATFORM_TENANT } from "../shared/constants.ts";
import { parseInvitationToken, parseToken, randomSecret, token } from "../shared/secrets.ts";
import { checkAccessToken, issueAccessToken, type Claims, type SigningKeys } from "./tokens.ts";

export interface KitApiOptions {
  /** A client whose key may submit and query; it signs every request. */
  kernable: Kernable;
  /** The keys access tokens are signed with. */
  keys: SigningKeys;
  /** The API's own identity (a service of the platform tenant with an API key, from the bootstrap). */
  service: { identityId: string; keyId: string };
}

export type SignInResult =
  | { ok: true; accessToken: string; refreshToken: string; accountId: string; sessionId: string }
  | { ok: false; reason: string };

/** The events of a committed outcome; a rejection as an error with its code. */
function events(outcome: Outcome): StoredEvent[] {
  if (!outcome.committed) throw Object.assign(new Error(outcome.message), { code: outcome.code });
  return outcome.events;
}

export class KitApi {
  private readonly kernable: Kernable;
  private readonly keys: SigningKeys;
  /** The request context of the API's own requests: lookups no one else may make. */
  readonly serviceContext: RequestContext;

  constructor(options: KitApiOptions) {
    this.kernable = options.kernable;
    this.keys = options.keys;
    this.serviceContext = {
      senderTenant: PLATFORM_TENANT,
      senderIdentity: options.service.identityId,
      senderSession: options.service.keyId,
    };
  }

  /**
   * Registers a person. Answers the same whether or not the email was taken: the caller
   * shows "check your email" either way.
   */
  async register(input: { email: string; name: string; password?: string }): Promise<{ accountId: string } | { ok: false; code: string }> {
    const accountId = randomUUID();
    const outcome = await this.kernable.submit(RegisterAccount.new(input), { aggregateId: accountId, tenant: PLATFORM_TENANT, context: {} });
    if (outcome.committed) return { accountId };
    if (outcome.code !== "unique_taken") return { ok: false, code: outcome.code };
    // The address is held: a registration that expired unverified is taken over (same aggregate);
    // otherwise the same answer as success, so nobody learns who has an account.
    const held = await this.kernable.query(FindAccountByEmail.new({ email: input.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
    if (held.status === "pending") {
      const again = await this.kernable.submit(RegisterAccount.new(input), { aggregateId: held.id, tenant: PLATFORM_TENANT, context: {} });
      if (again.committed) return { accountId: held.id };
    } else {
      // The account's owner learns that someone tried; the caller learns nothing.
      await this.kernable.submit(NoteRegistrationAttempt.new(), { aggregateId: held.id, tenant: PLATFORM_TENANT, context: this.serviceContext });
    }
    return { accountId: randomUUID() };
  }

  /** Confirms an email address with the token from the mail. */
  async verifyEmail(emailToken: string): Promise<boolean> {
    const parts = parseToken(emailToken);
    if (!parts) return false;
    const outcome = await this.kernable.submit(VerifyEmail.new({ token: emailToken }), { aggregateId: parts[0], tenant: PLATFORM_TENANT, context: {} });
    return outcome.committed;
  }

  /** Signs in with email and password: an access token and a refresh token, or why not. */
  async signIn(input: { email: string; password: string; ip?: string; userAgent?: string }): Promise<SignInResult> {
    let account: { id: string };
    try {
      account = await this.kernable.query(FindAccountByEmail.new({ email: input.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
    } catch {
      return { ok: false, reason: "wrong_password" }; // never say whether the account exists
    }
    const sessionId = randomUUID();
    const refreshToken = token(sessionId, 1, randomSecret());
    const outcome = await this.kernable.submit(
      SignIn.new({ sessionId, password: input.password, refreshToken, ip: input.ip, userAgent: input.userAgent }),
      { aggregateId: account.id, tenant: PLATFORM_TENANT, context: {} },
    );
    const [event] = events(outcome);
    if (event.type !== "SignedIn") return { ok: false, reason: (event.data as SignInFailed).reason };
    const factors = (event.data as SignedIn).factors;
    return {
      ok: true, accountId: account.id, sessionId, refreshToken,
      accessToken: issueAccessToken(this.keys, { sub: account.id, sid: sessionId, fac: factors }),
    };
  }

  /** The claims of a valid access token, else null. */
  check(accessToken: string): Claims | null {
    return checkAccessToken(this.keys, accessToken);
  }

  /**
   * The request context for the token's holder, in `tenant` (its identity there) or, without a
   * tenant, only as a signed-in account (for `authenticated` requests).
   */
  async contextFor(claims: Claims, tenant?: string): Promise<RequestContext> {
    const context: RequestContext = { senderAccount: claims.sub, senderSession: claims.sid };
    if (!tenant) return context;
    const found = await this.kernable.query(FindIdentityByAccount.new({ accountId: claims.sub }), { tenant, context: this.serviceContext });
    return { ...context, senderTenant: tenant, senderIdentity: found.identityId };
  }

  /**
   * A new access token and a rotated refresh token. An older refresh token is a reuse:
   * the session ends, and so does every token of it.
   */
  async refresh(refreshToken: string, ip?: string): Promise<{ ok: true; accessToken: string; refreshToken: string } | { ok: false; reason: string }> {
    const parts = parseToken(refreshToken);
    if (!parts) return { ok: false, reason: "invalid_token" };
    const [sessionId, generation] = parts;
    const next = token(sessionId, Number(generation) + 1, randomSecret());
    const outcome = await this.kernable.submit(RefreshSession.new({ refreshToken, newRefreshToken: next, ip }), {
      aggregateId: sessionId, tenant: PLATFORM_TENANT, context: {},
    });
    if (!outcome.committed) return { ok: false, reason: outcome.code };
    const [event] = outcome.events;
    if (event.type !== "SessionRefreshed") return { ok: false, reason: (event.data as { reason: string }).reason };
    const d = event.data as SessionRefreshed;
    return { ok: true, refreshToken: next, accessToken: issueAccessToken(this.keys, { sub: d.accountId, sid: sessionId, fac: d.factors }) };
  }

  /** Changes the password; the other sessions end, this one stays. */
  async changePassword(claims: Claims, current: string | undefined, next: string): Promise<{ ok: boolean; reason?: string }> {
    const outcome = await this.kernable.submit(ChangePassword.new({ current, new: next, keepSessionId: claims.sid }), {
      aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** Sends a reset mail if the address has an active account; answers the same either way. */
  async requestPasswordReset(email: string): Promise<void> {
    try {
      const held = await this.kernable.query(FindAccountByEmail.new({ email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
      await this.kernable.submit(RequestPasswordReset.new(), { aggregateId: held.id, tenant: PLATFORM_TENANT, context: this.serviceContext });
    } catch {
      // no such account: nothing to send, nothing to say
    }
  }

  /** Sets a new password with the mailed token; every session ends. */
  async resetPassword(resetToken: string, next: string): Promise<{ ok: boolean; reason?: string }> {
    const parts = parseToken(resetToken);
    if (!parts) return { ok: false, reason: "invalid_token" };
    const outcome = await this.kernable.submit(ResetPassword.new({ token: resetToken, new: next }), { aggregateId: parts[0], tenant: PLATFORM_TENANT, context: {} });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** Asks to change the account's address: a token goes to the new one. */
  async requestEmailChange(claims: Claims, email: string): Promise<{ ok: boolean; reason?: string }> {
    const outcome = await this.kernable.submit(RequestEmailChange.new({ email }), {
      aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** Confirms the new address with its token; a taken address is refused (`unique_taken`). */
  async confirmEmailChange(changeToken: string): Promise<{ ok: boolean; reason?: string }> {
    const parts = parseToken(changeToken);
    if (!parts) return { ok: false, reason: "invalid_token" };
    const outcome = await this.kernable.submit(ConfirmEmailChange.new({ token: changeToken }), { aggregateId: parts[0], tenant: PLATFORM_TENANT, context: {} });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** What the acceptance page shows for an invitation token. */
  async peekInvitation(invitationToken: string) {
    const t = parseInvitationToken(invitationToken);
    if (!t) return null;
    try {
      return await this.kernable.query(PeekInvitation.new({ token: invitationToken }), { tenant: t.tenant, context: this.serviceContext });
    } catch {
      return null;
    }
  }

  /**
   * Accepts an invitation: as the signed-in account (`claims`), or as a new person with a
   * name and password, registered in the same step (active: the token proved the email). The
   * accepting account's email must be the invited one.
   */
  async acceptInvitation(
    invitationToken: string,
    as: { claims: Claims } | { name: string; password: string },
  ): Promise<{ ok: true; accountId: string; tenant: string } | { ok: false; reason: string }> {
    const peek = await this.peekInvitation(invitationToken);
    if (!peek || !peek.email) return { ok: false, reason: "invalid_token" };
    let accountId: string;
    let email: string;
    const newAccount = !("claims" in as);
    if ("claims" in as) {
      accountId = as.claims.sub;
      const account = await this.kernable.query(GetAccount.new({ accountId }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
      email = account.email ?? "";
    } else {
      // Someone with an account signs in first, so nobody gets an account they could not use.
      try {
        await this.kernable.query(FindAccountByEmail.new({ email: peek.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
        return { ok: false, reason: "sign_in_first" };
      } catch {
        accountId = randomUUID();
        email = peek.email;
      }
    }
    const accepted = await this.kernable.submit(AcceptInvitation.new({ token: invitationToken, accountId, email, newAccount }), {
      aggregateId: peek.invitationId, tenant: peek.tenant, context: this.serviceContext,
    });
    if (!accepted.committed) return { ok: false, reason: accepted.code };
    if (!("claims" in as)) {
      const registered = await this.kernable.submit(RegisterAccount.new({ email, name: as.name, password: as.password, verified: true }), {
        aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext,
      });
      if (!registered.committed) return { ok: false, reason: registered.code };
    }
    return { ok: true, accountId, tenant: peek.tenant };
  }

  /** Signs out: ends the session. */
  async signOut(claims: Claims): Promise<void> {
    events(await this.kernable.submit(EndSession.new({ reason: "signed_out" }), {
      aggregateId: claims.sid, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    }));
  }
}
