// The API part: framework-neutral functions for what only the API can do. Wire them into
// your HTTP framework; `example/server.ts` shows one with node:http.

import { randomUUID } from "node:crypto";

import type { Kernable, Outcome, RequestContext, StoredEvent } from "@kernable/sdk";

import { EndSession, FindAccountByEmail, RegisterAccount, SignIn, VerifyEmail, type SignInFailed, type SignedIn } from "../account/index.ts";
import { FindIdentityByAccount } from "../identity/index.ts";
import { PLATFORM_TENANT } from "../shared/constants.ts";
import { parseToken, randomSecret, token } from "../shared/secrets.ts";
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
    // A taken email: the same answer as success (a mail to the existing account is still to do).
    if (outcome.code === "unique_taken") return { accountId: randomUUID() };
    return { ok: false, code: outcome.code };
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

  /** Signs out: ends the session. */
  async signOut(claims: Claims): Promise<void> {
    events(await this.kernable.submit(EndSession.new({ reason: "signed_out" }), {
      aggregateId: claims.sid, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    }));
  }
}
