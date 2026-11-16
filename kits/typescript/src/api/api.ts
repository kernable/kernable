// The API part: framework-neutral functions for what only the API can do. Wire them into
// your HTTP framework; `example/server.ts` shows one with node:http.

import { randomUUID } from "node:crypto";
import { resolveTxt } from "node:dns/promises";

import * as oidc from "openid-client";

import { Rejected, type Kernable, type Outcome, type RequestContext, type StoredEvent } from "@kernable/sdk";
import {
  generateAuthenticationOptions, generateRegistrationOptions, type AuthenticationResponseJSON, type PublicKeyCredentialCreationOptionsJSON,
  type PublicKeyCredentialRequestOptionsJSON, type RegistrationResponseJSON,
} from "@simplewebauthn/server";

import {
  AddPasskey, FindAccountByLink, FindSsoByDomain, GetSsoSecret, LinkProvider, SignInWithProvider, VerifySsoDomain, ChangePassword, ConfirmEmailChange, ConfirmTotp, ProvePasskey, ProveSecondFactor, SignInWithPasskey, StartTotp, EndSession, FindAccountByEmail, GetAccount, ListSessions, NoteRegistrationAttempt, RefreshSession, RegisterAccount, RequestEmailChange,
  RequestPasswordReset, ResetPassword, SignIn, VerifyEmail, type AccountRow, type SessionRefreshed, type SignInFailed, type SignedIn,
} from "../account/index.ts";
import {
  AcceptInvitation, CreateInvitationLink, JoinWithInvitationLink, PeekInvitation, PeekInvitationLink, linkToken, parseLinkToken, type InvitedGroup,
} from "../invitation/index.ts";
import { CreateApiKey, FindApiKey, FindIdentityByAccount, apiKey, parseApiKey } from "../identity/index.ts";
import { CHALLENGE_SECONDS, PASSKEYS, PLATFORM_TENANT } from "../shared/constants.ts";
import { GetSignUpPolicy } from "../tenancy/index.ts";
import { checkPassword, emailDomain, equalSecrets, parseInvitationToken, parseToken, randomSecret, token, totpSecret, validPassword, chars, trimSpace } from "../shared/secrets.ts";
import { checkAccessToken, issueAccessToken, type Claims, type SigningKeys } from "./tokens.ts";

export interface KitApiOptions {
  /** A client whose key may submit and query; it signs every request. */
  kernable: Kernable;
  /** The keys access tokens are signed with. */
  keys: SigningKeys;
  /** The API's own identity (a service of the platform tenant with an API key, from the bootstrap). */
  service: { identityId: string; keyId: string };
  /**
   * Social OAuth/OIDC providers by name (`google`, …); tenants' SSO is configured as `account.sso`.
   * An OpenID Connect provider is found by its `issuer`; a plain OAuth2 one (GitHub) names its
   * endpoints in `oauth2` (`github()` makes one).
   */
  providers?: Record<string, Provider>;
  /** Where providers send the browser back to; your route calls `oidcFinish` with the full URL. */
  redirectUri?: string;
  /** Allows http:// issuers: for development and tests only. */
  insecureIssuers?: boolean;
  /** How DNS TXT records are read (tests replace it). */
  resolveTxt?: (name: string) => Promise<string[][]>;
}

/** A social sign-in provider: OpenID Connect by its issuer, or plain OAuth2 by its endpoints. */
export interface Provider { issuer: string; clientId: string; clientSecret: string; oauth2?: OAuth2 }

/**
 * The endpoints of an OAuth2 provider without OpenID Connect: where the browser signs in, where
 * the code becomes an access token, and where the person (`user`: its `id` is the subject) and
 * their email addresses (`emails`: the primary verified one counts) are read.
 */
export interface OAuth2 { authorize: string; token: string; user: string; emails: string; scope: string }

/** GitHub (an OAuth app's client): `web` and `api` default to github.com's; GitHub Enterprise Server's otherwise. */
export function github(clientId: string, clientSecret: string, web = "https://github.com", api = "https://api.github.com"): Provider {
  const w = web.replace(/\/+$/, ""), a = api.replace(/\/+$/, "");
  return {
    issuer: w, clientId, clientSecret,
    oauth2: {
      authorize: `${w}/login/oauth/authorize`, token: `${w}/login/oauth/access_token`,
      user: `${a}/user`, emails: `${a}/user/emails`, scope: "read:user user:email",
    },
  };
}

/** The address an OAuth2 provider's email list says is the person's: the primary one if verified, else the first verified. */
export function oauth2Email(emails: unknown): string | null {
  if (!Array.isArray(emails)) return null;
  const verified = emails.filter((e) => e?.verified === true && typeof e.email === "string" && e.email);
  return (verified.find((e) => e.primary === true) ?? verified[0])?.email ?? null;
}

/** What `oidcStart` keeps in the cache until the browser comes back. */
interface OidcState { provider: string; tenant?: string; ssoId?: string; verifier: string; nonce: string; link?: { accountId: string } }

export type SignInResult =
  | {
      ok: true; accessToken: string; refreshToken: string; accountId: string; sessionId: string;
      /** The account has a second factor on: prove it (`proveSecondFactor`) before anything else. */
      secondFactor: boolean;
    }
  | { ok: false; reason: string };

/**
 * Why a sign-in failed, as the caller hears it: a locked account answers `wrong_password` like an
 * unknown address, so nobody learns from it that an account exists (its `SignInFailed` event
 * keeps `locked`).
 */
function signInReason(reason: string): string {
  return reason === "locked" ? "wrong_password" : reason;
}

/** The events of a committed outcome; a rejection as an error with its code. */
function events(outcome: Outcome): StoredEvent[] {
  if (!outcome.committed) throw Object.assign(new Error(outcome.message), { code: outcome.code });
  return outcome.events;
}

export class KitApi {
  private readonly kernable: Kernable;
  private readonly keys: SigningKeys;
  private readonly options: KitApiOptions;
  private readonly configs = new Map<string, Promise<oidc.Configuration>>();
  /** The request context of the API's own requests: lookups no one else may make. */
  readonly serviceContext: RequestContext;

  constructor(options: KitApiOptions) {
    this.kernable = options.kernable;
    this.keys = options.keys;
    this.options = options;
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
  async register(input: { email: string; name: string; password?: string }): Promise<{ ok: true; accountId: string } | { ok: false; reason: string }> {
    const accountId = randomUUID();
    const outcome = await this.kernable.submit(RegisterAccount.new(input), { aggregateId: accountId, tenant: PLATFORM_TENANT, context: {} });
    if (outcome.committed) return { ok: true, accountId };
    if (outcome.code !== "unique_taken") return { ok: false, reason: outcome.code };
    // The address is held: a registration that expired unverified is taken over (same aggregate);
    // otherwise the same answer as success, so nobody learns who has an account.
    const held = await this.kernable.query(FindAccountByEmail.new({ email: input.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
    if (held.status === "pending") {
      const again = await this.kernable.submit(RegisterAccount.new(input), { aggregateId: held.id, tenant: PLATFORM_TENANT, context: {} });
      if (again.committed) return { ok: true, accountId: held.id };
    } else {
      // The account's owner learns that someone tried; the caller learns nothing.
      await this.kernable.submit(NoteRegistrationAttempt.new(), { aggregateId: held.id, tenant: PLATFORM_TENANT, context: this.serviceContext });
    }
    return { ok: true, accountId: randomUUID() };
  }

  /** Confirms an email address with the token from the mail. */
  async verifyEmail(emailToken: string): Promise<boolean> {
    const parts = parseToken(emailToken);
    if (!parts) return false;
    const outcome = await this.kernable.submit(VerifyEmail.new({ token: emailToken }), { aggregateId: parts[0], tenant: PLATFORM_TENANT, context: {} });
    return outcome.committed;
  }

  /**
   * Signs in with email and password: an access token and a refresh token, or why not. An unknown
   * address and a locked account answer `wrong_password`, and an unknown address takes as long (a
   * password check against a dummy hash), so nobody learns whether an account exists.
   */
  async signIn(input: { email: string; password: string; ip?: string; userAgent?: string }): Promise<SignInResult> {
    let account: { id: string };
    try {
      account = await this.kernable.query(FindAccountByEmail.new({ email: input.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
    } catch {
      await checkPassword(null, input.password); // as slow as a wrong password
      return { ok: false, reason: "wrong_password" }; // never say whether the account exists
    }
    const sessionId = randomUUID();
    const refreshToken = token(sessionId, 1, randomSecret());
    const outcome = await this.kernable.submit(
      SignIn.new({ sessionId, password: input.password, refreshToken, ip: input.ip, userAgent: input.userAgent }),
      { aggregateId: account.id, tenant: PLATFORM_TENANT, context: {} },
    );
    const [event] = events(outcome);
    if (event.type !== "SignedIn") return { ok: false, reason: signInReason((event.data as SignInFailed).reason) };
    const { factors, mfa } = event.data as SignedIn;
    return {
      ok: true, accountId: account.id, sessionId, refreshToken, secondFactor: mfa,
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
    if (!/^[0-9]+$/.test(generation)) return { ok: false, reason: "invalid_token" };
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

  /** Proves the second factor (a TOTP or recovery code) for the session: a new access token with it. */
  async proveSecondFactor(claims: Claims, code: string): Promise<{ ok: true; accessToken: string } | { ok: false; reason: string }> {
    const outcome = await this.kernable.submit(ProveSecondFactor.new({ sessionId: claims.sid, code }), { aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: {} });
    if (!outcome.committed) return { ok: false, reason: outcome.code };
    const [event] = outcome.events;
    if (event.type !== "SecondFactorProved") return { ok: false, reason: "wrong_code" };
    const factor = (event.data as { method: string }).method === "recovery" ? "recovery" : "totp";
    return { ok: true, accessToken: issueAccessToken(this.keys, { sub: claims.sub, sid: claims.sid, fac: [...claims.fac, factor] }) };
  }

  /** Starts TOTP: a new secret, and the `otpauth:` URI an authenticator app reads (as a QR code). */
  async startTotp(claims: Claims, issuer: string, label: string): Promise<{ ok: true; secret: string; uri: string } | { ok: false; reason: string }> {
    const secret = totpSecret();
    const outcome = await this.kernable.submit(StartTotp.new({ secret }), { aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: this.own(claims) });
    if (!outcome.committed) return { ok: false, reason: outcome.code };
    const uri = `otpauth://totp/${encodeURIComponent(`${issuer}:${label}`)}?secret=${secret}&issuer=${encodeURIComponent(issuer)}`;
    return { ok: true, secret, uri };
  }

  /** Turns TOTP on with its first code: the recovery codes, shown this once. */
  async confirmTotp(claims: Claims, code: string): Promise<{ ok: true; recoveryCodes: string[] } | { ok: false; reason: string }> {
    const recoveryCodes = Array.from({ length: 10 }, () => randomSecret(8));
    const outcome = await this.kernable.submit(ConfirmTotp.new({ code, recoveryCodes }), { aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: this.own(claims) });
    return outcome.committed ? { ok: true, recoveryCodes } : { ok: false, reason: outcome.code };
  }

  /** A new API key for a service identity, shown this once. */
  async createApiKey(context: RequestContext, tenant: string, identityId: string, label = "", expiresAt: number | null = null): Promise<{ ok: true; key: string; keyId: string } | { ok: false; reason: string }> {
    const keyId = randomSecret(9).replace(/[^A-Za-z0-9]/g, "x");
    const secret = randomSecret();
    const outcome = await this.kernable.submit(CreateApiKey.new({ keyId, secret, label, expiresAt }), { aggregateId: identityId, tenant, context });
    return outcome.committed ? { ok: true, key: apiKey(keyId, secret), keyId } : { ok: false, reason: outcome.code };
  }

  private readonly keys_ = new Map<string, { context: RequestContext; until: number }>();

  /**
   * The request context of an API key, or null: checked against its hash, cached for a minute. A
   * revoked key is refused by the middleware at once, cache or not.
   */
  async checkApiKey(key: string): Promise<RequestContext | null> {
    const parsed = parseApiKey(key);
    if (!parsed) return null;
    const cached = this.keys_.get(parsed.hash);
    if (cached && cached.until > Date.now()) return cached.context;
    try {
      const found = await this.kernable.query(FindApiKey.new({ keyId: parsed.keyId }), { tenant: PLATFORM_TENANT, context: this.serviceContext, allPartitions: true });
      if (found.revoked || (found.expiresAt !== null && Date.now() > found.expiresAt) || !equalSecrets(found.hash, parsed.hash)) return null;
      const context = { senderTenant: found.tenant, senderIdentity: found.identityId, senderSession: parsed.keyId };
      this.keys_.set(parsed.hash, { context, until: Date.now() + 60_000 });
      return context;
    } catch {
      return null;
    }
  }

  /** The context of the token's account acting on itself. */
  private own(claims: Claims): RequestContext {
    return { senderAccount: claims.sub, senderSession: claims.sid };
  }

  /**
   * The token's account, read as itself: the middleware checks the session as for every signed-in
   * request (active, the account active, a second factor proved if it has one). For what the API
   * then does with its own context (accepting an invitation, joining through a link).
   */
  private async signedInAccount(claims: Claims): Promise<{ ok: true; account: AccountRow } | { ok: false; reason: string }> {
    try {
      return { ok: true, account: await this.kernable.query(GetAccount.new({ accountId: claims.sub }), { tenant: PLATFORM_TENANT, context: this.own(claims) }) };
    } catch (err) {
      if (err instanceof Rejected) return { ok: false, reason: err.code };
      throw err;
    }
  }

  /**
   * The token's account checked against a tenant it is about to join, where it holds no identity
   * yet: the middleware applies that tenant's checks to the account acting on itself (the tenant
   * active, its second-factor requirement, its enforced SSO). A cheap query of one's own sessions
   * in that tenant carries the check; its answer is not used.
   */
  private async mayJoin(claims: Claims, tenant: string): Promise<{ ok: true } | { ok: false; reason: string }> {
    try {
      await this.kernable.query(ListSessions.new({ accountId: claims.sub }), { tenant, context: this.own(claims) });
      return { ok: true };
    } catch (err) {
      if (err instanceof Rejected) return { ok: false, reason: err.code };
      throw err;
    }
  }

  // --- Passkeys: challenges live a few minutes in the kernable cache and count once. ---

  private async keepChallenge(challenge: string): Promise<void> {
    await this.kernable.cache.set("account", `challenge:${challenge}`, "1", { ttlMs: CHALLENGE_SECONDS * 1000, tenant: PLATFORM_TENANT, context: this.serviceContext });
  }

  /** The challenge a browser response answers, if it is one the API issued and nobody used yet. */
  private async takeChallenge(response: { response: { clientDataJSON: string } }): Promise<string | null> {
    try {
      const { challenge } = JSON.parse(Buffer.from(response.response.clientDataJSON, "base64url").toString("utf8")) as { challenge: string };
      const taken = await this.kernable.cache.delete("account", `challenge:${challenge}`, { tenant: PLATFORM_TENANT, context: this.serviceContext });
      return taken ? challenge : null;
    } catch {
      return null;
    }
  }

  /** What the browser needs to create a passkey for the signed-in account. */
  async passkeyRegistrationOptions(claims: Claims, userName: string): Promise<PublicKeyCredentialCreationOptionsJSON> {
    const options = await generateRegistrationOptions({
      rpName: PASSKEYS.rpName, rpID: PASSKEYS.rpId, userName, userID: new TextEncoder().encode(claims.sub),
      authenticatorSelection: { residentKey: "required", userVerification: "required" },
    });
    await this.keepChallenge(options.challenge);
    return options;
  }

  /** Adds the passkey the browser created. */
  async addPasskey(claims: Claims, response: RegistrationResponseJSON, label?: string): Promise<{ ok: boolean; reason?: string }> {
    const challenge = await this.takeChallenge(response);
    if (!challenge) return { ok: false, reason: "invalid_challenge" };
    const outcome = await this.kernable.submit(AddPasskey.new({ challenge, response, label }), { aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: this.own(claims) });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** What the browser needs to sign in with a passkey (or prove one): any of the user's passkeys. */
  async passkeySignInOptions(): Promise<PublicKeyCredentialRequestOptionsJSON> {
    const options = await generateAuthenticationOptions({ rpID: PASSKEYS.rpId, userVerification: "required" });
    await this.keepChallenge(options.challenge);
    return options;
  }

  /** Signs in with a passkey, without a password: the passkey names its account (its user handle). */
  async signInWithPasskey(response: AuthenticationResponseJSON, ip?: string): Promise<SignInResult> {
    const challenge = await this.takeChallenge(response);
    const accountId = response.response.userHandle ? Buffer.from(response.response.userHandle, "base64url").toString("utf8") : "";
    if (!challenge || !accountId) return { ok: false, reason: "wrong_password" };
    const sessionId = randomUUID();
    const refreshToken = token(sessionId, 1, randomSecret());
    const outcome = await this.kernable.submit(SignInWithPasskey.new({ sessionId, challenge, response, refreshToken, ip }), {
      aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext,
    });
    if (!outcome.committed) return { ok: false, reason: "wrong_password" };
    const signedIn = outcome.events.find((e) => e.type === "SignedIn");
    if (!signedIn) return { ok: false, reason: signInReason((outcome.events[0].data as SignInFailed).reason) };
    return {
      ok: true, accountId, sessionId, refreshToken, secondFactor: false,
      accessToken: issueAccessToken(this.keys, { sub: accountId, sid: sessionId, fac: (signedIn.data as SignedIn).factors }),
    };
  }

  /** Proves a passkey as the second factor of a password session. */
  async provePasskey(claims: Claims, response: AuthenticationResponseJSON): Promise<{ ok: true; accessToken: string } | { ok: false; reason: string }> {
    const challenge = await this.takeChallenge(response);
    if (!challenge) return { ok: false, reason: "invalid_challenge" };
    const outcome = await this.kernable.submit(ProvePasskey.new({ sessionId: claims.sid, challenge, response }), {
      aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: this.serviceContext,
    });
    if (!outcome.committed || !outcome.events.some((e) => e.type === "SecondFactorProved")) return { ok: false, reason: "wrong_passkey" };
    return { ok: true, accessToken: issueAccessToken(this.keys, { sub: claims.sub, sid: claims.sid, fac: [...claims.fac, "passkey"] }) };
  }

  // --- OAuth/OIDC and SSO: the API does the exchange; the account decides. ---

  private config(issuer: string, clientId: string, clientSecret: string): Promise<oidc.Configuration> {
    const key = `${issuer}|${clientId}`;
    let config = this.configs.get(key);
    if (!config) {
      const insecure = this.options.insecureIssuers ? { execute: [oidc.allowInsecureRequests] } : undefined;
      config = oidc.discovery(new URL(issuer), clientId, clientSecret, undefined, insecure);
      config.catch(() => this.configs.delete(key));
      this.configs.set(key, config);
    }
    return config;
  }

  /** The provider's settings: a social one by name, or the SSO of the tenant that verified the email's domain. */
  private async provider(by: { provider: string } | { email: string }): Promise<{ provider: string; issuer: string; clientId: string; clientSecret: string; oauth2?: OAuth2; tenant?: string; ssoId?: string } | null> {
    if ("provider" in by) {
      const p = this.options.providers?.[by.provider];
      return p ? { provider: by.provider, ...p } : null;
    }
    try {
      const found = await this.kernable.query(FindSsoByDomain.new({ domain: emailDomain(by.email) }), { tenant: PLATFORM_TENANT, context: this.serviceContext, allPartitions: true });
      const settings = await this.ssoSettings(found.tenant, found.ssoId);
      return settings ? { ...settings, tenant: found.tenant, ssoId: found.ssoId } : null;
    } catch {
      return null;
    }
  }

  /**
   * Where to send the browser: a social provider by name, or a person's tenant SSO by their email.
   * With `link`, the signed-in account links the login instead of signing in.
   */
  async oidcStart(by: { provider: string } | { email: string }, link?: Claims): Promise<{ ok: true; url: string } | { ok: false; reason: string }> {
    const p = await this.provider(by);
    if (!p || !this.options.redirectUri) return { ok: false, reason: "no_provider" };
    const verifier = oidc.randomPKCECodeVerifier();
    const state = oidc.randomState();
    const nonce = oidc.randomNonce();
    const kept: OidcState = { provider: p.provider, tenant: p.tenant, ssoId: p.ssoId, verifier, nonce, ...(link ? { link: { accountId: link.sub } } : {}) };
    await this.kernable.cache.set("account", `oidc:${state}`, JSON.stringify(kept), { ttlMs: CHALLENGE_SECONDS * 1000, tenant: PLATFORM_TENANT, context: this.serviceContext });
    if (p.oauth2) {
      // Plain OAuth2: no discovery, no nonce (nothing brings it back).
      if (!this.secure(p.oauth2.authorize)) return { ok: false, reason: "no_provider" };
      const url = new URL(p.oauth2.authorize);
      for (const [k, v] of Object.entries({
        response_type: "code", client_id: p.clientId, redirect_uri: this.options.redirectUri, scope: p.oauth2.scope, state,
        code_challenge_method: "S256", code_challenge: await oidc.calculatePKCECodeChallenge(verifier),
      })) url.searchParams.set(k, v);
      return { ok: true, url: url.href };
    }
    const config = await this.config(p.issuer, p.clientId, p.clientSecret);
    const url = oidc.buildAuthorizationUrl(config, {
      redirect_uri: this.options.redirectUri, scope: "openid email", state, nonce,
      code_challenge: await oidc.calculatePKCECodeChallenge(verifier), code_challenge_method: "S256",
    });
    return { ok: true, url: url.href };
  }

  /**
   * The browser came back (`callback` is the full URL). Signs in a linked login; links a tenant SSO
   * login to the account with the same verified email; registers a new person whose email nobody
   * has; refuses a social login whose email has an account (`not_linked`: sign in and link it). A
   * tenant's SSO answers only for its verified domains: an unlinked login with another email is
   * refused (`email_not_in_domain`), so a tenant's provider cannot take over anyone else's account.
   */
  async oidcFinish(callback: string, ip?: string): Promise<SignInResult | { ok: true; linked: true }> {
    const state = new URL(callback).searchParams.get("state") ?? "";
    const raw = await this.kernable.cache.get("account", `oidc:${state}`, { tenant: PLATFORM_TENANT, context: this.serviceContext });
    if (!raw || !(await this.kernable.cache.delete("account", `oidc:${state}`, { tenant: PLATFORM_TENANT, context: this.serviceContext }))) {
      return { ok: false, reason: "invalid_state" };
    }
    const kept = JSON.parse(Buffer.from(raw).toString("utf8")) as OidcState;
    const p: { provider: string; issuer: string; clientId: string; clientSecret: string; oauth2?: OAuth2; domains?: string[] } | null = kept.ssoId
      ? await this.ssoSettings(kept.tenant!, kept.ssoId)
      : this.options.providers?.[kept.provider] ? { provider: kept.provider, ...this.options.providers[kept.provider] } : null;
    if (!p) return { ok: false, reason: "no_provider" };
    let claims: { sub: string; email?: string; email_verified?: boolean };
    try {
      if (p.oauth2) {
        claims = await this.exchangeOAuth2(p, p.oauth2, callback, kept);
      } else {
        const tokens = await oidc.authorizationCodeGrant(await this.config(p.issuer, p.clientId, p.clientSecret), new URL(callback), {
          pkceCodeVerifier: kept.verifier, expectedState: state, expectedNonce: kept.nonce,
        });
        claims = tokens.claims() as typeof claims;
      }
    } catch {
      return { ok: false, reason: "provider_refused" };
    }
    const email = claims.email ?? "";
    const emailVerified = claims.email_verified === true;
    if (kept.link) {
      const linked = await this.kernable.submit(LinkProvider.new({ provider: kept.provider, subject: claims.sub, email }), {
        aggregateId: kept.link.accountId, tenant: PLATFORM_TENANT, context: this.serviceContext,
      });
      return linked.committed ? { ok: true, linked: true } : { ok: false, reason: linked.code };
    }
    const find = async (q: FindAccountByLink | FindAccountByEmail) => {
      try {
        return (await this.kernable.query(q as FindAccountByLink, { tenant: PLATFORM_TENANT, context: this.serviceContext })).id;
      } catch {
        return null;
      }
    };
    const sso = kept.provider.startsWith("sso:");
    let accountId = await find(FindAccountByLink.new({ provider: kept.provider, subject: claims.sub }));
    // Not linked yet: a tenant's SSO speaks only for the domains its tenant verified.
    if (!accountId && sso && !(p.domains ?? []).includes(emailDomain(email))) return { ok: false, reason: "email_not_in_domain" };
    if (!accountId && email) accountId = await find(FindAccountByEmail.new({ email }));
    if (!accountId) {
      // A new person: registered (verified if the provider says so), the login linked.
      if (!email) return { ok: false, reason: "no_email" };
      // A social login registers only where anyone may: with sign-up by invitation, the person
      // accepts an invitation first and links the login after. A tenant's SSO speaks for its own
      // verified domains, so it registers either way.
      if (!sso) {
        const policy = await this.kernable
          .query(GetSignUpPolicy.new(), { tenant: PLATFORM_TENANT, context: this.serviceContext })
          .catch(() => null);
        if (policy?.accounts !== "open") return { ok: false, reason: "sign_up_closed" };
      }
      accountId = randomUUID();
      const registered = await this.kernable.submit(RegisterAccount.new({ email, name: email.split("@")[0], verified: emailVerified }), {
        aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext,
      });
      if (!registered.committed) return { ok: false, reason: registered.code };
      if (!sso) {
        await this.kernable.submit(LinkProvider.new({ provider: kept.provider, subject: claims.sub, email }), { aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext });
      }
    }
    const sessionId = randomUUID();
    const refreshToken = token(sessionId, 1, randomSecret());
    const outcome = await this.kernable.submit(
      SignInWithProvider.new({ sessionId, provider: kept.provider, subject: claims.sub, email, emailVerified, autoLink: sso, domains: p.domains, refreshToken, ip }),
      { aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext },
    );
    if (!outcome.committed) return { ok: false, reason: outcome.code };
    const signedIn = outcome.events.find((e) => e.type === "SignedIn");
    if (!signedIn) return { ok: false, reason: signInReason((outcome.events[0].data as SignInFailed).reason) };
    const d = signedIn.data as SignedIn;
    return { ok: true, accountId, sessionId, refreshToken, secondFactor: d.mfa, accessToken: issueAccessToken(this.keys, { sub: accountId, sid: sessionId, fac: d.factors }) };
  }

  /** Whether `url` may be used: https, or anything with `insecureIssuers` (tests). */
  private secure(url: string): boolean {
    return this.options.insecureIssuers === true || url.startsWith("https://");
  }

  /**
   * For an OAuth2 provider: the person behind the code in `callback`, as the claims an ID token
   * would hold (`sub`: their `id`; `email`, `email_verified`: their primary verified address).
   */
  private async exchangeOAuth2(p: { clientId: string; clientSecret: string }, o: OAuth2, callback: string, kept: OidcState): Promise<{ sub: string; email?: string; email_verified?: boolean }> {
    for (const url of [o.token, o.user, o.emails]) if (!this.secure(url)) throw new Error(`${url} is not https`);
    const code = new URL(callback).searchParams.get("code");
    if (!code) throw new Error("no code");
    const res = await fetch(o.token, {
      method: "POST",
      headers: { accept: "application/json", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code", code, client_id: p.clientId, client_secret: p.clientSecret,
        redirect_uri: this.options.redirectUri ?? "", code_verifier: kept.verifier,
      }),
    });
    const tokens = (await res.json()) as { access_token?: string; error?: string };
    if (!tokens.access_token) throw new Error(`no access_token: ${tokens.error ?? res.status}`);
    const get = async (url: string) => (await fetch(url, { headers: { authorization: `Bearer ${tokens.access_token}`, accept: "application/json", "user-agent": "kernable-kit" } })).json();
    const user = (await get(o.user)) as { id?: number | string };
    if (user.id === undefined || user.id === "") throw new Error("no id");
    const email = oauth2Email(await get(o.emails));
    return { sub: String(user.id), email: email ?? "", email_verified: email !== null };
  }

  /** A tenant's SSO: what the API signs in with, and the domains it verified. */
  private async ssoSettings(tenant: string, ssoId: string) {
    try {
      const s = await this.kernable.query(GetSsoSecret.new({ ssoId }), { tenant, context: this.serviceContext });
      return s.clientSecret ? { provider: `sso:${ssoId}`, issuer: s.issuer, clientId: s.clientId, clientSecret: s.clientSecret, domains: s.domains } : null;
    } catch {
      return null;
    }
  }

  /** Looks for the SSO domain's TXT record (`_kernable-sso.<domain>`) and verifies the domain if it is there. */
  async verifySsoDomain(tenant: string, ssoId: string, domain: string): Promise<{ ok: boolean; reason?: string }> {
    let found: string[] = [];
    try {
      found = (await (this.options.resolveTxt ?? resolveTxt)(`_kernable-sso.${domain}`)).map((parts) => parts.join(""));
    } catch {
      // no record
    }
    const outcome = await this.kernable.submit(VerifySsoDomain.new({ domain, found }), { aggregateId: ssoId, tenant, context: this.serviceContext });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /** Changes the password; the other sessions end, this one stays. */
  async changePassword(claims: Claims, current: string | undefined, next: string): Promise<{ ok: boolean; reason?: string }> {
    const outcome = await this.kernable.submit(ChangePassword.new({ current, new: next, keepSessionId: claims.sid }), {
      aggregateId: claims.sub, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    });
    return outcome.committed ? { ok: true } : { ok: false, reason: outcome.code };
  }

  /**
   * Sends a reset mail if the address has an active account; answers the same either way. At most
   * one mail per account in `MAIL_COOLDOWN_MINUTES` (the command sends nothing within it).
   */
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
   * Accepts an invitation: as the signed-in account (`claims`, its session checked as for every
   * signed-in call, and against the invitation's tenant: active, its second factor, its enforced
   * SSO, except for an owner invitation: owners are exempt from it, so they can mend it), or as a
   * new person with a name and password, registered in the same step (active: the token proved
   * the email). The accepting account's email must be the invited one. A new person's name and
   * password are checked before the invitation is used, so a refused registration does not spend
   * it. Either way a tenant that is not active takes nobody in: `tenant_suspended` or
   * `tenant_not_active` (pending) from the middleware's check of a signed-in account, else
   * `tenant_not_active` (the command's requirement cannot tell suspended from pending).
   */
  async acceptInvitation(
    invitationToken: string,
    as: { claims: Claims } | { name: string; password: string },
  ): Promise<{ ok: true; accountId: string; tenant: string } | { ok: false; reason: string }> {
    const peek = await this.peekInvitation(invitationToken);
    if (!peek || !peek.email) return { ok: false, reason: "invalid_token" };
    let accountId: string;
    let email: string;
    if ("claims" in as) {
      const signedIn = await this.signedInAccount(as.claims);
      if (!signedIn.ok) return signedIn;
      const may = await this.mayJoin(as.claims, peek.tenant);
      // The middleware checks the tenant (exists, active), then its second factor, then its
      // enforced SSO: `sso_required` means every other check passed, and an owner is exempt from it.
      if (!may.ok && !(peek.owner && may.reason === "sso_required")) return may;
      accountId = as.claims.sub;
      email = signedIn.account.email ?? "";
    } else {
      if (typeof as.name !== "string" || !trimSpace(as.name) || chars(as.name) > 200) return { ok: false, reason: "invalid_name" };
      if (typeof as.password !== "string" || !validPassword(as.password)) return { ok: false, reason: "weak_password" };
      // Someone with an account signs in first, so nobody gets an account they could not use.
      try {
        await this.kernable.query(FindAccountByEmail.new({ email: peek.email }), { tenant: PLATFORM_TENANT, context: this.serviceContext });
        return { ok: false, reason: "sign_in_first" };
      } catch {
        accountId = randomUUID();
        email = peek.email;
      }
    }
    const accepted = await this.kernable.submit(AcceptInvitation.new({ token: invitationToken, accountId, email, newAccount: !("claims" in as) }), {
      aggregateId: peek.invitationId, tenant: peek.tenant, context: this.serviceContext,
    });
    // The command requires the tenant active (its fact): missing, the tenant is pending, suspended or gone.
    if (!accepted.committed) return { ok: false, reason: accepted.code === "requirement_missing" ? "tenant_not_active" : accepted.code };
    if (!("claims" in as)) {
      const registered = await this.kernable.submit(RegisterAccount.new({ email, name: as.name, password: as.password, verified: true }), {
        aggregateId: accountId, tenant: PLATFORM_TENANT, context: this.serviceContext,
      });
      if (!registered.committed) return { ok: false, reason: registered.code };
    }
    return { ok: true, accountId, tenant: peek.tenant };
  }

  /**
   * A reusable invitation link into `groups` of the tenant, as `context` (whose groups the
   * middleware checks): the token, shown this once, for a URL such as `/join?token=…`.
   */
  async createInvitationLink(
    context: RequestContext,
    tenant: string,
    link: { groups: InvitedGroup[]; label?: string; expiresAt?: number | null; maxUses?: number | null },
  ): Promise<{ ok: true; token: string; linkId: string } | { ok: false; reason: string }> {
    const linkId = randomUUID();
    const secret = randomSecret();
    const outcome = await this.kernable.submit(CreateInvitationLink.new({ secret, ...link }), { aggregateId: linkId, tenant, context });
    return outcome.committed ? { ok: true, token: linkToken(tenant, linkId, secret), linkId } : { ok: false, reason: outcome.code };
  }

  /** What the join page shows for a link token: the tenant, the link's label, whether it can be used. */
  async peekInvitationLink(link: string) {
    const t = parseLinkToken(link);
    if (!t) return null;
    try {
      return await this.kernable.query(PeekInvitationLink.new({ token: link }), { tenant: t.tenant, context: this.serviceContext });
    } catch {
      return null;
    }
  }

  /**
   * The signed-in account joins the link's tenant with its groups (its session checked as for
   * every signed-in call, and against the link's tenant: active, its second factor, its enforced
   * SSO). Someone without an account registers and verifies first: a link proves
   * no email.
   */
  async joinWithInvitationLink(claims: Claims, link: string): Promise<{ ok: true; tenant: string } | { ok: false; reason: string }> {
    const t = parseLinkToken(link);
    if (!t) return { ok: false, reason: "invalid_token" };
    const signedIn = await this.signedInAccount(claims);
    if (!signedIn.ok) return signedIn;
    const may = await this.mayJoin(claims, t.tenant);
    if (!may.ok) return may;
    const outcome = await this.kernable.submit(JoinWithInvitationLink.new({ token: link, accountId: claims.sub }), {
      aggregateId: t.linkId, tenant: t.tenant, context: this.serviceContext,
    });
    if (outcome.committed) return { ok: true, tenant: t.tenant };
    return { ok: false, reason: outcome.code === "requirement_missing" ? "tenant_not_active" : outcome.code };
  }

  /** Signs out: ends the session. */
  async signOut(claims: Claims): Promise<void> {
    events(await this.kernable.submit(EndSession.new({ reason: "signed_out" }), {
      aggregateId: claims.sid, tenant: PLATFORM_TENANT, context: { senderAccount: claims.sub, senderSession: claims.sid },
    }));
  }
}
