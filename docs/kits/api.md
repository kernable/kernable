# Your API: KitApi

Your HTTP API is the one process that sees passwords, tokens and answers from identity providers.
`KitApi` is the part of the kit that runs there: it turns "email and password" into signed
commands, issues and checks tokens, runs passkey and OIDC exchanges, and turns a token or API key
into the request context your own commands carry.

It is framework-neutral: plain methods you call from Express, FastAPI, axum or anything else.
`kit/example/server.ts` (`server.py`, `examples/server.rs`) wires it into a small server on the
standard library; copy its routes into your framework.

## Setting it up

```ts
import { KitApi } from "./kit/src/api/api.ts";
import { signingKeysFrom } from "./kit/src/api/tokens.ts";

const api = new KitApi({
  kernable,                                          // a client with the API's key
  keys: signingKeysFrom("token-signing.pem"),        // EdDSA key that signs access tokens
  service: { identityId: "api", keyId },             // from bootstrap(): the API's own identity
  providers: {                                       // optional: social logins
    google: { issuer: "https://accounts.google.com", clientId, clientSecret },
  },
  redirectUri: "https://example.com/oidc/callback",  // where providers send the browser back
});
```

In Python the same is `KitApi(kernable, signing_keys_from(...), service)`; it blocks like the
SDK's client, and `AsyncKitApi(KitApi(...))` offers the same methods for asyncio. In Rust,
`KitApi::new(...)` is async. Every method has the same name in snake_case.

The API's own identity holds a few permissions the kit needs before anyone is signed in, such as
finding an account by email or a tenant by slug. `bootstrap()` creates it with the `api` group;
see `API_PERMISSIONS` in `bootstrap`.

## Per request

```ts
const claims = api.check(accessToken);                    // the token's claims, or null; no kernel call
const context = await api.contextFor(claims, tenantId);   // senderAccount, senderSession, senderTenant, senderIdentity
await kernable.submit(PlaceOrder.new({ … }), { aggregateId, tenant: tenantId, context });
```

- `check` verifies the access token's signature and expiry locally.
- `contextFor` adds the person's identity in the tenant; without a tenant, the context is only
  the signed-in account (enough for `authenticated` requests such as listing one's tenants).
- For a service, `checkApiKey(key)` returns the context directly (the key's identity and tenant).

The context goes with the command; the [middleware](./authorization) decides on it. Your API
does not decide who may do what.

**Account requests go to the platform tenant.** Every account and session lives there, so send
`account` commands and queries (`GetAccount`, `ListSessions`, `EndSession`, `SuspendAccount` …)
with `tenant: PLATFORM_TENANT`, whatever tenant the caller works in. Sent to another tenant, an
account acting on itself meets that tenant's checks too (suspended, its second factor, its
enforced SSO), which is what the API uses when someone joins a tenant, not what a settings page
wants. The example servers route every `account` request to the platform tenant.

## Methods

| Task | Method | Returns |
|---|---|---|
| **Registration** | `register({ email, name, password? })` | `{ ok: true, accountId }`, the same whether or not the address was taken; `{ ok: false, reason }` for an invalid email, name or password |
| | `verifyEmail(token)` | whether it worked |
| **Password sign-in** | `signIn({ email, password, ip?, userAgent? })` | access and refresh token, session ID, and `secondFactor: true` if the account must prove one now; or a reason (`wrong_password` also for an unknown address and a locked account) |
| | `refresh(refreshToken, ip?)` | a new access token and the next refresh token; an older token of the session ends it (`reuse`), any other is refused (`invalid_token`) |
| | `signOut(claims)` | ends the session |
| **Second factor** | `proveSecondFactor(claims, code)` | a new access token with the factor, for a TOTP or recovery code |
| | `startTotp(claims, issuer, label)` | the secret and the `otpauth:` URI to show as a QR code |
| | `confirmTotp(claims, code)` | the recovery codes, shown this once |
| **Passkeys** | `passkeyRegistrationOptions(claims, userName)`, then `addPasskey(claims, response, label?)` | adds a passkey the browser created |
| | `passkeySignInOptions()`, then `signInWithPasskey(response, ip?)` | signs in without a password |
| | `provePasskey(claims, response)` | a passkey as the second factor of a password session |
| **OAuth/OIDC** | `oidcStart({ provider } \| { email }, link?)` | the provider's URL to redirect to; `{ email }` finds the tenant SSO of the email's domain; with `link`, links the login to the signed-in account |
| | `oidcFinish(callbackUrl, ip?)` | signs in, registers or links (see [Accounts](./account#other-ways-to-sign-in)); a tenant SSO's login outside its verified domains is refused (`email_not_in_domain`) |
| | `verifySsoDomain(tenant, ssoId, domain)` | looks up the `_kernable-sso.<domain>` TXT record and verifies the domain |
| **Account changes** | `changePassword(claims, current, next)` | ends every other session |
| | `requestPasswordReset(email)`, `resetPassword(token, next)` | the reset by mail; the request answers the same for unknown addresses, and mails at most once per `MAIL_COOLDOWN_MINUTES` |
| | `requestEmailChange(claims, email)`, `confirmEmailChange(token)` | the change, confirmed from the new address |
| **Invitations** | `peekInvitation(token)` | the tenant (its ID), the invitation ID, the email, whether it makes an owner, whether it expired; nothing for a wrong secret |
| | `acceptInvitation(token, { claims } \| { name, password })` | as the signed-in account (its session checked, also against the invitation's tenant: active, its second factor, its enforced SSO unless the invitation makes an owner), or as a new person; a tenant that is not active takes nobody in (`tenant_suspended`) ([Invitations](./invitation#the-flow)) |
| **Invitation links** | `createInvitationLink(context, tenant, { groups, label?, expiresAt?, maxUses? })` | the link's token, shown this once |
| | `peekInvitationLink(token)` | tenant, label, whether it can be used; nothing for a wrong token |
| | `joinWithInvitationLink(claims, token)` | the signed-in account (its session checked, also against the link's tenant) joins with the link's groups ([Invitation links](./invitation#invitation-links)) |
| **Services** | `createApiKey(context, tenant, identityId, label?, expiresAt?)` | the key, shown this once |
| | `checkApiKey(key)` | the request context of the key, or null |

## Security notes

- **Nothing reveals whether an account exists.** Sign-in answers `wrong_password` for unknown
  addresses and locked accounts (the `SignInFailed` event keeps `locked`), and checks an unknown
  address's password against a dummy hash so it takes as long; registration and password reset
  answer the same either way.
- **Tokens are short and narrow.** Access tokens last 15 minutes and are checked without the
  kernel; the middleware still refuses an ended session at once. Refresh tokens rotate, and a
  reused one ends its session; a made-up one does not. The calls the API makes in its own name for
  a signed-in person (accepting an invitation, joining through a link) check that person's session
  first, as the middleware does for every other signed-in request.
- **Passkey challenges and OIDC state** live a few minutes in the kernable cache, so any instance
  of your API can finish what another started. That is why the API's key needs `cache` on
  `account`.
- **Secrets never stay in the log.** Passwords, tokens and codes travel as secret command fields
  and leave the vault once the command completes; hashes and personal data stay in the vault and
  are erased with the person.

## Rate limits are yours

`KitApi` does not count requests per caller: it cannot see your load balancer, your proxies or
which addresses you trust. What the kit limits, it limits per account:

- **Lockout.** After `LOCKOUT_AFTER` (5) wrong passwords in a row, the account is locked for 1, 2,
  4 … minutes, at most `LOCKOUT_MAX_MINUTES` (15). Anyone who knows an address can therefore lock
  its account for up to `LOCKOUT_MAX_MINUTES` at a time, again and again.
- **Mails.** A password reset and the "you already have an account" mail go out at most once per
  account in `MAIL_COOLDOWN_MINUTES` (5); requests within it send nothing and answer the same.

Everything per IP address or per client is your API's job, in front of `KitApi`: throttle
`signIn`, `register`, `requestPasswordReset`, `verifyEmail`, `refresh`, `proveSecondFactor`,
`peekInvitation` and `oidcStart` per IP (a reverse proxy's rate limit, or middleware in your
framework), so that one client cannot lock many accounts, mail many addresses or guess tokens.
The example servers bind to loopback and leave this to the proxy in front of them.
