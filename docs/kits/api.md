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

## Methods

| Task | Method | Returns |
|---|---|---|
| **Registration** | `register({ email, name, password? })` | `{ accountId }`, the same whether or not the address was taken |
| | `verifyEmail(token)` | whether it worked |
| **Password sign-in** | `signIn({ email, password, ip?, userAgent? })` | access and refresh token, session ID, and `secondFactor: true` if the account must prove one now; or a reason |
| | `refresh(refreshToken, ip?)` | a new access token and the next refresh token |
| | `signOut(claims)` | ends the session |
| **Second factor** | `proveSecondFactor(claims, code)` | a new access token with the factor, for a TOTP or recovery code |
| | `startTotp(claims, issuer, label)` | the secret and the `otpauth:` URI to show as a QR code |
| | `confirmTotp(claims, code)` | the recovery codes, shown this once |
| **Passkeys** | `passkeyRegistrationOptions(claims, userName)`, then `addPasskey(claims, response, label?)` | adds a passkey the browser created |
| | `passkeySignInOptions()`, then `signInWithPasskey(response, ip?)` | signs in without a password |
| | `provePasskey(claims, response)` | a passkey as the second factor of a password session |
| **OAuth/OIDC** | `oidcStart({ provider } \| { email }, link?)` | the provider's URL to redirect to; `{ email }` finds the tenant SSO of the email's domain; with `link`, links the login to the signed-in account |
| | `oidcFinish(callbackUrl, ip?)` | signs in, registers or links (see [Accounts](./account#other-ways-to-sign-in)) |
| | `verifySsoDomain(tenant, ssoId, domain)` | looks up the `_kernable-sso.<domain>` TXT record and verifies the domain |
| **Account changes** | `changePassword(claims, current, next)` | ends every other session |
| | `requestPasswordReset(email)`, `resetPassword(token, next)` | the reset by mail; the request answers the same for unknown addresses |
| | `requestEmailChange(claims, email)`, `confirmEmailChange(token)` | the change, confirmed from the new address |
| **Invitations** | `peekInvitation(token)` | tenant name, email, whether it expired |
| | `acceptInvitation(token, { claims } \| { name, password })` | as the signed-in account, or as a new person ([Invitations](./invitation#the-flow)) |
| **Services** | `createApiKey(context, tenant, identityId, label?, expiresAt?)` | the key, shown this once |
| | `checkApiKey(key)` | the request context of the key, or null |

## Security notes

- **Nothing reveals whether an account exists.** Sign-in answers `wrong_password` for unknown
  addresses, and registration and password reset answer the same either way.
- **Tokens are short and narrow.** Access tokens last 15 minutes and are checked without the
  kernel; the middleware still refuses an ended session at once. Refresh tokens rotate, and a
  reused one ends its session.
- **Passkey challenges and OIDC state** live a few minutes in the kernable cache, so any instance
  of your API can finish what another started. That is why the API's key needs `cache` on
  `account`.
- **Secrets never stay in the log.** Passwords, tokens and codes travel as secret command fields
  and leave the vault once the command completes; hashes and personal data stay in the vault and
  are erased with the person.
