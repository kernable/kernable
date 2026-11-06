# Kits: the event catalogue

Status: decided, not built. The kits are the open set of tenancy, accounts, identities, groups, invitations and authorization that `kernable kits add` copies into a customer's project. Section by section, each decided before it was built. This catalogue is the contract the TypeScript, Python and Rust sets share: the shared fixture suite is generated from it.

## Conventions

- **Domains**: `tenancy`, `account`, `identity`, `group`, `invitation`. Types are written qualified here (`tenancy.TenantCreated`).
- **Names**: commands imperative (`SuspendTenant`), events past tense (`TenantSuspended`), queries what is asked (`GetTenant`).
- **Payloads**: JSON, camelCase. An event carries what changed, not the whole state. Every event is schema version 1 while kernable is unreleased.
- **Who and when**: not in the payload. The request context stored with every event says who (`senderIdentity`, `senderTenant`, `onBehalfOf`), the command's `received_at` says when (`now()` in the handler).
- **IDs**: random UUIDs, except where an aggregate's ID is another's (a tenant's ID is its `tenant_id`).
- **Personal and secret fields** are marked; they go to the vault (`personal`, subject named) or are `secret`.
- **Levels**: `public`, `authenticated`, `platform` (platform groups only), or a group permission (`tenancy.RenameTenant`); `owners` hold every permission in their tenant.
- **Constants** a customer may change in the copy are named in capitals.

## tenancy

One aggregate, `tenancy.tenant`. Its ID is the tenant ID and it lives under that `tenant_id`.

### State

| Field | Type | Notes |
|---|---|---|
| `status` | `none` · `pending` · `active` · `suspended` · `deleted` | `none` before the first event |
| `slug` | string | lowercase, a DNS label |
| `name` | string | display name |
| `owners` | identity IDs | at least one once the first owner joined; the only place owners are kept: `owners` is a virtual group (listed with the groups, holds every permission, cannot be deleted) |
| `suspension` | `{ reason, at }` or null | `reason`: `owner_requested` · `unpaid` · `abuse` · `by_platform` · `rejected` |
| `mfaRequired` | bool | the middleware refuses sessions without a second factor |
| `signUp` | `{ tenants, accounts }` | platform tenant only: `tenants`: `off` · `pending` · `active` (the status a self-service tenant starts with); `accounts`: `off` · `open` |

### Lifecycle

```
none → pending → active ⇄ suspended → deleted
         └──────────────────↗ (rejected)
```

### Commands and events

| Command | Allowed when | Event | Payload | Level |
|---|---|---|---|---|
| `CreateTenant` | `none`; slug free and not reserved | `TenantCreated` | `slug`, `name`, `status` (`active`, or `pending` by self-service), `owner`: `{ email }` (created by the platform) or `{ accountId }` (self-service: the sender's account) | `platform`; `authenticated` when `signUp.tenants` ≠ `off` |
| `ActivateTenant` | `pending` | `TenantActivated` | — | `platform` |
| `RenameTenant` | `pending`, `active` | `TenantRenamed` | `name` | group |
| `ChangeTenantSlug` | `active`; slug free and not reserved | `TenantSlugChanged` | `slug` | group |
| `SuspendTenant` | `pending`, `active` | `TenantSuspended` | `reason` | `platform`; owners with `owner_requested` |
| `ReactivateTenant` | `suspended` | `TenantReactivated` | — | `platform`; owners if `owner_requested` |
| `DeleteTenant` | `suspended` for at least `DELETE_AFTER_DAYS` (default 0) | `TenantDeleted` | — | `platform` |
| `AddOwner` | not `deleted`; identity active in this tenant (required fact) | `OwnerAdded` | `identityId` | owners |
| `RemoveOwner` | not the last owner | `OwnerRemoved` | `identityId` | owners |
| `RequireMfa` | `active` | `MfaRequirementChanged` | `required` | owners |
| `ChangeSignUpPolicy` | platform tenant | `SignUpPolicyChanged` | `tenants`, `accounts` | `platform` |

The platform tenant (`PLATFORM_TENANT`, default `platform`) cannot be suspended or deleted.

### Unique keys and facts

- `slug`: global, the slug while not `deleted`, else null (so deletion frees it). Validated `^[a-z0-9](-?[a-z0-9])*$`, 3–63 characters, not in `RESERVED_SLUGS` (`platform`, `www`, `api`, `admin`, `app`, `mail`, …).
- `owner`: tenant-scoped, one value per owner's identity ID. With the fact `exists`, it lets `identity.DeleteIdentity` require atomically that no tenant holds the identity as owner (an absent requirement by key).
- Fact `active`: status is `active`. Any command of the tenant may require it.
- Fact `exists`: status is neither `none` nor `deleted`.

### Vault

No personal fields: a tenant is an organisation. No free-text notes on suspension (they invite personal data); the reason is an enum and the sender is in the request context.

### Read model `tenancy.tenants`

| Query | Answers | Level |
|---|---|---|
| `GetTenant` | `{ id, slug, name, status, suspension, mfaRequired, owners }`; target `data.tenantId` | members of that tenant, `platform` |
| `GetTenantBySlug` | `{ id, name, status }` | the API's service identity only: subdomains and login pages resolve through the API, nothing open lists tenants |
| `ListTenants` | page of tenants, filter by status | `platform` |
| `GetSignUpPolicy` | `{ tenants, accounts }` | the API's service identity |

On `TenantDeleted` the read model drops the tenant.

### Reactions (in other domains)

- `TenantCreated` with an `accountId` owner (self-service): `identity` creates the owner's identity, `tenancy.AddOwner` follows. With an `email` owner (created by the platform): `invitation` invites them as owner; accepting adds them with `AddOwner`.
- `TenantSuspended`: nothing to submit; the middleware refuses the tenant's requests (except `platform`, and owners' `ReactivateTenant` after `owner_requested`).
- `TenantDeleted`: `identity` erases the tenant's identities (vault); `invitation` revokes open invitations; every read model of the set drops the tenant's rows.

### Bootstrap

A script in the set, run once with a privileged key (`bypassAuthorization`), creates the platform tenant, the first account and its identity as owner.

### Decided

Owners only on the Tenant, `owners` a virtual group; `SuspendTenant` also from `pending` (reason `rejected`); bootstrap by script; a tenant the platform creates gets its first owner by an owner invitation (2026-11-06).

## account

Three aggregates:

| Aggregate | ID | Lives under |
|---|---|---|
| `account.account` | random | the platform tenant |
| `account.session` | random, chosen by the API (`sessionId`) | the platform tenant |
| `account.sso` | random | the tenant it signs in to |

Emailed tokens have the form `<aggregateId>.<tokenId>.<secret>` (base64url), so a `public` command finds its aggregate from the token alone.

### Constants

| Constant | Default |
|---|---|
| `ARGON2` | argon2id, 19 MiB, 2 iterations, 1 lane (OWASP) |
| `LOCKOUT_AFTER` / `LOCKOUT_MAX_MINUTES` | 5 failures, then 1, 2, 4 … minutes, at most 15 |
| `VERIFICATION_HOURS` / `RESET_MINUTES` | 48 h / 60 min |
| `ACCESS_TOKEN_MINUTES` | 15 |
| `SESSION_IDLE_DAYS` / `SESSION_MAX_DAYS` | 14 / 90 |

### `account.account`: state

| Field | Type | Vault |
|---|---|---|
| `status` | `none` · `pending` · `active` · `suspended` · `deleted` | |
| `email`, `emailVerified` | string, bool | email personal (subject: the account) |
| `name` | string | personal |
| `password` | `{ hash, setAt }` or null | hash personal |
| `totp` | `{ secret, confirmed }` or null | secret personal |
| `recoveryCodes` | SHA-256 hashes | |
| `passkeys` | `[{ credentialId, publicKey, counter, label, addedAt }]` | credential ID and label personal |
| `links` | `[{ provider, subject, linkedAt }]` | subject personal |
| `failures` | `{ count, lastAt }` since the last success | |
| `openTokens` | `[{ tokenId, purpose, expiresAt }]` (`verify`, `reset`, `email_change`) | the token itself under subject `token:<tokenId>` |
| `pendingEmail` | string or null | personal |
| `suspension` | `{ reason, at }` or null | |

Locked while `failures.count ≥ LOCKOUT_AFTER` and `now < lastAt + min(2^(count − LOCKOUT_AFTER) min, LOCKOUT_MAX_MINUTES)`: computed, not stored.

### `account.account`: commands and events

`secret` marks command fields that leave the vault once the command completes. `api` is the level of the API's service identity.

| Command | Fields | Allowed when | Events | Level |
|---|---|---|---|---|
| `RegisterAccount` | `email`, `name`, `password`? (secret), `invitation`? | `none`; email free | `AccountRegistered` {`email`, `name`, `passwordHash`?}, `VerificationRequested` {`tokenId`, `token`, `expiresAt`} | `public` when `signUp.accounts` = `open`; else only from an invitation, active at once |
| `VerifyEmail` | `token` (secret) | the token is open and not expired | `EmailVerified` (pending → active; erases the token) | `public` (proof) |
| `RequestVerification` | — | `pending` | `VerificationRequested` (erases the previous token) | `api` |
| `SignIn` | `sessionId`, `password` (secret), `refreshToken` (secret), `ip`, `userAgent` | — | `SignedIn` {`sessionId`, `method`: `password`, `factors`, `refreshTokenHash`, `ip`, `userAgent`} or `SignInFailed` {`method`, `reason`: `wrong_password` · `locked` · `not_verified` · `not_active`, `ip`} | `public` (proof) |
| `SignInWithPasskey` | `sessionId`, `challenge`, `assertion`, `refreshToken` (secret), `ip`, `userAgent` | the credential is the account's | `SignedIn` (`method`: `passkey`, `factors`: `passkey`, counter raised) or `SignInFailed` | `api` (it issued the challenge) |
| `SignInWithProvider` | `sessionId`, `provider`, `subject`, `email`, `emailVerified`, `refreshToken` (secret), `ip`, `userAgent` | linked, or the tenant's SSO with a verified domain (then linked first) | [`ProviderLinked`], `SignedIn` (`method`: the provider) or `SignInFailed` (`not_linked`) | `api` |
| `ProveSecondFactor` | `sessionId`, `code` (secret) | TOTP confirmed | `SecondFactorProved` {`sessionId`, `method`: `totp` · `recovery`, `recoveryCodeHash`?} or `SecondFactorFailed` | `public` (proof) |
| `ChangePassword` | `current`? (secret), `new` (secret) | `current` matches, if there is a password | `PasswordChanged` {`hash`} | own account |
| `RequestPasswordReset` | — | `active` | `PasswordResetRequested` {`tokenId`, `token`, `expiresAt`} | `api` |
| `ResetPassword` | `token` (secret), `new` (secret) | the token is open | `PasswordReset` {`hash`} (erases the token) | `public` (proof) |
| `RequestEmailChange` | `email` | the new email free | `EmailChangeRequested` {`tokenId`, `token`, `email`, `expiresAt`} | own account |
| `ConfirmEmailChange` | `token` (secret) | the token is open | `EmailChanged` {`email`} (erases the token) | `public` (proof) |
| `UpdateAccount` | `name` | `active` | `AccountUpdated` {`name`} | own account |
| `StartTotp` | `secret` | no confirmed TOTP | `TotpStarted` {`secret`} | own account |
| `ConfirmTotp` | `code` (secret), `recoveryCodes` (secret) | started | `TotpConfirmed`, `RecoveryCodesGenerated` {`hashes`} | own account |
| `DisableTotp` | `code` (secret) | confirmed | `TotpDisabled` | own account |
| `RegenerateRecoveryCodes` | `recoveryCodes` (secret) | confirmed | `RecoveryCodesGenerated` | own account |
| `AddPasskey` | `challenge`, `attestation`, `label` | — | `PasskeyAdded` {`credentialId`, `publicKey`, `counter`, `label`} | own account, through `api` |
| `RemovePasskey` | `credentialId` | not the last sign-in method | `PasskeyRemoved` | own account |
| `LinkProvider` | `provider`, `subject`, `email` | not linked to another account (unique key) | `ProviderLinked` | own account, through `api` |
| `UnlinkProvider` | `provider`, `subject` | not the last sign-in method | `ProviderUnlinked` | own account |
| `RecordMailSent` | `tokenId` | — | `MailSent` {`tokenId`} | the mail reactor |
| `SuspendAccount` | `reason` | `pending`, `active` | `AccountSuspended` {`reason`} | `platform`; own account with `owner_requested` |
| `ReactivateAccount` | — | `suspended` | `AccountReactivated` | `platform` |
| `DeleteAccount` | — | `suspended` | `AccountDeleted` (erases the account's subject) | `platform` |

### `account.account`: unique keys and facts

- `email`: the verified email while not deleted (scope: the platform tenant, where every account lives). A pending change holds nothing until confirmed.
- `link`: one value per link, `<provider>:<subject>`.
- Fact `active`.

### `account.session`

State: `status` (`none` · `active` · `ended`), `accountId`, `method`, `factors`, `generation`, `refreshTokenHash`, `openedAt`, `lastRefreshAt`, `endReason`.

Refresh tokens have the form `<sessionId>.<generation>.<secret>`. A token of an older generation of the session is a reuse: the session ends.

| Command | Allowed when | Events | Level |
|---|---|---|---|
| `OpenSession` | `none`; the account wrote `SignedIn` for it | `SessionOpened` {`accountId`, `method`, `factors`, `refreshTokenHash`, `ip`, `userAgent`} | the session reactor |
| `RefreshSession` (`refreshToken` secret, `ip`) | `active` | `SessionRefreshed` {`generation`, `refreshTokenHash`, `ip`}, or `SessionEnded` {`reason`: `reuse` · `idle` · `expired`} | `public` (proof) |
| `AddSessionFactor` | `active` | `SessionFactorAdded` {`factor`} | the session reactor (after `SecondFactorProved`) |
| `EndSession` | `active` | `SessionEnded` {`reason`: `signed_out` · `revoked` · `account_suspended` · `password_changed` · `account_deleted`} | own session or own account; `platform`; the session reactor |

The IP and user agent are personal (subject: the account), so deleting an account erases its sessions' too.

### `account.sso`

State: `status` (`none` · `active` · `disabled` · `deleted`), `issuer`, `clientId`, `clientSecret` (personal, subject `sso:<id>`), `domains` (`[{ domain, txt, verified }]`), `enforced`.

| Command | Events | Level |
|---|---|---|
| `CreateSso` (`issuer`, `clientId`, `clientSecret` secret) | `SsoCreated` | owners |
| `UpdateSso` | `SsoUpdated` | owners |
| `AddSsoDomain` (`domain`) | `SsoDomainAdded` {`domain`, `txt`} | owners |
| `VerifySsoDomain` (`domain`) | `SsoDomainVerified` | `api` (it found the TXT record) |
| `RemoveSsoDomain` | `SsoDomainRemoved` | owners |
| `EnforceSso` (`enforced`) | `SsoEnforcementChanged` | owners |
| `DisableSso`, `EnableSso`, `DeleteSso` | `SsoDisabled`, `SsoEnabled`, `SsoDeleted` (erases `sso:<id>`) | owners |

Unique key `domain`: global, each verified domain, so one tenant per domain. With `enforced`, the middleware refuses requests in the tenant from sessions whose method is not this SSO.

### Read models

| Query | Answers | Level |
|---|---|---|
| `FindAccountByEmail`, `FindAccountByLink` | `{ id, status }` | `api` |
| `GetAccount` | profile, sign-in methods, MFA, links; target `data.accountId` | own account, `platform` |
| `ListAccounts` | a page | `platform` |
| `ListSessions` | the account's sessions (device, IP, last use); target `data.accountId` | own account, `platform` |
| `FindSsoByDomain` | `{ tenantId, ssoId, issuer, clientId }` | `api` |
| `GetSsoSecret` | the client secret | `api` |
| `GetSso` | the configuration without the secret | owners |

### Reactions

- `SignedIn` → `OpenSession`. `SecondFactorProved` → `AddSessionFactor`.
- `AccountSuspended`, `AccountDeleted`, `PasswordChanged`, `PasswordReset` → `EndSession` for every open session of the account (for a password change, every session except the current one).
- `VerificationRequested`, `PasswordResetRequested`, `EmailChangeRequested` → the mail reactor sends, then `RecordMailSent`; a failure is parked.
- `AccountDeleted` → `identity` erases the account's identities in every tenant.

### Decided

All five as proposed (2026-11-06):

1. **Registration does not reveal existing accounts.** `RegisterAccount` with a taken email is refused by the unique key; the API answers "check your email" either way and the existing account gets a "you already have an account" mail.
2. **A password change or reset ends the other sessions.**
3. **Session lifetime**: 14 days idle, 90 days at most.
4. **A passkey counts as two factors** (it verifies the user), so it satisfies a tenant's MFA requirement.
5. **Enforced SSO** applies per tenant: the same account may still sign in with a password for other tenants.

## identity

One aggregate, `identity.identity`, with a random ID, under its tenant's `tenant_id`.

### State

| Field | Type | Vault |
|---|---|---|
| `status` | `none` · `active` · `suspended` · `deleted` | |
| `kind` | `person` · `service` | |
| `accountId` | the account (persons only) | |
| `profile` | `{ displayName, title, email }` | personal (subject: `identity:<id>`) |
| `memberships` | `[{ membershipId, group, scope, expiresAt, status }]`; `scope` null (the whole tenant) or `{ type, id }`; `status` `active` · `suspended` | |
| `apiKeys` | `[{ keyId, hash, label, expiresAt, revoked }]` (services only) | |
| `suspension` | `{ reason, at }` or null | |

Who granted a membership and when are in the event's request context and time, not in the payload; the read model shows them.

### Commands and events

| Command | Fields | Allowed when | Events | Level |
|---|---|---|---|---|
| `CreateIdentity` | `accountId`, `displayName`, `title`?, `email`? | `none`; no identity of the account in this tenant (unique key) | `IdentityCreated` {`kind`: `person`, `accountId`, `profile`} | the identity reactor (invitation accepted, self-service tenant) |
| `CreateServiceIdentity` | `displayName` | `none` | `IdentityCreated` {`kind`: `service`, `profile`} | group |
| `UpdateProfile` | `displayName`?, `title`?, `email`? | `active` | `ProfileUpdated` | `self`, group |
| `GrantMembership` | `membershipId`, `group`, `scope`?, `expiresAt`? | `active`; the group `active` (required fact); not the same group and scope twice | `MembershipGranted` | group, and only groups the sender could grant |
| `SuspendMembership`, `ResumeMembership` | `membershipId` | it exists | `MembershipSuspended`, `MembershipResumed` | group |
| `RevokeMembership` | `membershipId` | it exists | `MembershipRevoked` | group; the identity reactor (group deleted) |
| `CreateApiKey` | `keyId`, `secret` (secret), `label`, `expiresAt`? | `service`, `active` | `ApiKeyCreated` {`keyId`, `hash`, `label`, `expiresAt`} | group |
| `RevokeApiKey` | `keyId` | not revoked | `ApiKeyRevoked` | group |
| `SuspendIdentity` | `reason` | `active` | `IdentitySuspended` | group, `platform` |
| `ReactivateIdentity` | — | `suspended` | `IdentityReactivated` | group, `platform` |
| `DeleteIdentity` | — | `suspended`, or `active` when the identity leaves by itself; never while a tenant holds it as owner (required absent) | `IdentityDeleted` (erases `identity:<id>`) | group, `platform`; `self` (leaving); the identity reactor (tenant or account deleted) |

API keys have the form `key_<keyId>_<secret>`. The API generates them and shows them once.

### Unique keys and facts

- `account`: the account ID while not deleted, scope the tenant.
- Fact `active`.

### Read model `identity.identities`

| Query | Answers | Level |
|---|---|---|
| `GetIdentity` | profile, status, memberships (with who granted them), API keys without hashes | `self`, group |
| `ListIdentities` | a page, filter by status, kind, group | group |
| `ListMyIdentities` | the account's identities in every tenant (all partitions); target `data.accountId` | own account |
| `FindIdentityByAccount` | `{ identityId, status }` for a tenant and account | `api` (sets `senderIdentity`) |
| `FindApiKey` | `{ identityId, tenantId, hash, expiresAt, revoked }` | `api` (checks the key) |

### Reactions

- `invitation.InvitationAccepted` → `CreateIdentity`, a `GrantMembership` per group, and `tenancy.AddOwner` for an owner invitation.
- `tenancy.TenantCreated` (self-service) → `CreateIdentity` for the creator, then `tenancy.AddOwner`.
- `tenancy.TenantDeleted`, `account.AccountDeleted` → `DeleteIdentity` for each identity concerned.
- `group.GroupDeleted` → `RevokeMembership` for each membership of the group.

### Decided (2026-11-06)

1. **No separate event per API key use**. Every command made with a key carries the key ID in `senderSession`, so the log already shows every use. Queries are not stored, for people neither.
2. **The API checks API keys** against the hash from `FindApiKey`, cached briefly, instead of a command per request. A revoked key is refused at once by the middleware, which learns of `ApiKeyRevoked` from the events.
3. **Leaving a tenant**: an identity may delete itself directly from `active`, but not while it is an owner (atomic, through the tenant's `owner` key).

## group

One aggregate, `group.group`, with a random ID, under its tenant's `tenant_id`. The `owners` group is virtual (tenancy).

### State

`status` (`none` · `active` · `deleted`), `name`, `description`, `permissions`: `[{ pattern, when?, viaParent? }]`. `pattern` in the grant syntax (`shop.PlaceOrder`, `shop.*`, `*`); `when` a relation of the target aggregate type, declared in code; `viaParent` lets the relation hold on the target's parent.

### Commands and events

| Command | Fields | Allowed when | Events | Level |
|---|---|---|---|---|
| `CreateGroup` | `name`, `description`, `permissions` | `none`; name free; each relation declared | `GroupCreated` | group, with only permissions the sender holds |
| `UpdateGroup` | `name`?, `description`? | `active` | `GroupUpdated` | group |
| `SetPermissions` | `permissions` | `active` | `GroupPermissionsSet` {`permissions`}, the whole list | group, with only permissions the sender holds |
| `DeleteGroup` | — | `active` | `GroupDeleted` | group |

Groups of the platform tenant hold their permissions in every tenant.

### Unique keys and facts

- `name`: lowercased, per tenant, while not deleted.
- Fact `active`.

### Read model and the middleware

| Query | Answers | Level |
|---|---|---|
| `GetGroup`, `ListGroups` | the groups with their permissions and member counts (the virtual `owners` included) | members of the tenant |
| `Explain` | for an identity, a type and optionally an aggregate: allowed or refused, and why (group, membership, relation, expiry) | `self`, group |

`Explain` is answered by the authorization middleware's read model, the same knowledge it decides from. The middleware gets its own section.

### Decided (2026-11-06)

1. **No escalation when editing groups either**: creating a group or changing its permissions is allowed only with permissions the sender holds; otherwise someone who may edit groups could give their own group `*`.
2. **`SetPermissions` replaces the whole list** in one event, rather than add and remove events per permission: simpler to read in the audit.

## invitation

One aggregate, `invitation.invitation`, with a random ID, under its tenant's `tenant_id`. Constant `INVITATION_DAYS` = 7.

### State

`status` (`none` · `open` · `accepted` · `declined` · `revoked`), `email` (personal, subject `invitation:<id>`), `groups` (`[{ group, scope? }]`), `owner` (bool), `tokenId`, `expiresAt`, `accountId` (once accepted). Expired is computed.

### Commands and events

| Command | Fields | Allowed when | Events | Level |
|---|---|---|---|---|
| `CreateInvitation` | `email`, `groups`, `owner`? | `none`; no open invitation for the email in the tenant | `InvitationCreated` {`email`, `groups`, `owner`, `tokenId`, `token`, `expiresAt`} | group, with only groups the sender could grant; owner invitations by owners and `platform` (and the tenancy reactor) |
| `ResendInvitation` | — | `open` | `InvitationResent` {`tokenId`, `token`, `expiresAt`} (erases the old token) | group |
| `RevokeInvitation` | — | `open` | `InvitationRevoked` (erases the token) | group; the invitation reactor (tenant deleted) |
| `DeclineInvitation` | `token` (secret) | `open`, not expired | `InvitationDeclined` (erases the token) | `public` (proof) |
| `AcceptInvitation` | `token` (secret), `accountId`, `email`, `newAccount` | `open`, not expired; `email` is the invited one, or of a domain the tenant's SSO verified | `InvitationAccepted` {`accountId`, `newAccount`} (erases the token) | `api` |
| `RecordMailSent` | `tokenId` | — | `MailSent` | the mail reactor |
| `EraseInvitation` | — | not `open` | `InvitationErased` (erases `invitation:<id>`) | `platform`; the invitation reactor (tenant deleted) |

The token is `<invitationId>.<tokenId>.<secret>` (base64url), in the vault under `token:<tokenId>` until used.

### Accepting

The API runs it, since the person may have no account yet:

1. It checks who is signed in. Signed in: `accountId` and `email` are the session's account. Not signed in: the person enters a name and password, the API picks a new account ID and sets `newAccount`.
2. `AcceptInvitation` checks the token and the email.
3. For a new person, the API then submits `account.RegisterAccount` with that ID (level `api`, active at once: the token proved the email).
4. The identity reactor creates the identity with its memberships, and the owner for an owner invitation.

### Unique keys

`email`: lowercased, per tenant, while `open`.

### Read model `invitation.invitations`

| Query | Answers | Level |
|---|---|---|
| `ListInvitations` | the tenant's invitations (open ones in user lists) | group |
| `PeekInvitation` | `{ tenantName, email, expired }` for a token, to show the acceptance page | `api` |

### Decided (2026-11-06)

1. **Accepting as described**, run by the API in up to two commands.
2. **Invitations keep the email until the tenant is deleted** (for the audit, who was invited when), unless the person asks to be erased (`EraseInvitation`).

## The authorization middleware

One listener, `authorization`, in the `group` domain, at order **-30**, with a middleware **before** every request (`*`): commands, queries, files and cache values. Its key needs `listen` on the set's domains and on the app events that declare relations, and `check` on `*`. The kernel keeps it registered and refuses every request while it is away (fail closed).

### What it knows, from events

| Index | From |
|---|---|
| tenants: status, owners, `mfaRequired`, enforced SSO | `tenancy.*`, `account.Sso*` |
| the sign-up policy | `tenancy.SignUpPolicyChanged` |
| accounts: status | `account.Account*` |
| sessions: status, account, method, factors | `account.Session*` |
| identities: tenant, status, account, memberships, API keys | `identity.*` |
| groups: status, permissions | `group.*` |
| relations: `(type, id) → relation → identity IDs`, and parents | the app's events, per its declarations |

### Declaring relations

In the domain code, next to the aggregate:

```ts
relation("shop.order", "owner")
  .set("OrderPlaced", (e) => e.data.customer)       // an identity ID, or a list
  .unset("OrderTransferred", (e) => e.data.from)
  .set("OrderTransferred", (e) => e.data.to);
parent("funds.vehicle").from("VehicleCreated", (e) => ({ type: "funds.fund", id: e.data.fund }));
```

A membership scoped to an aggregate counts as a relation set by hand. `self`, `own account` and `own session` are built in: they compare the target with the request context.

### The decision, in order

1. **`bypassAuthorization`** (the kernel already checked the key's `privileged` grant): allow.
2. **`public`** types: allow, unless the sign-up policy forbids it (`RegisterAccount`, self-service `CreateTenant`) or the tenant is suspended or deleted.
3. **Who is asking**: the session in `senderSession` is active and belongs to `senderAccount`, whose account is active; or `senderSession` names an API key that is neither revoked nor expired. Else `unauthenticated`.
4. **`authenticated`** types: allow.
5. **Tenant**: the request's tenant is active (else `tenant_suspended`). `senderTenant` is the request's tenant, or the platform tenant (platform groups). Else `forbidden`.
6. **Identity**: `senderIdentity` is active, in `senderTenant`, and of `senderAccount` (services: of the API key). The tenant's MFA requirement holds (two factors, or a passkey; else `mfa_required`), and so does its enforced SSO (else `sso_required`).
7. **Owners** of the tenant: allow.
8. **Memberships**: one that is active, not expired at the check, of an active group, with a permission whose pattern matches the type; its scope is the whole tenant or the target; its `when` relation holds on the target (or its parent with `viaParent`). The target is the command's aggregate, or a query's declared field; a permission with `when` never matches a request without a target. Else `forbidden`.
9. **No escalation**, for `GrantMembership`, `CreateInvitation`, `CreateGroup` and `SetPermissions`: the sender holds every permission of the group concerned (owners and `platform` always do). Else `forbidden`.

Refusals reach the client as `PERMISSION_DENIED` with the code. `group.Explain` runs the same steps and reports each.

### For the app

- **Its own middlewares** run after this one (`order` ≥ 0, which the kernel guarantees), for rules declarations cannot express.
- **Lists**: a query's answer is not filtered by this middleware. The set provides a fold, `visibleTo(identity, type)`, that the app's read models use on the same relation declarations and events.

### Orders

The set's listeners take kernable's reserved orders: the authorization middleware -30, the set's other read models -20, its reactors -10. Their keys are granted `reserved_order`; app listeners cannot go below 0.

### Decided (2026-11-06)

1. **Order -30** through a grant, not 0.
2. **`onBehalfOf` is refused** until impersonation for support comes (later).
3. **Refusals are logged and counted** (metrics), not stored as events: a stream of refused requests must not fill the log.
4. **The index is kept in a store** (the SDK's SQLite listener store), so a restart resumes instead of replaying the whole log while every request is refused.
5. **Escalation is judged conservatively**: only the sender's permissions without scope and `when` count as held.
