# Recipes

Common changes to the kit, each in a few steps. The examples are TypeScript; the Python and
Rust kits have the same files and names in their style.

## Open registration, or invitations only

The platform's sign-up policy decides who may register and who may create tenants. Set it in
`bootstrap()`, or change it later as the platform:

```ts
await bootstrap(kernable, { signUp: { accounts: "open", tenants: "active" } }); // a typical SaaS
await bootstrap(kernable, { signUp: { accounts: "off", tenants: "off" } });     // invitation only
```

| You want | `accounts` | `tenants` |
|---|---|---|
| anyone signs up and creates their own workspace | `open` | `active` |
| anyone signs up; new workspaces wait for your approval (`ActivateTenant`) | `open` | `pending` |
| you create tenants; people join by invitation | `off` | `off` |

Later: `ChangeSignUpPolicy.new({ accounts, tenants })` on the platform tenant.

## Let support staff into every tenant

Groups of the [platform tenant](./tenancy#the-platform-tenant) hold their permissions in every
tenant. Create a group there and invite your staff into it:

```ts
await kernable.submit(CreateGroup.new({
  name: "Support",
  description: "Reads customers' data to help them",
  permissions: [{ pattern: "shop.GetOrder" }, { pattern: "identity.GetIdentity" }, { pattern: "group.Explain" }],
}), { aggregateId: randomUUID(), tenant: PLATFORM_TENANT, context });

await kernable.submit(CreateInvitation.new({ email: "ann@example.com", groups: [{ group: supportGroupId }] }),
  { aggregateId: randomUUID(), tenant: PLATFORM_TENANT, context });
```

Everything they do is in the log with their identity, in the platform tenant, as the sender.

## Require a second factor in a tenant

An owner of the tenant sends `RequireMfa.new({ required: true })` on the tenant. From then on,
sessions without TOTP or a passkey get `mfa_required` there. Your UI should catch that code and
lead the person to set up TOTP ([`startTotp`, `confirmTotp`](./api#methods)) or a passkey.

## Single sign-on for a customer

1. An owner of the customer's tenant sends `CreateSso` with the identity provider's issuer, client
   ID and client secret (the secret goes to the vault).
2. They add their email domain: `AddSsoDomain({ domain: "acme.com" })`. The event carries the
   value for the DNS record.
3. They publish a TXT record `_kernable-sso.acme.com` with that value; your API calls
   `api.verifySsoDomain(tenant, ssoId, "acme.com")`.
4. Your sign-in page asks for the email first and, for a verified domain, sends the person to
   `api.oidcStart({ email })`.
5. Optionally `EnforceSso({ enforced: true })`: in this tenant, only SSO sessions count (owners
   exempt).

## A service that calls your API

```ts
const created = await kernable.submit(CreateServiceIdentity.new({ displayName: "Billing sync" }),
  { aggregateId: serviceId, tenant, context });
await kernable.submit(GrantMembership.new({ membershipId: randomUUID(), group: billingGroupId }),
  { aggregateId: serviceId, tenant, context });
const key = await api.createApiKey(context, tenant, serviceId, "billing sync");
// Show key.key once. The service sends `Authorization: ApiKey <key>`; your API calls api.checkApiKey().
```

Revoke it with `RevokeApiKey({ keyId })`; the middleware refuses it at once.

## Show only what someone may see in a list

Use [`visibility`](./authorization#lists-visibility) in the read model that answers the list, with
the same type its single-item query has (`shop.GetOrder`), so lists and detail pages agree.

## Change a lifetime or a limit

Edit `shared/constants`: lockout, token and session lifetimes, invitation validity, reserved
slugs. If a test case in `kit/fixtures` covers the old value, change the case too. That is
deliberate: the cases are the contract of the kit as you run it.

## Add a field

Say identities should have a phone number.

1. **Event**: add `phone!: string | null;` to `IdentityCreated` and `ProfileUpdated` in
   `identity/events.ts`, and add `"phone"` to their `personal` fields, so it goes to the vault and
   is erased with the identity.
2. **Command**: add `phone?: string;` to `UpdateProfile` in `identity/commands.ts`, and to its
   `personal` fields; validate it in the handler and pass it on to the event.
3. **State and rows**: apply it in `identity/aggregates.ts` if a rule needs it, and add it to the
   row in `identity/readmodels.ts` so `GetIdentity` returns it.
4. **Cases**: add `"phone": null` to the expected events in `kit/fixtures/identity.json`, and a
   case for a valid and an invalid number.

Events already in your log have no `phone`; read it as missing (`?? null`). Once kernable 1.0 is
out, use [event versions](../how-to/event-versions) for changes to stored events.

## Remove a domain you do not need

Say you never invite anyone. Delete `invitation/` and remove it from `kitDomains()`, then let the
compiler (or your tests) show what still refers to it: the identity reactor that turns accepted
invitations into identities, `acceptInvitation` and `peekInvitation` in `KitApi`, the owner
invitation for tenants the platform creates, and `fixtures/invitation.json`. Decide for each
whether you need a replacement. For example, tenants created by the platform then need their
first owner some other way, such as `CreateTenant` with `owner: { accountId }`.

Removing a domain from a running application also means its listeners stop:
[Remove a listener or aggregate](../how-to/remove-listener) explains how the kernel stops waiting
for them.
