# Add tenants, accounts and authorization: the kit

Most applications need the same groundwork before their own domain: tenants, people who sign in,
their roles in a tenant, invitations, and a middleware that decides every request. kernable's
**kit** is that groundwork as source code you copy into your project. It is yours afterwards:
change fields, rules and flows, or delete what you do not need. It is open source (Apache-2.0).

The kit is available for **TypeScript**, **Python** and **Rust**. Every kit writes the same
events, so a TypeScript API and a Rust service, say, work on one log.

## What you get

| Domain | What it does |
|---|---|
| `tenancy` | one tenant per customer organisation: pending, active, suspended, deleted; a slug unique across tenants; its owners; whether it requires a second factor; the sign-up policy |
| `account` | a person's account, the same in every tenant: registration and email verification, password, TOTP and recovery codes, passkeys, OAuth/OIDC logins; a session per sign-in; a tenant's single sign-on |
| `identity` | a person or service in one tenant: its profile, its group memberships (for the whole tenant or one aggregate, optionally expiring) and, for services, API keys |
| `group` | named sets of permissions per tenant, in the same patterns as [grants](../concepts/trust#keys-and-grants) (`shop.PlaceOrder`, `shop.*`), optionally tied to a relation (`when owner`) |
| `invitation` | invitations by email, accepted by a signed-in account or a new person |
| authorization | a [middleware](../concepts/queries-and-middleware#middleware) that runs before every other one and decides each request from what it learned from events |

Everything the kit does is events in your log: every sign-in, failed attempt, refresh, membership
and invitation is audited, and personal data (emails, names, IP addresses, password hashes) lives
in the [vault](../concepts/data), erased with the person.

## 1. Copy it

::: code-group
```sh [TypeScript]
npx @kernable/sdk add kit           # into ./kit; or: add kit <dir>
npm install @node-rs/argon2 @simplewebauthn/server openid-client
node --test kit/test/unit/*.test.ts
```
```sh [Python]
kernable-sdk add kit                # into ./kit, a package; or: add kit <dir>
pip install -r kit/requirements.txt
python -m unittest discover -s kit/tests -t .
```
```sh [Rust]
cargo install kernable-sdk
kernable-sdk add kit                # the crate ./kit; or: add kit <dir>
cargo test -p kit                   # after adding it to your workspace
```
:::

The kit comes with the SDK, so its version matches your SDK's. The tests are the kit's
contract: the cases every language's kit passes, as JSON in `kit/fixtures`. They pass in the kit
as shipped; when you change a rule, change its case too.

## 2. Run it

The kit is a set of [domains](../concepts/domains). Declare the relations your permissions use,
then run the kit's domains next to yours:

::: code-group
```ts [TypeScript]
import { Kernable } from "@kernable/sdk";
import { kitDomains, relation, useAuthorizationStore } from "./kit/src/kit.ts";
import { shopDomain } from "./shop/domain.ts";

// Who stands in which relation to your aggregates, from your events.
relation("shop.order", "owner").set("shop.OrderPlaced", (e) => e.data.customer);

useAuthorizationStore("sqlite:authorization.db"); // the middleware resumes after a restart
const app = new Kernable().app({ service: "kit", domains: [...kitDomains(), shopDomain] });
await app.run();
```
```python [Python]
from kernable import Kernable
from kit import kit_domains, relation, use_authorization_store
from shop.domain import shop_domain

# Who stands in which relation to your aggregates, from your events.
relation("shop.order", "owner").set("shop.OrderPlaced", lambda e: e.data["customer"])

use_authorization_store("sqlite:authorization.db")  # the middleware resumes after a restart
Kernable().app(service="kit", domains=[*kit_domains(), shop_domain]).run()
```
```rust [Rust]
use kernable::{App, Kernable};
use kit::authorization::{ids, relation, use_authorization_store};

// Who stands in which relation to your aggregates, from your events.
relation("shop.order", "owner").set("shop.OrderPlaced", |e| ids(&e.data["customer"]));

use_authorization_store("sqlite:authorization.db"); // the middleware resumes after a restart
let mut app = App::new().service("kit");
for d in kit::kit_domains().iter().chain([&shop_domain]) {
    app = app.domain(d);
}
Kernable::new("kit.key")?.app(app)?.run().await?;
```
:::

Once, `bootstrap()` creates the platform tenant (where accounts and the platform's
administrators live), the sign-up policy, the identity and key your API uses for itself, and
optionally the first administrator. It may run again; it skips what exists.

## 3. Grant the keys

| Process | Grant |
|---|---|
| the kit's service | `run` on `tenancy.*`, `account.*`, `identity.*`, `group.*`, `invitation.*`; `listen`, `answer` and `check` on `*`; `vault_read` and `vault_erase`; `reserved_order=*` (its listeners run before yours); `privileged=bypass_authorization` (its own follow-up commands) |
| your API | `submit` and `query` on what it sends; `cache` on `account` (passkey challenges and OIDC state live there for a few minutes) |

See [Keys per service](./keys) for issuing them.

## 4. Sign people in, in your API

Your API is the one process that sees passwords, tokens and provider answers. `KitApi` holds what
only it can do; wire it into your HTTP framework (`kit/example/server.ts` shows `node:http`,
`kit/example/server.py` shows `http.server`, `kit/examples/server.rs` axum):

::: code-group
```ts [TypeScript]
const api = new KitApi({ kernable, keys: signingKeysFrom("token-signing.pem"), service });

const signedIn = await api.signIn({ email, password, ip });          // access and refresh token
const claims = api.check(accessToken);                               // per request, without the kernel
const context = await api.contextFor(claims, tenantId);              // senderAccount, senderSession, senderTenant, senderIdentity
await kernable.submit(PlaceOrder.new({ … }), { aggregateId, tenant: tenantId, context });
```
```python [Python]
api = KitApi(kernable, signing_keys_from("token-signing.pem"), service)

signed_in = api.sign_in(email, password, ip=ip)                      # access and refresh token
claims = api.check(access_token)                                     # per request, without the kernel
context = api.context_for(claims, tenant_id)                         # sender_account, sender_session, sender_tenant, sender_identity
kernable.submit(PlaceOrder(...), aggregate_id=aggregate_id, tenant=tenant_id, context=context)
```
```rust [Rust]
let api = KitApi::new(kernable.clone(), signing_keys_from("token-signing.pem")?, &service_identity, &service_key_id);

let signed_in = api.sign_in(&email, &password, ip, None).await?;      // access and refresh token
let claims = api.check(&access_token);                               // per request, without the kernel
let context = api.context_for(&claims, Some(&tenant_id)).await?;     // sender_account, sender_session, sender_tenant, sender_identity
kernable.submit(Submit::command(PlaceOrder { .. }, &aggregate_id).tenant(&tenant_id).context(context)).await?;
```
:::

In Python, `KitApi` blocks like the SDK's client; for FastAPI or another asyncio framework,
`AsyncKitApi(KitApi(...))` has the same methods, awaited. In Rust, `KitApi` is async.

Access tokens live 15 minutes and are checked by your API alone; the middleware still refuses an
ended session, a suspended account or a revoked API key at once. Refresh tokens rotate, and using
one twice ends its session. `KitApi` also verifies emails, resets passwords, sets up TOTP and
passkeys, runs OAuth/OIDC (`oidcStart`, `oidcFinish`), checks API keys and accepts invitations.

## 5. Decide with groups and relations

A tenant's owners may do everything in it. Everyone else gets permissions through groups, for the
whole tenant or one aggregate:

::: code-group
```ts [TypeScript]
CreateGroup.new({ name: "Customers", permissions: [
  { pattern: "shop.PlaceOrder" },
  { pattern: "shop.CancelOrder", when: "owner" },   // only orders they placed
] });
```
```python [Python]
CreateGroup(name="Customers", permissions=[
    {"pattern": "shop.PlaceOrder"},
    {"pattern": "shop.CancelOrder", "when": "owner"},  # only orders they placed
])
```
```rust [Rust]
CreateGroup { name: "Customers".into(), description: None, permissions: vec![
    Permission { pattern: "shop.PlaceOrder".into(), ..Permission::default() },
    Permission { pattern: "shop.CancelOrder".into(), when: Some("owner".into()), via_parent: None }, // only orders they placed
] };
```
:::

Nobody grants a group with permissions they do not hold. `Explain` answers why a request is
allowed or refused, with the middleware's own steps. For lists, `visibility(readModel)` gives an
app read model the same decision, so a list shows only what each asker may get.

## Changing the kit

Each domain has the same files: `domain`, `events`, `aggregates`, `commands`, `readmodels`,
`queries`, `reactors` where it has any, and `index.ts`, `__init__.py` or `mod.rs`, which registers the parts.
Events, commands and queries are classes, so a changed field shows wherever it is used. Settings
you are likely to change (lockout, token lifetimes, reserved slugs, the passkey relying party) are
in `shared/constants`. Fields are camelCase in TypeScript and snake_case in Python and Rust; the
log holds camelCase either way.

A newer kit is not installed over yours: copy it into another directory and take over what you
want.
