# Getting started with the kit

This page takes you from an empty directory to people signing in to your API: copy the kit, run
its domains next to yours, grant its keys, send its mail, and turn a signed-in person into the
request context of your own commands. [The kit](./) explains what each domain holds.

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

## 3. Send its mail

Registration, password resets, email changes and invitations send mail from the kit's reactors.
Until you set a mailer they print to the console, which is enough to try the kit. In production,
pass an object with a `send(mail)` method that hands the mail to your provider:

::: code-group
```ts [TypeScript]
import { useMailer } from "./kit/src/shared/mail.ts";

useMailer({ async send(mail) { await smtp.send({ to: mail.to, subject: mail.subject, text: mail.text }); } });
```
```python [Python]
from kit.shared.mail import use_mailer

class Smtp:
    def send(self, mail):
        smtp.send(to=mail.to, subject=mail.subject, text=mail.text)

use_mailer(Smtp())
```
```rust [Rust]
use kit::shared::mail::{use_mailer, Mail, Mailer};

struct Smtp;
impl Mailer for Smtp {
    fn send(&self, mail: Mail) { /* hand mail.to, mail.subject, mail.text to your provider */ }
}
use_mailer(std::sync::Arc::new(Smtp));
```
:::

The texts are in the `reactors` files of `account` and `invitation`. When `send` throws, the
reactor gets the event again, backing off; after 5 attempts it parks the event, and
`kernable listeners retry` sends it once your provider is back (see
[Failures and recovery](../concepts/failures)). A sent invitation is recorded as
`InvitationMailSent`.

## 4. Grant the keys

| Process | Grant |
|---|---|
| the kit's service | `run` on `tenancy.*`, `account.*`, `identity.*`, `group.*`, `invitation.*`; `listen`, `answer` and `check` on `*`; `vault_read` and `vault_erase`; `reserved_order=*` (its listeners run before yours); `privileged=bypass_authorization` (its own follow-up commands) |
| your API | `submit` and `query` on what it sends; `cache` on `account` (passkey challenges and OIDC state live there for a few minutes) |

See [Keys per service](../how-to/keys) for issuing them.

## 5. Sign people in, in your API

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

## 6. Decide with groups and relations

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

## Next

- [Your API: KitApi](./api) covers every sign-in method, tokens and API keys.
- [Authorization](./authorization) explains each step of the middleware's decision.
- [Recipes](./recipes) for common changes.

## Changing the kit

Each domain has the same files: `domain`, `events`, `aggregates`, `commands`, `readmodels`,
`queries`, `reactors` where it has any, and `index.ts`, `__init__.py` or `mod.rs`, which registers the parts.
Events, commands and queries are classes, so a changed field shows wherever it is used. Settings
you are likely to change (lockout, token lifetimes, reserved slugs, the passkey relying party) are
in `shared/constants`. Fields are camelCase in TypeScript and snake_case in Python and Rust; the
log holds camelCase either way.

A newer kit is not installed over yours: copy it into another directory and take over what you
want.
