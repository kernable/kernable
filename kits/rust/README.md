# The kernable kit (Rust)

Tenancy, accounts, identities, groups, invitations and authorization for an application on
kernable, as source code in your project. It is yours: change fields, rules and flows, or delete
what you do not need. The kit is not a dependency you update; a newer one is copied anew, and you
take over what you want.

```sh
cargo install kernable-sdk
kernable-sdk add kit [dir]          # copies this kit, for your SDK's version, as the crate `dir` (default: kit)
cargo test -p kit                   # the contract: every case passes in the kit as shipped
```

Add it to your workspace and depend on it: `kit = { path = "kit" }`. The directory names the
crate and its library (`auth-kit` → `use auth_kit::...`).

## What is in it

| Module | What it does |
|---|---|
| `tenancy` | a Tenant per tenant: pending → active ⇄ suspended → deleted, a slug unique across tenants, its owners, MFA and sign-up policies |
| `account` | a person's Account (registration, email verification, password, TOTP and recovery codes, passkeys, OAuth/OIDC links), a Session per sign-in, a tenant's single sign-on (`Sso`) |
| `identity` | a person or service in one tenant, its profile, group memberships (scoped, expiring) and API keys |
| `group` | named sets of permissions per tenant; `owners` is virtual |
| `invitation` | invitations by email, accepted by a signed-in account or a new person |
| `authorization` | the middleware that decides every request, and `relation()`, `visibility()`, `Explain` |
| `api` | `KitApi`, async: what only your API can do (sign-in, tokens, OIDC, passkey challenges, API keys) |

Every domain has the same files: `mod.rs` (its types, and the function that registers the
parts), `events.rs`, `aggregates.rs`, `commands.rs`, `readmodels.rs`, `queries.rs`, and
`reactors.rs` where it has reactors. The constants you are most likely to change are in
`src/shared/constants.rs`.

## Running it

```rust
use kernable::{App, Kernable};
use kit::authorization::{ids, relation, use_authorization_store};

// Your relations first: who stands in which relation to your aggregates, from your events.
relation("shop.order", "owner").set("shop.OrderPlaced", |e| ids(&e.data["customer"]));
use_authorization_store("sqlite:authorization.db");
let mut app = App::new().service("kit");
for d in kit::kit_domains().iter().chain([&shop_domain]) {
    app = app.domain(d);
}
Kernable::new("kit.key")?.app(app)?.run().await?;
```

Once, with a key granted `privileged` for `bypass_authorization`, `kit::bootstrap::bootstrap()`
creates the platform tenant, the sign-up policy, the API's service identity and its key, and
optionally the first platform administrator. `examples/server.rs` shows an HTTP API over
`KitApi` with axum.

## Keys and grants

| Process | Needs |
|---|---|
| the kit's service | `run` on the kit's domains, `listen`, `answer`, `check` on `*`, `vault_read`, `vault_erase`, `reserved_order` (`*`: its listeners run before yours), `privileged` for `bypass_authorization` (its reactors) |
| your API | `submit` and `query` on what it sends, `cache` on `account` (passkey challenges, OIDC state) |

## Changing it

The events are the contract other processes read (`events.rs` in each domain), the same as the
other languages' kits: fields are snake_case here and camelCase in the log. `fixtures/` holds
the cases every language's kit passes. Change a rule and its fixture together, and the tests tell
you what else moved. Every domain, flow and setting is documented at https://kernable.com/docs/kits/, with a
reference of its commands, events and queries; the design with every decision is
`design/kits.md` in the kernable repository.
