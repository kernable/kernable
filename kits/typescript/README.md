# The kernable kit (TypeScript)

Tenancy, accounts, identities, groups, invitations and authorization for an application on
kernable, as source code in your project. It is yours: change fields, rules and flows, or delete
what you do not need. The kit is not a dependency you update; a newer one is copied anew, and you
take over what you want.

```sh
npx @kernable/sdk add kit [dir]        # copies this kit, for your SDK's version, into dir (default: kit)
npm install @node-rs/argon2 @simplewebauthn/server openid-client
node --test kit/test/unit/*.test.ts    # the contract: every case passes in the kit as shipped
```

## What is in it

| Domain | What it does |
|---|---|
| `tenancy` | a Tenant per tenant: pending → active ⇄ suspended → deleted, a slug unique across tenants, its owners, MFA and sign-up policies |
| `account` | a person's Account (registration, email verification, password, TOTP and recovery codes, passkeys, OAuth/OIDC links), a Session per sign-in, a tenant's single sign-on (`account.sso`) |
| `identity` | a person or service in one tenant, its profile, group memberships (scoped, expiring) and API keys |
| `group` | named sets of permissions per tenant; `owners` is virtual |
| `invitation` | invitations by email, accepted by a signed-in account or a new person |
| `authorization` | the middleware that decides every request, and `relation()`, `visibility()`, `Explain` |
| `api` | `KitApi`: what only your API can do (sign-in, tokens, OIDC, passkey challenges, API keys) |

Every domain has the same files: `domain.ts`, `events.ts`, `aggregates.ts`, `commands.ts`,
`readmodels.ts`, `queries.ts`, `reactors.ts` and `index.ts`, which registers the parts. The
constants you are most likely to change are in `src/shared/constants.ts`.

## Running it

```ts
import { Kernable } from "@kernable/sdk";
import { kitDomains, relation, usePasskeys, useAuthorizationStore } from "./kit/src/kit.ts";

relation("shop.order", "owner").set("shop.OrderPlaced", (e) => e.data.customer); // your relations first
useAuthorizationStore("sqlite:authorization.db");
const app = new Kernable().app({ service: "kit", domains: [...kitDomains(), shopDomain] });
await app.run();
```

Once, with a key granted `privileged` for `bypass_authorization`, `bootstrap()` creates the
platform tenant, the sign-up policy, the API's service identity and its key, and optionally the
first platform administrator. `example/server.ts` shows an HTTP API over `KitApi`.

## Keys and grants

| Process | Needs |
|---|---|
| the kit's service | `run` on the kit's domains, `listen`, `answer`, `check` on `*`, `vault_read`, `vault_erase`, `reserved_order` (`*`: its listeners run before yours), `privileged` for `bypass_authorization` (its reactors), `process` on `*` (it sets as whom each process acts) |
| your API | `submit` and `query` on what it sends, `cache` on `account` (passkey challenges, OIDC state) |

## Changing it

The events are the contract other processes read (`events.ts`); `kit/fixtures` holds the cases
every language's kit passes. Change a rule and its fixture together, and the tests tell you what
else moved. Every domain, flow and setting is documented at https://kernable.com/docs/kits/, with a
reference of its commands, events and queries; the design with every decision is
`design/kits.md` in the kernable repository.
