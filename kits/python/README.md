# The kernable kit (Python)

Tenancy, accounts, identities, groups, invitations and authorization for an application on
kernable, as source code in your project. It is yours: change fields, rules and flows, or delete
what you do not need. The kit is not a dependency you update; a newer one is copied anew, and you
take over what you want.

```sh
kernable-sdk add kit [dir]                         # or: python -m kernable add kit [dir]; default dir: kit
pip install -r kit/requirements.txt
python -m unittest discover -s kit/tests -t .      # the contract: every case passes in the kit as shipped
```

The directory becomes a Python package of that name (`kit`, `auth_kit`, …); the kit imports its
own modules relatively, and the copy's tests and example import it by your name.

## What is in it

| Package | What it does |
|---|---|
| `tenancy` | a Tenant per tenant: pending → active ⇄ suspended → deleted, a slug unique across tenants, its owners, MFA and sign-up policies |
| `account` | a person's Account (registration, email verification, password, TOTP and recovery codes, passkeys, OAuth/OIDC links), a Session per sign-in, a tenant's single sign-on (`Sso`) |
| `identity` | a person or service in one tenant, its profile, group memberships (scoped, expiring) and API keys |
| `group` | named sets of permissions per tenant; `owners` is virtual |
| `invitation` | invitations by email, accepted by a signed-in account or a new person |
| `authorization` | the middleware that decides every request, and `relation()`, `visibility()`, `Explain` |
| `api` | `KitApi`: what only your API can do (sign-in, tokens, OIDC, passkey challenges, API keys); `AsyncKitApi` for asyncio |

Every domain has the same files: `domain.py`, `events.py`, `aggregates.py`, `commands.py`,
`readmodels.py`, `queries.py`, `reactors.py` where it has reactors, and `__init__.py`, which registers the parts. The
constants you are most likely to change are in `shared/constants.py`.

## Running it

```python
from kernable import Kernable
from kit import kit_domains, relation, use_authorization_store

relation("shop.order", "owner").set("shop.OrderPlaced", lambda e: e.data["customer"])  # your relations first
use_authorization_store("sqlite:authorization.db")
Kernable().app(service="kit", domains=[*kit_domains(), shop_domain]).run()
```

Once, with a key granted `privileged` for `bypass_authorization`, `kit.bootstrap.bootstrap()`
creates the platform tenant, the sign-up policy, the API's service identity and its key, and
optionally the first platform administrator. `example/server.py` shows an HTTP API over
`KitApi` with `http.server`; in FastAPI or another asyncio framework, wrap it:
`AsyncKitApi(KitApi(...))` has the same methods, awaited.

## Keys and grants

| Process | Needs |
|---|---|
| the kit's service | `run` on the kit's domains, `listen`, `answer`, `check` on `*`, `vault_read`, `vault_erase`, `reserved_order` (`*`: its listeners run before yours), `privileged` for `bypass_authorization` (its reactors) |
| your API | `submit` and `query` on what it sends, `cache` on `account` (passkey challenges, OIDC state) |

## Changing it

The events are the contract other processes read (`events.py`), the same as the TypeScript kit's:
fields are snake_case here and camelCase in the log. `fixtures/` holds the cases every language's
kit passes. Change a rule and its fixture together, and the tests tell you what else moved. Every domain, flow and setting is documented at https://kernable.com/docs/kits/, with a
reference of its commands, events and queries; the design with every decision is
`design/kits.md` in the kernable repository.
