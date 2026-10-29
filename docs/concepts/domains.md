# Domains

A **domain** is one area of your business: `shop`, `billing`, `identity`. Its name scopes every type in it, so `shop.ItemAdded` and `billing.ItemAdded` never collide, and in code it is one object that holds everything that belongs to the area.

::: code-group

```ts [TypeScript]
import { domain } from "@kernable/sdk";

export const shopDomain = domain("shop");
```

```python [Python]
from kernable import domain

shop_domain = domain("shop")
```

```rust [Rust]
use kernable::domain;

let shop_domain = domain("shop");
```

:::

## What a domain holds

Everything you define on the domain object belongs to it:

| Defined with | What it is |
|---|---|
| `aggregate(name, initial)` | an aggregate: its state, its commands, the events it writes ([Event sourcing](./event-sourcing)) |
| `readModel(name)` | a read model: a view built from events, answering queries |
| `reactor(name)` | a reactor: side effects and follow-up commands |
| `.middleware(…)` on a read model | a middleware: checks commands and queries ([Queries and middleware](./queries-and-middleware)) |
| `event(type, version)` | an event's payload version, with an `upcast` from each older one ([Data over time](./data)) |
| `event(type, …).personal(…)`, `.erases(…)` | which event fields go to the vault, and which event erases a person |
| `command(type).personal(…)`, `.secret(…)` | which command fields go to the vault |

The names are TypeScript's; Python and Rust spell them `read_model`, and so on ([SDK reference](../reference/)).

Types without a domain are the domain's own: on `shopDomain`, `ItemAdded` means `shop.ItemAdded`. Types of other domains are qualified: `billing.InvoicePaid`, or a whole domain, `billing.*`.

## Rules

- **A domain writes only its own events.** Its aggregates decide; nothing outside the domain writes `shop.*` events.
- **It reacts to other domains through listeners.** A read model of `shop` may read `billing.InvoicePaid`; a reactor of `billing` may react to `shop.CheckedOut` and submit a `billing` command. Each domain stays the only one that decides about its own aggregates.
- **Other domains' event versions come with `use`.** A listener that reads another domain's events gets them upcast to the versions it knows with `use(billingDomain)`; its own domain needs no `use`.
- **The vault declarations travel with the domain.** A client that submits commands with personal or secret fields needs the domain's declarations, to move those fields out before signing: `kernable.use(shopDomain)`, also when the domain's aggregates run in another process or language.
- **Grants follow domains.** A key is granted per type or per domain: `run=shop.*`, `listen=shop.*,billing.InvoicePaid` ([Trust](./trust)).

## Domains, services and apps

A domain is a unit of **code and meaning**; a service is a unit of **deployment**: the processes that belong together, with one membership.

- **One service, several domains.** An app runs every aggregate, read model and reactor of its domains: `kernable.app({ service: "shop", domains: [shopDomain, billingDomain] })`.
- **One domain, one service** is the usual case and the simplest to reason about.
- **One domain across services** works too, for example its read models in a separate process. Then run each part with explicit workers and listeners instead of `app()`, and apply each service's membership ([memberships](../how-to/remove-listener)).

The membership belongs to the service, not the domain: it lists what the service runs, whichever domains that comes from.

## A layout that works

kernable does not prescribe files. One that stays clear as a domain grows: the domain object with its event versions and vault declarations in one file, each aggregate, read model and reactor in its own, and one entry point per service.

```
shop/
  domain.ts      domain("shop"), event versions, vault declarations
  cart.ts        the cart aggregate
  order.ts       the order aggregate
  carts.ts       a read model
  mails.ts       a reactor
billing/
  domain.ts
  invoice.ts
  invoicing.ts   reacts to shop.CheckedOut
app.ts           kernable.app({ service, version, domains: [shopDomain, billingDomain] })
```

The same works in Python (`shop/domain.py`, …) and Rust (`src/shop/mod.rs`, …). Each file defines its part on the domain object it imports, so the app only needs the domains.

## Naming

- **Domains and aggregates**: lowercase, singular: `shop`, `cart`, `order`.
- **Events**: what happened, in the past tense: `ItemAdded`, `OrderPaid`.
- **Commands**: what should happen, imperative: `AddItem`, `PayOrder`.
- **Queries**: what is asked: `GetCart`, `ListOrders`.
- **Read models and reactors**: what they keep or do, lowercase: `carts`, `invoicing`, `mails`.

Names are case-sensitive, and the conventions are not enforced: they keep a log that many teams read understandable.
