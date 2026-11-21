# Authorization

The kit decides every request in one place: a [middleware](../concepts/queries-and-middleware#middleware)
named `authorization`, which runs before every command, query, file and cache request of every
domain, yours included. It decides from what it learned from events (tenants, accounts,
sessions, identities, groups, and the relations you declare), so a decision needs no database
call and no request to another service.

It runs at order -30, before any middleware of yours (yours run at 0 or later). While it is away
the kernel refuses every request with `UNAVAILABLE`: it fails closed.

## The decision

Each request passes these steps in order. The first one that decides, decides.

| Step | Allows | Refuses with |
|---|---|---|
| 1. Privileged request | requests marked `bypassAuthorization`, which only keys granted `privileged` may send: the kit's reactors and `bootstrap()` | — |
| 2. Public types | `SignIn`, `VerifyEmail`, `ResetPassword` and others that carry their own proof (a password, a token) | `sign_up_closed`: `RegisterAccount` while registration is by invitation only |
| 3. Who is asking | an account acting on itself (its account and sessions); in another tenant than the platform's (how the API checks someone joining a tenant), only past that tenant's checks | `unauthenticated`: no active session of an active account, and no valid API key. An account with a second factor on may only sign out until the session proved it. For oneself in another tenant: `tenant_suspended`, `tenant_not_active`, `mfa_required`, `sso_required` as in steps 5 to 7 |
| 4. Signed in is enough | e.g. `ListMyIdentities`; `CreateTenant` for oneself when the sign-up policy allows it | — |
| 5. The tenant | — | `forbidden`: not a member; `tenant_suspended`: the tenant is suspended; `tenant_not_active`: it is pending (not yet activated) |
| 6. The identity | — | `forbidden`: no active identity of this account in the tenant; `mfa_required`: the tenant requires a second factor |
| 7. Owners | everything in their tenant | `sso_required`: the tenant enforces SSO and the session did not use it (owners exempt) |
| 8. Memberships | an active, unexpired membership of an active group with a permission that matches the type, in scope, with its relation holding | `forbidden` |
| 9. No escalation | — | `forbidden`: granting a group, inviting into one, or setting permissions beyond what the sender holds |

A refusal reaches the client as a rejection with the code. A refused command is never stored, so a
flood of refused requests cannot fill the log.

## Relations

A relation says who stands in which relation to an aggregate: the `owner` of an order, the
`manager` of a fund. You declare relations next to your aggregates, **from your events**; the
middleware keeps them up to date as the events arrive:

::: code-group
```ts [TypeScript]
import { parent, relation } from "./kit/src/kit.ts";

relation("shop.order", "owner")
  .set("shop.OrderPlaced", (e) => e.data.customer)         // an identity ID, or a list
  .unset("shop.OrderTransferred", (e) => e.data.from)
  .set("shop.OrderTransferred", (e) => e.data.to);

// Permissions with `viaParent` look at the fund a vehicle belongs to.
parent("funds.vehicle").from("funds.VehicleCreated", (e) => ({ type: "funds.fund", id: e.data.fund }));
```
```python [Python]
from kit import parent, relation

(relation("shop.order", "owner")
    .set("shop.OrderPlaced", lambda e: e.data["customer"])
    .unset("shop.OrderTransferred", lambda e: e.data["from"])
    .set("shop.OrderTransferred", lambda e: e.data["to"]))

parent("funds.vehicle").from_("funds.VehicleCreated", lambda e: {"type": "funds.fund", "id": e.data["fund"]})
```
```rust [Rust]
use kit::authorization::{ids, parent, relation};

relation("shop.order", "owner")
    .set("shop.OrderPlaced", |e| ids(&e.data["customer"]))
    .unset("shop.OrderTransferred", |e| ids(&e.data["from"]))
    .set("shop.OrderTransferred", |e| ids(&e.data["to"]));
```
```go [Go]
kit.Relation("shop.order", "owner").
	Set("shop.OrderPlaced", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["customer"]) }).
	Unset("shop.OrderTransferred", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["from"]) }).
	Set("shop.OrderTransferred", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["to"]) })

kit.Parent("funds.vehicle").From("funds.VehicleCreated", func(e kit.RelationEvent) *kit.Target {
	fund, _ := e.Data["fund"].(string)
	return &kit.Target{Type: "funds.fund", ID: fund}
})
```
:::

For queries, the middleware needs to know which aggregate a query is about. Declare it on the
query, once: `static target = { aggregateType: "order", field: "id" }` on a query class, or
`shopDomain.query("GetOrder").target("order", "id")` for a query given by name. Then
`{ pattern: "shop.GetOrder", when: "owner" }` lets each customer read only their own orders.

Declare relations **before** `kitDomains()`, which hands them to the middleware. The names `self` and
`owners` are built in. A group permission then uses the relation with `when: "owner"`
([Groups](./group#permissions)). A membership scoped to one aggregate counts as a relation set
by hand.

## Lists: `visibility`

The middleware decides single requests. A query that answers a **list** (all orders, all
invoices) is allowed or refused as a whole, so its read model must filter the rows itself.
`visibility(readModel)` teaches your read model what the middleware knows and returns a function
that decides the same way per row:

```ts
import { visibility } from "./kit/src/kit.ts";

const orders = shop.readModel("shop.orders");
const visible = visibility(orders);

orders.query(ListOrders, (_d, qry, ctx) =>
  rows.filter((o) => visible(qry.tenant, ctx.request, "shop.GetOrder", { type: "shop.order", id: o.id })));
```

Each row shows only if the asker could `GetOrder` it. The knowledge is kept in the read model's own
store, so it resumes after a restart.

## Why was it refused: `Explain`

`group.Explain` answers whether an identity may send a type, for an aggregate if you give one,
with every step of the decision:

```ts
const answer = await kernable.query(Explain.new({
  identityId, type: "shop.CancelOrder", aggregateType: "shop.order", aggregateId: "o-17",
}), { tenant, context });
// { allowed: false, code: "forbidden", steps: [one line per step it passed, and the one that refused] }
```

Anyone may explain their own requests; explaining someone else's needs the `group.Explain`
permission. Support screens and admin tools use it to show *why*.

## Your own rules

Rules that permissions and relations cannot express ("orders above 10,000 need a second
approver") belong in a middleware of your own. It runs after the kit's, so it sees only requests
that passed, with the identity already checked.

## Where it keeps its knowledge

The middleware keeps what it learned in a store (`useAuthorizationStore("sqlite:authorization.db")`
by default). After a restart it continues from where it stopped instead of reading the whole log
again, which matters, because it refuses every request until it has caught up.

## Settings

The levels of the kit's own types (who may send `SignIn`, `SuspendTenant` and so on) are data, in
`authorization/levels`. The reference tables of each domain show them under "Who may send it".
Types not listed there, and all of yours, need a group permission. The decision itself is in
`authorization/policy`, one function you can read top to bottom.
