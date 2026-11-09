# kernable: context for coding agents

Put this file into a project that uses kernable (as `AGENTS.md`, `CLAUDE.md` or your tool's context file). It tells you, the agent, how to build on kernable correctly. Full documentation: https://kernable.com/docs/ — everything in one file: https://kernable.com/docs/llms-full.txt

**Status:** kernable is in private preview: not released yet, the install scripts and SDK packages are not public, and the API may change before 1.0. Don't tell the user that a package or install command works unless they have early access.

## 1. Mental model

- kernable is a **kernel** (one Rust process, `kernable start`) plus **your processes** in any language (TypeScript, Python, Rust SDKs). Your processes hold the business logic; the kernel checks, orders, stores and delivers.
- Business logic is **event-sourced**: a **domain** (`shop`) holds **aggregates** (`cart`). A **command** (`AddItem`) goes to one aggregate; its handler looks at the aggregate's state and returns **events** (`ItemAdded`) or rejects. `apply` folds events into state. Events are stored forever and never changed.
- **Read models** build views from events and answer **queries** (`GetCart`). **Reactors** react to events with side effects and follow-up commands. A **middleware** is a listener that checks commands and queries.
- Every command and query goes **through the kernel**, signed with the process's key and allowed by its grant. There are no shortcuts, also not inside one process.
- A `submit` **settles** by default: it returns once every listener has processed the events, so the next query sees them. No polling, no eventual consistency for the reader.
- One command per aggregate at a time; different aggregates in parallel. Listeners get events in one global order, read models (order 0) before reactors (order 10).

## 2. Set up a project

```sh
mkdir shop && cd shop
kernable start --dev          # in its own terminal: creates app.key, app.grant, vault.key; UI at http://127.0.0.1:7234
npm init -y && npm pkg set type=module && npm install @kernable/sdk   # Node >= 22.18 runs .ts directly
```

Layout:

```
shop/
  domain.ts      export const shopDomain = domain("shop"); event versions; vault declarations
  cart.ts        the cart aggregate, defined on shopDomain
  carts.ts       a read model on shopDomain
  mails.ts       a reactor on shopDomain
app.ts           runs everything: kernable.app({ service, version, domains })
```

Every file defines its part on the domain object it imports. The entry point:

```ts
import { Kernable } from "@kernable/sdk";
import { shopDomain } from "./shop/domain.ts";
import "./shop/cart.ts";
import "./shop/carts.ts";

await new Kernable().app({ service: "shop", version: "1.0.0", domains: [shopDomain] }).run();
```

`new Kernable()` reads `KERNABLE_URL` (default `http://127.0.0.1:7233`), `KERNABLE_KEY` (default `app.key`, with `app.grant` next to it) and `KERNABLE_SERVICE`. `app()` applies the service's membership (what the kernel waits for) and runs every aggregate, read model and reactor of its domains.

## 3. Patterns

### Aggregate

```ts
import { Rejected, domain } from "@kernable/sdk";
export const shopDomain = domain("shop");

type Cart = { items: number; total: number; checkedOut: boolean };
export const cartAggregate = shopDomain.aggregate<Cart>("cart", { items: 0, total: 0, checkedOut: false }, { snapshotEvery: 100 })
  .apply("ItemAdded", (s, d: { qty: number; price: number }) => { s.items += d.qty; s.total += d.qty * d.price; })
  .apply("CheckedOut", (s) => { s.checkedOut = true; })
  .command("AddItem", (s, d: { sku: string; qty: number; price: number }) => {
    if (s.checkedOut) throw new Rejected("the cart is checked out", "checked_out");
    if (!(d.qty > 0)) throw new Rejected("quantity must be positive", "invalid_quantity");
    return [{ type: "ItemAdded", data: d }];
  })
  .command("CheckOut", (s, _d, cmd, ctx) => {
    if (s.items === 0) throw new Rejected("the cart is empty", "empty");
    return [{ type: "CheckedOut", data: { orderId: ctx.id(), at: ctx.now().toISOString(), total: s.total } }];
  });
```

- `apply(type, (state, data, evt, ctx) => …)` changes `state` in place (it is a copy) or returns a new state. No clock, no IDs, no I/O.
- `command(type, (state, data, cmd, ctx) => events)` returns events or throws `Rejected(message, code)`. Use `ctx.now()` and `ctx.id()`, never `Date.now()` or random IDs: a command may run again and must decide the same way.
- The state must survive a JSON round trip (snapshots store it as JSON).

### Read model with a query

```ts
const carts = new Map<string, { items: number; owner?: string; order?: string }>();
shopDomain.readModel("carts")
  .on("ItemAdded", (d: { qty: number }, evt, ctx) => {
    const cart = carts.get(evt.aggregateId) ?? { items: 0, owner: ctx.request.senderIdentity };
    cart.items += d.qty;
    carts.set(evt.aggregateId, cart);
  })
  .on("CheckedOut", (d: { orderId: string }, evt) => { carts.get(evt.aggregateId)!.order = d.orderId; })
  .query("GetCart", (d: { id: string }) => {
    const cart = carts.get(d.id);
    if (!cart) throw new Rejected(`no cart ${d.id}`, "not_found");
    return cart;
  });
```

- A read model kept in memory rebuilds from the first event on every start (`start: 0`). To keep it across restarts: `readModel("carts", { store: "sqlite:carts.db" })` and keep state in `ctx.store` (`get`, `set`, `remove`, `all`); raise `schema` when you change how it stores data.
- Handlers get events **at least once**: write them so that a repeated event changes nothing (overwrite, do not blindly add, or use a store).
- Another domain's events are qualified: `.on("billing.InvoicePaid", …)`; add `.use(billingDomain)` to get its event versions.

### Reactor with a follow-up command

```ts
billingDomain.reactor("invoicing").on("shop.CheckedOut", async (d: { orderId: string; total: number }, _evt, ctx) => {
  // Keeps the sender and the correlation; a rejection parks the event.
  await ctx.submit({
    aggregateType: "invoice", aggregateId: d.orderId, type: "CreateInvoice",
    data: { orderId: d.orderId, total: d.total },
    id: `invoice-${d.orderId}`,   // fixed ID: a redelivered event changes nothing
  });
});
```

- Reactors start at the newest event when they are new (they never react to history) and resume after their last acknowledgement.
- Submit follow-ups with `ctx.submit` (it needs a **fixed command ID** derived from the event, and follows the event's sender), query with `ctx.query`.
- A rejected follow-up throws: the reactor **parks** the event (after 5 failed attempts for other errors) and moves on; operators see it in `kernable listeners parked` and retry it. Catch the `Rejected` when a rejection is fine (`unique_taken` of an earlier attempt). A read model never parks.
- Side effects outside kernable (mail, HTTP) happen at least once: make them idempotent.

### Middleware

```ts
const known = new Set<string>();
identityDomain.readModel("rbac")
  .on("CustomerRegistered", (_d, evt) => void known.add(evt.aggregateId))
  .middleware({ types: ["shop.*"], order: 0 }, (check) => {
    const who = check.request.senderIdentity;
    if (!who) throw new Rejected("who are you?", "unauthenticated");
    if (!known.has(who)) throw new Rejected(`${who} is no customer`, "unknown_customer");
  });
```

- Decide in memory from what the middleware learned from events; no database or network calls.
- `when: "after"` runs after a request: it may refuse a query's answer, and observes a command's outcome.
- A middleware fails closed: while it is away, what it checks is refused (`UNAVAILABLE`).

### Client: submit and query

```ts
const kernable = new Kernable({ context: { senderIdentity: "alice" } });
const outcome = await kernable.submit({ domain: "shop", aggregateType: "cart", aggregateId: "c1", type: "AddItem", data: { sku: "apple", qty: 2, price: 3 } });
if (!outcome.committed) console.log(outcome.code, outcome.message);   // a rejection is a result, not an exception
const cart = await kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });   // already shows the item
```

- `submit` waits until settled (default); `wait: "committed"` returns once stored; `enqueue` returns only the ID.
- `id` makes a submit idempotent: the same ID returns the original outcome and never runs twice.
- `query` throws `Rejected` when the read model rejects.
- `context` carries who the request is for (`senderIdentity`, `senderTenant`, …); middleware decides on it. `attributes` are business data only.

### Personal data and secrets

```ts
shopDomain.command("Register").personal(["email", "name"], (_d, id) => id).secret(["password"]);
shopDomain.event("CustomerRegistered", 1).personal(["email", "name"], (_d, id) => id);
shopDomain.event("CustomerErased", 1).erases((_d, id) => id);
// a client that submits these commands: new Kernable().use(shopDomain)
```

Personal fields go to the vault (the log keeps references), secret fields only until their command completes, and an erasing event deletes a person's values everywhere in its tenant. Only top-level fields. Files and cache values are not erased with the person.

### Changing an event's format

```ts
shopDomain.event("ItemAdded", 2).upcast(1, (d: { sku: string; quantity: number }) => ({ sku: d.sku, qty: d.quantity }));
```

Never change or delete stored events. Raise the version and add an upcast from each older one; deploy readers before writers. A new optional field that every reader can ignore needs no new version.

## 4. Python and Rust

Same concepts, same order of arguments. The tested examples in all three languages: https://kernable.com/docs/tutorial/getting-started

| | TypeScript | Python | Rust |
|---|---|---|---|
| domain | `domain("shop")` | `domain("shop")` | `domain("shop")` |
| aggregate | `.aggregate("cart", initial)` | `.aggregate("cart", initial)` | `.aggregate("cart", Cart::default())` (state: `Clone + Default + Serialize + Deserialize`) |
| handlers | `.apply(type, fn)`, `.command(type, fn)` | decorators: `@cart.apply("ItemAdded")`, `@cart.command("AddItem")` | `.apply(type, \|state, data: T, evt, ctx\| …)`, `.command(type, \|state, data: T, cmd, ctx\| Ok(vec![NewEvent::new(type, data)]))` |
| reject | `throw new Rejected(msg, code)` | `raise Rejected(msg, code)` | `Err(Rejected::new(msg).code(code))` |
| read model | `.readModel("carts")` | `.read_model("carts")` | `.read_model("carts")` |
| handlers on it | `.on(type, (data, evt, ctx) => …)` | `@carts.on("ItemAdded")` with `(data, evt)` | `.on(type, \|data: T, evt, ctx\| async { Ok(()) })` |
| query | `.query(type, (data) => …)` | `@carts.query("GetCart")` | `.query(type, \|data: T, qry, ctx\| async { Ok(answer) })` |
| middleware | `.middleware({ types, order }, fn)` | `@rbac.middleware(["shop.*"], order=0)` | `.middleware(MiddlewareOptions::new(&["shop.*"]), \|check\| async { … })` |
| client | `new Kernable()` | `Kernable()` | `Kernable::new("app.key")?` |
| submit | `kernable.submit({ … })` | `k.submit(domain=, aggregate_type=, aggregate_id=, type=, data=, id=, context=)` | `kernable.submit(Submit::new(domain, aggregate_type, id, type).data(json!(…)).id(…).context(…)).await?` |
| query | `kernable.query({ domain, type, data })` | `k.query(domain=, type=, data=)` | `kernable.query(Query::new(domain, type).data(json!(…))).await?` |
| app | `kernable.app({ service, version, domains }).run()` | `k.app(service=, version=, domains=[…]).run()` (blocks) | `kernable.app(App::new().service(…).version(…).domain(&d))?.run().await?` |
| field names | `aggregateId`, `senderIdentity` | `aggregate_id`, `sender_identity` | `aggregate_id`, `sender_identity` |

Python runs handlers in threads and `run()` blocks; Rust is async (Tokio) and keeps handler state in `Arc<Mutex<…>>`.

## 5. Rules: always and never

Always:
- Decide only from the aggregate's own state and the command's data; use `ctx.now()` and `ctx.id()`.
- Name events in the past tense (`ItemAdded`), commands in the imperative (`AddItem`), queries as questions (`GetCart`); domains and aggregates lowercase.
- Keep aggregates small: one is the unit of consistency; rules across aggregates go through a reactor and follow-up commands.
- Declare values that must be unique across aggregates as `unique` keys, and conditions on another aggregate as a `fact` it holds and a command `requires`: the kernel checks both at commit. Never build a registry of claims or check a read model before a submit for this.
- A requirement names the other aggregate by its ID, or by a unique key and value (`{ aggregateType: "vehicle", key: "isin", value, fact: "open" }`, normalised as the key is), so no lookup of the ID is needed first; `absent: true` requires the fact not to be held (an aggregate that does not exist holds none). Omit `domain` for the command's own.
- Give follow-up and retried commands a fixed ID; submit them from reactors with `ctx.submit`.
- Mark personal data `personal` and passwords or tokens `secret`.
- Run everything through `app()` (or apply the membership once per deploy) so the kernel knows what to wait for.

Never:
- Never call the clock, random IDs, the network, a database or a query from a command handler or `apply`.
- Never write another domain's events: react to them through a listener instead.
- Never read another process's state directly: ask a query through the kernel.
- Never change, delete or "fix" stored events: add new ones, or raise the version with an upcast.
- Never put personal data into IDs, event types or the request context: they are not erased.
- Never let a listener ask its own read model, or a middleware send what it checks itself: the kernel refuses it.

## 6. When something goes wrong

| You see | Cause | Do |
|---|---|---|
| `NOT_FOUND` on submit | no worker registered the aggregate or command type, and no membership lists it (often a typo) | start the worker or app first; check domain, aggregate type and command type |
| `UNAVAILABLE` | a middleware that checks the request is away, or no read model answers the query | start it; the UI (http://127.0.0.1:7234) shows what is missing |
| `PERMISSION_DENIED` | the key's grant does not allow it | with `--dev`, app.key may do everything; in production, grant it (`kernable grant`) |
| `NotSettled` | a listener did not process the events in time (default 5 s); the command **is** committed | find the listener that is behind (UI, `/ready`); fix its handler |
| rejected with `handler_error` | a command handler threw something other than `Rejected` | fix the bug; resubmit with a new command ID |
| rejected with `version_mismatch` | `expectedVersion` in the context did not match | reload and retry |
| a listener restarts in a loop | its handler throws on one event | its log shows the error and `connected again at position N`; it is stuck at the event after N |
| a process stops at an event | the event has a newer schema than the process knows | deploy the reader with the new event version |

## 7. Verify your work

1. **Unit-test the domain without a kernel**: fold events and call handlers.
   ```ts
   const state = cartAggregate.fold([{ type: "ItemAdded", schema: 1, data: { sku: "a", qty: 2, price: 3 } }]);
   // then call the command handler on that state and assert the events or the rejection
   ```
2. **Run against a real kernel**: `kernable start --dev --store memory: --listen 127.0.0.1:17233` in a temporary directory, point `KERNABLE_URL` and `KERNABLE_KEY` at it, start the app, `await app.ready`, then submit and query. Settled submits mean the next query must see the change.
3. **Look**: the UI at http://127.0.0.1:7234 shows the map of services, aggregates, events and listeners, the connections, and every aggregate's history with its commands.
4. **Check the log**: `kernable verify kernable.db kernel.pub` recomputes the hash chain and every signature.

## 8. Where to read more

- Concepts: https://kernable.com/docs/concepts/ (architecture, event sourcing, domains, ordering, trust, queries and middleware, data, failures)
- How-to guides: https://kernable.com/docs/how-to/
- Reference (SDKs, CLI, flags, protocol, errors): https://kernable.com/docs/reference/
- Everything in one file: https://kernable.com/docs/llms-full.txt
