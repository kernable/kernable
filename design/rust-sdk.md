# Rust SDK

Status: built. Updated 2026-11-02 for parked events and `ctx.submit`/`ctx.query` and requirements by key.

## Problem

Business logic can be written in TypeScript and Python. The kernel, its CLI and the bench tool are Rust, and `kernable-core` already holds the protocol types, every signed message (checked against the shared test vectors) and the transport with mutual TLS. What is missing is the SDK layer on top: domains, aggregates, workers, listeners, queries, middleware, the vault and read-model stores.

## Decision

A crate `kernable` in `sdks/rust`, part of the workspace, built on `kernable-core`, with the scope of the TypeScript SDK and the same tests. It is async (tokio, which tonic needs anyway), typed through serde, and takes closures like the other SDKs.

```rust
use kernable::{domain, Kernable, NewEvent, Rejected};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Cart { items: u32, checked_out: bool }

#[derive(Serialize, Deserialize)]
struct ItemAdded { sku: String, qty: u32 }

let shop_domain = domain("shop");
let cart_aggregate = shop_domain.aggregate("cart", Cart::default());
cart_aggregate.apply("ItemAdded", |state, data: ItemAdded, _evt, _ctx| state.items += data.qty);
cart_aggregate.command("AddItem", |state, data: ItemAdded, _cmd, _ctx| {
    if state.checked_out {
        return Err(Rejected::new("Cart is checked out"));
    }
    Ok(vec![NewEvent::new("ItemAdded", data)])
});
let carts_readmodel = shop_domain.read_model("carts");
carts_readmodel.on("ItemAdded", |data: ItemAdded, _evt, _ctx| async move { /* … */ Ok(()) });

let kernable = Kernable::new("app.key")?;
tokio::join!(kernable.worker(&cart_aggregate).run(), kernable.listener(&carts_readmodel).run());
```

## Shape

- **Definitions are shared handles.** `Domain`, `Aggregate<S>`, `Listener`, `ListenerHandle` and `Worker` are cheap to clone (an `Arc` inside) and take `&self`, so they can be built in separate statements, as the guides do in TypeScript and Python, or chained. A domain's event and command schemas may be declared after its aggregates.
- **Typed state and data.** The state `S` is `Clone + Serialize + DeserializeOwned`; snapshots store it as JSON. Handlers name the type of their data (`data: ItemAdded`); a payload that does not deserialize fails the command (`handler_error`) or the listener's batch. Upcasts work on `serde_json::Value`, since old payload versions usually have no type of their own.
- **`apply(type, |state: &mut S, data: D, evt: &EventMeta, ctx: &ApplyContext| …)`** changes the state in place; `*state = …` replaces it. `ctx.request` is the causing command's request context; there is no clock and no ID source.
- **`command(type, |state: &S, data: D, cmd: &CommandMeta, ctx: &mut CommandContext| -> Result<Vec<NewEvent>, Rejected>)`.** Command handlers are synchronous: they must be deterministic and free of side effects. `ctx.now()` and `ctx.id()` as in the other SDKs. A panic is reported as `handler_error`.
- **Listeners are defined on a domain, like aggregates, and run through the connection, like workers.** `domain.read_model(name)` and `domain.reactor(name)` give a `Listener` definition (no connection); `kernable.listener(&listener)` gives a `ListenerHandle` with `run()`, `stop()`, `ready()`, `position()` and `opened_store()`. A type without a domain (`ItemAdded`, `GetCart`) is the listener's own domain's; another domain's is qualified (`identity.AccountRegistered`, `identity.*`); the SDK sends qualified types, so the protocol is unchanged. Middleware's `*` still means everything. The own domain's event schemas upcast its events without `with_domain`, which adds other domains.
- **`on(type, |data: D, evt: StoredEventMeta, ctx: ListenerContext| async …)`** and **`query(type, |data: D, qry: QueryMeta, ctx: QueryContext| async …)`**: payload, meta, context, like `apply` and `command`. `ctx.request` is the causing command's (an event) or the asker's (a query) request context.
- **Listener, query and middleware handlers are async** (they may submit and query) and return `Result<_, BoxError>`; returning a `Rejected` rejects with its code, any other error is `handler_error` / `middleware_error`.
- **Failing event handlers.** A reactor parks an event whose handler returned a `Rejected` at once, and one that failed otherwise after `park_after` attempts (default 5); the batch's other events commit, each in a savepoint of the listener's store. Errors meaning the kernel is away or busy (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `NotSettled`) and store failures never park: the batch is delivered again. A read model never parks (`park_after` 0), nor an erasing event.
- **Requests from handlers** name their listener (the `listener` field), as in the other SDKs, through a task-local. `ctx.submit(Submit)` sends a follow-up command: it needs a fixed ID, defaults domain, tenant and context from the event, and returns the events or fails with the rejection, which parks the event. `ctx.query(Query)` defaults the tenant and keeps the client's own context.
- **Unique keys and requirements.** `aggregate.unique(name, …)` and `aggregate.fact(name, …)`; a command's `requires` returns `Requirement`s: `Requirement::of(type, id, fact)` in the command's own domain, `Requirement::by_key(domain, type, key, value, fact)` naming the aggregate by a unique key (the value normalised as the key function does; the SDK sends its hash), `.absent()` for a fact that must not be held. A rejection carries the kernel's `key` or `requirement`.
- **Errors.** `kernable::Error` for client calls: transport, rejected queries, `NotSettled` (with the committed events), invalid configuration. Definition errors (an invalid domain name, a qualified event type) panic, like an invalid literal regex would.
- **Connection.** `Kernable::new(options)` (or `Kernable::new("app.key")` with the defaults) is synchronous and connects lazily, so processes can start before the kernel and reconnect after it restarts. `Options` defaults: `KERNABLE_URL`, else `http://127.0.0.1:7233`; the grant from `KERNABLE_GRANT` or next to the key file.
- **Stores** for read models: `sqlite:<path>` or `memory:`, with the TypeScript store's tables, so either SDK can open the other's file.
- **Names.** Rust variables stay `snake_case` (`let shop_domain = domain("shop")`); the domain and type names in strings follow the convention.

## Order of work

1. Client: submit, enqueue, query, subscribe; options, TLS, grants.
2. Worker: aggregates, snapshots, upcasts.
3. Listeners: read models and reactors, queries, stores.
4. Middleware.
5. Vault: personal and secret fields, erasing events.
6. Docs: a Rust tab next to TypeScript and Python, the reference, `make check`.

Each step ports the TypeScript tests that cover it, against the real kernel binary.
