# Test your domain

Aggregates are plain functions of events and commands ([Decide and fold](../concepts/event-sourcing#decide-and-fold)), so most of your domain can be tested without a kernel: fold past events into a state with the aggregate's `fold`, then call a command handler on that state and check the events it returns, or the rejection.

## Aggregates, without a kernel

::: code-group
<<< @/examples/how-to/order.test.ts#test [order.test.ts]
<<< @/examples/how-to/test_order.py#test [test_order.py]
<<< @/examples/shop/rs/tests/order.rs#test [tests/order.rs]
:::

These run with `node --test`, `python -m unittest` and `cargo test`. They test the [tutorial's](../tutorial/shop/2-commands) `order` aggregate.

- **Given** is the history you fold: events as `{ type, schema, data }` in TypeScript, `(type, schema, data)` tuples in Python, `FoldEvent::new(type, data)` in Rust (schema 1).
- **When** is the handler you call, with a fixed clock and fixed IDs in the context, so the result is the same on every run.
- **Then** is the list of new events, or a `Rejected` with its `code`.

`fold` upcasts every event first, as a worker does. To test an upcast ([Change an event's format](./event-versions)), fold an event at its old schema, such as `{ type: "ItemAdded", schema: 1, data: { sku: "apple", qty: 2 } }`, and check the state.

To test unique keys and facts, pass a folded state to the aggregate's `index`: it returns the keys and facts a worker would send for it ([Unique keys and conditions](../concepts/uniqueness#testing)).

## Read models, reactors and middleware, against a real kernel

These depend on the kernel: order, settling, queries and checks. Test them end to end:

1. Start a kernel that keeps nothing, in a temporary directory:

   ```sh
   cd "$(mktemp -d)"
   kernable start --dev --store memory: --listen 127.0.0.1:17233 --metrics-listen 127.0.0.1:17234
   ```

   `--dev` creates the keys there, among them `app.key` with a grant that may do everything.

2. Start your processes against it, with `KERNABLE_URL=http://127.0.0.1:17233` and `KERNABLE_KEY=<that directory>/app.key`.
3. Wait until they are connected: `await app.ready` in TypeScript, `app.ready.wait(5)` in Python, `.ready().await` in Rust. Do not sleep.
4. Send commands and queries as your API would. A submit settles by default, so the next query already sees its events.

Do not rely on `/ready` alone to wait for your processes: before they connect for the first time, the kernel does not know them, unless their service's membership was applied.

To see how kernable sets this up for its own tests, look at `sdks/typescript/test/kernel.ts`, `sdks/python/tests/kernel.py` and `sdks/rust/tests/common/mod.rs` in the repository. These helpers are not part of the SDKs.
