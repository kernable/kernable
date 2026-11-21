# 2. Aggregates and commands

An **aggregate** is one thing your business makes decisions about: a customer, an order. It has a **state**, built from its **events**, and **commands** that ask it to change. A command handler looks at the state and either returns new events or rejects the command. Nothing else changes state: the events are the truth, and the kernel stores them forever.

Aggregates belong to a **domain**, here `shop`: one area of the business, and in code one object that holds everything of it, its aggregates, read models and reactors, its event versions and which fields go to the vault. Types are unique within a domain and named `shop.Register` elsewhere. A domain writes only its own events and learns about other domains through listeners; [Domains](../../concepts/domains) explains the rules and a code layout for when it grows.

## The domain

Start `domain.ts` (or `domain.py`, `src/domain.rs`, `domain.go`) with the domain and the customer:

::: code-group
<<< @/examples/shop/ts/domain.ts#domain [domain.ts]
<<< @/examples/shop/py/domain.py#domain [domain.py]
<<< @/examples/shop/rs/src/domain.rs#domain [domain.rs]
<<< @/examples/shop/go/domain.go#domain [domain.go]
:::

::: code-group
<<< @/examples/shop/ts/domain.ts#customer [domain.ts]
<<< @/examples/shop/py/domain.py#customer [domain.py]
<<< @/examples/shop/rs/src/domain.rs#customer [domain.rs]
<<< @/examples/shop/go/domain.go#customer [domain.go]
:::

`apply` turns an event's data into the next state; `command` decides. `Rejected` carries a message and a code (`exists`, `invalid`) your callers can act on.

Add the order:

::: code-group
<<< @/examples/shop/ts/domain.ts#order [domain.ts]
<<< @/examples/shop/py/domain.py#order [domain.py]
<<< @/examples/shop/rs/src/domain.rs#order [domain.rs]
<<< @/examples/shop/go/domain.go#order [domain.go]
:::

Handlers must be **deterministic** and free of side effects: the kernel may run a command again (after a crash, or when another command changed the aggregate in between), and it must decide the same way.

## The worker

A **worker** runs the commands of its aggregates. It connects, tells the kernel which command types it handles, and waits:

::: code-group
<<< @/examples/shop/ts/worker.ts#worker [worker.ts]
<<< @/examples/shop/py/worker.py#worker [worker.py]
<<< @/examples/shop/rs/src/bin/worker.rs#worker [worker.rs]
<<< @/examples/shop/go/worker/main.go#worker [worker/main.go]
:::

::: code-group
```sh [TypeScript]
node worker.ts
```
```sh [Python]
.venv/bin/python worker.py
```
```sh [Rust]
cargo run -q --bin worker
```
```sh [Go]
go run ./worker
```
:::

You can start more workers; the kernel spreads the commands over them and never runs two commands of one aggregate at a time.

## Send commands

::: code-group
<<< @/examples/shop/ts/part2.ts#try [part2.ts]
<<< @/examples/shop/py/part2.py#try [part2.py]
<<< @/examples/shop/rs/src/bin/part2.rs#try [part2.rs]
<<< @/examples/shop/go/part2/main.go#try [part2/main.go]
:::

::: code-group
<<< @/examples/shop/ts/part2.out{text} [TypeScript]
<<< @/examples/shop/py/part2.out{text} [Python]
<<< @/examples/shop/rs/part2.out{text} [Rust]
<<< @/examples/shop/go/part2.out{text} [Go]
:::

What happened:

- Each committed command reports the aggregate's new **version**: one per event.
- `register again` and `add after submit` were **rejected** by the handlers. A rejected command is stored as rejected, like a committed one.
- `same command again` used the same command **ID** (`add-1`) and got the original outcome back: the item was added once. Give a command a fixed ID when you might send it twice, for example when retrying after a timeout.

Next: [3. Read models and queries](./3-queries).
