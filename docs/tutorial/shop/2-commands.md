# 2. Aggregates and commands

An **aggregate** is one thing your business makes decisions about: a customer, an order. It has a **state**, built from its **events**, and **commands** that ask it to change. A command handler looks at the state and either returns new events or rejects the command. Nothing else changes state: the events are the truth, and the kernel stores them forever.

Aggregates belong to a **domain**, here `shop`. Types are unique within a domain and named `shop.Register` elsewhere.

## The domain

Start `domain.ts` (or `domain.py`) with the domain and the customer:

::: code-group
<<< @/examples/shop/ts/domain.ts#domain [domain.ts]
<<< @/examples/shop/py/domain.py#domain [domain.py]
:::

::: code-group
<<< @/examples/shop/ts/domain.ts#customer [domain.ts]
<<< @/examples/shop/py/domain.py#customer [domain.py]
:::

`apply` turns an event into the next state; `command` decides. `Rejected` carries a message and a code (`exists`, `invalid`) your callers can act on.

Add the order:

::: code-group
<<< @/examples/shop/ts/domain.ts#order [domain.ts]
<<< @/examples/shop/py/domain.py#order [domain.py]
:::

Handlers must be **deterministic** and free of side effects: the kernel may run a command again (after a crash, or when another command changed the aggregate in between), and it must decide the same way.

## The worker

A **worker** runs the commands of its aggregates. It connects, tells the kernel which command types it handles, and waits:

::: code-group
<<< @/examples/shop/ts/worker.ts#worker [worker.ts]
<<< @/examples/shop/py/worker.py#worker [worker.py]
:::

::: code-group
```sh [TypeScript]
node worker.ts
```
```sh [Python]
.venv/bin/python worker.py
```
:::

You can start more workers; the kernel spreads the commands over them and never runs two commands of one aggregate at a time.

## Send commands

::: code-group
<<< @/examples/shop/ts/part2.ts#try [part2.ts]
<<< @/examples/shop/py/part2.py#try [part2.py]
:::

::: code-group
<<< @/examples/shop/ts/part2.out{text} [TypeScript]
<<< @/examples/shop/py/part2.out{text} [Python]
:::

What happened:

- Each committed command reports the aggregate's new **version**: one per event.
- `register again` and `add after submit` were **rejected** by the handlers. A rejected command is stored as rejected, like a committed one.
- `same command again` used the same command **ID** (`add-1`) and got the original outcome back: the item was added once. Give a command a fixed ID when you might send it twice, for example when retrying after a timeout.

Next: [3. Read models and queries](./3-queries).
