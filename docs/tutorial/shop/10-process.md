# 10. A business process

A confirmed order is not done: the customer pays, then the shop ships, and an order nobody pays
within three days is cancelled. You could build this from a reactor, a scheduled command and a read
model that remembers where each order stands. Here you define it once as a **business process**;
the kernel runs it, and its UI shows where every order stands.

This part builds on the shop of parts 2 to 7 (the worker, the read models, the reactor and the
guard), without the kit of part 9.

## Paying, shipping, cancelling

The order gets three commands and their events, in `domain.ts`:

::: code-group
<<< @/examples/shop/ts/domain.ts#fulfil [domain.ts]
<<< @/examples/shop/py/domain.py#fulfil [domain.py]
<<< @/examples/shop/rs/src/domain.rs#fulfil [domain.rs]
:::

and the orders read model follows them, in `read-models.ts`:

::: code-group
<<< @/examples/shop/ts/read-models.ts#fulfilled [read-models.ts]
<<< @/examples/shop/py/read_models.py#fulfilled [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#fulfilled [read_models.rs]
:::

## The process

Also in `domain.ts`, next to the aggregates:

::: code-group
<<< @/examples/shop/ts/domain.ts#process [domain.ts]
<<< @/examples/shop/py/domain.py#process [domain.py]
<<< @/examples/shop/rs/src/domain.rs#process [domain.rs]
:::

Read it from the top:

- **`start`:** when an order is confirmed (the reactor of part 4 confirms it), a **case** starts
  for it. The order's ID is the case's key: one running case per order. The case keeps it as
  `orderId`.
- **`wait("paid")`:** the case waits for `OrderPaid` of the same order. If none comes within
  `PAY_WITHIN` (three days), it goes to `cancel` instead.
- **`command("ship")`:** the kernel sends `Ship` to the order and, once it committed, goes on to
  `shipped`.
- **`command("cancel")`:** sends `Cancel` with the reason `unpaid`, then goes on to the next step
  written, the end `unpaid`.
- **`end`:** the two ways a case ends. The UI counts cases by them.

Each step has a name: the UI, the statistics and a later version of the process know it by its
name. [Business processes](../../concepts/processes) lists every kind of step.

## Let the process act

The kernel sends the process's commands as `process:shop.fulfilment`, and the guard of part 6
refuses anyone who is not the order's customer. Tell it what the process may send, in `guard.ts`:

::: code-group
<<< @/examples/shop/ts/guard.ts#guard [guard.ts]
<<< @/examples/shop/py/guard.py#guard [guard.py]
<<< @/examples/shop/rs/src/bin/guard.rs#guard [guard.rs]
:::

Without it, the first `Ship` would be refused `forbidden`, and the case would stop there as an
**incident**: nothing lost, the case waits at `ship` until you fix the cause and run
`kernable processes retry`.

## Register it

The kernel runs the process once it is registered, signed with a key that may send what the
process sends. An app (`kernable.app`) registers the processes of its domains itself; the shop runs
its parts separately, so it registers the process in a script of its own, once per deploy, after
the worker is up:

::: code-group
<<< @/examples/shop/ts/fulfilment.ts#register [fulfilment.ts]
<<< @/examples/shop/py/fulfilment.py#register [fulfilment.py]
<<< @/examples/shop/rs/src/bin/fulfilment.rs#register [fulfilment.rs]
:::

Restart the worker, the read models and the guard, then register the process with a short time
to pay, to see an unpaid order cancelled:

```sh
PAY_WITHIN=10s node fulfilment.ts    # or .venv/bin/python fulfilment.py, or cargo run --bin fulfilment
```

It prints `shop.fulfilment version 1`. Registering the same process again changes nothing; a
changed one becomes version 2, and running cases finish on the version they started with.

## Try it

Two orders: Ann pays the first, not the second.

::: code-group
<<< @/examples/shop/ts/part10.ts#try [part10.ts]
<<< @/examples/shop/py/part10.py#try [part10.py]
<<< @/examples/shop/rs/src/bin/part10.rs#try [part10.rs]
:::

::: code-group
<<< @/examples/shop/ts/part10.out{text} [TypeScript]
<<< @/examples/shop/py/part10.out{text} [Python]
<<< @/examples/shop/rs/part10.out{text} [Rust]
:::

The script ends after ten seconds: `o2` waited for its payment until its time was up.

## Watch it

Open the UI (http://127.0.0.1:7234) and **Processes**: the fulfilment as a diagram, from the
confirmed order on the left to its two ends on the right, each path as wide as the orders that
took it, with how many are at each step now. Above it: how many ended shipped and unpaid, and how
long orders waited for their payment. Click `paid` to list the orders waiting for it, and a case
to read its story: what it waited for, what it sent and how it ended, each linked to the order's
history. Searching for `o1` in the top bar finds the order and its case.

The case's events are in the log like any other, written and signed by the kernel:

```sh
kernable processes cases kernable.db shop.fulfilment
kernable processes case kernable.db shop.fulfilment o1#1
```

Next, see [Business processes](../../concepts/processes) for decisions, queries and timers, and
what happens on a rejection.
