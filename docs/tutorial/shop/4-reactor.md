# 4. A reactor

A **reactor** is a listener that acts on events: it sends emails, calls other systems, or sends follow-up commands. Here it confirms every submitted order to the customer's email address.

## Confirming an order

The order gets a command and an event for the confirmation, in `domain.ts`:

::: code-group
<<< @/examples/shop/ts/domain.ts#confirm [domain.ts]
<<< @/examples/shop/py/domain.py#confirm [domain.py]
<<< @/examples/shop/rs/src/domain.rs#confirm [domain.rs]
<<< @/examples/shop/go/domain.go#confirm [domain.go]
:::

and the orders read model learns about it, in `read-models.ts`:

::: code-group
<<< @/examples/shop/ts/read-models.ts#confirmed [read-models.ts]
<<< @/examples/shop/py/read_models.py#confirmed [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#confirmed [read_models.rs]
<<< @/examples/shop/go/read-models/main.go#confirmed [read-models/main.go]
:::

## The reactor

::: code-group
<<< @/examples/shop/ts/confirmations.ts#reactor [confirmations.ts]
<<< @/examples/shop/py/confirmations.py#reactor [confirmations.py]
<<< @/examples/shop/rs/src/bin/confirmations.rs#reactor [confirmations.rs]
<<< @/examples/shop/go/confirmations/main.go#reactor [confirmations/main.go]
:::

Three things make it safe:

- **Order.** Read models have order 0, reactors order 10. A listener gets an event only after every listener with a lower order has processed it, so the reactor's queries always find the order and the customer.
- **A fixed command ID.** Delivery is at least once: after a crash the reactor may get the event again. With `confirm-<order>` as the ID, the second confirmation returns the first one's outcome. `ctx.submit` refuses a follow-up without one.
- **`ctx.submit`** gives the follow-up command the sender and correlation of the command that caused the event (`ctx.follow()`), so you can trace a confirmation back to the submit. Its queries (`ctx.query`) stay the reactor's own.

If the confirmation is rejected, `ctx.submit` throws, and the reactor **parks** the event: it moves on to the next order, and the kernel keeps the event with the rejection until someone looks at it (`kernable listeners parked`, the [UI](../../how-to/monitor#the-ui)) and retries or dismisses it. A handler that fails for another reason (a bug, a mail server that is down) gets the event again a few times first; while the kernel is unreachable it simply waits. See [Failures](../../concepts/failures).

Its own submit settles too, but only on the listeners before it: a listener handles one event at a time, so the kernel never makes it wait for itself.

Start it (`node confirmations.ts`, `.venv/bin/python confirmations.py`, `cargo run -q --bin confirmations` or `go run ./confirmations`) and submit an order:

::: code-group
<<< @/examples/shop/ts/part4.ts#try [part4.ts]
<<< @/examples/shop/py/part4.py#try [part4.py]
<<< @/examples/shop/rs/src/bin/part4.rs#try [part4.rs]
<<< @/examples/shop/go/part4/main.go#try [part4/main.go]
:::

::: code-group
<<< @/examples/shop/ts/part4.out{text} [TypeScript]
<<< @/examples/shop/py/part4.out{text} [Python]
<<< @/examples/shop/rs/part4.out{text} [Rust]
<<< @/examples/shop/go/part4.out{text} [Go]
:::

The `Submit` settled after the reactor, which had confirmed the order by then: the query shows it confirmed.

Next: [5. A second language](./5-languages).
