# 6. Authorization

So far any customer could do anything. Now customers may act only on themselves and their own orders.

Grants decide what a **key**, that is a process, may do: send `shop.Register`, run the shop's commands, read its events. They know nothing about the people using your shop. Who the **person** behind a request is, is business data: the request's **request context**, signed with the request, carries it as `senderIdentity`. A **middleware** checks every command and query against it before the kernel accepts it.

## The guard

A middleware is a listener with a check: it builds its knowledge from events, like a read model, and answers every check from it.

::: code-group
<<< @/examples/shop/ts/guard.ts#guard [guard.ts]
<<< @/examples/shop/py/guard.py#guard [guard.py]
<<< @/examples/shop/rs/src/bin/guard.rs#guard [guard.rs]
<<< @/examples/shop/go/guard/main.go#guard [guard/main.go]
:::

The check sees the command or query, with its data and request context, before the kernel stores or routes it. Throwing `Rejected` refuses it; a refused command is never stored. Middleware runs in a chain by `order`, and the kernel asks a middleware only once it has processed every event committed before, so it never decides on stale knowledge.

The guard's key needs the `check` action on the types it checks; `app`, which `--dev` lets do everything, has it. The registration is kept by the kernel: **while the guard is not running, every request it covers is refused**. Authorization fails closed.

## Try it

Start the guard (`node guard.ts`, `.venv/bin/python guard.py`, `cargo run -q --bin guard` or `go run ./guard`) and send requests as different customers:

::: code-group
<<< @/examples/shop/ts/part6.ts#try [part6.ts]
<<< @/examples/shop/py/part6.py#try [part6.py]
<<< @/examples/shop/rs/src/bin/part6.rs#try [part6.rs]
<<< @/examples/shop/go/part6/main.go#try [part6/main.go]
:::

::: code-group
<<< @/examples/shop/ts/part6.out{text} [TypeScript]
<<< @/examples/shop/py/part6.out{text} [Python]
<<< @/examples/shop/rs/part6.out{text} [Rust]
<<< @/examples/shop/go/part6.out{text} [Go]
:::

The reactor still confirms orders: its follow-up commands carry the customer's identity (`ctx.submit` follows the event), and its own queries (`ctx.query`) come from `svc-confirmations`, a service the guard lets read.

The kernel checks that the request was signed by a key with a grant to send it, not who `senderIdentity` names: any process that may submit can claim any identity. So in a real application the identity comes from your authentication, for example a verified token at your API, and only processes that verify it get a grant to submit. Part 8 gives each service its own key, so the reactor's key may submit nothing but `shop.Confirm`.

Next: [7. Personal data](./7-personal-data).
