# 6. Authorization

So far anyone could do anything. Now customers may act only on themselves and their own orders.

The kernel's allowlists decide which **key** (which process) may send which command type. Who the **person** behind a request is, is business data: the request's **request context**, signed with the request, carries it as `senderIdentity`. A **middleware** checks every command and query against it before the kernel accepts it.

## The guard

A middleware is a listener with a check: it builds its knowledge from events, like a read model, and answers every check from it.

::: code-group
<<< @/examples/shop/ts/guard.ts#guard [guard.ts]
<<< @/examples/shop/py/guard.py#guard [guard.py]
:::

The check sees the command or query, with its data and request context, before the kernel stores or routes it. Throwing `Rejected` refuses it; a refused command is never stored. Middleware runs in a chain by `order`, and the kernel asks a middleware only once it has processed every event committed before, so it never decides on stale knowledge.

The registration is kept by the kernel: **while the guard is not running, every request it covers is refused**. Authorization fails closed.

## Try it

Start the guard (`node guard.ts` or `.venv/bin/python guard.py`) and send requests as different customers:

::: code-group
<<< @/examples/shop/ts/part6.ts#try [part6.ts]
<<< @/examples/shop/py/part6.py#try [part6.py]
:::

::: code-group
<<< @/examples/shop/ts/part6.out{text} [TypeScript]
<<< @/examples/shop/py/part6.out{text} [Python]
:::

The reactor still confirms orders: its follow-up commands carry the customer's identity (`follow()`), and its own queries come from `svc-confirmations`, a service the guard lets read.

In a real application the identity comes from your authentication, for example a verified token at your API; only keys you trust may assert it.

Next: [7. Personal data](./7-personal-data).
