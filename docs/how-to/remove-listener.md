# Remove a listener, middleware or aggregate

The kernel remembers every listener that ever connected, with its position. A listener you stopped using still counts:

- listeners with a higher `order` get no events past its position;
- submits that settle wait for it, until `--settle-timeout-ms`;
- `/ready` answers 503;
- if it was a middleware, everything it checked is refused.

It also remembers every aggregate a worker registered. An aggregate you stopped running still counts: its commands wait in the queue for a worker that never comes, and `/ready` answers 503.

## Let the service retire it

Give the process a service name, and the kernel learns which workers and listeners the service still has:

```ts
const kernable = new Kernable({ service: "shop-api" }); // or KERNABLE_SERVICE=shop-api
const worker = kernable.worker(cartAggregate);
const carts = kernable.listener(cartsReadmodel);
const mails = kernable.listener(mailsReactor);
await Promise.all([worker.run(), carts.run(), mails.run()]);
```

Each worker declares all aggregates of its service when it connects, and each listener all listeners of its service, so create them all before running any. One service is one program: its replicas run the same workers and listeners.

When the next version of `shop-api` no longer has an **aggregate**, the kernel unregisters it as soon as a worker of the service connects: its commands are refused with `NOT_FOUND`, and `/ready` no longer waits for it. An aggregate that a worker still runs, such as an older version during a rolling deploy, stays registered.

When it no longer has a **listener**, the kernel **retires** it as soon as a listener of the service connects:

- nobody waits for it any more: higher orders, settling submits and `/ready` go on without it;
- it keeps its position: declared again, it resumes where it left off;
- **its middleware keeps refusing** what it covers, so a security check never switches itself off. Remove it explicitly (below) to drop the check.

Only the service's own aggregates and listeners of the same key are touched; those without a service, or of another service, never are. `kernable listeners` shows `service shop-api (retired)`.

## Remove it for good

With the kernel stopped:

```sh
kernable listeners sqlite:shop.db                     # name, order, position, types, service, middleware, last seen
kernable listeners remove sqlite:shop.db old-orders
kernable workers sqlite:shop.db                       # aggregate, command types, key, service, last seen
kernable workers remove sqlite:shop.db shop.Wishlist
```

`last seen` tells you when a listener, or a worker of an aggregate, was last connected. Then start the kernel. Renaming a listener or an aggregate is the same: the old name stays registered until its service drops it or you remove it.

## Listeners that need no registration

A **live** listener (`live: true`) is never registered: it has no stored position, starts at the newest event, nobody waits for it, and the kernel forgets it when it disconnects. Use it for reactions that only matter while the process runs, such as pushing updates to connected browsers. It cannot be a middleware.
