# Remove a listener, middleware or aggregate

The kernel remembers every listener that ever connected, with its position. A listener you stopped using still counts:

- listeners with a higher `order` get no events past its position;
- submits that settle wait for it, until `--settle-timeout-ms`;
- `/ready` answers 503;
- if it was a middleware, everything it checked is refused.

It also remembers every aggregate a worker registered. An aggregate you stopped running still counts: its commands wait in the queue for a worker that never comes, and `/ready` answers 503.

## Apply the service's membership

Workers and listeners register themselves whenever they connect. Which of them the kernel waits for is a separate list: the service's **membership**. Your deploy applies it, once per deploy:

```ts
// apply.ts, run by the deploy before or after starting the new version
const kernable = new Kernable({ service: "shop" }); // or KERNABLE_SERVICE=shop
await kernable.apply({
  workers: [cartAggregate],
  listeners: [cartsReadmodel, mailsReactor],
  middlewares: [guardReadmodel],
  metadata: { version: "1.4.0" },
});
```

If the service is simply made of whole domains, let the SDK build the membership: `app()` lists every aggregate, read model and reactor defined on the domains (live ones excepted, those that check requests as middlewares), applies it, and runs all of them:

```ts
await new Kernable().app({ service: "shop", version: "1.4.0", domains: [shopDomain, billingDomain] }).run();
```

One service is one app: two different processes with the same service would replace each other's membership.

Or from the pipeline, with a key granted `manage=shop`:

```sh
kernable apply 127.0.0.1:7233 deploy.key shop.json
# shop.json: {"service": "shop", "workers": ["shop.cart"], "listeners": ["carts", "mails"],
#             "middlewares": ["guard"], "metadata": {"version": "1.4.0"}}
```

The processes of the service name it (`service` option or `KERNABLE_SERVICE`). Then:

- **Listed** workers and listeners count, also before they ever connected: `/ready` waits for them, and commands of a listed aggregate wait in the queue until its worker comes.
- **Not listed** ones of the service are **dynamic**. They run, but nobody waits for them. A dynamic listener keeps its position; a dynamic aggregate is forgotten once its last worker leaves.
- **Middlewares** must be listed as such: a listener of the service that is not listed under `middlewares` cannot connect as one. Taking a middleware off the list removes its check. A check never appears or disappears without a signed membership, so a missing process never switches security off: while a listed middleware is away, what it checks is refused.

So to remove a listener, an aggregate or a middleware, apply a membership without it. Apply it **once per deploy**, not from every process: the latest apply wins, and an old replica that restarted during a rolling deploy would otherwise bring the old list back.

Processes without a service, and services without a membership, keep the simple rule: everything registered counts. `kernable services` shows each service's membership, what it lists that never registered, and its dynamic listeners.

## Remove it for good

With the kernel stopped:

```sh
kernable listeners sqlite:shop.db                     # name, order, position, types, service, middleware, last seen
kernable listeners remove sqlite:shop.db old-orders
kernable workers sqlite:shop.db                       # aggregate, command types, key, service, last seen
kernable workers remove sqlite:shop.db shop.Wishlist
```

`last seen` tells you when a listener, or a worker of an aggregate, was last connected. Then start the kernel.

## Listeners that need no registration

A **live** listener (`live: true`) is never registered: it has no stored position, starts at the newest event, nobody waits for it, and the kernel forgets it when it disconnects. Use it for reactions that only matter while the process runs, such as pushing updates to connected browsers. It cannot be a middleware.
