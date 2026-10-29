# Remove a listener, middleware or aggregate

Use this page when you delete or rename a read model, reactor, middleware or aggregate. Taking it out of the code is not enough: the kernel remembers every listener that ever connected, with its position, and every aggregate a worker registered. One that stops running still **counts** ([Who counts](../concepts/ordering#who-counts)):

- listeners with a higher `order` get no events past its position;
- submits that settle wait for it, until `--settle-timeout-ms`, then throw `NotSettled`;
- `/ready` answers 503 (`waiting for listeners: old-orders`);
- if it was a middleware, everything it checked is refused;
- if it was an aggregate, its commands wait in the queue for a worker that never comes.

## Steps

1. **Delete it from the code** and deploy.
2. **Apply the service's membership without it** (below). With `app()`, step 1 already does this: the new version applies a membership built from its domains when it starts.
3. **Check** with `kernable services <store>`, or in the kernel UI. The service no longer lists it, and `/ready` answers 200.
4. Optionally, **remove it for good** (below), so it no longer shows up and its name is free.

## Apply the service's membership

Workers and listeners register themselves whenever they connect. Which of them the kernel waits for is a separate, signed list: the service's **membership**. Your deploy applies it, once per deploy:

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

If the service is simply made of whole domains, let the SDK build the membership: `app()` lists every aggregate, read model and reactor defined on the domains (live ones excepted, and those that check requests as middlewares), applies it, and runs all of them:

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

Workers are named `domain.aggregate`, listeners and middlewares by their name. The processes of the service name it too (`service` option or `KERNABLE_SERVICE`). Then:

- **Listed** workers and listeners count, also before they ever connected: `/ready` waits for them, and commands of a listed aggregate wait in the queue until its worker comes.
- **Not listed** ones of the service are **dynamic**. They run, but nobody waits for them. A dynamic listener keeps its stored position; a dynamic aggregate is forgotten once its last worker leaves, and its commands are refused from then on.
- **Middlewares** must be listed as such: a listener of the service that is not listed under `middlewares` cannot connect as one. Taking a middleware off the list removes its check. A check never appears or disappears without a signed membership, so a missing process never switches security off: while a listed middleware is away, what it checks is refused.

Apply it **once per deploy**, not from every process: the latest apply wins, and an old replica that restarted during a rolling deploy would otherwise bring the old list back.

Processes without a service, and services without a membership, keep the simple rule: everything registered counts. To remove one of those, remove it for good.

`kernable services <store>` shows each service's membership, what it lists that never registered, and its dynamic listeners:

```
shop version=1.4.0 (applied … by …)
  worker shop.cart
  listener carts
  listener mails
  middleware guard
  dynamic listener old-orders
```

## Remove it for good

A dynamic listener no longer holds anything back, but the kernel still keeps its name and position. If you later start a reactor under the same name, it continues from that old position. To forget it, stop the kernel and run:

```sh
kernable listeners sqlite:shop.db                     # name, order, position, types, service, middleware, last seen
kernable listeners remove sqlite:shop.db old-orders
kernable workers sqlite:shop.db                       # aggregate, command types, key, service, last seen
kernable workers remove sqlite:shop.db shop.wishlist
```

`last seen` tells you when a listener, or a worker of an aggregate, was last connected; a listener its service no longer lists shows `(not listed: counts for nobody)`. Then start the kernel. Removing a listener also removes its middleware. Commands for a removed aggregate are refused.

## Listeners that need no registration

A **live** listener (`live: true`) is never registered: it has no stored position, starts at the newest event, nobody waits for it, and the kernel forgets it when it disconnects. Use it for reactions that only matter while the process runs, such as pushing updates to connected browsers. There is nothing to remove. It cannot be a middleware.
