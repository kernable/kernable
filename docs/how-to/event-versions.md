# Change an event's format

Use this page when an event must carry something different: a field renamed, removed, split, or of another type. Stored events are never rewritten. You raise the event's version and tell every reader how to turn the old payload into the new one ([Event versions](../concepts/data#event-versions)).

## Steps

1. **Declare the new version and an upcast** on the domain:

   ::: code-group
   <<< @/examples/concepts/upcast.ts#upcast{ts} [TypeScript]

   ```python [Python]
   shop_domain = domain("shop")
   shop_domain.event("ItemAdded", 2).upcast(1, lambda d: {"sku": d["sku"], "count": d["qty"]})
   ```

   ```rust [Rust]
   let shop_domain = domain("shop");
   shop_domain
       .event("ItemAdded", 2)
       .upcast(1, |d| json!({ "sku": d["sku"], "count": d["qty"] }));
   ```
   :::

   One upcast per old version: `upcast(1, …)` turns 1 into 2, `upcast(2, …)` turns 2 into 3, and they chain. The domain's aggregates and listeners use them before `apply` and before their handlers. A listener of **another** domain that reads the event needs the domain too: `use(shopDomain)` (TypeScript, Python) or `with_domain(&shop_domain)` (Rust).

2. **Change the readers**: `apply` of the aggregate, and every read model, reactor and middleware that handles the event, now get the new payload only.

3. **Write the new format** in the command handlers.

4. **Deploy the readers first**, then the workers. Every process that reads the event needs the new domain definition, in whatever language it is written.

5. **Check** with `kernable schemas <store>`. It prints the highest stored version per event type, for example `shop.ItemAdded schema 2`.

## What happens during the deploy

- **A reader that meets a version it does not know never misreads it.** A read model skips the event and counts it (`kernable_listener_skipped`, the UI); a reactor parks it with the code `unknown_event`. Both log `shop.ItemAdded has schema 2, but this process knows it only up to 1` once per type and version. Once the process is updated, [rebuild](./rebuild-read-model) the read model (an in-memory one does on every start) and retry the reactor's parked events (`kernable listeners retry`).
- **A worker cannot write an older version** than the store holds. Once a version-2 event is stored, the kernel refuses an old worker's result, ends its session and puts the command back in the queue for a current worker. Old and new workers never write side by side. With [deployments](./deployments), an older deployment may write the older version again after a rollback, and the kernel refuses its commands on the aggregates that hold the newer one (`unknown_history`).

If readers and writers run in one app, deploy it as usual: old processes still running skip or park the new events until they are replaced.

## Pitfalls

- **A listener of another domain without `use`** knows the event only at version 1, so it skips (or parks) every version-2 event.
- **State shapes are separate.** If the change also changes an aggregate's state, raise the aggregate's `schema` so old snapshots are rebuilt. If it changes what a read model with a store keeps, raise the read model's `schema` ([Rebuild a read model](./rebuild-read-model)).
- **Avoid a new version** when you can: a new optional field that every reader can ignore or default needs none. **Use a new event type** when the meaning changes, not only the shape.

Archives and exports that must see events exactly as stored mark their handler `raw` (`{ raw: true }` in TypeScript, `raw=True` in Python, `on_raw` in Rust). They get every version unchanged, with `event.schema`.
