# Rebuild a read model

A read model is derived from the events, so you can always build it again. Rebuild one after fixing a bug in a handler, when it should hold something new, or when its stored data changes shape.

## How

It depends on where the read model keeps its data ([Snapshots and read model stores](../concepts/data#snapshots-and-read-model-stores)):

- **In memory** (no `store`, the default): deploy the fixed version, or restart it. A read model starts at position 0 by default (`start: 0`), so it reads every event from the first on every start.
- **With a store** (`store: "sqlite:carts.db"`): a restart continues after the stored position, so a fix does not reach old data by itself. Raise its `schema` (`schema: "2"`; `schema="2"` in Python, `.store(url, "2")` in Rust). On start it clears the store and reads every event from the first. Raise it whenever the handlers store data differently, too.

  Or stop the process and delete the store file, with its `-wal` and `-shm` files. It starts empty, at position 0.

## While it rebuilds

It is **behind** until it reaches the events that were stored when it connected:

- queries with `minPosition` wait for it;
- submits that settle wait for it, up to `--settle-timeout-ms` (default 5 s), then throw `NotSettled`;
- listeners with a higher order, such as reactors, wait behind it;
- `/ready` answers 503 with `waiting for listeners: carts`, and the kernel UI shows it as behind, with its lag ([Monitor](./monitor)).

When `/ready` answers 200 again, it has caught up.

For a large log, rebuild outside peak hours, or **rebuild beside the old one**:

1. Run the new version under a **new name** (and, with a store, a new store file) next to the old one.
2. Wait until it caught up: `kernable_listener_lag` for it is near 0.
3. Switch the queries over to the new name.
4. Remove the old one ([Remove a listener, middleware or aggregate](./remove-listener)).

If the service has a membership that does not list the new name yet, the new read model is dynamic while it rebuilds: nobody waits for it. Add it to the membership when you switch over. `app()` lists every read model of its domains, so there the new one counts from its first start.

## Reactors are not rebuilt

A reactor acts on events: reading every event again would act again, send every mail again. A reactor continues after its last acknowledged event, and a new one starts at the newest event.

So never give a reactor `start: 0`, and never raise the `schema` of a reactor's store or delete its store file: each makes it read every event from the first.
