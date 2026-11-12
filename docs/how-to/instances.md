# Run a listener on several machines

Read this when the same read models and reactors run on more than one machine, for example on both machines of a [standby pair](./standby): each machine keeps its own copy of every read model, so a lost machine takes nothing with it.

Beside a [reader](./readers) (a read-only copy) it is different: each reader is a kernel of its own, so the read models connected to it form their own group there, and its reactors and workers wait (the writer's act). Name the instance all the same, so the same code runs everywhere.

Several processes then run one listener under one name, each as an **instance**. The kernel treats them as one listener:

- **It is through an event once one instance has handled it.** A settling submit, a listener of a later order and `/ready` wait for the fastest instance, not for all of them. An instance that is gone, or rebuilding from the first event, holds nothing back.
- **Read models are replicated.** Every instance gets every event, from its own position, and keeps its own state. A query goes to any instance that has reached the query's `min_position`, so it sees what the submit before it wrote, whichever instance answers. A middleware check goes to any instance that has caught up.
- **Reactors are single.** One instance at a time gets events; the others wait. When the active one goes away, the first waiting one takes over after the last event the group acknowledged, so an email is sent once, not once per machine. Delivery stays at least once: the events the old instance had not acknowledged come again.

## Name the instance

Set `KERNABLE_INSTANCE` for each process, for example to the machine's name:

```sh
# machine A
KERNABLE_INSTANCE=node-a KERNABLE_URL=https://10.0.0.1:7233,https://10.0.0.2:7233 node app.ts

# machine B
KERNABLE_INSTANCE=node-b KERNABLE_URL=https://10.0.0.1:7233,https://10.0.0.2:7233 node app.ts
```

An instance ID has 1 to 128 letters, digits, `-`, `_` and `.`, and is unique within its listener. The code does not change: every read model and reactor of the process joins its group. Without `KERNABLE_INSTANCE`, a listener has one connection, and a second process with the same listener is refused (`ALREADY_EXISTS`).

To set it per listener instead, or to change the kind, use the listener's options: `instance` and `single` in TypeScript (`ListenerOptions`) and Python (`read_model`, `reactor`), `.instance(…)` and `.single(…)` in Rust. A read model is replicated (`single: false`) unless you say otherwise; a reactor is single. A live listener is never single: each process gets every event, for its own connected browsers.

## What to keep in mind

- **Every instance of a listener agrees on its kind.** A replicated instance next to a single one is refused (`FAILED_PRECONDITION`, `disagree on single`): change both, or neither, in one deploy.
- **A replicated listener cannot park events.** Each instance has its own copy, and a parked event would be missing from one copy only; read models do not park by default. Make a listener single if it parks.
- **A read model with a store keeps its position on its machine.** Back on, it continues after its own stored position and gets what it missed. One in memory rebuilds from the first event, as on every start.
- **Both machines run every reactor's code, one at a time.** A reactor whose side effects must not happen from a second place at all, such as a scheduled job that ignores events, belongs in one process only.
- **The newest registration counts.** Instances normally run the same code. During a rolling deploy they may differ in order, event types or middleware for a while: the kernel takes the registration of the instance that connected last, and logs a warning, in case the difference lasts.
- **At most 64 instances** of one listener are connected at once.

The browser UI shows a listener's connected instances, each with its position; `kernable listeners` reads the store, which keeps only the group's position.
