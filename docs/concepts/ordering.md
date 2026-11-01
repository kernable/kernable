# Order and consistency

kernable gives you a strict order where it matters and parallelism where it does not.

## Commands

<FlowDiagram name="lanes" bare silent :autoplay="false" />

- **One at a time per aggregate.** The kernel queues commands per aggregate in submission order and leases one at a time to a worker. Commands of different aggregates run in parallel, on as many workers as you start.
- **Optimistic commit.** A worker's events are committed only on the version they were based on. If the aggregate changed in between, the kernel runs the command again with the new history.
- **Idempotent by ID.** A command ID is stored with the command. Submitting the same ID again returns the original outcome instead of running it twice: give retried commands a fixed ID.
- `expected_version` in the request context rejects a command unless the aggregate is still at the version the client saw.

## Listeners

Events have a **global position**. Every listener receives them in that order, one at a time, and acknowledges what it has processed.

There is **one order for everything**: all domains and all tenants share the global position. A read model that reads `shop` and `billing` events sees them interleaved exactly as they were committed ([a projection across domains](./event-sourcing#write-side-and-read-side)).

<FlowDiagram name="ordering" />

- **Lower orders first.** A listener gets an event only once every listener with a lower order has acknowledged it. Read models default to 0 and reactors to 10, so a reactor's queries always see the event it reacts to. Listeners of the same order run in parallel.
- **At least once.** After a crash a listener gets the events after its last acknowledgement again. Handlers must be idempotent: reactors submit with fixed command IDs, read models overwrite rather than add. A read model with a store writes data and position together and takes effect exactly once.

## Who counts

Not every listener holds the others back. Those that **count** decide when higher orders get an event, when a submit settles, and when the kernel is ready (`/ready`):

| Listener | Counts | Why |
|---|---|---|
| registered, in a service without a membership, or without a service | yes | the simple rule: everything registered counts |
| listed in its service's membership | yes, also before it ever connected | the service consists of it ([memberships](../how-to/remove-listener)) |
| registered, but not listed in its service's membership | no (dynamic) | it runs, keeps its position, nobody waits for it |
| live (`live: true`) | no | never registered: starts at the newest event, forgotten when it disconnects |

A **middleware** counts like a listener and fails closed besides: while it is away, what it checks is refused.

## When a listener is slow or stuck

A listener that counts and does not acknowledge holds things up, on purpose, rather than letting a reader see an incomplete state:

- **Settling submits wait** up to `--settle-timeout-ms` (default 5 s), then throw `NotSettled`. The command **is committed**; only the guarantee that every read model shows it is missing. Retrying with the same command ID returns the same outcome.
- **Higher orders wait** behind it: reactors do not get events the read models before them have not processed.
- **The kernel is not ready** (`/ready` answers 503 and names the listener), and its UI shows the listener as *behind*, with how many events.
- A listener that is away, not slow, is the same case: it counts until its service's membership no longer lists it, or it is removed ([Remove a listener](../how-to/remove-listener)).

## Settling: reading your own writes

By default `submit` **settles**: it returns once every listener has processed the command's events. The next query, from anyone, sees the change. No polling, no "eventually".

| `wait` | returns when | use it for |
|---|---|---|
| `settled` (default) | every listener processed the events | APIs that read right after writing |
| `committed` | the events are stored | throughput, or when you do not read back |
| `accepted` (`enqueue`) | the command is stored in the queue | fire and forget; resubmit the ID to get the outcome |

A query can also wait on its own: `minPosition` answers only after the read model has processed that position.

**A settled submit includes the reactions it caused.** A reactor (order 10) acknowledges an event only after its handler finished, including the follow-up command it submitted and awaited. So when the original submit settles, the follow-up is committed too, and the read models already show its events. Only reactions the reactor starts without waiting for them come later.

## What is consistent, and from when

| | Consistent | How |
|---|---|---|
| Within one aggregate | always | one command at a time, committed on the version it was decided on |
| A read after a settled submit | from the moment the submit returns | every listener that counts has processed the events |
| A read after a `committed` submit | from the given position on | ask with `minPosition`, or the read model may not have it yet |
| Unique keys and required facts | at every commit | the kernel checks them atomically with the commit ([Unique keys and conditions](./uniqueness)) |
| Across aggregates | step by step | a reactor turns one aggregate's event into the next command ([rules across aggregates](./event-sourcing#rules-across-aggregates)); each step is consistent in itself |
| Across domains and tenants | in one global order | every listener sees the same sequence of positions |

## A listener never waits for itself

A listener handles one event at a time, so a request from inside its handler must not wait for that listener. The SDKs name the listener in such requests, and the kernel adapts:

- a settling submit from a listener waits only for listeners of a **lower** order: a reactor still reads its writes in the read models;
- a query from a listener goes to another read model, or is refused at once instead of timing out;
- a middleware cannot send what it checks itself.
