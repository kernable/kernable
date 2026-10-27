# Event sourcing in kernable

In a CRUD application, a table holds the current state and an update overwrites it. In an event-sourced application, the **events** are stored, never changed, and the state is derived from them.

| | CRUD | Event sourcing in kernable |
|---|---|---|
| What is stored | the current state | every event, in order, forever |
| A change | an update | a command, decided into events |
| History and audit | added separately, if at all | built in: every event names its command and sender |
| Another view of the data | a migration, a query | another read model, built from the events |
| Fixing a bug in a view | repair the data | fix the read model, rebuild it |

## Aggregates

An **aggregate** is the unit of decision: a customer, an order. It has

- a **state**, folded from its events by `apply`;
- **commands**: a handler looks at the state and returns events, or rejects the command with a reason;
- a **version**: the number of its events. Every event gets the next version.

Aggregates belong to a **domain** (`shop`); types are named `shop.Register` outside their domain. A domain writes only its own events; it learns about other domains through listeners.

## Rules for handlers

- **Deterministic and free of side effects.** A command may run again (after a crash, or after another command changed the aggregate first), and must decide the same way. Use `ctx.now()` and `ctx.id()` instead of the clock and random IDs: they are the same every time the command runs.
- **Small aggregates.** All commands of one aggregate run one after another; commands of different aggregates run in parallel. Draw boundaries where decisions must be consistent, not along entities.
- **Rejections are results.** A rejected command is stored like a committed one, with its code and message.

## Events

An event has a **type**, a JSON **payload**, its aggregate's **version**, a **global position** in the whole log, the **request context** of the command that caused it, and a **schema** version of its payload format. See [Data over time](./data) for how events change format without being rewritten.
