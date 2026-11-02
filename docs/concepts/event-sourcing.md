# Event sourcing in kernable

In a CRUD application, a table holds the current state and an update overwrites it. In an event-sourced application, the **events** are stored, never changed, and the state is derived from them.

| | CRUD | Event sourcing in kernable |
|---|---|---|
| What is stored | the current state | every event, in order, forever |
| A change | an update | a command, decided into events |
| History and audit | added separately, if at all | built in: every event names its command and sender |
| Another view of the data | a migration, a query | another read model, built from the events |
| Fixing a bug in a view | repair the data | fix the read model, rebuild it |

## Decide and fold

Two small functions carry all of it. **Decide**: a command handler looks at the current state and the command, and returns events, or rejects the command. **Fold**: `apply` turns the events, one after another, into the state. The events are what is stored; the state is only ever folded from them.

<FlowDiagram name="decide" loop bare silent />

One cart, step by step:

| | Command | State before | Decision | Event stored | Version | State after |
|---|---|---|---|---|---|---|
| 1 | `AddItem` apple × 2 | empty | accept | `ItemAdded` {qty 2} | 1 | 2 items |
| 2 | `AddItem` pear × 1 | 2 items | accept | `ItemAdded` {qty 1} | 2 | 3 items |
| 3 | `CheckOut` | 3 items | accept | `CheckedOut` {orderId} | 3 | 3 items, checked out |
| 4 | `AddItem` fig × 1 | checked out | reject: "Cart is checked out" | none | 3 | unchanged |

Row 4 changes nothing: a rejection is an answer to the sender, stored with its command, but no event. To know the cart's state at any time, fold its events up to that version; that is also how a worker gets the state for the next command (or from a [snapshot](./data) plus the events after it).

## Aggregates

An **aggregate** is the unit of decision: a customer, an order. It has

- a **state**, folded from its events by `apply`: it changes the state (a copy) or returns a new one;
- **commands**: a handler looks at the state and returns events, or rejects the command with a reason;
- a **version**: the number of its events. Every event gets the next version.

Aggregates belong to a **domain** (`shop`); types are named `shop.Register` outside their domain. By convention, domain and aggregate names are lowercase and message types (events, commands, queries) capitalized; both cases are valid, and names are case-sensitive. A domain writes only its own events; it learns about other domains through listeners.

**Aggregates are short-lived.** None stays in memory between commands: for every command, the worker gets the aggregate's history (or a snapshot and the events after it), folds it into the state, decides, and forgets it again. That is why any worker can take any command, why a crashed worker loses nothing, and why handlers must decide the same way every time they run.

## Streams and the log

Every aggregate has its own **stream**, `shop/cart/c1`: its events, numbered by **version** 1, 2, 3, without gaps. All streams together form **one log**, numbered by **global position**, in the order the kernel committed them.

<FlowDiagram name="streams" bare silent :autoplay="false" />

- **Within a stream**, the version decides: a command is decided on version 2 and commits as version 3, or not at all if another command came first. One command per aggregate runs at a time, so this rarely happens.
- **Across streams**, the position decides: listeners get every event in log order, whatever aggregate it belongs to, so a read model of all carts sees c1 and c2 interleaved exactly as they happened.
- A query can wait for a position (`minPosition`), and `kernable verify` checks the log position by position along its hash chain.

What that looks like in the store, the `events` table of `kernable.db` (shortened):

| global_position | domain | aggregate_type | aggregate_id | version | event_type | payload |
|---|---|---|---|---|---|---|
| 41 | shop | cart | c1 | 1 | ItemAdded | `{"sku":"apple","qty":2,"price":3}` |
| 42 | shop | cart | c2 | 1 | ItemAdded | `{"sku":"fig","qty":1,"price":2}` |
| 43 | shop | cart | c1 | 2 | ItemAdded | `{"sku":"pear","qty":1,"price":1}` |
| 44 | shop | cart | c1 | 3 | CheckedOut | `{"orderId":"0b9c…","total":7}` |

Each row also keeps the command that caused it, its request context, the worker's signature and the hash chain. The same rows, with their commands, are in the kernel's [UI](../how-to/monitor#the-ui) under History.

## Write side and read side

Event sourcing in kernable splits an application into two sides, often called CQRS:

- **The write side decides.** Aggregates get commands and return events. They see only their own state, never a read model, so a decision depends on nothing but the aggregate's own history.
- **The read side is built from events.** Read models fold the events of any aggregates and domains into views that fit their queries: a cart list, a revenue report, a search index. Reactors do the same to cause side effects and follow-up commands.

Each side can change on its own: a new view is a new read model rebuilt from the existing events, without a migration of the write side. And because a settled submit returns only once the read side has the events, the two sides never disagree for a reader: [A command's way](./architecture#a-command-s-way) and [A query's way](./architecture#a-query-s-way) show both in motion.

## Handlers

Every handler gets the same kinds of arguments, in the same order: the **data** (the payload of the event, command or query), the **envelope** (what arrived: type, IDs, version), and a **context** (in which request, and what the handler may use).

| Handler | Arguments | Envelope | Context |
|---|---|---|---|
| `apply(type, …)` | `state, data, evt, ctx` | `evt`: type, version, domain, aggregate type and ID, tenant, command ID | `ctx`: the request context of the command that caused the event |
| `command(type, …)` | `state, data, cmd, ctx` | `cmd`: command ID, type, domain, aggregate type and ID, tenant | `ctx`: the request context, `now()`, `id()`, the deadline |
| `query(type, …)` | `data, qry, ctx` | `qry`: query ID, type, domain, tenant | `ctx`: the request context of who asks, the listener's store |
| `on(type, …)` | `data, evt, ctx` | `evt`: as for `apply`, plus position, payload version, redacted fields and when the kernel committed it | `ctx`: the request context, `replaying` (committed before this connection: a rebuild, or a reactor's backlog after a restart, so never a reason to skip a reactor's side effects), `follow()`, `submit()` and `query()` for follow-ups, the store |

`apply` gets no `now()` and no `id()`: replaying the events must give the same state every time, so a time or an ID the state needs goes into the event's data.

## Rules for handlers

- **Deterministic and free of side effects.** A command may run again (after a crash, or after another command changed the aggregate first), and must decide the same way. Use `ctx.now()` and `ctx.id()` instead of the clock and random IDs: they are the same every time the command runs.
- **Small aggregates.** All commands of one aggregate run one after another; commands of different aggregates run in parallel. Draw boundaries where decisions must be consistent, not along entities.
- **Rejections are results.** A rejected command is stored like a committed one, with its code and message.

## What a submit returns

A submit returns the outcome once every listener has processed the events (`settled`, the default), once the events are stored (`committed`), or, with `enqueue`, only the command ID once the command is queued. Every way is idempotent on the command ID. [Order and consistency](./ordering#settling-reading-your-own-writes) compares them.

## Events

An event has a **type**, a JSON **payload**, its aggregate's **version**, a **global position** in the whole log, the **request context** of the command that caused it, and a **schema** version of its payload format. See [Data over time](./data) for how events change format without being rewritten.

**Naming.** Events say what happened, in the past tense: `ItemAdded`, `OrderPaid`, `CustomerErased`. Commands say what should happen, in the imperative: `AddItem`, `PayOrder`, `EraseCustomer`. Queries ask: `GetCart`, `ListOrders`. Each `domain.Type` exists once; the domain keeps the rest apart (`shop.ItemAdded`, `billing.ItemAdded`).

**What events give you**, beyond the current state:

- **An audit trail.** Every change names its command, its sender, its key and its time, and the log is verifiable.
- **Every view, at any time.** A new read model folds the existing events; nothing has to be migrated first.
- **Reactions.** Reactors turn events into follow-up commands, mails or messages to other systems.
- **Time travel.** Fold an aggregate up to any version to see its state back then, or a read model up to any position.
- **Debugging.** When a state looks wrong, its events show how it came about, step by step.

## Rules across aggregates

An aggregate decides alone, on its own history. A rule that spans two aggregates becomes a short conversation between them, carried by a reactor:

<FlowDiagram name="saga" loop bare silent />

- **The reactor submits follow-up commands** with `ctx.submit`, so the sender and the correlation of the original request carry over, and with a fixed command ID (`reserve-o-7`), so a redelivered event changes nothing. If a follow-up is rejected, the reactor parks the event and moves on; an operator sees it and retries or dismisses it ([Failures](./failures)). A step whose rejection is part of the conversation (stock ran out) catches it and submits the compensating command instead.
- **Each step is a decision of one aggregate**, with its own rejection. Until the last step, the order is in an intermediate state (`placed`, not yet `confirmed`): model it explicitly.
- **Uniqueness and conditions are checked by the kernel.** "Every email only once" or "open an account only while its fund is open" need no reactor: an aggregate declares unique keys and facts from its state, a command the facts it requires, and the kernel checks them when the command commits ([Unique keys and conditions](./uniqueness)).

## When it fits

Event sourcing pays off when the history matters, or many views read the same facts:

- decisions with rules (orders, bookings, approvals, money), where you must know why something is as it is;
- audit and compliance: every change names its command, its sender and its key, and the log is verifiable;
- several views of the same data, added over time, or integration of other systems through reactors.

It costs a little more where none of that matters: a form that only stores what the user typed, or reference data nobody reasons about. Keep such data out of aggregates: in the kernel's [files and cache](../how-to/data-and-cache), or in a plain database next to kernable.

## Glossary

| Term | Meaning |
|---|---|
| **Domain** | A name that scopes aggregates and message types: `shop`, `billing`. Outside it, types are qualified: `shop.ItemAdded`. |
| **Aggregate** | The unit of decision: its state, its commands and its events (`shop/cart/c1`). |
| **Command** | A request to change one aggregate, signed by its sender: `AddItem`. Decided into events, or rejected. |
| **Event** | A fact that happened, stored forever: `ItemAdded`. |
| **Stream** | The events of one aggregate, numbered by **version**. |
| **Log** | All events of all streams, numbered by **global position**, chained by hash. |
| **Query** | A request to read, answered by a read model: `GetCart`. |
| **Listener** | A process that gets events in log order: a read model, a reactor, or a middleware. |
| **Read model** | A listener that keeps a view and answers queries. |
| **Reactor** | A listener that causes side effects and follow-up commands. |
| **Middleware** | A listener that checks commands and queries before (or after) the kernel handles them. |
| **Worker** | A process that runs aggregates: it decides commands. |
| **Service** | Your processes that belong together; its **membership** tells the kernel what to wait for. |
| **Settled** | A submit is settled once every listener has processed its events: the next read sees them. |

