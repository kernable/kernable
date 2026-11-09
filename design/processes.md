# Processes

Status: designed together 2026-11-09, not built. A business process is a flow across
commands, events and queries ("an order is confirmed, paid within 3 days and shipped, else
cancelled"), defined once, run by the kernel, and shown in the kernel's UI: its diagram, where
each case stands, and statistics.

## Decisions

| Topic | Decision | Rejected |
|---|---|---|
| Engine | **The kernel runs processes.** A process is a model (data); one engine in Rust, the same for every language | a process helper per SDK (three engines) |
| Definition | **In code** (TypeScript, Python, Rust), next to the domain; the SDK builds the model and registers it. The UI shows processes, it does not edit them (for now) | editing in the UI in v1 |
| Model | **kernable's own JSON model** with BPMN's vocabulary (start, command, wait, timer, decision, end), signed and versioned | BPMN 2.0 XML (large, mostly unsupported, the common editor is not MIT) |
| Correlation | **By a field:** each start and wait names where the case's key is (the aggregate ID, or a field such as `orderId`); one case per key | only the events of one aggregate |
| Time | **Durable timers in the core:** "deliver this command at T", in the log, surviving restarts, failover and moves; usable without processes | timers inside processes only |
| Acting as | **The process's own service identity** (`process:shop.fulfilment`), with group permissions like any service; the command's context also names the case | the identity of whoever started the case |
| Rejections | **An error path per rejection code if the model has one, else an incident**: the case stops, shown in the UI and metrics, with retry, skip or cancel by an operator | incidents only |
| Case state | **Events in the log:** each case is an aggregate of its own; history, statistics, backup, standby, moves and verify cover it | a table of the engine's own |
| Decisions | **Conditions on the case's data, and queries:** a small expression (`total > 1000`), or a query to a read model whose answer picks the path | queries only; data only |
| Versions | **A case finishes on the version it started with;** new cases start on the newest | moving running cases (later, with a mapping of steps) |
| Ownership | **Code only;** a code deploy is the only way a process changes | UI drafts, two owners |

## Scope

- **v1:** durable timers (kernel, protocol, three SDKs); process models built in code (three SDKs) and registered signed; the engine (start on event, command, wait for event, timer, decision by data or query, error paths, incidents, end); the UI's views: the diagram, live cases, a case's history, statistics; metrics; docs and a tutorial part.
- **Later:** editing in the UI (drafts, publishing signed by a key that may manage processes), human tasks (a task list for "wait for a person's decision"), moving running cases to a new version, BPMN export.

## Example

The shop's fulfilment, as the model the SDK builds (the code API is designed next):

```json
{
  "name": "shop.fulfilment",
  "version": 3,
  "start": { "on": "shop.OrderSubmitted", "key": "aggregateId", "data": { "orderId": "aggregateId", "total": "data.total" } },
  "steps": {
    "confirm": { "command": "shop.order.Confirm", "aggregate": "case.orderId", "next": "large" },
    "large":   { "decide": [{ "if": "case.total > 1000", "next": "approval" }], "else": "paid" },
    "approval": { "query": "shop.NeedsApproval", "data": { "orderId": "case.orderId" }, "paths": { "true": "approved", "false": "paid" } },
    "approved": { "wait": "shop.OrderApproved", "key": "aggregateId", "next": "paid" },
    "paid":    { "wait": "payments.PaymentReceived", "key": "data.orderId", "timeout": "P3D", "next": "ship", "onTimeout": "cancel" },
    "ship":    { "command": "shop.order.Ship", "aggregate": "case.orderId", "rejected": { "out_of_stock": "notify" }, "next": "done" },
    "notify":  { "command": "shop.order.NotifyDelay", "aggregate": "case.orderId", "next": "done" },
    "cancel":  { "command": "shop.order.Cancel", "aggregate": "case.orderId", "next": "done" },
    "done":    { "end": true }
  }
}
```

- **Case data** starts with what the start maps from its event and grows with what each wait maps
  from its event. Commands and queries get their data from it by reference (`case.orderId`).
  Nothing else runs inside the kernel: logic stays in commands, read models and queries.
- **Expressions** are small and safe: field references, literals, comparisons, `and`, `or`, `not`.
  Anything more is a query.

## The code API

Decided 2026-11-09: a builder next to aggregates and reactors, with field
references and conditions as short strings.

- **Steps run in the order written**, unless a step names `next`; branching steps (`decide`,
  `query`) and error paths name their targets. Step names are required: they are the stable IDs
  of versions, the UI and the statistics.
- **References** are `aggregateId`, `data.<field>` (the event's), `case.<field>` (the case's data)
  and, in a query step, `answer` or `answer.<field>`. Each language writes field names in its own
  style (`case.order_id` in Python and Rust); the SDK converts them to the wire names (`orderId`)
  when it builds the model, as it does for vault fields.
- **Conditions** are small expressions: references, literals, `== != < <= > >=`, `and`, `or`,
  `not`. Anything more is a query.
- **Durations** are `30s`, `15m`, `12h`, `3d`.
- **Checked when registered:** an unknown step, field, type or a malformed condition fails the
  registration with the process, step and reason named, so a typo stops the deploy, not a case.
- **Registered by the app that runs the domain:** `kernable.app({ domains: [shop] })` registers the
  domain's processes when it connects, signed with its key. The key needs a grant `process` for the
  process, and `submit` for every command type the model sends: a process never does more than
  the service that defines it. A new version registers with the next deploy.
- **A query step** matches its `paths` against the answer itself (a boolean, string or number) or,
  for an object, against the field `answer` names; `into` keeps the answer in the case's data.

### Steps

| Step | Does | Options |
|---|---|---|
| `start(Event, …)` | starts a case when the event commits | `key` (where the case's key is), `data` (case data from the event) |
| `command(name, Command, …)` | sends the command as the process | `aggregate`, `data`, `rejected` (code → step), `next` |
| `wait(name, Event, …)` | waits for an event of this case | `key`, `data` (merged into the case), `timeout` + `onTimeout`, `next` |
| `timer(name, …)` | waits for a time | `after` (a duration) or `at` (a reference to a time), `next` |
| `decide(name, …)` | picks a path by the case's data | `when` + `then`, or a list of them; `otherwise` |
| `query(name, Query, …)` | asks a read model; the answer picks a path | `data`, `answer`, `paths`, `into`, `otherwise` |
| `end(name)` | ends the case | |

### The example in the three languages

```ts
export const fulfilment = shop.process("fulfilment")
  .start(OrderSubmitted, { key: "aggregateId", data: { orderId: "aggregateId", total: "data.total" } })
  .command("confirm", Confirm, { aggregate: "case.orderId" })
  .decide("large", { when: "case.total > 1000", then: "approval", otherwise: "paid" })
  .query("approval", NeedsApproval, { data: { orderId: "case.orderId" }, paths: { true: "approved", false: "paid" } })
  .wait("approved", OrderApproved, { key: "aggregateId" })
  .wait("paid", PaymentReceived, { key: "data.orderId", timeout: "3d", onTimeout: "cancel" })
  .command("ship", Ship, { aggregate: "case.orderId", rejected: { out_of_stock: "notify" }, next: "done" })
  .command("notify", NotifyDelay, { aggregate: "case.orderId", next: "done" })
  .command("cancel", Cancel, { aggregate: "case.orderId" })
  .end("done");
```

```python
fulfilment = (
    shop.process("fulfilment")
    .start(OrderSubmitted, key="aggregate_id", data={"order_id": "aggregate_id", "total": "data.total"})
    .command("confirm", Confirm, aggregate="case.order_id")
    .decide("large", when="case.total > 1000", then="approval", otherwise="paid")
    .query("approval", NeedsApproval, data={"order_id": "case.order_id"}, paths={True: "approved", False: "paid"})
    .wait("approved", OrderApproved, key="aggregate_id")
    .wait("paid", PaymentReceived, key="data.order_id", timeout="3d", on_timeout="cancel")
    .command("ship", Ship, aggregate="case.order_id", rejected={"out_of_stock": "notify"}, next="done")
    .command("notify", NotifyDelay, aggregate="case.order_id", next="done")
    .command("cancel", Cancel, aggregate="case.order_id")
    .end("done")
)
```

```rust
let fulfilment = shop
    .process("fulfilment")
    .start::<OrderSubmitted>(Start::key("aggregate_id").data([("order_id", "aggregate_id"), ("total", "data.total")]))
    .command::<Confirm>("confirm", Send::aggregate("case.order_id"))
    .decide("large", Decide::when("case.total > 1000", "approval").otherwise("paid"))
    .query::<NeedsApproval>("approval", Ask::data([("order_id", "case.order_id")]).path(true, "approved").path(false, "paid"))
    .wait::<OrderApproved>("approved", Wait::key("aggregate_id"))
    .wait::<PaymentReceived>("paid", Wait::key("data.order_id").timeout("3d", "cancel"))
    .command::<Ship>("ship", Send::aggregate("case.order_id").rejected("out_of_stock", "notify").next("done"))
    .command::<NotifyDelay>("notify", Send::aggregate("case.order_id").next("done"))
    .command::<Cancel>("cancel", Send::aggregate("case.order_id"))
    .end("done");
```

Messages may also be given by name (`"shop.OrderSubmitted"`), like everywhere in the SDKs.

## A case in the log

Each case is an aggregate `<process>` of the domain `process`, its ID the case's key, under the
tenant of the event that started it. Its events: `ProcessStarted` {version, data},
`StepReached` {step}, `CommandSent` {step, commandId}, `StepCompleted` {step, outcome, data},
`TimerSet` {step, at}, `TimerFired` {step}, `IncidentRaised` {step, code, message},
`IncidentResolved` {action: retry · skip · cancel, by}, `ProcessEnded` {end}. The engine keeps an
index of waiting cases by (event type, key), rebuilt from these events.

## Open, decided step by step while building

- The timer protocol: a command with a delivery time, and cancelling one.
- The process's service identity with the kit (an identity of the platform tenant, whose groups
  hold in every tenant), and without it.
- The UI: the diagram's layout, the live view, the statistics (cases per step, durations, incidents).
