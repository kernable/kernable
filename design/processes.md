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

## A case in the log

Each case is an aggregate `<process>` of the domain `process`, its ID the case's key, under the
tenant of the event that started it. Its events: `ProcessStarted` {version, data},
`StepReached` {step}, `CommandSent` {step, commandId}, `StepCompleted` {step, outcome, data},
`TimerSet` {step, at}, `TimerFired` {step}, `IncidentRaised` {step, code, message},
`IncidentResolved` {action: retry · skip · cancel, by}, `ProcessEnded` {end}. The engine keeps an
index of waiting cases by (event type, key), rebuilt from these events.

## Open, decided step by step while building

- The code API in each language (a builder next to aggregates and reactors).
- The timer protocol: a command with a delivery time, and cancelling one.
- How a model is registered and signed (like a membership), and the grants: the signer must hold
  `submit` for every command type the model sends, so a process never does more than its author may.
- The UI: the diagram's layout, the live view, the statistics (cases per step, durations, incidents).
