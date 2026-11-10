# Processes

Status: v1 built 2026-11-09 (stages 1–4: models, engine, kit identities, UI views and docs). A business process is a flow across
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

## Timers: commands with a delivery time (built 2026-11-09)

Decided 2026-11-09, usable without processes:

- **A time on the command:** `submit(Cancel.new(), { aggregateId: "o1", at, until? })`. `at` and
  `until` are part of the signed command (`Command.deliver_at`, `deliver_until`, ms). The kernel
  answers `accepted` at once, whatever the wait, and stores the command as pending: durable,
  replicated to the standby, carried along by moves and exports, rebuilt after a restart.
- **Not in the aggregate's queue until due:** the queue is first in, first out per aggregate, so a
  command due in 3 days waits in a timer list beside it and enters its aggregate's queue at `at`;
  other commands of the aggregate pass meanwhile.
- **Checked twice:** when scheduled (signature, grant, middleware: mistakes show at once), and
  when due (the middleware again with what it knows then, and the key's revocation).
- **Late runs late:** after an outage a due command runs as soon as the kernel is back, with its
  scheduled time; past `until` it is rejected `expired` instead.
- **Cancelling:** `kernable.cancel(commandId)`, signed by a key granted `submit` for the command's
  type, checked by the middleware as a request of its own kind (the kit decides it as the command
  itself). It completes the command as rejected `cancelled`; a command already running or done
  answers `too_late`.
- **Limits:** `at` at most `--schedule-max-days` (default 366) ahead; scheduled commands count
  towards the queue's limits like queued ones.

## The model format (stage 1, decided 2026-11-09)

What every SDK's builder produces and the kernel registers (`ProcessService.Register`, the JSON as
bytes, signed). `proto/test-vectors/process-model.json` holds the example above as the builders
must produce it; each SDK's tests build the example and compare structurally.

```json
{ "name": "shop.fulfilment",
  "start": { "on": "shop.OrderSubmitted", "key": "aggregateId", "data": { "orderId": "aggregateId" } },
  "steps": [ { "name": "confirm", "kind": "command", … }, … ] }
```

- **`name`**: qualified, `<domain>.<process>`; the process's domain is the domain it is defined on.
- **`cancelOn`** (optional, added 2026-11-10): an array of `{ "event", "key", "end" }`: an event
  (qualified) that ends a running case of its key at once, whatever step it is at, at the `end`
  step named (which must be of kind `end`; an end reached only this way counts as reachable).
  The engine writes `CaseCancelled` (step, event, position, end), then the end. Events whose
  correlation is the case itself (caused by its own commands) do not cancel it. One entry per
  event.
- **`steps`**: an array in the order written in code (the UI and the statistics keep it). Every
  step has `name` (unique; letters, digits, `_`, `-`) and `kind`. The builder resolves the implicit
  order: every step but `end` carries an explicit `next` (or its kind's own targets), so the model
  never depends on order.
- **Types are qualified:** events and queries `<domain>.<Type>`, commands
  `<domain>.<aggregateType>.<Type>`.
- **References** are strings: `aggregateId` and `data.<path>` (the event being handled, in `start`
  and `wait`), `case.<path>` (the case's data), and in a query step `answer` / `answer.<path>`.
  Paths are dotted wire names (camelCase). A **literal** is `{ "value": <json> }` (builders:
  `value(…)`).
- **Mappings** (`data` of `start`, `wait`, `command`, `query`): an object of field → reference or
  literal. `start.data` and `wait.data` write into the case's data; `command.data` and
  `query.data` build the request's data.
- **Conditions** (`decide`): `expr := or; or := and ("or" and)*; and := not ("and" not)*;
  not := "not" not | cmp; cmp := operand (("==" | "!=" | "<" | "<=" | ">" | ">=") operand)?;
  operand := ref | number | 'string' | "string" | true | false | null | "(" expr ")"`, refs
  `case.<path>` only. Parsed by the kernel when registering (the SDKs only convert field names).
- **Durations** are milliseconds in the model (`timeoutMs`, `afterMs`); builders take `30s`,
  `15m`, `12h`, `3d`.

| `kind` | Fields |
|---|---|
| `command` | `command`, `aggregate` (ref), `data`?, `rejected`? (code → step), `next` |
| `wait` | `event`, `key` (ref into the event), `data`?, `timeoutMs`? with `onTimeout`, `next` |
| `timer` | `afterMs` or `at` (a ref to a time in ms), `next` |
| `decide` | `when`: [{ `if`, `next` }], `otherwise` |
| `query` | `query`, `data`?, `answer`? (a path in the answer to match; none: the answer itself), `paths`: [{ `value`, `next` }], `into`? (case field to keep the answer in), `otherwise`? |
| `end` | — |

**Checked when registered** (kernel; the SDK checks what it can earlier): names, kinds and fields;
every target names a step; every step is reachable from the start; references well-formed and
`case.` fields written by an earlier step or the start on every path (else a warning, not an
error: a field may be absent); conditions parse; durations positive; each command type known (a
worker registered it or a membership lists its aggregate); the key holds the grants. A failure is
INVALID_ARGUMENT naming the process, the step and the reason.

**Versions** are numbered by the kernel: it compares the canonical JSON (keys sorted, no
whitespace) with the newest version's; equal registers nothing, different is the next version.
Kept as signed records like memberships, with when and by which key each version was registered.

**Removing:** a service's membership lists its processes (`Membership.processes`). One it no
longer lists starts no new cases, its running cases finish; `kernable processes remove <name>`
does the same without memberships. A later registration makes it active again.

**SDKs:** `domain.process(name)` returns the builder; `kernable.app({ domains })` registers the
domains' processes after its workers are registered, and lists them in the membership it applies.

## As whom a process acts

Decided 2026-11-09:

- **Without the kit:** the engine's commands carry `senderIdentity: "process:<domain>.<name>"`
  and name the case (`causationId`: the event or timer that moved it); any middleware decides on
  that.
- **With the kit, created automatically:** when an app registers a process, the kit creates its
  identity in the platform tenant (kind `process`), without permissions. A platform admin grants
  it groups ("Fulfilment: shop.Confirm, shop.Ship, shop.Cancel"); platform groups hold in every
  tenant. Until then its commands are refused, and the cases show incidents.
- **An API key per process:** the process identity gets an API key, and the engine's commands
  carry it (`senderSession`), as a service's do. Revoking the key in the kit stops the process at
  once, without a deploy. The kit's middleware sees the key's ID, not its secret, so the trust in
  the request itself still rests on the kernel's signature; how the key reaches the engine is
  decided while building.

### With the kit: mechanics (stage 3, decided 2026-11-09)

- **The kernel announces processes in the log:** registering a process's first version, a new
  version, removing it and re-activating it write `ProcessRegistered` {name, version} and
  `ProcessRemoved` {name} to a registry stream of the domain `process`. The UI and the audit see
  them too.
- **The kernel learns as whom a process acts by a call:** `ProcessService.Act` {name, context},
  signed by a key granted `process` for it, sets the request context the engine's commands and
  queries carry (sender tenant, identity, session). Kept with the process's state, replicated to
  every partition. Without it a process acts as `process:<name>`. The kernel knows nothing of the
  kit.
- **The kit reacts:** a reactor of the `identity` domain takes `ProcessRegistered` for a process it
  has no identity for, creates its identity in the platform tenant (kind `process`, no
  permissions) and an API key (the secret discarded: only the key's ID travels), and calls `Act`
  with { senderTenant: platform, senderIdentity, senderSession: the key's ID }. Revoking that key
  stops the process at once: its commands are refused, the cases show incidents.

## The UI's views (read-only; built in stage 4)

Built as the UI's Processes tab (`ui.html`), reading `/api/processes`, `/api/cases` and `/api/case` (`ui.rs`, `process_view.rs`): the view counts from the case events, following the log only while the UI is open, keeps the running cases and the newest 1,000 ended ones per process, and per-minute statistics for a week (durations in 10% buckets).


Decided 2026-11-09:

- **The diagram:** a flow from left to right, laid out from the model: steps as boxes, paths as
  arrows, decisions as diamonds, waits and timers with a clock. Each box shows how many cases are
  there now; incidents in red. One diagram per version.
- **Live cases:** per process and version, filtered by step and state (running, waiting,
  incident, ended); a case's page shows its events as a timeline, with the commands it sent and the
  events that moved it, linked to those aggregates' histories.
- **Incidents:** shown with the step, the rejection and the exact command to resolve them:
  `kernable processes retry|skip|cancel <process> <case>`, signed with an operator's key and
  stored in the case's events. No buttons: the UI has no login.
- **Statistics:** per step the cases there now, entered and left over the last hour, day and
  week, the time spent (median and 95th percentile), timeouts and incidents; per process started,
  ended by each end, and running. Counted by the kernel from the case events; also as metrics
  (`kernable_process_cases`, `kernable_process_step_seconds`, `kernable_process_incidents`).

## The engine (stage 2, decided 2026-11-09)

- **One running case per key:** a start event for a key whose case still runs is ignored (and
  logged); after the case ended, the next one starts a new case for that key, kept separately with
  its own history.
- **Early events are kept for the case:** an event of a case's key that a later wait of the case
  may need, arriving before the case reaches that wait, counts as soon as it does. No race between
  a fast payment and a slow confirmation.
- **Tenants:** a case lives in the tenant of the event that started it; its commands, queries and
  waits are in that tenant.
- **Exactly once:** the engine's commands have IDs fixed by case, step and attempt, so a crash or
  failover never sends one twice; a query step asks with the position of the event that moved the
  case, so the read model has seen it.
- **Timeouts** are scheduled commands of the case, cancelled when the awaited event comes.
- **Partitions:** process models and their states travel to every partition like memberships;
  each partition runs the cases of its own tenants.

## A case in the log

Each case is an aggregate `<process>` of the domain `process` (`shop.fulfilment`), its ID
`<key>#<n>` (`o1#1`, then `o1#2` for the key's next case), under the tenant of the event that started it. Its events: `ProcessStarted` {version, data},
`StepReached` {step}, `CommandSent` {step, commandId}, `StepCompleted` {step, outcome, data},
`TimerSet` {step, at}, `TimerFired` {step}, `IncidentRaised` {step, code, message},
`IncidentResolved` {action: retry · skip · cancel, by}, `EventKept` {event, position} (an early event a later wait may need), `ProcessEnded` {end}. The kernel writes them itself, signed with its key (`kernable verify` refuses `process` events signed by another key); a running case holds the fact `running`, so a restart replays only running cases. The engine keeps an
index of waiting cases by (event type, key), rebuilt from these events.

## Open, decided step by step while building

- The timer protocol: a command with a delivery time, and cancelling one.
- The process's service identity with the kit (an identity of the platform tenant, whose groups
  hold in every tenant), and without it.
- The UI: the diagram's layout, the live view, the statistics (cases per step, durations, incidents).
