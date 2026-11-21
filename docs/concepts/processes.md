# Business processes

A business process is a flow across commands, events and queries: *a confirmed order is paid
within 3 days and shipped, else cancelled.* Without help, such a flow ends up spread over reactors,
scheduled commands and a read model that remembers where each order stands, and nobody can see
it. In kernable you define it once, in code next to your domain; the **kernel runs it**, keeps
every step in the log, and its UI shows the diagram, where each case stands, and statistics.

A process is optional, and it builds on the other pieces: it sends ordinary commands to your
aggregates and waits for the events they write; it never changes your data itself. "When X, do
Y" is a reactor; a process is for flows that wait, have deadlines, and whose cases you want to
see. [How the pieces fit](./pieces) shows where it belongs, on an account's onboarding.

## A process

The shop's fulfilment, from [part 10 of the tutorial](../tutorial/shop/10-process):

::: code-group
<<< @/examples/shop/ts/domain.ts#process [domain.ts]
<<< @/examples/shop/py/domain.py#process [domain.py]
<<< @/examples/shop/rs/src/domain.rs#process [domain.rs]
<<< @/examples/shop/go/domain.go#process [domain.go]
:::

The SDK turns this into a **model**, plain JSON, and the kernel runs it. One engine, in the
kernel, runs the processes of every language; your processes only define and register them.

| Step | Does | Options |
|---|---|---|
| `start(Event, …)` | starts a case when the event commits | `key`: where the case's key is; `data`: the case's data from the event |
| `command(name, Command, …)` | sends the command | `aggregate`, `data`, `rejected` (code → step), `next` |
| `wait(name, Event, …)` | waits for an event of this case | `key`, `data` (merged into the case's data), `timeout` with `onTimeout`, `next` |
| `timer(name, …)` | waits for a time | `after` (a duration) or `at` (a field of the case holding a time in ms), `next` |
| `decide(name, …)` | picks a path by the case's data | `when` with `then`, or a list of them; `otherwise` |
| `query(name, Query, …)` | asks a read model; its answer picks the path | `data`, `answer`, `paths`, `into`, `otherwise` |
| `end(name)` | ends the case | |
| `cancelOn(Event, …)` | ends a running case at once when the event comes, at any step (not a step itself) | `key`, `end`: an `end` step |

- **Steps run in the order written**, unless a step names `next`. Step names are required: the
  UI, the statistics and new versions know a step by its name.
- **References** say where a value comes from: `aggregateId` and `data.<field>` of the event being
  handled (in `start` and `wait`), `case.<field>` of the case's data, and in a query step `answer`
  or `answer.<field>`. Python and Rust write fields in their own style (`case.order_id`); the
  SDK converts them. Go writes them as on the wire (`case.orderId`). A literal is `value(…)`.
- **Conditions** (`decide`) are small expressions on the case's data: references, numbers,
  strings, `true`, `false`, `null`, `== != < <= > >=`, `and`, `or`, `not` and parentheses:
  `case.total > 1000 and not case.trusted`. Anything more is a query to a read model you write.
- **Durations** are `30s`, `15m`, `12h` or `3d`.
- **Names:** an event or query without a domain is of the process's domain; a command is
  `aggregate.Command` (`order.Ship`) or `domain.aggregate.Command`. Classes work too; in Rust
  the typed steps take the type (`command::<Ship>`) and `start_named`, `wait_named`,
  `command_named` and `query_named` take a name.

No other logic runs in the kernel: what a step does is a command, and what decides is the case's
data or a read model's answer.

## How a case runs

- **It starts** when its start event commits: one case per key (here the order). While a case
  for a key runs, another start event for that key is ignored; after it ended, the next one
  starts a new case. Cases are numbered per key: `o1#1`, then `o1#2`.
- **A command step** sends its command and waits for the outcome. Its ID is fixed by the case and
  the step, so a crash or a failover never sends it twice. Committed, the case goes on. Rejected,
  it goes to the step `rejected` names for that code; any other rejection stops the case as an
  [incident](#incidents).
- **A wait** completes with the first event of its type whose key is the case's. An event that
  comes before the case reaches its wait (a fast payment, a slow confirmation) is kept for the
  case and counts when it gets there.
- **A cancelling event** (`cancelOn`) of the case's key ends it at once at the end step named,
  whatever step it is at: its timer is withdrawn, a command or query it still awaits is ignored
  when it comes, an incident ends with it. Events the case's own commands caused (their
  correlation is the case) do not cancel it, so a step that closes the account does not cut its
  own case short.
- **A timeout or a timer** is a [scheduled command](../how-to/schedule) of the case, cancelled when
  the awaited event comes first. It survives restarts, failover and moves like any other.
- **A query step** asks with the position of the event that moved the case, so the read model has
  seen it ([reading your own writes](./ordering)). While no read model answers (one reconnecting
  after a restart, say), it asks again every second, for up to 30 s, before the case stops as an
  incident `query_failed`.
- **Tenants:** a case lives in the tenant of the event that started it, and its commands, queries
  and waits stay in that tenant. With [partitions](../how-to/partitions), each partition runs the
  cases of its own tenants, and a tenant's running cases move with it. An event a case kept early
  for a later wait does not move with it: the case waits on the target for that event again (or
  its timeout), and the kernel logs a warning naming the case.

## Cases in the log

Each case is an aggregate of its own: domain `process`, type the process's name, ID the case's.
Its events say what happened, in order:

| Event | When |
|---|---|
| `ProcessStarted` | the case started: the version, the key, the case's data, the event that started it |
| `StepReached` | the case reached a step |
| `CommandSent` | a command step sent its command (with its ID) |
| `StepCompleted` | a step completed: how (`committed`, `event`, `timeout`, `answer`, a rejection code, `skipped`), and the step next |
| `TimerSet`, `TimerFired` | a timeout or a timer was set, and fell due |
| `EventKept` | an event a later wait of the case may need came early |
| `IncidentRaised`, `IncidentResolved` | the case stopped, and an operator resolved it |
| `CaseCancelled` | an event named in `cancelOn` ended the case: the step it was at, the event and its position, the end |
| `ProcessEnded` | the case ended, at which end |

The kernel writes and signs them itself; `kernable verify` refuses case events signed by any other
key. Because they are ordinary events, backups, the standby, exports and the UI's Explore cover
cases like everything else, and you can read them like any other events.

## As whom a process acts

The engine's commands and queries carry the request context `senderIdentity:
"process:shop.fulfilment"`, with the case in `correlationId` (`shop.fulfilment/o1#1`) and the
`process` and `case` attributes. Your middleware decides on them as on any sender: in the
tutorial, the guard lets `process:shop.fulfilment` send `Ship` and `Cancel`. With the
[kit](../kits/identity#processes), each process gets an identity and an API key of its own, with
permissions through groups like a service.

## Registering, versions and removing

`kernable.app({ domains })` registers the processes of its domains once its workers are
connected; `kernable.registerProcess(fulfilment)` does it on its own, for example in a deploy
script. The key needs a grant `process` for the process, and `submit` for each command, `query`
for each query and `listen` for each event its model names: a process never does more than the
key that registered it. The kernel checks the model when it is registered (every step reachable,
every target a step, every condition well-formed, every command type known), so a typo fails the
deploy and names the step, not a case later.

- **Versions:** registering the same model again changes nothing. A changed one becomes the next
  version: new cases start on it, **running cases finish on the version they started with**.
- **Removing:** a process the service's membership no longer lists, or one removed with
  `kernable processes remove`, starts no new cases; its running cases finish. Registering it again
  makes it active.

## Incidents

When a case cannot go on, it stops as an **incident**: a rejection with no path for its code, a
query that fails or has no path for its answer, a missing field the step needs, or decisions that
loop ([the codes](../reference/errors)). Nothing is lost: the case waits at its step. Fix the
cause, then resolve it, signed with an operator's key granted `process` for the process:

```sh
kernable processes retry  <addr> <key> shop.fulfilment o1#1   # run the step again
kernable processes skip   <addr> <key> shop.fulfilment o1#1   # go on to the step's next step
kernable processes cancel <addr> <key> shop.fulfilment o1#1   # end the case
```

The resolution is an event of the case (`IncidentResolved`, with the key that signed it).

## Watching processes

- **The UI's Processes** draw each version as a diagram (left to right, or top to bottom when it
  is long), each path as wide as the cases that took it, with the cases at each step now,
  incidents in red, and the conditions of its decisions; show outcomes over time and how long
  open cases have waited; list the cases and tell one, at its own address, as a story linked to
  the aggregates it touched, with its incident and the commands that resolve it first; over the
  last hour, day or week ([Monitor kernable](../how-to/monitor#the-ui)). Explore's **Catalog**
  lists every process with what it starts on, waits for, sends and asks, and its open cases and
  incidents.
- **Metrics:** `kernable_process_cases` by process and state, `kernable_process_incidents` and
  `kernable_process_step_seconds` per step ([Metrics](../reference/metrics)).
- **The command line:** `kernable processes list`, `show`, `cases` and `case` read the store, also
  with the kernel stopped ([CLI](../reference/cli)).

## Not yet

Processes are edited in code only; the UI shows them. There are no human tasks ("wait for a
person's decision" is a wait for the event their command writes), and running cases are not moved
to a new version.
