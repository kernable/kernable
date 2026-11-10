# Monitor kernable

Use this page to see what a running kernel is doing: in a browser while you develop or debug, and with health checks, metrics and alerts in production. To find the cause of a specific problem, start at [Failures and recovery](../concepts/failures#finding-the-cause).

| Want to | Use |
|---|---|
| see at a glance whether anything needs attention | the UI's **Overview** (`--ui-listen`) |
| see where the cases of a business process stand, and resolve its incidents | the UI's **Processes**, `kernable processes` |
| see services, lag, parked events, rejections, the standby, the store | the UI's **Operations** |
| see the domain, the log, or one aggregate's history | the UI's **Explore** |
| gate traffic and deploys | `GET /ready` (`--metrics-listen`) |
| alert and graph | `GET /metrics` (`--metrics-listen`) |
| list registrations with the kernel stopped or running | `kernable services`, `kernable workers`, `kernable listeners` |

## The UI

With `--ui-listen` (on by default with `--dev`, at http://127.0.0.1:7234), the kernel shows itself in a browser. The start banner shows its address in the `UI` line, or `disabled`. The top bar has the four places below, a search box, and the **verdict**: *Healthy*, *Degraded* or the number of problems, with the reasons on hover. The page does not ask over and over: the kernel tells it when the log moved on or the verdict changed (server-sent events), and the page then reloads what it shows, at most every 2 s; without that connection it asks every 5 s. Every view has its own address (`/processes/shop.fulfilment`, `/processes/shop.fulfilment/cases/o-1` for the newest case of order `o-1`, `/explore/events?type=shop.Order`; another tenant than `default` as `?tenant=`), so a reload keeps it and a link can be sent to someone with the same tunnel.

- **Overview:** commands and events over the kernel's run (up to a day, counted once a minute in memory: a restart starts over), cases started and open, incidents and rejected commands; a chart of activity per 10 minutes; **Needs attention**, every problem in one list with what fixes it (a listener missing or behind, parked events, incidents, a standby not in sync, a backup follower away, the store near its quota, a burst of rejections); the processes with their outcomes; and the system at a glance.
- **Processes:** each [business process](../concepts/processes), one version at a time, over the last hour, day or week:
  - figures: cases started, ended per end with their share, the median and 95th percentile at the first wait, open cases and incidents;
  - the diagram from left to right, each path as wide as the number of cases that took it, with its count and share where a step branches, and the cases at each step now (incidents in red). Click a step to list its cases;
  - outcomes over time, and the open cases by how long they have waited at their step, with the oldest one;
  - **Cases**, newest first, filtered by state, step and case, key or tenant; **Incidents** with the commands that resolve them (`kernable processes retry|skip|cancel`, ready to copy); and the statistics per step.

  A case opens with the diagram showing its own way (the steps it reached and how long it stayed at each, the rest faded) and as a story in plain words: what started it, what it waited for, what it sent and with what outcome, where it ended, each linked to the aggregates it touched; its raw events are one click away. The UI counts from the case events in the log, starting when you first open it: on a large log it says how far it has read until it has caught up. It keeps the running cases and the newest 1,000 ended ones per process, and its statistics for a week.
- **Operations:** the kernel (version, uptime, queued and scheduled commands), the standby and witness, the backup follower, the store's size against `--max-store-bytes`, what limits refused; every worker and listener with its key, how far it was behind over the last hour and its parked events; **Parked events** with the command that delivers each again; and **Rejected commands** grouped by code, with why (the unique key taken, the requirement that did not hold). The process engine's own refused commands (a timer no longer needed) are left out unless you ask for them. Commands the kernel refuses before it accepts them (a bad signature, a missing grant) are not stored and do not show here; the kernel's log has them.
- **Explore:** for developers.
  - **Domains:** one card per domain with its aggregates and their commands, the events they write, who reads them (read models, reactors, processes), the queries it answers, the middleware that checks its commands, and its processes; a dot shows whether its workers and readers are there. A domain opens with its numbers (events in the log, events and commands in the last hour with the share rejected, the last event), then per aggregate its commands (how many came in the last hour, how many were rejected, which processes send them) beside the events it writes (how many the log holds, the last hour per minute, the last one, who reads them); then the read models, listeners and middleware concerned with how far each is behind, its processes, and the events it uses from other domains. The UI counts the events the first time it opens a domain, from the start of the log, and from then on as they come.
  - **Catalog:** every event type, command, query and process, grouped by domain, in one table to filter. Events: who writes and who reads them, how many the log holds, the last hour per minute and the last one. Commands: how many came in the last hour and how many were rejected, who checks them and which processes send them; a domain whose aggregate has no worker says so. Queries: the read model that answers, what it reads and how far it is behind. Processes: the event they start on, what they wait for, send and ask, their open cases and incidents, and how many started. A row opens the event log, the command log or the process.
  - **Event log:** the whole log, newest first, each event with who sent the command behind it (from the request context the event carries, no lookup needed), filtered by event type, tenant, aggregate ID, command or correlation ID, and text in the payload, without the process engine's events unless you include them; **follow** shows new ones as they commit. One search reads at most 20,000 events, so on a large log it may stop early and say how far back it got.
  - **Command log:** every stored command, newest first: when it was received, the aggregate, who sent it (a person, a service, or a process and its case), and what came of it: committed with the events it wrote and how long it took from receipt (or its due time) to the commit, rejected with the code and why, waiting, or scheduled for when. Filter by type, tenant, aggregate ID, sender, command or correlation ID and outcome; the process engine's own commands only on request; **follow** shows new ones and updates waiting ones. Open one for its payload and full request context.
  - **Aggregates:** any aggregate's events in order, each in one line with the command and sender that caused it, its payload and signatures one click away. Vault fields show as `personal · <subject>`, `secret` or `erased`; the UI never decrypts the vault, and it does not show snapshot states, which may hold personal values.
- **Search:** an order, case, command or correlation ID finds its aggregates, its cases and its events.

Keys show by their ID. Click one for its full ID (to copy), what it is used for now (the workers, listeners and processes it signs), and a name such as "app" or "operator" to show instead, kept in your browser only.

The UI has no login, so it listens on a loopback address only; the kernel refuses to start with another address. Reach it on a server through an SSH tunnel:

```sh
ssh -L 7234:127.0.0.1:7234 host   # then open http://127.0.0.1:7234
```

If the address is taken (another kernel in development), the kernel runs on without the UI and logs the warning `the UI is off`.

### Watching from a script

The UI's own stream is open to anything on the machine, like the UI: server-sent events at `/api/watch`, read-only and without login, on the loopback address only. Choose what to get with `topics` and narrow it with filters:

```sh
curl -N 'http://127.0.0.1:7234/api/watch?topics=events&domain=shop&type=order&id=o-1'   # one order's events
curl -N 'http://127.0.0.1:7234/api/watch?topics=commands&status=rejected'               # rejected commands
curl -N 'http://127.0.0.1:7234/api/watch?topics=cases&process=shop.fulfilment'          # a process's cases
curl -N 'http://127.0.0.1:7234/api/watch?topics=health,pulse'                           # the verdict, counts, listener positions
```

| Topic | Each message |
|---|---|
| `events` | the new events that match, with their sender and data (vault fields as references only) |
| `commands` | commands that match when stored, and again when they complete: committed with their events and duration, rejected with why |
| `cases` | the case events of a process (`process=`, `case=`) |
| `head` | the log moved on, with the aggregates the new events changed |
| `health` | the verdict, when it changed |
| `pulse` | the head, the counts and each listener's position, when any changed |

Filters: `tenant`, `domain`, `type` (aggregate type), `id` (aggregate ID), `event` (part of `domain.EventType`), `command` (command or correlation ID), `sender` (part of the identity), `process`, `case`, `status` (of commands); the process engine's own events and commands only with `engine=1`. Messages come at most once a second per topic. `events` and `cases` carry the log position as their id: connected again with `Last-Event-ID` (browsers do it by themselves) or `from=<position>`, the stream goes on from there. More than 500 events behind, it sends `gap` instead: read what you missed from `/api/events`. For services elsewhere, subscribe through the SDKs instead: they are signed and checked against grants.

## Health checks

Set `--metrics-listen` (for example `127.0.0.1:9233`). It serves plain HTTP with no login: bind it to a private address. On a loopback address it answers only requests whose `Host` is a loopback name (`localhost`, `127.0.0.1`, `[::1]`), as the UI does, so a web page cannot read it through DNS rebinding; health checks and Prometheus on the same machine send one anyway.

- **Liveness:** `GET /health` answers 200 `ok` while the process runs.
- **Readiness:** `GET /ready` answers 200 `ready` when the kernel can serve, and 503 with the reason otherwise:

  | 503 body | Meaning |
  |---|---|
  | `waiting for workers: shop.Cart` | no worker is connected for these aggregates |
  | `waiting for listeners: carts, guard` | these listeners or middlewares are not connected, or have not caught up yet |
  | `shutting down` | the kernel got SIGTERM and finishes running requests |

  It waits for what **counts**: what a service's membership lists, also members that never registered, and for services without a membership everything they registered. Dynamic members (registered, but not listed) and live listeners do not count ([Order and consistency](../concepts/ordering#who-counts)). Use it to gate traffic and deploys ([Deploy and upgrade](./deploy)).

## Alerts worth having

```yaml
groups:
  - name: kernable
    rules:
      - alert: KernableNotReady          # a worker or listener that counts is missing or behind
        expr: kernable_ready == 0
        for: 2m
      - alert: KernableListenerLagging   # a listener falls behind the log
        expr: kernable_listener_lag > 1000
        for: 5m
      - alert: KernableListenerDown
        expr: kernable_listener_connected == 0
        for: 2m
      - alert: KernableEventsParked      # a reactor gave up on events: retry or dismiss them
        expr: kernable_listener_parked > 0
        for: 1m
      - alert: KernableNoWorkers         # commands queue up with nobody to run them
        expr: kernable_worker_sessions == 0 and kernable_queued_commands > 0
        for: 1m
      - alert: KernableMiddlewareFailing # checks refused because a middleware is away or slow
        expr: rate(kernable_middleware_total{outcome="failed"}[5m]) > 0
        for: 5m
      - alert: KernableQueriesFailing    # no read model answered in time
        expr: rate(kernable_queries_total{outcome="failed"}[5m]) > 0
        for: 5m
      - alert: KernableLimitReached      # requests refused by a limit (--max-*)
        expr: rate(kernable_refused_total[5m]) > 0
        for: 5m
      - alert: KernableStoreFilling      # the stores passed 80 % of --max-store-bytes
        expr: kernable_store_bytes / kernable_store_bytes_max > 0.8
        for: 15m
```

- `kernable_refused_total` names the limit in its `reason` label ([Limits](../reference/configuration#limits)). A growing `queued_commands` or `queued_per_aggregate` usually means a worker is away; `requests_per_key` a client that does not wait for its answers; `handshake_timeout` or `connections` something that opens connections and never uses them. `rate` is a key that starts more requests per second than `--max-requests-per-second-per-key`; `quota_storage` a kernel whose stores reached `--max-store-bytes`, which refuses new commands and file puts until data is deleted or the limit raised.
- The per-listener metrics (`kernable_listener_lag`, `kernable_listener_connected`) cover every listener the kernel knows, also dynamic ones that nobody waits for. Exclude those by their `listener` label, or remove them ([Remove a listener](./remove-listener)).
- A listener stuck at an event it cannot handle (a throwing handler, or a newer event version) shows as lag that keeps growing while the listener stays connected ([When a listener is slow or stuck](../concepts/ordering#when-a-listener-is-slow-or-stuck)).

## Useful views

| Query | Shows |
|---|---|
| `sum by (outcome) (rate(kernable_commands_total[5m]))` | throughput and the share of rejections |
| `rate(kernable_execution_seconds_sum[5m]) / rate(kernable_execution_seconds_count[5m])` | how long a command takes from lease to commit |
| `rate(kernable_settle_seconds_sum[5m]) / rate(kernable_settle_seconds_count[5m])` | how long submits wait for listeners |
| `rate(kernable_query_seconds_sum[5m]) / rate(kernable_query_seconds_count[5m])` | query latency through the kernel |
| `rate(kernable_middleware_seconds_sum[5m]) / rate(kernable_middleware_seconds_count[5m])` | time spent in middleware checks |
| `kernable_queued_commands` | commands waiting or running |

The timings are summaries without quantiles: they give averages. All metrics are in the [reference](../reference/metrics).

## From the command line

These read the store, whether the kernel runs or not; the UI shows the live state:

```sh
kernable services sqlite:shop.db    # each membership: who applied it, what it lists, (never registered), dynamic listeners
kernable workers sqlite:shop.db     # aggregate, command types, key, service, last seen
kernable listeners sqlite:shop.db   # name, order, position, types, key, service, middleware, parked, last seen
kernable listeners parked sqlite:shop.db   # each parked event: listener, position, type, code, attempts, when, message
```

`last seen` tells you when a worker or listener was last connected. See the [CLI reference](../reference/cli).

## Usage beside the store

The kernel writes `<store>.usage` beside its store (`kernable.db.usage`) every minute and when it stops, one line of JSON:

```json
{"at":1793996832409,"events":205200,"commands":205211,"rejected":3,"queries":1200,"store_bytes":241172480,"sig":"9f3c…"}
```

`events` is the store's head, `store_bytes` what its stores take on the disk; `commands` (committed), `rejected` and `queries` (answered) go on across restarts: a kernel starts from the counts in the file, so a crash loses at most a minute of them. For whoever bills or watches the machine without scraping metrics. `sig` is the kernel key's signature (Ed25519, hex) over the other fields, one per line after the tag `kernable-usage-1`, in the order `at`, `events`, `commands`, `rejected`, `queries`, `store_bytes`, `replication` (empty without replication): checked with the kernel's public key, the counts are the kernel's, not another process's on the machine.

## Parked events

A reactor that gives up on an event parks it and moves on ([Failures](../concepts/failures)): its follow-up was rejected, or its handler failed five times. The kernel keeps the event's position with the reason until you act on it; the UI shows the count on the listener, the first ones in its details and all of them under Connections, and `kernable_listener_parked` counts them.

```sh
kernable listeners parked sqlite:shop.db
# confirmations position 18204 shop.OrderSubmitted not_found attempts 1 parked 2026-11-02T14:03:00Z: no such customer
# mailer position 18517 shop.OrderSubmitted handler_error attempts 5 parked 2026-11-02T14:21:07Z: TypeError
```

For a handler error the message is only the error's type; the reactor's log has the whole error, at the time it parked the event. Fix the cause, deploy, then deliver the events again, or drop those that no longer matter:

```sh
kernable listeners retry 127.0.0.1:7233 ops.key mailer 18517     # or --all
kernable listeners dismiss 127.0.0.1:7233 ops.key confirmations --all
```

The key needs `manage` for the listener's service. A retried event goes to the reactor on its own, before anything new, the next time it is connected; if it fails again, it is parked again. A follow-up command keeps its fixed ID, so retrying a rejected one returns the same rejection: retry once the handler deals with it differently, or dismiss it.

## Logs

The kernel logs to stderr. `KERNABLE_LOG` sets the filter (default `info`; e.g. `debug` or `warn`) and `KERNABLE_LOG_FORMAT=json` switches to JSON for your log pipeline. The start banner appears only on a terminal, and not with JSON logs. Watch for `key revoked` and `drain timed out` warnings.

The SDKs print what their workers and listeners do to stderr, one line each:

```text
12:04:10  worker  connected to http://127.0.0.1:7233 · key app (69212a19f36f…) · runs shop.AddItem
```

There, `KERNABLE_LOG` is `off`, `error`, `warn`, `info` (default) or `debug`. Lines have colors on a terminal unless `NO_COLOR` is set. An app prints an overview of its workers and listeners once all are connected.
