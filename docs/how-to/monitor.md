# Monitor kernable

Use this page to see what a running kernel is doing: in a browser while you develop or debug, and with health checks, metrics and alerts in production. To find the cause of a specific problem, start at [Failures and recovery](../concepts/failures#finding-the-cause).

| Want to | Use |
|---|---|
| see services, aggregates, listeners and their state, or one aggregate's history | the UI (`--ui-listen`) |
| gate traffic and deploys | `GET /ready` (`--metrics-listen`) |
| alert and graph | `GET /metrics` (`--metrics-listen`) |
| list registrations with the kernel stopped or running | `kernable services`, `kernable workers`, `kernable listeners` |

## The UI

With `--ui-listen` (on by default with `--dev`, at http://127.0.0.1:7234), the kernel shows itself in a browser. The start banner shows its address in the `UI` line, or `disabled`.

- **Map:** services, aggregates, event types, listeners and queries, and what connects them: which service runs what, which aggregate writes which event, who reacts to it, who answers which query, and which middleware checks which commands. Each node shows its state: connected, behind, missing (the kernel waits for it), dynamic or away (counts for nobody), or live. Click one for its details.
- **Connections:** every connected worker process and every listener, with its service, key, position and since when.
- **History:** any aggregate's events in order, each with the command that caused it, its sender and correlation. Vault fields show as `personal · <subject>`, `secret` or `erased`; the UI never decrypts the vault, and it does not show snapshot states, which may hold personal values.

The UI has no login, so it listens on a loopback address only; the kernel refuses to start with another address. Reach it on a server through an SSH tunnel:

```sh
ssh -L 7234:127.0.0.1:7234 host   # then open http://127.0.0.1:7234
```

If the address is taken (another kernel in development), the kernel runs on without the UI and logs the warning `the UI is off`.

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
```

- `kernable_refused_total` names the limit in its `reason` label ([Limits](../reference/configuration#limits)). A growing `queued_commands` or `queued_per_aggregate` usually means a worker is away; `requests_per_key` a client that does not wait for its answers; `handshake_timeout` or `connections` something that opens connections and never uses them.
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

## Parked events

A reactor that gives up on an event parks it and moves on ([Failures](../concepts/failures)): its follow-up was rejected, or its handler failed five times. The kernel keeps the event's position with the reason until you act on it; the UI shows the count on the listener and the first ones in its details, and `kernable_listener_parked` counts them.

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
