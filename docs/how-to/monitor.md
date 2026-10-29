# Monitor kernable

With `--metrics-listen` set, the kernel serves Prometheus metrics at `/metrics`, and health checks at `/health` and `/ready` ([reference](../reference/metrics)). Bind it to a private address.

## Health checks

- **Liveness:** `GET /health` answers 200 while the process runs.
- **Readiness:** `GET /ready` answers 200 once every registered aggregate has a worker connected and every registered listener is connected and has caught up, and 503 with the missing workers or listeners otherwise. Use it to gate traffic and deploys.

## Alerts worth having

```yaml
groups:
  - name: kernable
    rules:
      - alert: KernableNotReady          # a listener is down or behind
        expr: kernable_ready == 0
        for: 2m
      - alert: KernableListenerLagging   # a listener falls behind the log
        expr: kernable_listener_lag > 1000
        for: 5m
      - alert: KernableListenerDown
        expr: kernable_listener_connected == 0
        for: 2m
      - alert: KernableNoWorkers         # commands queue up with nobody to run them
        expr: kernable_worker_sessions == 0 and kernable_queued_commands > 0
        for: 1m
      - alert: KernableMiddlewareFailing # checks refused because a middleware is away or slow
        expr: rate(kernable_middleware_total{outcome="failed"}[5m]) > 0
        for: 5m
```

A listener stopping at an event it cannot read (a newer event version) shows as lag that keeps growing while the listener stays connected.

## Useful views

- `rate(kernable_commands_total[5m])` by outcome: throughput and the share of rejections.
- `kernable_settle_seconds_sum / kernable_settle_seconds_count`: how long submits wait for listeners.
- `kernable_query_seconds_sum / kernable_query_seconds_count`: query latency through the kernel.

## Logs

The kernel logs to stderr. `KERNABLE_LOG` sets the level (default `info`) and `KERNABLE_LOG_FORMAT=json` switches to JSON for your log pipeline.
