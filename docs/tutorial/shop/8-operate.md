# 8. Operate

The shop works. This part runs it the way you would in production.

## The production kernel

Over a network, the kernel speaks TCP with **mutual TLS**: every process presents a certificate from your CA. For trying it out, `kernable certs new` makes a development CA and certificates; in production use your own PKI.

```sh
mkdir -p ~/shop-prod/data ~/shop-prod/keys && cd ~/shop-prod
kernable keys new keys/kernel >/dev/null
kernable keys new keys/vault >/dev/null
APP=$(kernable keys new keys/app)
kernable certs new certs app          # CA, kernel certificate, a certificate for "app"
```

<<< @/examples/shop/kernable.toml{toml}

Besides TLS, three things differ from part 1:

- **Snapshots** go to their own SQLite file. A worker sends a snapshot of an aggregate every 100 events, so a long-lived aggregate does not have to replay its whole history. Snapshots are a cache: delete them, and workers rebuild them.
- **Metrics** and health checks are served on `metrics_listen`; keep that address private.
- Paths point into `data/` and `keys/`. Back up `data/` (the log, the vault) and `keys/`; `kernable verify` checks a log and its signatures at any time.

## Connect the processes

Your processes read the connection from the environment (`connect`, part 1), so they run unchanged:

```sh
export KERNABLE_URL=kernel.example.internal:7233
export KERNABLE_KEY=keys/app.key
export KERNABLE_TLS_CA=certs/ca.tls.crt KERNABLE_TLS_CERT=certs/app.tls.crt KERNABLE_TLS_KEY=certs/app.tls.key
node worker.ts
```

## Read models that survive a restart

A read model in memory reads every event again when it starts. With a **store** it keeps its rows in a file, writes them together with its position per batch, and continues where it left off:

::: code-group
<<< @/examples/shop/ts/customers-persistent.ts#persistent [customers-persistent.ts]
<<< @/examples/shop/py/customers_persistent.py#persistent [customers_persistent.py]
:::

When you change how the handlers store data, raise `schema`: the store is cleared and rebuilt from the first event.

## Readiness, deploys and shutdown

- `GET /ready` on the metrics address answers 200 once every registered listener is connected and has caught up; let your deploy wait for it. `GET /health` answers while the process runs, and `GET /metrics` serves Prometheus metrics.
- **Deploy readers before writers.** When an event's format changes (a new event version), a process that does not know the new version stops at it instead of misreading it.
- On SIGTERM the kernel **drains**: it refuses new requests, lets running ones finish for up to `drain_timeout_ms`, then stores its state and exits. Commands that did not finish run right after the restart.
- Restarting a worker or a listener loses nothing: commands go to another worker, and listeners continue after their last acknowledged event.

The [reference](../../reference/) lists every configuration key, metric and error.
