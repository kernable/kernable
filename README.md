# kernable

Event sourcing / CQRS runtime with a Rust kernel and business logic in any language.

Workers connect to the kernel, which runs their command handlers and commits the events they return. All messages are signed, and stored events form a verifiable hash chain. Read models and reactors are ordered, durable listeners; a submit returns once they have processed the command's events. Every read is a query through the kernel to the read model that answers it, so the same signatures and allowlists apply to reading as to writing. Middleware (authorization, quotas, ...) is a listener that checks commands and queries before the kernel accepts them, in a fixed chain order, and the chain fails closed. This is the MVP; it is private.

## Layout

| Path | What it is |
|---|---|
| `proto/` | The protocol (`kernable.proto`), the file-log format (`storage.proto`), `SIGNING.md` and shared test vectors |
| `crates/kernable-core` | Protocol types, signing, hash chain, client transport |
| `crates/kernable-store` | `Store` trait and backends: `sqlite:`, `file:`, `memory:` |
| `crates/kernable-kernel` | The kernel server (`kernable-kernel <kernable.toml>`) |
| `crates/kernable-cli` | `kernable keys new \| certs new \| submit \| verify \| checkpoint \| listeners` |
| `crates/kernable-bench`, `bench/` | Load generator, benchmark matrix, results |
| `sdks/typescript` | `@kernable/sdk` |
| `sdks/python` | `kernable` for Python, same API |
| `examples/order-ts` | Order domain in TypeScript |
| `docs/` | Getting started |
| `examples/shop` | Reference app: identity in Python, orders and the HTTP API in TypeScript |

## Start here

`docs/getting-started.md` walks through a kernel, a TypeScript worker, a Python read model and a request that reads its own writes, in about ten minutes.

## Develop

```sh
make check          # Rust, TypeScript and Python tests against every storage backend
                    # (Python ≥ 3.10; the first run installs the Python SDK's venv)
make check-packages # builds the npm package and the Python wheel, installs and runs them
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```

## Operate

- **Mutual TLS** (TCP only): `kernable certs new <dir> <client>...` creates a development CA and certificates. Then add `[tls]` to `kernable.toml` (see `crates/kernable-kernel/src/config.rs`) and pass `tls` to the SDKs (`{ ca, cert, key }` in TypeScript, `Tls(ca, cert, key)` in Python).
- **Metrics**: `metrics_listen = "127.0.0.1:9233"` serves Prometheus text at `/metrics`: commands by outcome, queue depth, worker sessions, execution, settle, query and middleware time, queries and middleware checks by outcome, and position, lag and connection state per listener.
- **Middleware** is registered in code (`listener.middleware({ types, order }, check)`) and allowed per key in `[middlewares]`. The kernel keeps the registration: while a middleware is away, everything it covers is refused with `UNAVAILABLE`. `kernable listeners <store>` shows the chain; `kernable listeners remove` (kernel stopped) unregisters a middleware for good.
- **Logs**: structured, to stderr. `KERNABLE_LOG` sets the filter (default `info`), and `KERNABLE_LOG_FORMAT=json` switches to JSON.
