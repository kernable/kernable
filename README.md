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
| `docs/` | The documentation site: tutorial, reference (generated), runnable examples |
| `examples/shop` | Reference app: identity in Python, orders and the HTTP API in TypeScript |

## Start here

The documentation lives in `docs/` (VitePress): `make docs` serves it at http://localhost:5173. Start with the tutorial `docs/tutorial/getting-started.md`, which walks through a kernel, a TypeScript worker, a Python read model and a request that reads its own writes, in about ten minutes. The reference in `docs/reference/` is generated from the code; every example in the docs runs in `make check`.

## Develop

```sh
make check          # Rust, TypeScript and Python tests against every storage backend
                    # (Python ≥ 3.10; the first run installs the Python SDK's venv)
make check-packages # builds the npm package and the Python wheel, installs and runs them
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```

Robustness: `make check` also aborts the real kernel once at each crash point (`crates/kernable-kernel/src/crash.rs`, built only with `--features crash-points`) and runs property tests on damaged logs, all storage backends and arbitrary requests. `make torture ROUNDS=20 CASES=10000` runs them for longer; `make fuzz FUZZ_SECONDS=300` fuzzes the file log and the wire messages (needs nightly Rust and `cargo install cargo-fuzz`).

`crates/kernable-kernel/tests/fixtures/format-<n>` holds frozen stores of every data format; `make check` opens, verifies and runs them. Never regenerate them: a new format adds a directory (`KERNABLE_WRITE_FIXTURES=1` fills only missing ones) and a `kernable migrate` step.

## Operate

- **Mutual TLS** (TCP only): `kernable certs new <dir> <client>...` creates a development CA and certificates. Then add `[tls]` to `kernable.toml` (see `crates/kernable-kernel/src/config.rs`) and pass `tls` to the SDKs (`{ ca, cert, key }` in TypeScript, `Tls(ca, cert, key)` in Python).
- **Metrics**: `metrics_listen = "127.0.0.1:9233"` serves Prometheus text at `/metrics`: commands by outcome, queue depth, worker sessions, execution, settle, query and middleware time, queries and middleware checks by outcome, and position, lag and connection state per listener.
- **Health and readiness** (on `metrics_listen`): `GET /health` answers 200 while the process runs. `GET /ready` answers 200 once every registered listener is connected and has caught up with the events committed before it connected, otherwise 503 naming the listeners it waits for (`kernable_ready` in the metrics). Until then, requests that need a missing middleware are refused with `UNAVAILABLE`.
- **Shutdown**: on SIGTERM or SIGINT the kernel refuses new submits, queries and subscriptions (`UNAVAILABLE`), lets running ones finish and settle for up to `drain_timeout_ms` (default 10 s; workers and listeners stay connected meanwhile), then saves listener positions and a final checkpoint. Commands that did not finish stay queued and run right after the restart.
- **Middleware** is registered in code (`listener.middleware({ types, order }, check)`) and allowed per key in `[middlewares]`. The kernel keeps the registration: while a middleware is away, everything it covers is refused with `UNAVAILABLE`. `kernable listeners <store>` shows the chain; `kernable listeners remove` (kernel stopped) unregisters a middleware for good.
- **Versions**: SDKs send the protocol version they speak; the kernel accepts a range (today 1 to 1) and refuses others with `FAILED_PRECONDITION`, saying whether the SDK or the kernel needs an update. Stores record their data format (SQLite `user_version`, a first record in the file log); a kernel refuses a store of another format at startup. Upgrades never happen on their own: stop the kernel, back up the store and run `kernable migrate <store>`.
- **Vault** (personal data and secrets): declare which fields the vault keeps, on the domain: `identity.command("RegisterAccount").personal(["email"], (d, id) => id).secret(["password"])`, `identity.event("AccountRegistered", 1).personal(["email"], (d, id) => id)`, and an erasing event `identity.event("AccountErased", 1).erases((d, id) => id)`. The signed log then holds references only. The kernel keeps the values in `[vault]` (SQLite, AES-256-GCM with the key in `key`, deleted rows overwritten) and hands them to workers and to the listener and middleware keys in `[vault.readers]`. Secrets are deleted once their command completed. An erasing event (by a worker key in `[vault.erasers]`) deletes the subject's values everywhere and drops the snapshots of every stream that used them; read models drop the subject's data when they see it, rebuilt they read `null`. Subscriptions and submit results get references only.
- **Persistent read models**: `kernable.readModel("accounts", { store: "sqlite:accounts.db", schema: "1" })` (Python: `store=`, `schema=`) keeps the listener's state in a key-value store with namespaces (`ctx.store.get/set/remove/all/reset`). Data and position are written in one transaction per batch, before the acknowledgement, so after a crash every event took effect exactly once; a restart continues after the stored position. A new `schema` clears the store and rebuilds from the first event. Node prints an experimental warning for `node:sqlite` the first time a store is opened.
- **Snapshots** live apart from the log: `snapshots = "sqlite:snapshots.db"` (or `memory:`, the default). They are a cache that workers rebuild at any time.
- **Event schemas**: a domain declares the payload version it writes and how older ones are upcast (`orders.event("OrderPlaced", 2).upcast(1, fn)`); listeners get them with `use(orders)`. The kernel refuses a worker that would write an older version than the store holds (`kernable schemas <store>` lists the stored ones). A listener that meets a version it does not know stops and logs until it is updated; because listeners run in order, so do the listeners after it. Deploy readers before writers.
- **Keys**: `kill -HUP` reloads `kernable.toml`: allowlists, `[successors]` (a listener moves to a new key and keeps its place) and `[revoked]` (a key with the time from which it is untrusted: refused at once, stored by the kernel for good, and flagged by `kernable verify`, which exits 2 on suspects). `kernable verify` takes every kernel key the store was signed with; `kernable vault rekey` re-encrypts the vault.
- **Logs**: structured, to stderr. `KERNABLE_LOG` sets the filter (default `info`), and `KERNABLE_LOG_FORMAT=json` switches to JSON.
