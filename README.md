# kernable

Event sourcing / CQRS runtime with a Rust kernel and business logic in any language.

Workers connect to the kernel, which runs their command handlers and commits the events they return. All messages are signed, and stored events form a verifiable hash chain. Read models and reactors are ordered, durable listeners; a submit returns once they have processed the command's events. This is the MVP in progress; it is private.

## Layout

| Path | What it is |
|---|---|
| `proto/` | The protocol (`kernable.proto`), the file-log format (`storage.proto`), `SIGNING.md` and shared test vectors |
| `crates/kernable-core` | Protocol types, signing, hash chain, client transport |
| `crates/kernable-store` | `Store` trait and backends: `sqlite:`, `file:`, `memory:` |
| `crates/kernable-kernel` | The kernel server (`kernable-kernel <kernable.toml>`) |
| `crates/kernable-cli` | `kernable keys new \| submit \| verify \| checkpoint \| listeners` |
| `crates/kernable-bench`, `bench/` | Load generator, benchmark matrix, results |
| `sdks/typescript` | `@kernable/sdk` |
| `sdks/python` | `kernable` for Python, same API |
| `examples/order-ts` | Order domain in TypeScript |
| `examples/shop` | Reference app: identity in Python, orders and the HTTP API in TypeScript |

## Develop

```sh
make check          # Rust, TypeScript and Python tests against every storage backend
                    # (Python ≥ 3.10; the first run installs the Python SDK's venv)
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```
