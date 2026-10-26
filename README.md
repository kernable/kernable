# kernable

Event sourcing / CQRS runtime with a Rust kernel and business logic in any language.

Workers connect to the kernel, which runs their command handlers and commits the events they return. All messages are signed, and stored events form a verifiable hash chain. This is the proof of concept; it is private.

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
| `examples/` | Order domain in TypeScript (SDK) and Python (raw stubs) |

## Develop

```sh
make check          # Rust + TypeScript tests against every storage backend
make check-py       # Python worker end to end (Python ≥ 3.10)
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```
