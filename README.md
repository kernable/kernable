# kernable

Event sourcing and CQRS with a Rust kernel and business logic in any language.

The kernel stores commands and events, signs and checks every message, runs commands one at a time per aggregate, and delivers events to listeners in a fixed order. Your processes hold the business logic: workers decide, read models answer queries, reactors act, middleware checks. They connect over gRPC; TypeScript and Python have SDKs with the same API.

## Documentation

`docs/` is the documentation site (VitePress): `make docs` serves it at http://localhost:5173.

- **Tutorials:** getting started in ten minutes, and "Build a shop" in eight parts.
- **Concepts:** architecture, order and consistency, trust, data over time, failures.
- **How-to guides:** deploying, backups, monitoring, TLS, keys, event versions, erasure requests, new SDKs.
- **Reference:** `kernable.toml`, the CLI, both SDKs, the protocol, signatures, errors and metrics. Generated from the code.

Every code example in the documentation is a file that runs in `make check` against a real kernel.

## Layout

| Path | What it is |
|---|---|
| `proto/` | The protocol (`kernable.proto`), the store's log format (`storage.proto`), `SIGNING.md` and test vectors |
| `crates/kernable-core` | Protocol types, signing, hash chain, client transport |
| `crates/kernable-store` | The event store (`sqlite:`, `file:`, `memory:`), the snapshot store and the vault |
| `crates/kernable-kernel` | The kernel (`kernable-kernel <kernable.toml>`) |
| `crates/kernable-cli` | The `kernable` command: keys, certificates, verify, migrate, listeners, vault |
| `crates/kernable-bench`, `bench/` | Load generator, benchmark matrix, results |
| `sdks/typescript`, `sdks/python` | The SDKs |
| `examples/shop` | Reference application: identity in Python, orders and an HTTP API in TypeScript, with kill tests |
| `docs/` | The documentation site and its runnable examples |
| `fuzz/` | Fuzz targets (cargo-fuzz) |

## Develop

```sh
make check          # Rust, TypeScript, Python and the documentation, against every storage backend
                    # (Python ≥ 3.10; the first run installs the Python SDK's venv)
make check-packages # builds the npm package and the Python wheel, installs and runs them
make docs           # the documentation site; `npm run reference` in docs/ regenerates the reference
make torture        # crash points and property tests, many rounds (ROUNDS=20 CASES=10000)
make fuzz           # fuzzes the file log and the wire messages (nightly Rust, cargo-fuzz)
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```

`make check` also aborts the real kernel at every crash point (`crates/kernable-kernel/src/crash.rs`, built only with `--features crash-points`) and runs property tests on damaged logs, all storage backends and arbitrary requests.

`crates/kernable-kernel/tests/fixtures/format-<n>` holds frozen stores of every data format; `make check` migrates, verifies and runs them. Never regenerate them: a new format adds a directory (`KERNABLE_WRITE_FIXTURES=1` fills only missing ones) and a `kernable migrate` step.

The repository is private.
