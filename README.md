<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="crates/kernable-kernel/src/logo-dark.png">
    <img src="crates/kernable-kernel/src/logo.png" alt="kernable" width="360">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/kernable/kernable/actions/workflows/ci.yml"><img src="https://github.com/kernable/kernable/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license"></a>
  <a href="https://www.npmjs.com/package/@kernable/sdk"><img src="https://img.shields.io/npm/v/@kernable/sdk?label=npm" alt="npm"></a>
  <a href="https://pypi.org/project/kernable-sdk/"><img src="https://img.shields.io/pypi/v/kernable-sdk.svg?label=PyPI" alt="PyPI"></a>
  <a href="https://crates.io/crates/kernable-sdk"><img src="https://img.shields.io/crates/v/kernable-sdk?label=crates.io" alt="crates.io"></a>
  <a href="https://kernable.com/docs/"><img src="https://img.shields.io/badge/docs-kernable.com-0b5fff.svg" alt="Documentation"></a>
</p>

**Event sourcing and CQRS with a Rust kernel and business logic in any language.**

The kernel stores commands and events, signs and checks every message, runs commands one at a time per aggregate, and delivers events to listeners in a fixed order. Your services hold the business logic: workers decide, read models answer queries, reactors act, middleware checks. They connect over gRPC; TypeScript, Python and Rust have SDKs with the same API.

## What you get

- **Read your own writes.** A submit returns once every listener has processed the command's events, so the next query already sees them.
- **Signed and verifiable.** Every command, result and query is signed; the log is a hash chain with signed checkpoints, and `kernable verify` checks all of it.
- **Business processes.** A flow across commands, events, timers and queries, defined in code next to your domain; the kernel runs it, keeps every step in the log and shows each case in its UI.
- **Unique keys and requirements,** checked atomically at commit: no second account with the same email, no order for a closed customer.
- **Personal data, deletable.** Marked fields go to an encrypted vault instead of the log; erasing a person removes their data everywhere.
- **Operations built in.** A synchronous standby with automatic failover, partitions by tenant with live moves, continuous backups to object storage, scheduled commands, metrics and a web UI.
- **Kits.** Tenancy, accounts, identities, groups, invitations and authorization, copied into your project as code you own.

## Quickstart

```sh
curl -fsSL https://kernable.com/install.sh | sh     # macOS and Linux, x86_64 and arm64
mkdir demo && cd demo
kernable start --dev                                # a kernel on 127.0.0.1:7233, its UI on :7234
```

The kernel runs on Linux and macOS (Unix) only; on Windows, run it in WSL.

`--dev` creates the keys in the directory, among them `app.key`, which may do everything. In a second terminal, a worker in TypeScript (`npm install @kernable/sdk`; Python: `pip install kernable-sdk`, Rust: `cargo add kernable-sdk`):

```ts
import { Kernable, Rejected, domain } from "@kernable/sdk";

const cart = domain("shop").aggregate("cart", { items: 0 });
cart.apply("ItemAdded", (state, data: { qty: number }) => { state.items += data.qty; });
cart.command("AddItem", (state, data: { sku: string; qty: number }) => {
  if (data.qty < 1) throw new Rejected("At least one");
  return [{ type: "ItemAdded", data }];
});

await new Kernable({ key: "app.key" }).worker(cart).run();
```

The [getting-started guide](https://kernable.com/docs/tutorial/getting-started) goes on from here: a read model, a query that reads its own writes, and the same in Python and Rust. A container image runs the same kernel, listening with mutual TLS outside the container (see `Dockerfile`).

## Documentation

[kernable.com/docs](https://kernable.com/docs/), built from `docs/` (`make docs` serves it locally):

- **Tutorials:** [getting started](https://kernable.com/docs/tutorial/getting-started) in ten minutes, and [build a shop](https://kernable.com/docs/tutorial/shop/) in ten parts, from the first command to sign-in and a business process.
- **Concepts:** architecture, order and consistency, trust, unique keys, personal data, business processes, failures.
- **How-to guides:** deploying, standby and failover, partitions, backups, monitoring, TLS, keys, event versions, erasure requests, new SDKs.
- **Kits:** the domains you copy into your project, with their commands, events and permissions.
- **Reference:** the `kernable start` flags, the CLI, the SDKs, the protocol, signatures, errors and metrics, generated from the code.

Every code example in the documentation is a file that runs in `make check` against a real kernel.

## Layout

| Path | What it is |
|---|---|
| `proto/` | The protocol (`kernable.proto`), the store's log format (`storage.proto`), `SIGNING.md` and test vectors |
| `crates/kernable-core` | Protocol types, signing, hash chain, client transport |
| `crates/kernable-store` | The event store (SQLite, or `memory:` for tests), the snapshot store and the vault |
| `crates/kernable-kernel` | The kernel and the `kernable` command: `kernable start --root <root.pub>`, keys, grants, revocation, certificates, verify, listeners, vault |
| `crates/kernable-bench`, `bench/` | Load generator, benchmark matrix and results |
| `crates/kernable-soak` | The soak: the kernel and an app per SDK under load and random faults (`make soak`) |
| `sdks/typescript`, `sdks/python`, `sdks/rust` | The SDKs |
| `kits/` | The kits in TypeScript, Python and Rust, and the fixtures all three are tested against |
| `docs/` | The documentation site and its runnable examples (the tutorial's shop in three languages, tested as written) |
| `fuzz/` | Fuzz targets (cargo-fuzz) |
| `design/` | Design notes: decisions and the reasons behind them, for contributors |
| `scripts/` | The install script, release builds, the package check and the soak on a server |

## Develop

```sh
make test           # the simple tier: fmt, clippy and in-process tests, no kernel processes (about a minute)
make check          # everything, in four parts CI runs in parallel (Python ≥ 3.10, cargo-audit):
                    #   check-rust   lints, known vulnerable dependencies, every test on every storage backend
                    #   check-crash  the real kernel aborted at every crash point
                    #   check-sdks   the TypeScript and Python SDKs against a real kernel
                    #   check-docs   the documentation builds and every example runs
make check-packages # builds the npm package and the Python wheel, installs and runs them
make docs           # the documentation site; `npm run reference` in docs/ regenerates the reference
make torture        # crash points and property tests, many rounds (ROUNDS=20 CASES=10000)
make fuzz           # 8 targets: the store's files and log, the wire messages, config, grants,
                    #   the HTTP endpoints and registrations (nightly Rust, cargo-fuzz)
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```

`make check` also aborts the real kernel at every crash point (`crates/kernable-kernel/src/crash.rs`, built only with `--features crash-points`) and runs property tests on damaged logs, all storage backends and arbitrary requests.

`crates/kernable-kernel/tests/fixtures/format-<n>` holds a frozen store of the current data format; `make check` verifies and runs it. Before 1.0 the format stays 1 and changes in place: after a change, delete the directory and write it again with `KERNABLE_WRITE_FIXTURES=1` (see `tests/compat.rs`).

## Status

kernable is before 1.0. The protocol, the store formats and the SDKs' APIs may still change between
0.x releases, and there are no migrations between them: a store written by one 0.x version is not
guaranteed to open in the next. Pin the version you run, and read the release notes before you
update.

## Contributing

Issues and pull requests are welcome: see [CONTRIBUTING.md](.github/CONTRIBUTING.md). Report
security issues privately, as [SECURITY.md](.github/SECURITY.md) describes.

## License

kernable is open source under the [MIT license](LICENSE): the kernel, the store, the tools, the SDKs, the kits and the documentation's examples.

"kernable" and "kernable Cloud" are trademarks of kernable (proprietor: Artur Susdorf); the license grants no rights to the names.
