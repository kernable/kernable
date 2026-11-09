# kernable

Event sourcing and CQRS with a Rust kernel and business logic in any language.

The kernel stores commands and events, signs and checks every message, runs commands one at a time per aggregate, and delivers events to listeners in a fixed order. Your processes hold the business logic: workers decide, read models answer queries, reactors act, middleware checks. They connect over gRPC; TypeScript, Python and Rust have SDKs with the same API.

## Quickstart

```sh
curl -fsSL https://kernable.com/install.sh | sh     # macOS and Linux, x86_64 and arm64
mkdir demo && cd demo
kernable start --dev                                # a kernel on 127.0.0.1:7233, its UI on :7234
```

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

The [getting-started guide](docs/tutorial/getting-started.md) goes on from here: a read model, a query that reads its own writes, and the same in Python and Rust. A container image runs the same kernel, listening with mutual TLS outside the container (see `Dockerfile`).

## Documentation

`docs/` is the documentation site (VitePress): `make docs` serves it at http://localhost:5173.

- **Tutorials:** getting started in ten minutes, and "Build a shop" in eight parts.
- **Concepts:** architecture, order and consistency, trust, data over time, failures.
- **How-to guides:** deploying, backups, monitoring, TLS, keys, event versions, erasure requests, new SDKs.
- **Reference:** the `kernable start` flags, the CLI, the SDKs, the protocol, signatures, errors and metrics. Generated from the code.

Every code example in the documentation is a file that runs in `make check` against a real kernel.

## Layout

| Path | What it is |
|---|---|
| `proto/` | The protocol (`kernable.proto`), the store's log format (`storage.proto`), `SIGNING.md` and test vectors |
| `crates/kernable-core` | Protocol types, signing, hash chain, client transport |
| `crates/kernable-store` | The event store (`sqlite:`, `file:`, `memory:`), the snapshot store and the vault |
| `crates/kernable-kernel` | The kernel and the `kernable` command: `kernable start --root <root.pub>`, keys, grants, revocation, certificates, verify, listeners, vault |
| `crates/kernable-bench`, `bench/` | Load generator, benchmark matrix and results |
| `sdks/typescript`, `sdks/python`, `sdks/rust` | The SDKs |
| `examples/shop` | Reference application: identity in Python, orders and an HTTP API in TypeScript, with kill tests |
| `docs/` | The documentation site and its runnable examples |
| `fuzz/` | Fuzz targets (cargo-fuzz) |

## Develop

```sh
make test           # the simple tier: fmt, clippy and in-process tests, no kernel processes (about a minute)
make check          # Rust, TypeScript, Python, the SDKs and the documentation, against every storage backend,
                    # and known vulnerable dependencies (cargo install cargo-audit)
                    # (Python ≥ 3.10; the first run installs the Python SDK's venv)
make check-packages # builds the npm package and the Python wheel, installs and runs them
make docs           # the documentation site; `npm run reference` in docs/ regenerates the reference
make torture        # crash points and property tests, many rounds (ROUNDS=20 CASES=10000)
make fuzz           # fuzzes the file log and the wire messages (nightly Rust, cargo-fuzz)
./bench/run.sh      # benchmark matrix; STORE=file:k.log or STORE=memory: picks the backend
```

`make check` also aborts the real kernel at every crash point (`crates/kernable-kernel/src/crash.rs`, built only with `--features crash-points`) and runs property tests on damaged logs, all storage backends and arbitrary requests.

`crates/kernable-kernel/tests/fixtures/format-<n>` holds a frozen store of the current data format; `make check` verifies and runs it. There are no migrations before the first release: a format change bumps `kernable_store::FORMAT`, and the fixture is regenerated in a new directory (`KERNABLE_WRITE_FIXTURES=1` fills only missing ones).

## Status

kernable is before 1.0. The protocol, the store formats and the SDKs' APIs may still change between
0.x releases, and there are no migrations between them: a store written by one 0.x version is not
guaranteed to open in the next. Pin the version you run, and read the release notes before you
update.

## Contributing

Issues and pull requests are welcome: see [CONTRIBUTING.md](.github/CONTRIBUTING.md). Report
security issues privately, as [SECURITY.md](.github/SECURITY.md) describes.

## License

kernable is open source under the [MIT license](LICENSE): the kernel, the store, the tools, the SDKs, the kits and the examples.

"kernable" and "kernable Cloud" are trademarks of kernable (proprietor: Artur Susdorf); the license grants no rights to the names.
