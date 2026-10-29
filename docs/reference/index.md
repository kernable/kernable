# Reference

Look things up here; the [concepts](../concepts/) explain why they are so. Most pages are generated from the sources and checked in every build, so they match the code they ship with: the flags, the CLI, the protocol, the metrics and the three SDKs. [Signatures](./signing) is included from `proto/SIGNING.md`. [Errors](./errors) is written by hand, and the build fails if a rejection code in the sources is missing from it.

| Page | What it covers |
|---|---|
| [kernable start](./configuration) | Every flag and environment variable of the kernel, with defaults, by topic: trust, stores, network, timeouts; `--dev` |
| [CLI](./cli) | The `kernable` command: run the kernel; keys, grants, revocation and certificates; apply a membership and list services, workers, listeners and schemas; verify and checkpoint a store; remove registrations, migrate a store, rekey the vault; submit a command |
| [TypeScript SDK](./sdk-typescript) | `@kernable/sdk` |
| [Python SDK](./sdk-python) | `kernable` for Python |
| [Rust SDK](./sdk-rust) | `kernable` for Rust |
| [Protocol](./protocol) | The gRPC services and messages (`kernable.v1`), for writing an SDK |
| [Signatures](./signing) | Signed byte layouts, the hash chain, vault references, test vectors |
| [Errors](./errors) | Rejection codes and gRPC statuses, and what a client should do about each |
| [Metrics and health](./metrics) | Prometheus metrics, `/health` and `/ready` |

The kernel's UI in a browser (`--ui-listen`) has no reference page: [Monitor kernable](../how-to/monitor#the-ui) describes it.
