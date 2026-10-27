# Reference

Everything here except the error list is generated from the sources and checked in every build, so it matches the code it ships with.

| Page | What it covers |
|---|---|
| [kernable.toml](./configuration) | Every configuration key of the kernel, with defaults |
| [CLI](./cli) | The `kernable` command: keys, certificates, verify, migrate, listeners |
| [TypeScript SDK](./sdk-typescript) | `@kernable/sdk` |
| [Python SDK](./sdk-python) | `kernable` for Python |
| [Protocol](./protocol) | The gRPC services and messages (`kernable.v1`), for writing an SDK |
| [Signatures](./signing) | Signed byte layouts, the hash chain, vault references, test vectors |
| [Errors](./errors) | Rejection codes and gRPC statuses |
| [Metrics and health](./metrics) | Prometheus metrics, `/health` and `/ready` |
