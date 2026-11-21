# 5. A second language

So far every process was in one language. Nothing ties them together but the kernel's protocol, so each can be in another: the worker in Python, the read models in TypeScript, the reactor in Rust or Go.

Stop your processes and start them mixed, from the files of parts 2 to 4:

```sh
.venv/bin/python worker.py &
node read-models.ts &
cargo run -q --bin confirmations &
node part4.ts
```

The output is the same as in part 4 for the language of `part4`. kernable's tests run the shop in six mixes, each of the four languages in every role.

What a language needs to take part:

- **Its own definitions.** `domain.ts`, `domain.py`, `domain.rs` and `domain.go` each declare the shop's aggregates and, in part 7, its personal fields. A read model in Python that reads a TypeScript worker's events needs no aggregate, only event handlers; but declarations that change how events are stored or read (personal fields, event versions) must exist in every language that writes or reads them.
- **JSON payloads.** Commands, events and query answers are JSON, so a `dict` in Python, an object in TypeScript, a serde struct in Rust and a struct with JSON tags in Go are the same data. One limit: TypeScript reads every JSON number as a JavaScript number, exact only up to 2^53 (9,007,199,254,740,991); a larger integer, such as a 64-bit ID, comes back rounded there. Send such values as strings. Python, Rust and Go keep them exact (Go reads numbers in generic data, `any` and `map[string]any`, as `json.Number`).
- **An SDK.** TypeScript, Python, Rust and Go have one. Any other language can join with the gRPC protocol and the signing rules in the [reference](../../reference/protocol); test vectors check an implementation.

Next: [6. Authorization](./6-authorization).
