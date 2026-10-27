# 5. A second language

So far every process was in one language. Nothing ties them together but the kernel's protocol, so each can be in another: the worker in Python, the read models in TypeScript, the reactor in Python again.

Stop your processes and start them mixed, from the files of parts 2 to 4:

```sh
.venv/bin/python worker.py &
node read-models.ts &
.venv/bin/python confirmations.py &
node part4.ts
```

The output is the same as in part 4, in either language of `part4`. kernable's tests run the shop in both mixes.

What a language needs to take part:

- **Its own definitions.** `domain.ts` and `domain.py` each declare the shop's aggregates and, in part 7, its personal fields. A read model in Python that reads a TypeScript worker's events needs no aggregate, only event handlers; but declarations that change how events are stored or read (personal fields, event versions) must exist in every language that writes or reads them.
- **JSON payloads.** Commands, events and query answers are JSON, so a `dict` in Python and an object in TypeScript are the same data.
- **An SDK.** TypeScript and Python have one. Any other language can join with the gRPC protocol and the signing rules in the [reference](../../reference/protocol); test vectors check an implementation.

Next: [6. Authorization](./6-authorization).
