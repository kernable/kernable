# 3. Read models and queries

Aggregates decide; they do not answer questions like "what is in order o1?". That is the job of **read models**: listeners that receive events in order and keep a view of them, shaped for the questions you ask.

Read models are defined on the domain, like aggregates, and run through the connection with `listener`, like an aggregate with `worker`. A read model's state is private. Everything else reads it through **queries**, which go through the kernel like commands do: they are signed, allowed by the sender's grant, and checked by middleware (part 6).

## The read models

Create `read-models.ts` (or `read_models.py`, `src/bin/read_models.rs`, `read-models/main.go`) with the customers:

::: code-group
<<< @/examples/shop/ts/read-models.ts#customers [read-models.ts]
<<< @/examples/shop/py/read_models.py#customers [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#customers [read_models.rs]
<<< @/examples/shop/go/read-models/main.go#customers [read-models/main.go]
:::

and the orders:

::: code-group
<<< @/examples/shop/ts/read-models.ts#orders [read-models.ts]
<<< @/examples/shop/py/read_models.py#orders [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#orders [read_models.rs]
<<< @/examples/shop/go/read-models/main.go#orders [read-models/main.go]
:::

and run both:

::: code-group
<<< @/examples/shop/ts/read-models.ts#run [read-models.ts]
<<< @/examples/shop/py/read_models.py#run [read_models.py]
<<< @/examples/shop/rs/src/bin/read_models.rs#run [read_models.rs]
<<< @/examples/shop/go/read-models/main.go#run [read-models/main.go]
:::

A read model is rebuilt from the first event every time it starts; part 8 shows how to keep it in a file instead. Start it next to the worker (`node read-models.ts`, `.venv/bin/python read_models.py`, `cargo run -q --bin read_models` or `go run ./read-models`).

## Read your own writes

::: code-group
<<< @/examples/shop/ts/part3.ts#try [part3.ts]
<<< @/examples/shop/py/part3.py#try [part3.py]
<<< @/examples/shop/rs/src/bin/part3.rs#try [part3.rs]
<<< @/examples/shop/go/part3/main.go#try [part3/main.go]
:::

::: code-group
<<< @/examples/shop/ts/part3.out{text} [TypeScript]
<<< @/examples/shop/py/part3.out{text} [Python]
<<< @/examples/shop/rs/part3.out{text} [Rust]
<<< @/examples/shop/go/part3.out{text} [Go]
:::

The query right after `AddItem` already shows the apples. By default a submit **settles**: it returns only after every listener has processed the command's events. There is no polling and no "eventually". If you do not need that, submit with `wait: "committed"` and it returns once the events are stored.

Next: [4. A reactor](./4-reactor).
