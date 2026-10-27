# 3. Read models and queries

Aggregates decide; they do not answer questions like "what is in order o1?". That is the job of **read models**: listeners that receive events in order and keep a view of them, shaped for the questions you ask.

A read model's state is private. Everything else reads it through **queries**, which go through the kernel like commands do: they are signed, allowed per key, and checked by middleware (part 6).

## The read models

Create `read-models.ts` (or `read_models.py`) with the customers:

::: code-group
<<< @/examples/shop/ts/read-models.ts#customers [read-models.ts]
<<< @/examples/shop/py/read_models.py#customers [read_models.py]
:::

and the orders:

::: code-group
<<< @/examples/shop/ts/read-models.ts#orders [read-models.ts]
<<< @/examples/shop/py/read_models.py#orders [read_models.py]
:::

and run both:

::: code-group
<<< @/examples/shop/ts/read-models.ts#run [read-models.ts]
<<< @/examples/shop/py/read_models.py#run [read_models.py]
:::

A read model is rebuilt from the first event every time it starts; part 8 shows how to keep it in a file instead. Start it next to the worker (`node read-models.ts` or `.venv/bin/python read_models.py`).

## Read your own writes

::: code-group
<<< @/examples/shop/ts/part3.ts#try [part3.ts]
<<< @/examples/shop/py/part3.py#try [part3.py]
:::

::: code-group
<<< @/examples/shop/ts/part3.out{text} [TypeScript]
<<< @/examples/shop/py/part3.out{text} [Python]
:::

The query right after `AddItem` already shows the apples. By default a submit **settles**: it returns only after every listener has processed the command's events. There is no polling and no "eventually". If you do not need that, submit with `wait: "committed"` and it returns once the events are stored.

Next: [4. A reactor](./4-reactor).
