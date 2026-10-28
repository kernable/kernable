# Test your domain

Aggregates are plain functions of events and commands, so most of your domain can be tested without a kernel: fold events into a state, then call a command handler on it.

::: code-group
<<< @/examples/how-to/order.test.ts#test [order.test.ts]
<<< @/examples/how-to/test_order.py#test [test_order.py]
<<< @/examples/shop/rs/tests/order.rs#test [tests/order.rs]
:::

These run with `node --test`, `python -m unittest` and `cargo test`. They test the [tutorial's](../tutorial/shop/2-commands) `order` aggregate.

## Against a real kernel

Read models, reactors and middleware are best tested end to end: run `kernable start --store memory:` (it keeps nothing) with keys and grants in a temporary directory, start your processes against it, and send commands and queries as your API would. kernable's own SDK tests do this in `sdks/typescript/test/kernel.ts`, `sdks/python/tests/kernel.py` and `sdks/rust/tests/common/mod.rs`; every example in this documentation runs that way.

Wait for readiness instead of sleeping: poll the kernel's `/ready`, or its metrics for your listeners being connected.
