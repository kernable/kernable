# Test your domain

Aggregates are plain functions of events and commands, so most of your domain can be tested without a kernel: fold events into a state, then call a command handler on it.

::: code-group
<<< @/examples/how-to/order.test.ts#test [order.test.ts]
<<< @/examples/how-to/test_order.py#test [test_order.py]
:::

These run with `node --test` and `python -m unittest`. They test the [tutorial's](../tutorial/shop/2-commands) `Order`.

## Against a real kernel

Read models, reactors and middleware are best tested end to end: start `kernable-kernel` with a configuration in a temporary directory (`store = "memory:"` keeps nothing), start your processes against it, and send commands and queries as your API would. kernable's own SDK tests do this in `sdks/typescript/test/kernel.ts` and `sdks/python/tests/kernel.py`; every example in this documentation runs that way.

Wait for readiness instead of sleeping: poll the kernel's `/ready`, or its metrics for your listeners being connected.
