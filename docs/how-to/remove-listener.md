# Remove a listener or middleware

The kernel remembers every listener that ever connected, with its position. A listener you stopped using still counts:

- submits that settle wait for it, until `--settle-timeout-ms`;
- `/ready` answers 503;
- if it was a middleware, everything it checked is refused.

Remove it for good with the kernel stopped:

```sh
kernable listeners sqlite:shop.db                     # name, order, position, types, middleware
kernable listeners remove sqlite:shop.db old-orders
```

Then start the kernel. Renaming a listener is the same: the old name stays registered until you remove it.
