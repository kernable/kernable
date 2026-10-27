# kernable for Python

The same API as the TypeScript SDK: aggregates with `apply` and `command`, a worker, `submit`
(waits until every listener has processed the events by default), `enqueue`, and ordered,
durable listeners via `read_model` (order 0, rebuilt in memory on start) and `reactor` (order 10,
resumes).

```sh
make py                                       # from the repo root: venv and generated stubs
PYTHONPATH=sdks/python sdks/python/.venv/bin/python my_service.py
```

`kernable/v1/` holds the generated gRPC stubs; it is not checked in.

A settling submit from a listener's handler waits only for the listeners with a lower order:
a listener handles one event at a time and never waits for itself.

The documentation (`docs/`) has the tutorials and the generated API reference; `examples/shop/identity/identity.py` is a complete service.
