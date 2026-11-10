# kernable-sdk

Event sourcing and CQRS with a Rust kernel and business logic in any language. This is the
Python SDK: aggregates, read models, reactors, middleware and business processes in Python, run by
the [kernable](https://github.com/kernable/kernable) kernel. The TypeScript, Python and Rust SDKs
share one API.

```sh
pip install kernable-sdk        # Python 3.10 or newer; import it as `kernable`
```

Start a kernel (macOS and Linux):

```sh
curl -fsSL https://kernable.com/install.sh | sh
kernable start --dev            # a kernel on 127.0.0.1:7233, its UI on :7234, keys in this directory
```

A worker that decides on a cart's commands:

```python
from kernable import Kernable, Rejected, domain

cart = domain("shop").aggregate("cart", {"items": 0})


@cart.apply("ItemAdded")
def _(state, data):
    state["items"] += data["qty"]


@cart.command("AddItem")
def _(state, data):
    if data["qty"] < 1:
        raise Rejected("At least one")
    return [{"type": "ItemAdded", "data": data}]


Kernable(key="app.key").worker(cart).run()
```

The [getting-started guide](https://kernable.com/docs/tutorial/getting-started) goes on from
here: a read model, a query that reads its own writes, the same in the other languages. Then
[build a shop](https://kernable.com/docs/tutorial/shop/) in ten parts, and the
[Python reference](https://kernable.com/docs/reference/sdk-python).

`kernable-sdk add kit` copies the kits (tenancy, accounts, identities, groups, invitations and
authorization) into your project as code you own: see [Kits](https://kernable.com/docs/kits/).

kernable is before 1.0: the protocol and the APIs may still change between 0.x releases. MIT
license.

## Developing this SDK

From the repository root, `make py` makes the venv and generates the gRPC stubs in
`kernable/v1/` (not checked in); `make test` and `make check-sdks` run the tests.
