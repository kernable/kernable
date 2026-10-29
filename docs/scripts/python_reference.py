"""Prints the Python SDK reference (Markdown) from the sources' docstrings and `#:` comments,
for the names in `kernable.__all__`. Reads the sources only; nothing is imported."""

import ast
import sys
from pathlib import Path

package = Path(sys.argv[1])
modules = {p.stem: (p.read_text(), ast.parse(p.read_text())) for p in package.glob("*.py")}
init_src, init = modules["__init__"]
exported = next(ast.literal_eval(n.value) for n in init.body
                if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "__all__")


def comments_before(src: str, line: int) -> str:
    lines = src.splitlines()[: line - 1]
    out = []
    for l in reversed(lines):
        if l.strip().startswith("#:"):
            out.insert(0, l.strip()[2:].strip())
        else:
            break
    return " ".join(out)


def signature(fn: ast.FunctionDef) -> str:
    args = ast.unparse(fn.args)
    returns = f" -> {ast.unparse(fn.returns)}" if fn.returns else ""
    return f"{fn.name}({args}){returns}".replace("self, ", "").replace("(self)", "()")


def one_line(doc: str | None) -> str:
    return " ".join((doc or "").split())


INTRO = '''# Python SDK

```sh
pip install kernable
```

Python ≥ 3.10; it brings `grpcio`, `protobuf` and `cryptography`. A kernel to connect to: `kernable start --dev` (see [Getting started](../tutorial/getting-started)).

A cart aggregate, a read model that answers a query, and a client that submits and reads back, in one process:

```python
import threading

from kernable import Kernable, Rejected, domain

shop_domain = domain("shop")
cart_aggregate = shop_domain.aggregate("cart", {"items": 0})
carts_readmodel = shop_domain.read_model("carts")
rows = {}


@cart_aggregate.apply("ItemAdded")
def _(state, data):
    state["items"] += data["qty"]


@cart_aggregate.command("AddItem")
def _(state, data):
    if data["qty"] < 1:
        raise Rejected("qty must be at least 1")
    return [{"type": "ItemAdded", "data": data}]


@carts_readmodel.on("ItemAdded")
def _(data, evt):
    rows[evt.aggregate_id] = rows.get(evt.aggregate_id, 0) + data["qty"]


@carts_readmodel.query("GetCart")
def _(data):
    return {"items": rows.get(data["id"], 0)}


k = Kernable(key="app.key")
app = k.app(service="shop", domains=[shop_domain])
threading.Thread(target=app.run, daemon=True).start()
app.ready.wait(10)

k.submit("shop", "cart", "c1", "AddItem", {"sku": "apple", "qty": 2})
print(k.query("shop", "GetCart", {"id": "c1"}))  # {'items': 2}
```

How the SDK works:

- **Blocking calls.** Every call blocks the calling thread; there is no `asyncio`. `run()` of a worker, a listener or an app blocks until `stop()`; run it in a thread (`threading.Thread(target=x.run, daemon=True).start()`) to do something else, and wait for the connection with `x.ready.wait(seconds)`.
- **Decorators.** `apply`, `command`, `on`, `query`, `middleware` and `upcast` register a handler as a decorator (`@cart_aggregate.command("AddItem")`) or directly (`cart_aggregate.command("AddItem", fn)`).
- **Optional parameters.** A handler takes only the parameters it needs, in order: `def _(state, data)` or `def _(state, data, cmd, ctx)`.
- **Keyword arguments.** Options after the required arguments are keyword-only where the signature has a `*`: `k.submit(..., wait="committed")`, `domain.read_model("carts", store="sqlite:carts.db")`.
- **Errors.** A rejected command returns an `Outcome` with `committed=False`; a rejected query raises `Rejected`; a request the kernel refuses raises `grpc.RpcError` ([Errors](./errors)).

The rest of this page is generated from the docstrings in `sdks/python/kernable`.
'''

out = [INTRO]
for name in exported:
    for src, tree in modules.values():
        node = next((n for n in tree.body if getattr(n, "name", None) == name), None)
        if node is None and name == "PROTOCOL":
            node = next((n for n in tree.body if isinstance(n, ast.Assign)
                         and getattr(n.targets[0], "id", "") == "PROTOCOL"), None)
            if node:
                out += [f"### {name}", "", f"`{name} = {ast.unparse(node.value)}`: {comments_before(src, node.lineno)}", ""]
                break
        if node is None:
            continue
        if isinstance(node, ast.ClassDef):
            out += [f"### {name}", "", one_line(ast.get_docstring(node)), ""]
            for item in node.body:
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and not item.target.id.startswith("_"):
                    note = comments_before(src, item.lineno)
                    out.append(f"- `{item.target.id}: {ast.unparse(item.annotation)}`" + (f": {note}" if note else ""))
                elif isinstance(item, ast.FunctionDef) and (not item.name.startswith("_") or item.name == "__init__"):
                    doc = one_line(ast.get_docstring(item))
                    sig = signature(item).replace("__init__", name)
                    out.append(f"- `{sig}`" + (f": {doc}" if doc else ""))
            out.append("")
        elif isinstance(node, ast.FunctionDef):
            out += [f"### {name}()", "", f"`{signature(node)}`", "", one_line(ast.get_docstring(node)), ""]
        break
print("\n".join(out))
