# Getting started

In this guide you run a kernel, write business logic in TypeScript, read its events from Python, and see that a request reads its own writes. Then you run the reference application.

You need Rust (stable), Node.js ≥ 22.18 and Python ≥ 3.10.

## 1. Build the kernel and the SDK packages

```sh
git clone <this repository> kernable && cd kernable
cargo build --release
make py                                   # Python SDK: venv and generated stubs
(cd sdks/typescript && npm install && npm pack --pack-destination /tmp)
sdks/python/.venv/bin/pip wheel sdks/python -w /tmp --no-deps
export PATH="$PWD/target/release:$PATH"
```

You now have the binaries `kernable-kernel` and `kernable`, plus `/tmp/kernable-sdk-0.1.0.tgz` and `/tmp/kernable-0.1.0-py3-none-any.whl`.

## 2. Configure and start a kernel

Every party signs what it sends with an Ed25519 key. The kernel trusts a key only for what its allowlists grant.

```sh
mkdir -p ~/kernable-demo && cd ~/kernable-demo
kernable keys new kernel >/dev/null      # signs checkpoints
APP=$(kernable keys new app)             # this demo uses one key for everything
cat > kernable.toml <<TOML
store = "sqlite:demo.db"                 # or "file:demo.log" or "memory:"
listen = "unix:$PWD/k.sock"
kernel_key = "kernel.key"

[clients]                                # who may submit which commands
"$APP" = ["*"]
[workers]                                # who may run which domain's commands
"$APP" = ["shop.*"]
[listeners]                              # who may read which events
"$APP" = ["*"]
[queriers]                               # who may ask which queries
"$APP" = ["*"]
[query_handlers]                         # which read models may answer which queries
"$APP" = ["shop.*"]
TOML
kernable-kernel kernable.toml
```

Leave the kernel running and continue in a second terminal.

## 3. Business logic in TypeScript

An aggregate belongs to a **domain**. `apply` folds events into state; `command` decides which events a command produces, or rejects it.

```sh
cd ~/kernable-demo
npm init -y >/dev/null && npm pkg set type=module && npm install /tmp/kernable-sdk-0.1.0.tgz
cat > cart.js <<'JS'
import { Kernable, Rejected, domain } from "@kernable/sdk";

export const cart = domain("shop").aggregate("Cart", { items: 0, checkedOut: false })
  .apply("ItemAdded", (s, e) => ({ ...s, items: s.items + e.qty }))
  .apply("CheckedOut", (s) => ({ ...s, checkedOut: true }))
  .command("AddItem", (s, cmd) => {
    if (s.checkedOut) throw new Rejected("Cart is checked out");
    return [{ type: "ItemAdded", data: { sku: cmd.sku, qty: cmd.qty } }];
  })
  .command("CheckOut", (s, _cmd, ctx) => {
    if (s.items === 0) throw new Rejected("Cart is empty");
    // ctx.now() and ctx.id() stay the same if the command runs again.
    return [{ type: "CheckedOut", data: { at: ctx.now().toISOString(), orderId: ctx.id() } }];
  });

const kernable = new Kernable({ url: `unix:${process.cwd()}/k.sock`, key: "app.key" });
await kernable.worker(cart).run();
JS
node cart.js
```

The worker connects to the kernel and waits for commands. Leave it running.

## 4. A read model in Python

A read model is a **listener** with an order. Read models default to order 0 and are rebuilt in memory on every start. Reactors default to order 10, so they run after the read models they query.

A read model's state is private. Everything else reads it through **queries**, which always go through the kernel, so signatures and allowlists apply to every read.

```sh
cd ~/kernable-demo
python3 -m venv py && py/bin/pip install -q /tmp/kernable-0.1.0-py3-none-any.whl
cat > carts.py <<'PY'
import os
from kernable import Kernable, Rejected

carts = {}
k = Kernable(f"unix:{os.getcwd()}/k.sock", "app.key")
model = k.read_model("carts")

@model.on("shop.ItemAdded")
def _(e):
    carts.setdefault(e.aggregate_id, {"items": 0})["items"] += e.data["qty"]

@model.on("shop.CheckedOut")
def _(e):
    carts[e.aggregate_id]["order"] = e.data["orderId"]

@model.query("shop.GetCart")
def _(data):
    if data["id"] not in carts:
        raise Rejected("no such cart", "not_found")
    return carts[data["id"]]

model.run()
PY
py/bin/python carts.py
```

## 5. Submit and read your own writes

```sh
cd ~/kernable-demo
cat > submit.js <<'JS'
import { Kernable } from "@kernable/sdk";
const kernable = new Kernable({ url: `unix:${process.cwd()}/k.sock`, key: "app.key" });
const cart = (type, data) => kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: "c1", type, data });
const getCart = () => kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });

await cart("AddItem", { sku: "apple", qty: 2 });
// submit returned only after every listener processed the event: no polling needed.
console.log(await getCart());
await cart("CheckOut");
console.log(await getCart());
console.log(await cart("AddItem", { sku: "pear", qty: 1 }));   // rejected: checked out
kernable.close();
JS
node submit.js
```

The output looks like this (the order ID will differ):

```
{ items: 2 }
{ items: 2, order: '0b9c…' }
{ committed: false, code: 'rejected', message: 'Cart is checked out' }
```

Check the integrity of everything stored so far:

```sh
kernable verify sqlite:demo.db kernel.pub
```

## 6. The reference application

`examples/shop` is a two-domain shop. The `identity` domain is written in Python, the `orders` domain and the HTTP API in TypeScript. A reactor confirms orders using the other domain's read model. Its acceptance tests run everything over TCP with mutual TLS and kill each process once:

```sh
make check            # all tests, including examples/shop/shop.test.ts
```

## Where to go next

- `proto/kernable/v1/kernable.proto` describes the protocol, and `proto/SIGNING.md` the exact signed byte layouts (with test vectors, so you can write an SDK for another language).
- `README.md` → *Operate* covers mutual TLS (`kernable certs new`), Prometheus metrics and logs.
- Rules worth knowing:
  - **Command handlers must be deterministic.** Use `ctx.now()` and `ctx.id()`.
  - **Reactors submit with `wait: "committed"`** and a fixed command ID. A settling submit inside a reactor would wait for the reactor itself.
  - **A domain only writes its own events.** It reacts to other domains through listeners.
