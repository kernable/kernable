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
```

Save this as `cart.ts` (Node ≥ 22.18 runs TypeScript directly):

<<< @/examples/getting-started/cart.ts

```sh
node cart.ts
```

The worker connects to the kernel and waits for commands. Leave it running.

## 4. A read model in Python

A read model is a **listener** with an order. Read models default to order 0. Reactors default to order 10, so they run after the read models they query.

This one keeps its state in memory and reads every event again when it starts. With `store="sqlite:carts.db"` it would keep its state in a file and continue where it left off.

A read model's state is private. Everything else reads it through **queries**, which always go through the kernel, so signatures and allowlists apply to every read.

```sh
cd ~/kernable-demo
python3 -m venv py && py/bin/pip install -q /tmp/kernable-0.1.0-py3-none-any.whl
```

Save this as `carts.py`:

<<< @/examples/getting-started/carts.py

```sh
py/bin/python carts.py
```

## 5. Submit and read your own writes

Save this as `submit.ts`:

<<< @/examples/getting-started/submit.ts

```sh
node submit.ts
```

The output looks like this (the order ID will differ):

<<< @/examples/getting-started/submit.out{text}

## 6. The reference application

`examples/shop` is a two-domain shop. The `identity` domain is written in Python, the `orders` domain and the HTTP API in TypeScript. A reactor confirms orders using the other domain's read model. Its acceptance tests run everything over TCP with mutual TLS and kill each process once:

```sh
make check            # all tests, including examples/shop/shop.test.ts
```

## Where to go next

- The [reference](../reference/) covers every [configuration key](../reference/configuration), the [CLI](../reference/cli) (also `kernable certs new` for mutual TLS), the SDKs, [metrics](../reference/metrics), the [protocol](../reference/protocol) and the exact [signed byte layouts](../reference/signing), with test vectors to write an SDK for another language.
- Rules worth knowing:
  - **Command handlers must be deterministic.** Use `ctx.now()` and `ctx.id()`.
  - **Reactors submit with a fixed command ID**, so a redelivered event is harmless. Their submits settle on the listeners before them (the read models), not on themselves: a listener handles one event at a time and never waits for itself. The kernel refuses at once what would wait for the sending listener, such as a query to its own read model.
  - **A domain only writes its own events.** It reacts to other domains through listeners.
  - **Personal data and secrets go to the vault**, never into the signed log: mark the fields on the domain (`personal`, `secret`) and erase a person with an erasing event (`erases`); see `EventSchema` and `CommandSchema` in the [TypeScript SDK](../reference/sdk-typescript).
  - **Events are never changed.** To change an event's payload, raise its version and add an upcast from the old one: `shop.event("ItemAdded", 2).upcast(1, (d) => ({ ...d, count: d.qty }))`. Listeners in other processes and languages need the same definition (`use(shop)`); a listener that meets an unknown version stops until it is updated.
