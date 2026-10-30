# Getting started

In this guide you run a kernel, write business logic and a read model in TypeScript, Python or Rust, in the same language or two different ones, and see that a request reads its own writes. Then you run the reference application.

You need Node.js ≥ 22.18 (step 5 submits from TypeScript), and Python ≥ 3.10 or Rust (stable) for the steps you write in them.

## 1. Install the kernel

::: warning Not released yet
kernable is in private preview: the install scripts and the SDK packages (`@kernable/sdk` on npm, `kernable` on PyPI and crates.io) are not public yet, so the commands below do not work for everyone yet. [Request early access](https://kernable.com/#access) to get the binaries and the SDKs.
:::

::: code-group

```sh [macOS, Linux]
curl -fsSL https://kernable.com/install.sh | sh
```

```powershell [Windows]
irm https://kernable.com/install.ps1 | iex
```

:::

This puts `kernable` on your `PATH`: `kernable start` runs the kernel, the other commands create keys and grants and verify a log (`kernable` alone lists them).

## 2. Start a kernel

```sh
mkdir kernable-demo
cd kernable-demo
kernable start --dev
```

The kernel keeps its events in `kernable.db` in this directory and listens on `http://127.0.0.1:7233`, for this machine only. `--dev` also creates the keys this guide uses, among them `app.key`, which may do everything here. [Keys and grants](#keys-and-grants) below explains what that means.

`--dev` also turns on the kernel's UI: open **http://127.0.0.1:7234** in a browser. It stays empty until the next steps connect something; then it shows them live.

Leave the kernel running and continue in a second terminal.

## 3. Business logic

A shopping cart: commands add items and check it out; events record what happened, and the cart's state is rebuilt from them. Pick TypeScript, Python or Rust; the tabs switch together.

**1. Register the aggregate** with its initial state:

::: code-group
<<< @/examples/getting-started/cart.ts#aggregate{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#aggregate{python} [Python]
<<< @/examples/getting-started/cart.rs#aggregate{rust} [Rust]
:::

**2. Apply events** to the state:

::: code-group
<<< @/examples/getting-started/cart.ts#events{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#events{python} [Python]
<<< @/examples/getting-started/cart.rs#events{rust} [Rust]
:::

**3. Decide commands:** return events, or reject with `Rejected`:

::: code-group
<<< @/examples/getting-started/cart.ts#commands{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#commands{python} [Python]
<<< @/examples/getting-started/cart.rs#commands{rust} [Rust]
:::

**4. Run a worker** that executes the cart's commands:

::: code-group
<<< @/examples/getting-started/cart.ts#worker{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#worker{python} [Python]
<<< @/examples/getting-started/cart.rs#worker{rust} [Rust]
:::

In a second terminal, in `kernable-demo`, set up a project, save the whole file and run it. Leave it running; it prints where it connected and what it runs:

```text
12:04:10  worker  connected to http://127.0.0.1:7233 · key app (69212a19f36f…) · runs shop.AddItem, shop.CheckOut
```

::: code-group

```sh [TypeScript]
npm init -y
npm pkg set type=module
npm install @kernable/sdk
# save cart.ts, then (Node ≥ 22.18 runs TypeScript directly):
node cart.ts
```

```sh [Python]
python3 -m venv py                # Windows: python -m venv py
py/bin/pip install kernable       # Windows: py\Scripts\pip install kernable
# save cart.py, then:
py/bin/python cart.py             # Windows: py\Scripts\python cart.py
```

```sh [Rust]
cargo init --name demo
cargo add kernable serde_json
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
# save src/bin/cart.rs, then:
cargo run --bin cart
```

:::

:::: details The whole file
::: code-group
<<< @/examples/getting-started/cart.ts#file{ts} [cart.ts]
<<< @/examples/getting-started/cart.py#file{python} [cart.py]
<<< @/examples/getting-started/cart.rs#file{rust} [src/bin/cart.rs]
:::
::::

More on aggregates and their handlers: [Event sourcing](../concepts/event-sourcing).

## 4. A read model

A read model is a **listener**: it builds its own view from the events and answers **queries** from it. Queries always go through the kernel, so signatures and grants apply to every read. The read model doesn't have to be in the worker's language: pick any tab, independent of step 3.

**1. Register the read model** on the domain, like the aggregate: its event and query types are then the domain's (`ItemAdded`, `GetCart`); another domain's would be qualified (`billing.InvoicePaid`). It keeps its rows in memory and reads every event again when it starts; with a store (`sqlite:carts.db`) it would keep them in a file and continue where it left off:

::: code-group
<<< @/examples/getting-started/carts.ts#register{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#register{python} [Python]
<<< @/examples/getting-started/carts.rs#register{rust} [Rust]
:::

**2. Handle events** and update the rows:

::: code-group
<<< @/examples/getting-started/carts.ts#events{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#events{python} [Python]
<<< @/examples/getting-started/carts.rs#events{rust} [Rust]
:::

**3. Answer queries** from the rows:

::: code-group
<<< @/examples/getting-started/carts.ts#queries{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#queries{python} [Python]
<<< @/examples/getting-started/carts.rs#queries{rust} [Rust]
:::

**4. Run it** through the connection, as the worker runs the aggregate. Read models run before reactors (order 0 and 10), so reactors can query them:

::: code-group
<<< @/examples/getting-started/carts.ts#run{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#run{python} [Python]
<<< @/examples/getting-started/carts.rs#run{rust} [Rust]
:::

In a third terminal, in `kernable-demo`, set up the language as in step 3 if it is new to you, save the whole file and run it. It prints where it starts and what it reads and answers:

```text
12:04:31  listener carts  connected to http://127.0.0.1:7233 · key app (69212a19f36f…) · from 0 · reads shop.ItemAdded, shop.CheckedOut · answers shop.GetCart
```

::: code-group

```sh [TypeScript]
node carts.ts
```

```sh [Python]
py/bin/python carts.py            # Windows: py\Scripts\python carts.py
```

```sh [Rust]
cargo run --bin carts             # saved as src/bin/carts.rs
```

:::

:::: details The whole file
::: code-group
<<< @/examples/getting-started/carts.ts#file{ts} [carts.ts]
<<< @/examples/getting-started/carts.py#file{python} [carts.py]
<<< @/examples/getting-started/carts.rs#file{rust} [src/bin/carts.rs]
:::
::::

## 5. Submit and read your own writes

A client submits commands and asks the read model. A command names the aggregate it goes to by domain, aggregate type and ID: `shop`, `cart`, `c1`. The comments show the output; the order ID will differ.

::: code-group
<<< @/examples/getting-started/submit.ts{ts} [submit.ts]
<<< @/examples/getting-started/submit.py{python} [submit.py]
<<< @/examples/getting-started/submit.rs{rust} [src/bin/submit.rs]
:::

In a fourth terminal, in `kernable-demo`, save the file and run it:

::: code-group

```sh [TypeScript]
node submit.ts
```

```sh [Python]
py/bin/python submit.py           # Windows: py\Scripts\python submit.py
```

```sh [Rust]
cargo run --bin submit            # saved as src/bin/submit.rs
```

:::

Now look at it in the UI, http://127.0.0.1:7234:

- **Map:** the cart writes `ItemAdded` and `CheckedOut`, the carts read model reads them and answers `GetCart`; green dots show both connected and up to date.
- **Connections:** the worker and the read model, each with its key and since when.
- **History:** pick `c1` and see its events in order, each with the command that caused it and who sent it.

## 6. One app

The worker and the read model ran as two processes. When they belong together, run them as one **app**: it runs everything defined on its domains and tells the kernel what the service consists of, so the kernel knows what to wait for (see [Remove a listener, middleware or aggregate](../how-to/remove-listener)). In one file with the aggregate and the read model, replace both run blocks with:

::: code-group

```ts [TypeScript]
await new Kernable({ key: "app.key" })
  .app({ service: "shop", version: "1.0.0", domains: [shopDomain] })
  .run();
```

```python [Python]
Kernable(key="app.key").app(service="shop", version="1.0.0", domains=[shop_domain]).run()
```

```rust [Rust]
use kernable::App;

let kernable = Kernable::new("app.key")?;
let app = kernable.app(App::new().service("shop").version("1.0.0").domain(&shop_domain))?;
app.run().await?;
```

:::

It prints one overview once everything is connected:

```text
kernable · shop 1.0.0
  kernel      http://127.0.0.1:7233 · key app (69212a19f36f…)
  membership  applied: 1 worker, 1 listener

  worker      cart   runs AddItem, CheckOut
  listener    carts  from 0 · reads ItemAdded, CheckedOut
                     answers GetCart

  ready in 12 ms
```

## 7. The reference application

`examples/shop` is a two-domain shop. The `identity` domain is written in Python, the `orders` domain and the HTTP API in TypeScript. A reactor confirms orders using the other domain's read model. Its acceptance tests run everything over TCP with mutual TLS and kill each process once:

```sh
make check            # all tests, including examples/shop/shop.test.ts
```

## Keys and grants

Every process signs what it sends with its own Ed25519 key, so the kernel and `kernable verify` know who sent every command and event. A signature proves who sent a message, but anyone can make a key. So the kernel trusts one **root key**, and the root key **grants** each key what it may do: which commands it may submit or run, which events it may read, which queries it may ask or answer.

`kernable start --dev` made a root key, `app.key` and a grant that lets `app` do everything; the SDKs send `app.grant` with every request signed by `app.key`. In production, every service gets its own key and only the grant it needs, and the root key stays offline:

```sh
kernable keys new root && kernable keys new kernel && kernable keys new orders
kernable grant root.key orders.pub 'run=shop.*' 'listen=shop.*'   # writes orders.grant
kernable start --root root.pub
```

[Part 8 of the shop tutorial](./shop/8-operate) sets this up for a whole application; [Trust](../concepts/trust) explains the model.

## Where to go next

- The [reference](../reference/) covers every [kernel flag](../reference/configuration), the [CLI](../reference/cli) (also `kernable certs new` for mutual TLS), the SDKs, [metrics](../reference/metrics), the [protocol](../reference/protocol) and the exact [signed byte layouts](../reference/signing), with test vectors to write an SDK for another language.
- Rules worth knowing:
  - **Command handlers must be deterministic.** Use `ctx.now()` and `ctx.id()`.
  - **Reactors submit with a fixed command ID**, so a redelivered event is harmless. Their submits settle on the listeners before them (the read models), not on themselves: a listener handles one event at a time and never waits for itself. The kernel refuses at once what would wait for the sending listener, such as a query to its own read model.
  - **A domain only writes its own events.** It reacts to other domains through listeners.
  - **Personal data and secrets go to the vault**, never into the signed log: mark the fields on the domain (`personal`, `secret`) and erase a person with an erasing event (`erases`); see `EventSchema` and `CommandSchema` in the [TypeScript SDK](../reference/sdk-typescript).
  - **Events are never changed.** To change an event's payload, raise its version and add an upcast from the old one: `shopDomain.event("ItemAdded", 2).upcast(1, (d) => ({ ...d, count: d.qty }))`. Listeners in other processes and languages need the same definition (`use(shopDomain)`); a listener that meets an unknown version stops until it is updated.
