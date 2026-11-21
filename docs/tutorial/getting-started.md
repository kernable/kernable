# Getting started

In this guide you run a kernel, write business logic and a read model in TypeScript, Python, Rust or Go, in the same language or two different ones, and see that a request reads its own writes. Then you run both as one app.

You need Node.js ≥ 22.18, Python ≥ 3.10, Rust (stable) or Go ≥ 1.26 for the steps you write in them.

## 1. Install the kernel

```sh
curl -fsSL https://kernable.com/install.sh | sh
```

The script installs the kernel on macOS and Linux; on Windows, run it and all the steps below in WSL, with the Linux commands.

It installs `kernable` into `~/.local/bin`; if that is not on your `PATH` yet, the script says so and how to add it. `kernable start` runs the kernel, the other commands create keys and grants and verify a log (`kernable` alone lists them).

## 2. Start a kernel

```sh
mkdir kernable-demo
cd kernable-demo
kernable start --dev
```

The kernel keeps its events in `kernable.db` in this directory and listens on `http://127.0.0.1:7233`, for this machine only. `--dev` also creates the keys this guide uses, among them `app.key`, which may do everything here. [Keys and grants](#keys-and-grants) below explains what that means.

`--dev` also turns on the kernel's UI: open **http://127.0.0.1:7234** in a browser. It stays empty until the next steps connect something; then it shows them live.

Leave the kernel running and continue in a second terminal. One kernel per address: if `kernable start` says `127.0.0.1:7233 is in use`, a kernel from an earlier try still runs; stop it first (`Ctrl-C` in its terminal).

## 3. Business logic

A shopping cart: commands add items and check it out; events record what happened, and the cart's state is rebuilt from them. Pick TypeScript, Python, Rust or Go; the tabs switch together.

**1. Register the aggregate** with its initial state:

::: code-group
<<< @/examples/getting-started/cart.ts#aggregate{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#aggregate{python} [Python]
<<< @/examples/getting-started/cart.rs#aggregate{rust} [Rust]
<<< @/examples/getting-started/go/cart/main.go#aggregate{go} [Go]
:::

**2. Apply events** to the state:

::: code-group
<<< @/examples/getting-started/cart.ts#events{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#events{python} [Python]
<<< @/examples/getting-started/cart.rs#events{rust} [Rust]
<<< @/examples/getting-started/go/cart/main.go#events{go} [Go]
:::

**3. Decide commands:** return events, or reject with `Rejected`:

::: code-group
<<< @/examples/getting-started/cart.ts#commands{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#commands{python} [Python]
<<< @/examples/getting-started/cart.rs#commands{rust} [Rust]
<<< @/examples/getting-started/go/cart/main.go#commands{go} [Go]
:::

**4. Run a worker** that executes the cart's commands:

::: code-group
<<< @/examples/getting-started/cart.ts#worker{ts} [TypeScript]
<<< @/examples/getting-started/cart.py#worker{python} [Python]
<<< @/examples/getting-started/cart.rs#worker{rust} [Rust]
<<< @/examples/getting-started/go/cart/main.go#worker{go} [Go]
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
python3 -m venv py
py/bin/pip install kernable-sdk
# save cart.py, then:
py/bin/python cart.py
```

```sh [Rust]
cargo init --name demo
cargo add kernable-sdk serde_json
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
# save src/bin/cart.rs, then:
cargo run --bin cart
```

```sh [Go]
go mod init demo
go get github.com/kernable/kernable/sdks/go
# save cart/main.go, then:
go run ./cart
```

:::

:::: details The whole file
::: code-group
<<< @/examples/getting-started/cart.ts#file{ts} [cart.ts]
<<< @/examples/getting-started/cart.py#file{python} [cart.py]
<<< @/examples/getting-started/cart.rs#file{rust} [src/bin/cart.rs]
<<< @/examples/getting-started/go/cart/main.go#file{go} [cart/main.go]
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
<<< @/examples/getting-started/go/carts/main.go#register{go} [Go]
:::

**2. Handle events** and update the rows:

::: code-group
<<< @/examples/getting-started/carts.ts#events{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#events{python} [Python]
<<< @/examples/getting-started/carts.rs#events{rust} [Rust]
<<< @/examples/getting-started/go/carts/main.go#events{go} [Go]
:::

**3. Answer queries** from the rows:

::: code-group
<<< @/examples/getting-started/carts.ts#queries{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#queries{python} [Python]
<<< @/examples/getting-started/carts.rs#queries{rust} [Rust]
<<< @/examples/getting-started/go/carts/main.go#queries{go} [Go]
:::

**4. Run it** through the connection, as the worker runs the aggregate. Read models run before reactors (order 0 and 10), so reactors can query them:

::: code-group
<<< @/examples/getting-started/carts.ts#run{ts} [TypeScript]
<<< @/examples/getting-started/carts.py#run{python} [Python]
<<< @/examples/getting-started/carts.rs#run{rust} [Rust]
<<< @/examples/getting-started/go/carts/main.go#run{go} [Go]
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
py/bin/python carts.py
```

```sh [Rust]
cargo run --bin carts             # saved as src/bin/carts.rs
```

```sh [Go]
go run ./carts                    # saved as carts/main.go
```

:::

:::: details The whole file
::: code-group
<<< @/examples/getting-started/carts.ts#file{ts} [carts.ts]
<<< @/examples/getting-started/carts.py#file{python} [carts.py]
<<< @/examples/getting-started/carts.rs#file{rust} [src/bin/carts.rs]
<<< @/examples/getting-started/go/carts/main.go#file{go} [carts/main.go]
:::
::::

## 5. Submit and read your own writes

A client submits commands and asks the read model. A command names the aggregate it goes to by domain, aggregate type and ID: `shop`, `cart`, `c1`. The comments show the output; the order ID will differ.

::: code-group
<<< @/examples/getting-started/submit.ts{ts} [submit.ts]
<<< @/examples/getting-started/submit.py{python} [submit.py]
<<< @/examples/getting-started/submit.rs{rust} [src/bin/submit.rs]
<<< @/examples/getting-started/go/submit/main.go{go} [submit/main.go]
:::

In a fourth terminal, in `kernable-demo`, save the file and run it:

::: code-group

```sh [TypeScript]
node submit.ts
```

```sh [Python]
py/bin/python submit.py
```

```sh [Rust]
cargo run --bin submit            # saved as src/bin/submit.rs
```

```sh [Go]
go run ./submit                   # saved as submit/main.go
```

:::

Now look at it in the UI, http://127.0.0.1:7234:

- **Overview:** *Healthy*, and the commands and events so far.
- **Explore › Domains:** the `shop` domain with the cart, its commands and the events it writes (`ItemAdded`, `CheckedOut`), read by the carts read model, which answers `GetCart`; green dots show both connected and up to date.
- **Operations:** the worker and the read model, each with its key, since when, and how far behind.
- **Search** `c1`: the cart and its events in order, each with the command that caused it and who sent it.

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

```go [Go]
client, err := kernable.New(kernable.Options{Key: "app.key"})
if err != nil {
	log.Fatal(err)
}
app, err := client.App(kernable.AppOptions{Service: "shop", Version: "1.0.0", Domains: []*kernable.Domain{shopDomain}})
if err != nil {
	log.Fatal(err)
}
log.Fatal(app.Run(context.Background()))
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

- [Build a shop](./shop/) builds an application step by step, in TypeScript, Python, Rust and Go: commands, read models, reactors, an HTTP API with sign-in, operations and a business process. Every part runs as written in the tests (`make check`), in each language and mixed, and with each process killed once.
- The [reference](../reference/) covers every [kernel flag](../reference/configuration), the [CLI](../reference/cli) (also `kernable certs new` for mutual TLS), the SDKs, [metrics](../reference/metrics), the [protocol](../reference/protocol) and the exact [signed byte layouts](../reference/signing), with test vectors to write an SDK for another language.
- Rules worth knowing:
  - **Command handlers must be deterministic.** Use `ctx.now()` and `ctx.id()`.
  - **Reactors submit with a fixed command ID**, so a redelivered event is harmless. Their submits settle on the listeners before them (the read models), not on themselves: a listener handles one event at a time and never waits for itself. The kernel refuses at once what would wait for the sending listener, such as a query to its own read model.
  - **A domain only writes its own events.** It reacts to other domains through listeners.
  - **Personal data and secrets go to the vault**, never into the signed log: mark the fields on the domain (`personal`, `secret`) and erase a person with an erasing event (`erases`); see `EventSchema` and `CommandSchema` in the [TypeScript SDK](../reference/sdk-typescript).
  - **Events are never changed.** To change an event's payload, raise its version and add an upcast from the old one: `shopDomain.event("ItemAdded", 2).upcast(1, (d) => ({ ...d, count: d.qty }))`. Listeners in other processes and languages need the same definition (`use(shopDomain)`); a listener that meets an unknown version stops until it is updated.
