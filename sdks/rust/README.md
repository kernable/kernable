# kernable-sdk

Event sourcing and CQRS with a Rust kernel and business logic in any language. This is the
Rust SDK: aggregates, read models, reactors, middleware and business processes in Rust, run by
the [kernable](https://github.com/kernable/kernable) kernel. The TypeScript, Python and Rust SDKs
share one API.

```sh
cargo add kernable-sdk tokio --features tokio/full serde --features serde/derive
```

The crate is `kernable-sdk`; your code imports it as `kernable`.

Start a kernel (macOS and Linux):

```sh
curl -fsSL https://kernable.com/install.sh | sh
kernable start --dev            # a kernel on 127.0.0.1:7233, its UI on :7234, keys in this directory
```

A worker that decides on a cart's commands:

```rust
use kernable::{BoxError, Kernable, NewEvent, Rejected, domain};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Cart {
    items: u32,
}

#[derive(Serialize, Deserialize)]
struct Item {
    sku: String,
    qty: u32,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cart = domain("shop").aggregate("cart", Cart::default());
    cart.apply("ItemAdded", |state, data: Item, _evt, _ctx| state.items += data.qty);
    cart.command("AddItem", |_state, data: Item, _cmd, _ctx| {
        if data.qty < 1 {
            return Err(Rejected::new("At least one"));
        }
        Ok(vec![NewEvent::new("ItemAdded", data)])
    });

    Kernable::new("app.key")?.worker(&cart).run().await;
    Ok(())
}
```

The [getting-started guide](https://kernable.com/docs/tutorial/getting-started) goes on from
here: a read model, a query that reads its own writes, the same in the other languages. Then
[build a shop](https://kernable.com/docs/tutorial/shop/) in ten parts, and the
[Rust reference](https://kernable.com/docs/reference/sdk-rust).

`kernable-sdk add kit` copies the kits (tenancy, accounts, identities, groups, invitations and
authorization) into your project as code you own: see [Kits](https://kernable.com/docs/kits/).

kernable is before 1.0: the protocol and the APIs may still change between 0.x releases. MIT
license.
