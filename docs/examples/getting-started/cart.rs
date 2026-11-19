// #region file
use kernable::{BoxError, Kernable, NewEvent, Rejected, domain};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region aggregate
    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Cart {
        items: u32,
        checked_out: bool,
    }

    let shop_domain = domain("shop");
    let cart_aggregate = shop_domain.aggregate("cart", Cart::default());
    // #endregion aggregate

    // #region events
    #[derive(Serialize, Deserialize)]
    struct Item {
        sku: String,
        qty: u32,
    }

    cart_aggregate.apply("ItemAdded", |state, data: Item, _evt, _ctx| {
        state.items += data.qty
    });
    cart_aggregate.apply("CheckedOut", |state, _: Value, _evt, _ctx| {
        state.checked_out = true
    });
    // #endregion events

    // #region commands
    cart_aggregate.command("AddItem", |state, data: Item, _cmd, _ctx| {
        if state.checked_out {
            return Err(Rejected::new("Cart is checked out"));
        }
        Ok(vec![NewEvent::new("ItemAdded", data)])
    });

    cart_aggregate.command("CheckOut", |state, _: Value, _cmd, ctx| {
        if state.items == 0 {
            return Err(Rejected::new("Cart is empty"));
        }
        if state.checked_out {
            return Err(Rejected::new("Cart is checked out"));
        }
        // ctx.now() and ctx.id() stay the same if the command runs again.
        Ok(vec![NewEvent::new(
            "CheckedOut",
            json!({ "at": ctx.now().to_string(), "orderId": ctx.id() }),
        )])
    });
    // #endregion commands

    // #region worker
    let kernable = Kernable::new("app.key")?;
    kernable.worker(&cart_aggregate).run().await;
    // #endregion worker
    Ok(())
}
// #endregion file
