//! Part 10: the fulfilment process ships a paid order and cancels an unpaid one.
use std::time::Duration;

use kernable::{BoxError, Kernable, Options, Query, RequestContext, Submit};
use serde::Deserialize;
use serde_json::{Value, json};
use shop::shop;

#[derive(Deserialize)]
struct OrderRow {
    status: String,
    cancelled: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let me = RequestContext {
        sender_identity: "c1".into(),
        ..Default::default()
    };
    let kernable = Kernable::new(Options::default().context(me))?.with_domain(&shop().domain);
    let send = |aggregate_type, aggregate_id, command_type, data: Value| {
        kernable.submit(Submit::new("shop", aggregate_type, aggregate_id, command_type).data(data))
    };
    // The order's status once it is `status`: the process moves it on its own, a little later.
    let until = async |id: &str, status: &str| -> Result<OrderRow, BoxError> {
        loop {
            let query = Query::new("shop", "GetOrder").data(json!({ "id": id }));
            let order: OrderRow = kernable.query(query).await?;
            if order.status == status {
                return Ok(order);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    // #region try
    let register = json!({ "email": "ann@example.com", "name": "Ann" });
    send("customer", "c1", "Register", register).await?;
    for id in ["o1", "o2"] {
        send("order", id, "Place", json!({ "customerId": "c1" })).await?;
        send("order", id, "AddItem", json!({ "sku": "apple", "qty": 1 })).await?;
        send("order", id, "Submit", Value::Null).await?; // confirmed by the reactor: a case starts for the order
    }
    send("order", "o1", "Pay", Value::Null).await?;
    println!("o1: {}", until("o1", "shipped").await?.status);
    // o2 is never paid: when its time is up, the process cancels it.
    let o2 = until("o2", "cancelled").await?;
    println!("o2: {} {}", o2.status, o2.cancelled.unwrap_or_default());
    // #endregion try
    Ok(())
}
