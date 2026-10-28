//! Part 4: the reactor confirms a submitted order.
use kernable::{BoxError, Kernable, Options, Query, Submit};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let kernable = Kernable::new(Options::default())?;
    let send = |aggregate_type, aggregate_id, command_type, data: Value| {
        kernable.submit(Submit::new("shop", aggregate_type, aggregate_id, command_type).data(data))
    };

    // #region try
    send(
        "customer",
        "c1",
        "Register",
        json!({ "email": "ann@example.com", "name": "Ann" }),
    )
    .await?;
    send("order", "o1", "Place", json!({ "customerId": "c1" })).await?;
    send(
        "order",
        "o1",
        "AddItem",
        json!({ "sku": "apple", "qty": 2 }),
    )
    .await?;
    send("order", "o1", "Submit", Value::Null).await?;
    // The submit settled after the reactor, which confirmed the order before it returned.
    let order: Value = kernable
        .query(Query::new("shop", "GetOrder").data(json!({ "id": "o1" })))
        .await?;
    println!("{order}");
    // #endregion try
    Ok(())
}
