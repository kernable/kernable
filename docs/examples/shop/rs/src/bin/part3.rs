//! Part 3: read your own writes through queries.
use kernable::{BoxError, Error, Kernable, Options, Query, Submit};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let kernable = Kernable::new(Options::default())?;
    let send = |aggregate_type, aggregate_id, command_type, data: Value| {
        kernable.submit(Submit::new("shop", aggregate_type, aggregate_id, command_type).data(data))
    };

    // #region try
    let get_order = |id: &str| {
        kernable.query::<Value>(Query::new("shop", "GetOrder").data(json!({ "id": id })))
    };

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
    // The submit returned after the read model processed the event: the query sees it.
    println!("{}", get_order("o1").await?);
    let customer: Value = kernable
        .query(Query::new("shop", "GetCustomer").data(json!({ "id": "c1" })))
        .await?;
    println!("{customer}");
    if let Err(Error::Rejected(r)) = get_order("o9").await {
        println!("{}: {}", r.code, r.message);
    }
    // #endregion try
    Ok(())
}
