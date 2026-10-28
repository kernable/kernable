//! Part 2: commands, rejections and idempotency.
use kernable::{BoxError, Kernable, Options, Outcome, Submit};
use serde_json::{Value, json};

fn show(what: &str, outcome: Outcome) {
    match outcome {
        Outcome::Committed(events) => println!(
            "{what}: committed, version {}",
            events.last().map_or(0, |e| e.version)
        ),
        Outcome::Rejected(r) => println!("{what}: {} ({})", r.code, r.message),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let kernable = Kernable::new(Options::default())?;

    // #region try
    let send = |aggregate_type, aggregate_id, command_type, data: Value| {
        kernable.submit(Submit::new("shop", aggregate_type, aggregate_id, command_type).data(data))
    };
    let ann = json!({ "email": "ann@example.com", "name": "Ann" });

    show(
        "register",
        send("customer", "c1", "Register", ann.clone()).await?,
    );
    show(
        "register again",
        send("customer", "c1", "Register", ann).await?,
    );
    show(
        "place",
        send("order", "o1", "Place", json!({ "customerId": "c1" })).await?,
    );
    let apples = Submit::new("shop", "order", "o1", "AddItem")
        .data(json!({ "sku": "apple", "qty": 2 }))
        .id("add-1");
    show("add apples", kernable.submit(apples.clone()).await?);
    show("same command again", kernable.submit(apples).await?);
    show("submit", send("order", "o1", "Submit", Value::Null).await?);
    show(
        "add after submit",
        send("order", "o1", "AddItem", json!({ "sku": "pear", "qty": 1 })).await?,
    );
    // #endregion try
    Ok(())
}
