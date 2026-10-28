//! Part 6: the guard decides who may do what.
use kernable::{BoxError, Error, Kernable, Options, Outcome, Query, RequestContext, Submit};
use serde_json::{Value, json};

fn show(what: &str, outcome: Outcome) {
    match outcome {
        Outcome::Committed(_) => println!("{what}: committed"),
        Outcome::Rejected(r) => println!("{what}: {} ({})", r.code, r.message),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let kernable = Kernable::new(Options::default())?;

    // #region try
    // Who sends a request is part of its signed request context.
    let as_ = |who: &str| RequestContext {
        sender_identity: who.into(),
        ..Default::default()
    };
    let send = |who, aggregate_type, aggregate_id, command_type, data: Value| {
        let command = Submit::new("shop", aggregate_type, aggregate_id, command_type)
            .data(data)
            .context(as_(who));
        kernable.submit(command)
    };

    show(
        "c1 registers",
        send(
            "c1",
            "customer",
            "c1",
            "Register",
            json!({ "email": "ann@example.com", "name": "Ann" }),
        )
        .await?,
    );
    show(
        "c2 registers",
        send(
            "c2",
            "customer",
            "c2",
            "Register",
            json!({ "email": "bob@example.com", "name": "Bob" }),
        )
        .await?,
    );
    show(
        "c1 places o1",
        send("c1", "order", "o1", "Place", json!({ "customerId": "c1" })).await?,
    );
    show(
        "c2 adds to o1",
        send(
            "c2",
            "order",
            "o1",
            "AddItem",
            json!({ "sku": "apple", "qty": 1 }),
        )
        .await?,
    );
    show(
        "c2 places for c1",
        send("c2", "order", "o2", "Place", json!({ "customerId": "c1" })).await?,
    );
    let get_order = |who| {
        kernable.query::<Value>(
            Query::new("shop", "GetOrder")
                .data(json!({ "id": "o1" }))
                .context(as_(who)),
        )
    };
    println!("c1 reads o1: {}", get_order("c1").await?);
    if let Err(Error::Rejected(r)) = get_order("c2").await {
        println!("c2 reads o1: {} ({})", r.code, r.message);
    }
    // #endregion try
    Ok(())
}
