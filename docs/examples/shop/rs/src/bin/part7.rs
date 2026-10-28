//! Part 7: personal data in the vault, passwords as secrets, erasure.
use kernable::{BoxError, Error, Kernable, Options, Outcome, Query, RequestContext, Submit};
use serde_json::{Value, json};
use shop::shop;

fn show(what: &str, outcome: Outcome) {
    match outcome {
        Outcome::Committed(_) => println!("{what}: committed"),
        Outcome::Rejected(r) => println!("{what}: {} ({})", r.code, r.message),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region try
    // with_domain: the client moves the fields the domain declares personal or secret to the vault.
    let me = RequestContext {
        sender_identity: "c1".into(),
        ..Default::default()
    };
    let kernable = Kernable::new(Options::default().context(me))?.with_domain(&shop().domain);
    let customer = |command_type, data: Value| {
        kernable.submit(Submit::new("shop", "customer", "c1", command_type).data(data))
    };
    let read =
        || kernable.query::<Value>(Query::new("shop", "GetCustomer").data(json!({ "id": "c1" })));

    show(
        "register",
        customer(
            "Register",
            json!({ "email": "ann@example.com", "name": "Ann" }),
        )
        .await?,
    );
    show(
        "set password",
        customer("SetPassword", json!({ "password": "correct horse" })).await?,
    );
    show(
        "sign in, wrong password",
        customer("SignIn", json!({ "password": "wrong horse" })).await?,
    );
    show(
        "sign in",
        customer("SignIn", json!({ "password": "correct horse" })).await?,
    );
    println!("{}", read().await?);
    show("erase", customer("Erase", Value::Null).await?);
    // Erased: the customer's data is gone, and with it the right to act.
    if let Err(Error::Rejected(r)) = read().await {
        println!("read: {}", r.code);
    }
    // #endregion try
    Ok(())
}
