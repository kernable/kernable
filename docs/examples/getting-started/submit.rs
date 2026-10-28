use kernable::{BoxError, Kernable, Query, Submit};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let kernable = Kernable::new("app.key")?;
    let get_cart_c1 = Query::new("shop", "GetCart").data(json!({ "id": "c1" }));

    let add_apples =
        Submit::new("shop", "cart", "c1", "AddItem").data(json!({ "sku": "apple", "qty": 2 }));
    kernable.submit(add_apples).await?;
    // submit returned only after every listener processed the event: no polling needed.
    let cart: Value = kernable.query(get_cart_c1.clone()).await?;
    println!("{cart}");
    // {"items":2}

    kernable
        .submit(Submit::new("shop", "cart", "c1", "CheckOut"))
        .await?;
    let cart: Value = kernable.query(get_cart_c1).await?;
    println!("{cart}");
    // {"items":2,"order":"0b9c5e1a-6f3d-8a42-9c1e-5d7f2b8e4a10"}

    let add_pears =
        Submit::new("shop", "cart", "c1", "AddItem").data(json!({ "sku": "pear", "qty": 1 }));
    let outcome = kernable.submit(add_pears).await?;
    println!("{outcome:?}");
    // Rejected(Rejected { code: "rejected", message: "Cart is checked out" })

    Ok(())
}
