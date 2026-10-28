//! The customers read model with a store: it keeps its rows in a file and continues after a
//! restart where it left off, instead of reading every event again.
use kernable::{BoxError, Kernable, Options, Rejected};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use shop::shop;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region persistent
    #[derive(Serialize, Deserialize)]
    struct CustomerRow {
        id: String,
        email: String,
        name: String,
    }
    #[derive(Deserialize)]
    struct Id {
        id: String,
    }

    let shop_domain = shop().domain;
    let customers_readmodel = shop_domain
        .read_model("customers")
        .store("sqlite:customers.db", "1");
    customers_readmodel.on("CustomerRegistered", |data: Value, evt, ctx| async move {
        let (email, name) = (
            data["email"].as_str().unwrap_or_default(),
            data["name"].as_str().unwrap_or_default(),
        );
        let row = CustomerRow {
            id: evt.aggregate_id.clone(),
            email: email.into(),
            name: name.into(),
        };
        ctx.store
            .expect("a store")
            .set("customers", &evt.aggregate_id, &row)?;
        Ok(())
    });
    customers_readmodel.on("CustomerErased", |_: Value, evt, ctx| async move {
        ctx.store
            .expect("a store")
            .remove("customers", &evt.aggregate_id)?;
        Ok(())
    });
    customers_readmodel.query("GetCustomer", |data: Id, _qry, ctx| async move {
        let row: Option<CustomerRow> = ctx.store.expect("a store").get("customers", &data.id)?;
        Ok(row.ok_or_else(|| Rejected::new(format!("No customer {}", data.id)).code("not_found"))?)
    });
    Kernable::new(Options::default())?
        .listener(&customers_readmodel)
        .run()
        .await;
    // #endregion persistent
    Ok(())
}
