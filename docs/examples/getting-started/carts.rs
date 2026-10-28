// #region file
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use kernable::{BoxError, Kernable, Rejected, domain};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region register
    #[derive(Clone, Default, Serialize)]
    struct Row {
        items: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<String>,
    }

    let rows = Arc::new(Mutex::new(HashMap::<String, Row>::new()));
    let shop_domain = domain("shop");
    let carts_readmodel = shop_domain.read_model("carts");
    // #endregion register

    // #region events
    #[derive(Deserialize)]
    struct ItemAdded {
        qty: u32,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckedOut {
        order_id: String,
    }

    let r = Arc::clone(&rows);
    carts_readmodel.on("ItemAdded", move |data: ItemAdded, evt, _ctx| {
        r.lock().unwrap().entry(evt.aggregate_id).or_default().items += data.qty;
        async { Ok(()) }
    });

    let r = Arc::clone(&rows);
    carts_readmodel.on("CheckedOut", move |data: CheckedOut, evt, _ctx| {
        r.lock().unwrap().entry(evt.aggregate_id).or_default().order = Some(data.order_id);
        async { Ok(()) }
    });
    // #endregion events

    // #region queries
    #[derive(Deserialize)]
    struct GetCart {
        id: String,
    }

    let r = Arc::clone(&rows);
    carts_readmodel.query("GetCart", move |data: GetCart, _qry, _ctx| {
        let row = r.lock().unwrap().get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such cart").code("not_found"))?) }
    });
    // #endregion queries

    // #region run
    let kernable = Kernable::new("app.key")?;
    kernable.listener(&carts_readmodel).run().await;
    // #endregion run
    Ok(())
}
// #endregion file
