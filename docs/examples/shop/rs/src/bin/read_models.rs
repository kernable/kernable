//! The read models: customers and orders, in memory, answering queries.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use kernable::{BoxError, Kernable, Options, Rejected};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use shop::shop;

#[derive(Deserialize)]
struct Id {
    id: String,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let shop_domain = shop().domain;

    // #region customers
    #[derive(Clone, Serialize, Deserialize)]
    struct CustomerRow {
        id: String,
        email: String,
        name: String,
    }
    #[derive(Deserialize)]
    struct Registered {
        email: String,
        name: String,
    }
    let customers = Arc::new(Mutex::new(HashMap::<String, CustomerRow>::new()));

    let rows = Arc::clone(&customers);
    let customers_readmodel = shop_domain.read_model("customers").on(
        "CustomerRegistered",
        move |data: Registered, evt, _ctx| {
            let Registered { email, name } = data;
            let id = evt.aggregate_id;
            rows.lock()
                .unwrap()
                .insert(id.clone(), CustomerRow { id, email, name });
            async { Ok(()) }
        },
    );
    let rows = Arc::clone(&customers);
    customers_readmodel.query("GetCustomer", move |data: Id, _qry, _ctx| {
        let row = rows.lock().unwrap().get(&data.id).cloned();
        async move {
            Ok(row.ok_or_else(|| {
                Rejected::new(format!("No customer {}", data.id)).code("not_found")
            })?)
        }
    });
    // #endregion customers

    // #region orders
    #[derive(Clone, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct OrderRow {
        id: String,
        customer_id: String,
        status: String,
        items: serde_json::Map<String, serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        confirmed_to: Option<Option<String>>,
    }
    let orders = Arc::new(Mutex::new(HashMap::<String, OrderRow>::new()));

    let rows = Arc::clone(&orders);
    let orders_readmodel =
        shop_domain
            .read_model("orders")
            .on("OrderPlaced", move |data: Value, evt, _ctx| {
                let customer_id = data["customerId"].as_str().unwrap_or_default().to_owned();
                let id = evt.aggregate_id;
                let row = OrderRow {
                    id: id.clone(),
                    customer_id,
                    status: "draft".into(),
                    items: Default::default(),
                    confirmed_to: None,
                };
                rows.lock().unwrap().insert(id, row);
                async { Ok(()) }
            });
    let rows = Arc::clone(&orders);
    orders_readmodel.on("ItemAdded", move |data: Value, evt, _ctx| {
        let (sku, qty) = (
            data["sku"].as_str().unwrap_or_default(),
            data["qty"].as_i64().unwrap_or(0),
        );
        let mut rows = rows.lock().unwrap();
        let items = &mut rows.get_mut(&evt.aggregate_id).expect("placed").items;
        let count = items.get(sku).and_then(|n| n.as_i64()).unwrap_or(0) + qty;
        items.insert(sku.to_owned(), count.into());
        async { Ok(()) }
    });
    let rows = Arc::clone(&orders);
    orders_readmodel.on("OrderSubmitted", move |_: Value, evt, _ctx| {
        rows.lock()
            .unwrap()
            .get_mut(&evt.aggregate_id)
            .expect("placed")
            .status = "submitted".into();
        async { Ok(()) }
    });
    let rows = Arc::clone(&orders);
    orders_readmodel.query("GetOrder", move |data: Id, _qry, _ctx| {
        let row = rows.lock().unwrap().get(&data.id).cloned();
        async move {
            Ok(row
                .ok_or_else(|| Rejected::new(format!("No order {}", data.id)).code("not_found"))?)
        }
    });
    // #endregion orders

    // #region confirmed
    let rows = Arc::clone(&orders);
    orders_readmodel.on("OrderConfirmed", move |data: Value, evt, _ctx| {
        let mut rows = rows.lock().unwrap();
        let row = rows.get_mut(&evt.aggregate_id).expect("placed");
        row.status = "confirmed".into();
        row.confirmed_to = Some(data["email"].as_str().map(str::to_owned));
        async { Ok(()) }
    });
    // #endregion confirmed

    // #region erased
    // An erased customer's personal data must not outlive the erasure in a read model either.
    let rows = Arc::clone(&customers);
    customers_readmodel.on("CustomerErased", move |_: Value, evt, _ctx| {
        rows.lock().unwrap().remove(&evt.aggregate_id);
        async { Ok(()) }
    });
    let rows = Arc::clone(&orders);
    orders_readmodel.on("CustomerErased", move |_: Value, evt, _ctx| {
        for row in rows.lock().unwrap().values_mut() {
            if row.customer_id == evt.aggregate_id && row.confirmed_to.is_some() {
                row.confirmed_to = Some(None);
            }
        }
        async { Ok(()) }
    });
    // #endregion erased

    // #region run
    let kernable = Kernable::new(Options::default())?;
    let customers_readmodel = kernable.listener(&customers_readmodel);
    let orders_readmodel = kernable.listener(&orders_readmodel);
    tokio::join!(customers_readmodel.run(), orders_readmodel.run());
    // #endregion run
    Ok(())
}
