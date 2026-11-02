//! The reactor: confirms every submitted order to the customer's email.
use kernable::{BoxError, Kernable, Options, Query, RequestContext, Submit};
use serde::Deserialize;
use serde_json::{Value, json};
use shop::shop;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region reactor
    // Who this service is (part 6), and the domain's declarations, e.g. which fields are personal (part 7).
    let me = RequestContext {
        sender_identity: "svc-confirmations".into(),
        ..Default::default()
    };
    let shop_domain = shop().domain;
    let kernable = Kernable::new(Options::default().context(me))?.with_domain(&shop_domain);

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct OrderRow {
        customer_id: String,
    }
    #[derive(Deserialize)]
    struct CustomerRow {
        email: String,
    }

    let confirmations_reactor = shop_domain.reactor("confirmations").on(
        "OrderSubmitted",
        |_: Value, evt, ctx| async move {
            // The read models (order 0) have processed this event before the reactor (order 10) gets it.
            let get =
                |query_type, id: &str| Query::new("shop", query_type).data(json!({ "id": id }));
            let order: OrderRow = ctx.query(get("GetOrder", &evt.aggregate_id)).await?;
            let customer: CustomerRow = ctx.query(get("GetCustomer", &order.customer_id)).await?;
            // Same sender and correlation as the submit that caused the event; a rejection parks the event.
            let confirm = Submit::new("shop", "order", &evt.aggregate_id, "Confirm")
                .data(json!({ "customerId": order.customer_id, "email": customer.email }))
                .id(format!("confirm-{}", evt.aggregate_id)); // a redelivered event confirms once
            ctx.submit(confirm).await?;
            Ok(())
        },
    );
    kernable.listener(&confirmations_reactor).run().await;
    // #endregion reactor
    Ok(())
}
