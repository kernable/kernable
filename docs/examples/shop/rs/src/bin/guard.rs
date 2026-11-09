//! The guard: a middleware that lets customers act only on themselves and their own orders.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use kernable::{BoxError, CheckKind, Kernable, MiddlewareOptions, Options, Rejected};
use serde_json::Value;
use shop::shop;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region guard
    const SERVICES: [&str; 1] = ["svc-confirmations"];
    // What a business process may send (part 10): it acts as `process:<name>`.
    const PROCESSES: [(&str, &[&str]); 1] = [("process:shop.fulfilment", &["Ship", "Cancel"])];
    let registered = Arc::new(Mutex::new(HashSet::<String>::new()));
    let owner_of = Arc::new(Mutex::new(HashMap::<String, String>::new())); // order -> customer

    let shop_domain = shop().domain;
    let guard_readmodel = shop_domain.read_model("guard");
    let r = Arc::clone(&registered);
    guard_readmodel.on("CustomerRegistered", move |_: Value, evt, _ctx| {
        r.lock().unwrap().insert(evt.aggregate_id);
        async { Ok(()) }
    });
    let r = Arc::clone(&registered);
    guard_readmodel.on("CustomerErased", move |_: Value, evt, _ctx| {
        r.lock().unwrap().remove(&evt.aggregate_id);
        async { Ok(()) }
    });
    let o = Arc::clone(&owner_of);
    guard_readmodel.on("OrderPlaced", move |data: Value, evt, _ctx| {
        let customer = data["customerId"].as_str().unwrap_or_default().to_owned();
        o.lock().unwrap().insert(evt.aggregate_id, customer);
        async { Ok(()) }
    });
    let (r, o) = (Arc::clone(&registered), Arc::clone(&owner_of));
    guard_readmodel.middleware(MiddlewareOptions::new(&["shop.*"]), move |check| {
        let who = check.request.sender_identity.clone();
        let allowed = check.type_name == "Register" // anyone may sign up
            || (check.kind == CheckKind::Query && SERVICES.contains(&who.as_str())) // services may read
            || (check.kind == CheckKind::Command
                && PROCESSES.iter().any(|(process, types)| *process == who && types.contains(&check.type_name.as_str())))
            || {
                let field = |name: &str| check.data[name].as_str().map(str::to_owned);
                let target = match check.kind {
                    CheckKind::Command | CheckKind::Cancel => check.aggregate_id.clone(),
                    CheckKind::Query => field("id"),
                    // The shop keeps no files: no customer's to protect.
                    CheckKind::Data | CheckKind::Cache => None,
                };
                let owner = if check.type_name == "Place" {
                    field("customerId")
                } else if check.type_name == "GetCustomer" || check.aggregate_type.as_deref() == Some("customer") {
                    target
                } else {
                    target.and_then(|t| o.lock().unwrap().get(&t).cloned())
                };
                r.lock().unwrap().contains(&who) && owner.as_deref() == Some(who.as_str())
            };
        let who = if who.is_empty() { "anonymous".to_owned() } else { who };
        let refusal = format!("{} is not allowed for {who}", check.type_name);
        async move {
            if !allowed {
                return Err(Rejected::new(refusal).code("forbidden").into());
            }
            Ok(())
        }
    });
    let kernable = Kernable::new(Options::default())?;
    kernable.listener(&guard_readmodel).run().await;
    // #endregion guard
    Ok(())
}
