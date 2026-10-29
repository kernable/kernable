//! The rbac middleware of "Queries and middleware".

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use kernable::{Listener, MiddlewareOptions, Rejected, domain};
use serde_json::Value;

/// A read model that is also a middleware: it learns from events, and decides on every shop request.
pub fn rbac() -> Listener {
    // #region rbac
    let identity_domain = domain("identity");
    let known = Arc::new(Mutex::new(HashSet::new()));
    let blocked = Arc::new(Mutex::new(HashSet::new()));
    let (k, b) = (Arc::clone(&known), Arc::clone(&blocked));
    let rbac_readmodel = identity_domain
        .read_model("rbac")
        .on("CustomerRegistered", move |_: Value, evt, _ctx| {
            k.lock().unwrap().insert(evt.aggregate_id);
            async { Ok(()) }
        })
        .on("CustomerBlocked", move |_: Value, evt, _ctx| {
            b.lock().unwrap().insert(evt.aggregate_id);
            async { Ok(()) }
        });
    let (k, b) = (Arc::clone(&known), Arc::clone(&blocked));
    rbac_readmodel.middleware(MiddlewareOptions::new(&["shop.*"]), move |check| {
        let who = check.request.sender_identity.clone();
        let refusal = if who.is_empty() {
            Some(Rejected::new("who are you?").code("unauthenticated"))
        } else if !k.lock().unwrap().contains(&who) {
            Some(Rejected::new(format!("{who} is no customer")).code("unknown_customer"))
        } else if b.lock().unwrap().contains(&who) {
            Some(Rejected::new(format!("{who} is blocked")).code("blocked"))
        } else {
            None
        };
        async move { refusal.map_or(Ok(()), |r| Err(r.into())) }
    });
    // #endregion rbac
    rbac_readmodel
}
