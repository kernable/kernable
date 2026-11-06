//! Lists that show only what one may see: the middleware decides on single requests; a read
//! model answering a list filters its rows with the same decision. `visibility(&listener)` teaches
//! an app's read model what the middleware knows; ask it in the query handler:
//!
//! ```ignore
//! let orders = shop_domain.read_model("shop.orders");
//! orders.on(OrderPlaced, ...);                 // your handlers first
//! let visible = visibility(&orders);
//! orders.query(ListOrders, move |_, qry, ctx| {
//!     let mine = rows.iter().filter(|o| visible.allows(&ctx.store, &qry.tenant, &ctx.request, "shop.GetOrder", &Target::new("shop.order", &o.id)));
//!     ...
//! });
//! ```

use std::sync::Arc;

use kernable::{Check, CheckKind, Listener, RequestContext, Store, Timestamp};
use serde_json::Value;

use super::middleware::{Seen, learn, learned_events};
use super::policy::{KeyValues, Knowledge, MemoryKeyValues, decide};
use super::relations::Target;

/// The listener's store under a prefix of its own, so the kit's namespaces never meet the app's.
struct Prefixed<'a>(&'a Store);

impl KeyValues for Prefixed<'_> {
    fn get(&self, ns: &str, key: &str) -> Option<Value> {
        KeyValues::get(self.0, &format!("kit:{ns}"), key)
    }
    fn set(&self, ns: &str, key: &str, value: Value) -> Result<(), String> {
        KeyValues::set(self.0, &format!("kit:{ns}"), key, value)
    }
    fn remove(&self, ns: &str, key: &str) -> Result<(), String> {
        KeyValues::remove(self.0, &format!("kit:{ns}"), key)
    }
    fn keys(&self, ns: &str) -> Vec<String> {
        KeyValues::keys(self.0, &format!("kit:{ns}"))
    }
}

/// What the middleware knows, in an app's read model: ask [`Visibility::allows`].
#[derive(Clone, Default)]
pub struct Visibility {
    /// Without a store: memory.
    memory: Arc<MemoryKeyValues>,
}

impl Visibility {
    fn with<T>(&self, store: &Option<Store>, f: impl FnOnce(&Knowledge) -> T) -> T {
        match store {
            Some(s) => f(&Knowledge { kv: &Prefixed(s) }),
            None => f(&Knowledge { kv: &*self.memory }),
        }
    }

    /// Whether the asker (`request`) may `type` (a qualified type, e.g. `shop.GetOrder`) on
    /// `target`, as the middleware would decide. `store`: the query handler's `ctx.store`.
    pub fn allows(
        &self,
        store: &Option<Store>,
        tenant: &str,
        request: &RequestContext,
        r#type: &str,
        target: &Target,
    ) -> bool {
        let (domain, name) = r#type.split_once('.').unwrap_or(("", r#type));
        let check = Check {
            kind: CheckKind::Command,
            id: String::new(),
            tenant: tenant.to_owned(),
            domain: domain.to_owned(),
            type_name: name.to_owned(),
            aggregate_type: target.r#type.split_once('.').map(|(_, a)| a.to_owned()),
            aggregate_id: Some(target.id.clone()),
            key: None,
            data: Value::Object(Default::default()),
            request: request.clone(),
            outcome: None,
            store: None,
        };
        self.with(store, |k| {
            decide(k, &check, Timestamp::now().millis(), None, false).is_none()
        })
    }
}

/// Adds the middleware's event handlers to `listener` (after its own, for the same events: call it
/// after registering yours) and returns what answers [`Visibility::allows`]. Declare the
/// relations first. Kept in the listener's store, if it has one, else in memory.
pub fn visibility(listener: &Listener) -> Visibility {
    let visibility = Visibility::default();
    for r#type in learned_events() {
        let (v, t) = (visibility.clone(), r#type.clone());
        listener.also_on(r#type.as_str(), move |payload: Value, evt, ctx| {
            let (v, t) = (v.clone(), t.clone());
            async move {
                let seen = Seen {
                    r#type: &t,
                    tenant: &evt.tenant,
                    aggregate_id: &evt.aggregate_id,
                    domain: &evt.domain,
                    aggregate_type: &evt.aggregate_type,
                };
                v.with(&ctx.store, |k| learn(k, &seen, payload))
            }
        });
    }
    visibility
}
