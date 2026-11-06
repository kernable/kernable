//! Events, commands and queries as types: defined once, typed, used by reference.
//!
//! The attributes [`event`](crate::event), [`command`](crate::command) and
//! [`query`](crate::query) turn a braced struct into a message of a domain:
//!
//! ```
//! use kernable::{NewEvent, domain, event, command};
//!
//! #[event("shop")]
//! pub struct OrderPlaced { pub customer_id: String, pub note: Option<String> }
//!
//! #[command("shop", aggregate = "order")]
//! pub struct PlaceOrder { pub customer_id: String }
//!
//! #[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
//! struct Order { placed: bool }
//!
//! let shop_domain = domain("shop");
//! let order_aggregate = shop_domain.aggregate("order", Order::default());
//! order_aggregate.apply(OrderPlaced, |s, _d, _evt, _ctx| s.placed = true);
//! order_aggregate.command(PlaceOrder, |_s, d, _cmd, _ctx| {
//!     Ok(vec![OrderPlaced { customer_id: d.customer_id, note: None }.into()])
//! });
//! ```
//!
//! Fields are snake_case in Rust and camelCase in the log (`customer_id` is `customerId`), so a
//! Rust, a Python and a TypeScript process write and read the same events. An event writes an
//! `Option` field that is `None` as `null` (it is part of the record); a command or query leaves
//! it out; a missing field reads as `None`. A field with its own `#[serde(...)]` attribute is left
//! as it says (an event's optional field: `#[serde(default, skip_serializing_if =
//! "Option::is_none")]`). The struct's name is its type (`type = "..."` overrides it), and
//! also a constant of type [`Of`] that stands for the type where the SDK takes one: in
//! `apply`, `command`, `requires`, a listener's `on` and `query`; with [`crate::Submit::command`]
//! and [`crate::Query::of`] the value itself goes.

use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};

use crate::Domain;

/// A message of a domain: implemented by the attributes, not by hand.
pub trait Message: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The domain it belongs to.
    const DOMAIN: &'static str;
    /// Its type within the domain.
    const TYPE: &'static str;
    /// Declares its schema on `domain` (an event's version, upcasts and vault fields; a
    /// command's vault fields). The SDK calls it once per domain.
    fn declare(domain: &Domain);
}

/// An event: `#[event("domain")]`.
pub trait EventType: Message {}

/// A command of an aggregate: `#[command("domain", aggregate = "name")]`.
pub trait CommandType: Message {
    /// The aggregate type that runs it.
    const AGGREGATE: &'static str;
}

/// A query and its answer: `#[query("domain", answer = Row)]`.
pub trait QueryType: Message {
    /// What the read model answers.
    type Answer: Serialize + DeserializeOwned;
}

/// Stands for the message type `M` where the SDK takes a type: the attributes declare one
/// named like the struct, so `apply(TenantCreated, ...)` reads like the type itself.
pub struct Of<M>(PhantomData<fn() -> M>);

impl<M> Of<M> {
    #[doc(hidden)]
    pub const fn new() -> Self {
        Of(PhantomData)
    }
}

impl<M> Clone for Of<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for Of<M> {}

/// What a type parameter of `apply`, `command`, `requires`, `on` and `query` resolves to.
#[doc(hidden)]
pub struct Key {
    /// The type's own domain, for a message type; a string says nothing.
    pub(crate) domain: Option<&'static str>,
    pub(crate) name: String,
    pub(crate) declare: Option<fn(&Domain)>,
}

/// A type as `apply`, `command`, `requires`, `on` and `query` take it: a name (`"ItemAdded"`,
/// `"orders.OrderPlaced"`) with the data as any `D`, or a message type's constant
/// (`OrderPlaced`), whose data is then that type.
pub trait TypeKey<D> {
    #[doc(hidden)]
    fn key(self) -> Key;
}

impl<D> TypeKey<D> for &str {
    fn key(self) -> Key {
        Key {
            domain: None,
            name: self.to_owned(),
            declare: None,
        }
    }
}

impl<M: Message> TypeKey<M> for Of<M> {
    fn key(self) -> Key {
        Key {
            domain: Some(M::DOMAIN),
            name: M::TYPE.to_owned(),
            declare: Some(M::declare),
        }
    }
}

impl Key {
    /// The type within `domain`, declared there; a message type of another domain is an error.
    pub(crate) fn within(self, domain: &Domain, what: &str) -> String {
        if let Some(own) = self.domain {
            assert!(
                own == domain.name(),
                "{own}.{} belongs to {own}; {what} of {} takes the types of its own domain",
                self.name,
                domain.name()
            );
            if let Some(declare) = self.declare {
                domain.declare_once(&self.name, declare);
            }
        }
        self.name
    }

    /// The type as a listener names it: qualified for another domain's message type.
    pub(crate) fn qualified_for(&self, domain: &Domain) -> String {
        match self.domain {
            Some(own) if own != domain.name() => format!("{own}.{}", self.name),
            _ => self.name.clone(),
        }
    }
}

/// The schema of a message type of `M::DOMAIN`, declared on a domain object of that name.
pub(crate) fn domain_of<M: Message>() -> Domain {
    let d = crate::domain(M::DOMAIN);
    d.declare_once(M::TYPE, M::declare);
    d
}

#[doc(hidden)]
pub mod __private {
    pub use serde;
    pub use serde_json;

    use super::{Deserialize, Deserializer};

    /// A vault field: `null` once erased, read as the field's default.
    pub fn null_as_default<'de, D: Deserializer<'de>, T: Deserialize<'de> + Default>(
        d: D,
    ) -> Result<T, D::Error> {
        Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};

    use crate::{
        CommandContext, FoldEvent, NewEvent, Query, RequestContext, Submit, Timestamp, command,
        domain, event, query,
    };

    fn own(_d: &Value, id: &str) -> String {
        format!("account:{id}")
    }

    #[event("shopt", version = 2, upcasts = add_note, personal(fields = [email_address], subject = own))]
    #[derive(Clone, Debug, PartialEq)]
    pub struct Placed {
        pub customer_id: String,
        pub email_address: String,
        pub note: Option<String>,
        pub r#type: String,
    }

    fn add_note(schema: crate::EventSchema) -> crate::EventSchema {
        schema.upcast(1, |mut d| {
            d["type"] = json!("v1");
            d
        })
    }

    #[event("shopt")]
    pub struct Cancelled {}

    #[command("shopt", aggregate = "order", secret = [password])]
    pub struct Place {
        pub customer_id: String,
        pub password: String,
    }

    #[query("shopt", answer = Vec<String>)]
    pub struct ListOrders {
        pub status: Option<String>,
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Order {
        customer: String,
        cancelled: bool,
    }

    #[test]
    fn fields_are_camel_case_and_an_event_writes_none_as_null() {
        let p = Placed {
            customer_id: "c1".into(),
            email_address: "a@b".into(),
            note: None,
            r#type: "x".into(),
        };
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            json!({ "customerId": "c1", "emailAddress": "a@b", "note": null, "type": "x" })
        );
        // A query (or command) leaves an unset option out.
        assert_eq!(
            serde_json::to_value(ListOrders { status: None }).unwrap(),
            json!({})
        );
        // An erased vault value is null: read as the field's default.
        let back: Placed = serde_json::from_value(
            json!({ "customerId": "c1", "emailAddress": null, "type": "x" }),
        )
        .unwrap();
        assert_eq!(back.email_address, "");
        assert_eq!(back.note, None);
    }

    #[test]
    fn apply_and_command_take_the_type_and_declare_its_schema() {
        let shop = domain("shopt");
        let order = shop.aggregate("order", Order::default());
        order.apply(Placed, |s, d, _, _| s.customer = d.customer_id);
        order.apply(Cancelled, |s, _d, _, _| s.cancelled = true);
        order.command(Place, |_s, d, _, _| {
            Ok(vec![
                Placed {
                    customer_id: d.customer_id,
                    email_address: "a@b".into(),
                    note: None,
                    r#type: "t".into(),
                }
                .into(),
            ])
        });
        // The event's version and upcast, from its declaration.
        assert_eq!(shop.schema_of("Placed"), 2);
        let v1 = FoldEvent {
            schema: 1,
            ..FoldEvent::new(
                "Placed",
                json!({ "customerId": "c1", "emailAddress": "a@b" }),
            )
        };
        let state = order.fold([v1], None).unwrap();
        assert_eq!(state.customer, "c1");
        let mut ctx = CommandContext::new("cmd-1", RequestContext::default(), Timestamp(0));
        let events = order
            .handle(
                &state,
                "Place",
                json!({ "customerId": "c2", "password": "p" }),
                &mut ctx,
            )
            .unwrap();
        assert_eq!(
            events,
            vec![NewEvent::new(
                "Placed",
                json!({ "customerId": "c2", "emailAddress": "a@b", "note": null, "type": "t" })
            )]
        );
        // The command's vault fields and the event's personal ones, declared on the domain.
        assert_eq!(
            shop.command_vault("Place").unwrap().secret,
            vec!["password"]
        );
        assert_eq!(
            shop.event_def("Placed").unwrap().vault.personal[0].0,
            vec!["emailAddress"]
        );
    }

    #[test]
    #[should_panic(expected = "belongs to shopt")]
    fn another_domain_s_message_type_is_refused() {
        domain("other")
            .aggregate("order", Order::default())
            .apply(Placed, |_, _, _, _| {});
    }

    #[test]
    fn a_listener_learns_another_domain_s_event_schema() {
        let billing = domain("billingt");
        let listener = billing
            .read_model("orders")
            .on(Placed, |d, _evt, _ctx| async move {
                assert!(!d.customer_id.is_empty());
                Ok(())
            });
        assert_eq!(
            listener.domain_of("shopt").map(|d| d.schema_of("Placed")),
            Some(2)
        );
        listener.query(ListOrders, |_d, _q, _ctx| async move {
            Ok(vec!["o1".to_owned()])
        });
    }

    #[test]
    fn submit_and_query_carry_the_type() {
        let s = Submit::command(
            Place {
                customer_id: "c".into(),
                password: "p".into(),
            },
            "o1",
        );
        let shown = format!("{s:?}");
        assert!(
            shown.contains("domain: \"shopt\"")
                && shown.contains("aggregate_type: \"order\"")
                && shown.contains("command_type: \"Place\""),
            "{shown}"
        );
        assert!(shown.contains("secret: [\"password\"]"), "{shown}");
        let q = format!("{:?}", Query::of(ListOrders { status: None }));
        assert!(
            q.contains("query_type: \"ListOrders\"") && q.contains("data: Object {}"),
            "{q}"
        );
    }
}
