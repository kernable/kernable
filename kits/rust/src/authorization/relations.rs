//! Relations derived from events: who stands in which relation to an aggregate. Declare
//! them next to your aggregates, before the kit's domains are built:
//!
//! ```ignore
//! relation("shop.order", "owner").set("shop.OrderPlaced", |e| ids(&e.data["customer"]));
//! parent("funds.vehicle").from("funds.VehicleCreated", |e| Some(Target::new("funds.fund", e.data["fund"].as_str()?)));
//! ```
//!
//! `e.data` is the event's payload as the log holds it (camelCase); `e.aggregate_id` and
//! `e.tenant` name the aggregate.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An aggregate a relation or a scope points at: `{type: "shop.order", id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub r#type: String,
    pub id: String,
}

impl Target {
    pub fn new(r#type: &str, id: &str) -> Target {
        Target {
            r#type: r#type.to_owned(),
            id: id.to_owned(),
        }
    }
}

/// What a relation's or parent's function gets of an event.
pub struct RelationEvent {
    pub data: Value,
    pub aggregate_id: String,
    pub tenant: String,
}

/// The identity IDs in a payload field: one ID, a list of them, or none.
pub fn ids(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) if !s.is_empty() => vec![s.clone()],
        Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        _ => vec![],
    }
}

type Ids = Arc<dyn Fn(&RelationEvent) -> Vec<String> + Send + Sync>;
type ParentOf = Arc<dyn Fn(&RelationEvent) -> Option<Target> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct RelationRule {
    pub r#type: String,
    pub name: String,
    pub event: String,
    pub set: bool,
    pub ids: Ids,
}

#[derive(Clone)]
pub(crate) struct ParentRule {
    pub r#type: String,
    pub event: String,
    pub parent: ParentOf,
}

static RELATIONS: RwLock<Vec<RelationRule>> = RwLock::new(Vec::new());
static PARENTS: RwLock<Vec<ParentRule>> = RwLock::new(Vec::new());

fn qualified(t: &str) -> bool {
    t.split_once('.').is_some_and(|(d, n)| {
        d.starts_with(|c: char| c.is_ascii_lowercase())
            && [d, n].iter().all(|p| {
                !p.is_empty()
                    && p.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            })
    })
}

fn check(r#type: &str, event: &str) {
    assert!(
        qualified(r#type),
        "a relation's aggregate type is qualified (shop.order): {type}"
    );
    assert!(
        qualified(event),
        "a relation's event type is qualified (shop.OrderPlaced): {event}"
    );
}

/// A relation of an aggregate type, set and unset by events: see [`relation`].
pub struct Relation {
    r#type: String,
    name: String,
}

impl Relation {
    fn rule(
        self,
        event: &str,
        set: bool,
        ids: impl Fn(&RelationEvent) -> Vec<String> + Send + Sync + 'static,
    ) -> Relation {
        check(&self.r#type, event);
        RELATIONS.write().unwrap().push(RelationRule {
            r#type: self.r#type.clone(),
            name: self.name.clone(),
            event: event.to_owned(),
            set,
            ids: Arc::new(ids),
        });
        self
    }

    /// `ids(event)` gain the relation.
    pub fn set(
        self,
        event: &str,
        ids: impl Fn(&RelationEvent) -> Vec<String> + Send + Sync + 'static,
    ) -> Relation {
        self.rule(event, true, ids)
    }

    /// `ids(event)` lose it.
    pub fn unset(
        self,
        event: &str,
        ids: impl Fn(&RelationEvent) -> Vec<String> + Send + Sync + 'static,
    ) -> Relation {
        self.rule(event, false, ids)
    }
}

/// Declares relation `name` of aggregates of `type` (qualified: `shop.order`), set and unset by
/// events of that aggregate.
///
/// # Panics
///
/// If `name` is not lowercase, or is built in (`self`, `owners`).
pub fn relation(r#type: &str, name: &str) -> Relation {
    let lower = name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    assert!(lower, "a relation's name is lowercase: {name}");
    assert!(!matches!(name, "self" | "owners"), "{name} is built in");
    Relation {
        r#type: r#type.to_owned(),
        name: name.to_owned(),
    }
}

/// The parent of an aggregate type, for permissions `via_parent`: see [`parent`].
pub struct Parent {
    r#type: String,
}

impl Parent {
    pub fn from(
        self,
        event: &str,
        parent: impl Fn(&RelationEvent) -> Option<Target> + Send + Sync + 'static,
    ) {
        check(&self.r#type, event);
        PARENTS.write().unwrap().push(ParentRule {
            r#type: self.r#type,
            event: event.to_owned(),
            parent: Arc::new(parent),
        });
    }
}

/// Declares the parent of aggregates of `type`.
pub fn parent(r#type: &str) -> Parent {
    Parent {
        r#type: r#type.to_owned(),
    }
}

pub(crate) fn relation_rules() -> Vec<RelationRule> {
    RELATIONS.read().unwrap().clone()
}

pub(crate) fn parent_rules() -> Vec<ParentRule> {
    PARENTS.read().unwrap().clone()
}

/// Every event type a relation or parent is derived from.
pub fn relation_events() -> Vec<String> {
    let mut all: Vec<String> = relation_rules()
        .into_iter()
        .map(|r| r.event)
        .chain(parent_rules().into_iter().map(|p| p.event))
        .collect();
    all.sort();
    all.dedup();
    all
}
