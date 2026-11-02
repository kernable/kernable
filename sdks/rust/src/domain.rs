//! Domains, their event and command schemas, and aggregates.

use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, RwLock},
};

use kernable_core::{proto, signing};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::{
    BoxError, Error, Listener, Rejected, RequestContext,
    context::Timestamp,
    index::{Index, KeySubject, Requirement, Unique, UniqueKey, reindex_type},
    vault::{self, VaultFields},
};

type Upcast = Arc<dyn Fn(Value) -> Value + Send + Sync>;
/// Whose personal data a payload holds: from the payload and the aggregate's ID.
pub(crate) type Subject = Arc<dyn Fn(&Value, &str) -> String + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct EventDef {
    pub schema: u32,
    pub steps: HashMap<u32, Upcast>,
    pub vault: VaultFields,
    pub eraser: Option<Subject>,
}

struct DomainInner {
    name: String,
    events: RwLock<HashMap<String, EventDef>>,
    commands: RwLock<HashMap<String, VaultFields>>,
    /// The aggregates created through it, in order, whatever their state type.
    aggregates: RwLock<Vec<Arc<dyn Runnable>>>,
    /// The read models and reactors created through it, in order.
    listeners: RwLock<Vec<Listener>>,
}

/// A domain: a name that scopes its aggregates, commands and events, lowercase by convention
/// (`orders`) like aggregates, while message types are capitalized (`orders.PlaceOrder`). Both
/// cases are valid; names are case-sensitive. Types are unique within a domain; elsewhere they
/// are named `domain.Type`. Cloning gives another handle to the same domain.
///
/// Make it with [`domain`]: `let shop_domain = domain("shop");`. A domain remembers the
/// aggregates, read models and reactors created through it, so that an app runs all of them
/// (see [`crate::Kernable::app`]); they live as long as the domain does.
#[derive(Clone)]
pub struct Domain {
    inner: Arc<DomainInner>,
}

/// A domain named `name`, on which you define its aggregates, read models, reactors and event
/// and command schemas: `let shop_domain = domain("shop");`.
///
/// # Panics
///
/// If `name` does not start with a letter or holds characters other than letters, digits,
/// `-` and `_`.
pub fn domain(name: &str) -> Domain {
    let valid = name.starts_with(|c: char| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    assert!(valid, "invalid domain name {name:?}");
    Domain {
        inner: Arc::new(DomainInner {
            name: name.to_owned(),
            events: RwLock::default(),
            commands: RwLock::default(),
            aggregates: RwLock::default(),
            listeners: RwLock::default(),
        }),
    }
}

impl Domain {
    /// The domain's name, e.g. `shop`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Declares that this domain writes `event_type` events at version `schema`, with an
    /// `upcast` per older version, and which of their fields the vault keeps; see
    /// [`EventSchema`]. The domain's aggregates and listeners use it; other domains' listeners
    /// get it with [`Listener::with_domain`]. Types not declared are at version 1.
    ///
    /// # Panics
    ///
    /// If `event_type` is qualified or `schema` is 0.
    pub fn event(&self, event_type: &str, schema: u32) -> EventSchema {
        assert!(
            unqualified(event_type),
            "event types are unqualified within their domain, not {event_type:?}"
        );
        assert!(schema >= 1, "a schema is a version from 1, not {schema}");
        self.inner.events.write().unwrap().insert(
            event_type.to_owned(),
            EventDef {
                schema,
                ..EventDef::default()
            },
        );
        EventSchema {
            domain: self.clone(),
            event_type: event_type.to_owned(),
        }
    }

    /// Declares which fields of `command_type` commands the vault keeps (`personal`,
    /// `secret`); see [`CommandSchema`]. Clients that submit them need the domain:
    /// `kernable.with_domain(&domain)`.
    ///
    /// # Panics
    ///
    /// If `command_type` is qualified.
    pub fn command(&self, command_type: &str) -> CommandSchema {
        assert!(
            unqualified(command_type),
            "command types are unqualified within their domain, not {command_type:?}"
        );
        self.inner
            .commands
            .write()
            .unwrap()
            .insert(command_type.to_owned(), VaultFields::default());
        CommandSchema {
            domain: self.clone(),
            command_type: command_type.to_owned(),
        }
    }

    /// An aggregate of this domain named `name` (lowercase by convention, like `cart`), whose
    /// state starts as `initial`; see [`Aggregate`]. Example:
    /// `let cart_aggregate = shop_domain.aggregate("cart", Cart::default());`.
    ///
    /// # Panics
    ///
    /// If the domain has an aggregate named `name` already.
    pub fn aggregate<S: State>(&self, name: &str, initial: S) -> Aggregate<S> {
        let taken = self.aggregates().iter().any(|a| a.name() == name);
        assert!(
            !taken,
            "{} has an aggregate {name} already",
            self.inner.name
        );
        let aggregate = Aggregate {
            inner: Arc::new(AggregateInner {
                domain: self.clone(),
                name: name.to_owned(),
                initial,
                settings: Mutex::new((100, "1".to_owned())),
                appliers: RwLock::default(),
                handlers: RwLock::default(),
                keys: RwLock::default(),
                facts: RwLock::default(),
                requires: RwLock::default(),
            }),
        };
        self.inner
            .aggregates
            .write()
            .unwrap()
            .push(Arc::new(aggregate.clone()));
        aggregate
    }

    /// A read model of this domain named `name`: builds a view from events and answers queries
    /// from it; see [`Listener`]. Kept in memory and rebuilt from the first event on every start
    /// (order 0; [`Listener::store`] keeps it in a file instead). Run it with
    /// [`crate::Kernable::listener`] or in an app. Example:
    /// `let carts_readmodel = shop_domain.read_model("carts");`.
    ///
    /// # Panics
    ///
    /// If the domain has a read model or reactor named `name` already.
    pub fn read_model(&self, name: &str) -> Listener {
        self.remember(Listener::new(self, name, 0, Some(0), 0))
    }

    /// A reactor of this domain named `name`: triggers side effects and follow-up commands; see
    /// [`Listener`]. It starts at the newest event when it is new, then resumes after the last
    /// acknowledged event (order 10). A settling submit from its handler waits for the
    /// listeners before it (the read models), not for the reactor itself, which handles one
    /// event at a time. Submit follow-ups with `ctx.submit` and a fixed ID, so a redelivered
    /// event does not run the command twice; a rejected one parks the event (see
    /// [`Listener::park_after`]). Run it with [`crate::Kernable::listener`] or in an app.
    ///
    /// # Panics
    ///
    /// If the domain has a read model or reactor named `name` already.
    pub fn reactor(&self, name: &str) -> Listener {
        self.remember(Listener::new(
            self,
            name,
            10,
            None,
            crate::listener::PARK_AFTER,
        ))
    }

    fn remember(&self, listener: Listener) -> Listener {
        let taken = self.listeners().iter().any(|l| l.name() == listener.name());
        assert!(
            !taken,
            "{} has a listener {} already",
            self.inner.name,
            listener.name()
        );
        self.inner.listeners.write().unwrap().push(listener.clone());
        listener
    }

    /// The aggregates created through this domain, in order.
    pub(crate) fn aggregates(&self) -> Vec<Arc<dyn Runnable>> {
        self.inner.aggregates.read().unwrap().clone()
    }

    /// The read models and reactors created through this domain, in order.
    pub(crate) fn listeners(&self) -> Vec<Listener> {
        self.inner.listeners.read().unwrap().clone()
    }

    pub(crate) fn event_def(&self, event_type: &str) -> Option<EventDef> {
        self.inner.events.read().unwrap().get(event_type).cloned()
    }

    pub(crate) fn event_types(&self) -> Vec<String> {
        self.inner.events.read().unwrap().keys().cloned().collect()
    }

    pub(crate) fn command_vault(&self, command_type: &str) -> Option<VaultFields> {
        self.inner
            .commands
            .read()
            .unwrap()
            .get(command_type)
            .cloned()
    }

    /// The payload version this domain writes `event_type` events at.
    pub fn schema_of(&self, event_type: &str) -> u32 {
        self.event_def(event_type).map_or(1, |d| d.schema)
    }

    /// `data` of an `event_type` event stored at `schema`, upcast to the version this domain
    /// knows.
    pub(crate) fn upcast(
        &self,
        event_type: &str,
        schema: u32,
        data: Value,
    ) -> Result<Value, SchemaError> {
        upcast(Some(self), &self.inner.name, event_type, schema, data)
    }
}

fn unqualified(t: &str) -> bool {
    !t.is_empty() && !t.contains(['.', '*'])
}

/// The payload format of one event type: its current version and the steps that bring older
/// payloads up to it. Events are stored as written and never changed; readers upcast them.
///
/// Made with [`Domain::event`]; its methods return it, so they chain:
/// `shop_domain.event("ItemAdded", 2).upcast(1, |mut d| { d["qty"] = json!(1); d });` or
/// `shop_domain.event("CustomerRegistered", 1).personal(&["email"], |_, id| id.to_owned());`.
/// A subject function gets the payload and the aggregate's ID, e.g.
/// `|d, _| d["customerId"].as_str().unwrap_or_default().to_owned()`.
pub struct EventSchema {
    domain: Domain,
    event_type: String,
}

impl EventSchema {
    fn update(&self, f: impl FnOnce(&mut EventDef)) {
        let mut events = self.domain.inner.events.write().unwrap();
        f(events
            .get_mut(&self.event_type)
            .expect("declared by Domain::event"));
    }

    /// Keeps these top-level fields in the vault, as personal data of the subject `subject`
    /// returns from the payload and the aggregate's ID (see [`EventSchema`] for examples).
    /// Events then hold only references; the kernel hands the values to workers and to
    /// listeners allowed to read them, until an event erases the subject.
    pub fn personal(
        self,
        fields: &[&str],
        subject: impl Fn(&Value, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        let fields = fields.iter().map(|f| (*f).to_owned()).collect();
        self.update(|d| d.vault.personal.push((fields, Arc::new(subject))));
        self
    }

    /// Makes this an erasing event: it deletes every vault value of the subject `subject`
    /// returns, in every stream, and the snapshots that may hold them. Read models should drop
    /// the subject's data when they see it; rebuilt, they read `null`.
    pub fn erases(self, subject: impl Fn(&Value, &str) -> String + Send + Sync + 'static) -> Self {
        self.update(|d| d.eraser = Some(Arc::new(subject)));
        self
    }

    /// Turns a payload of version `from` into one of version `from + 1`: `step` gets the old
    /// payload and returns the new one. Declare one step per older version.
    ///
    /// # Panics
    ///
    /// If `from` is not a version below the current one.
    pub fn upcast(self, from: u32, step: impl Fn(Value) -> Value + Send + Sync + 'static) -> Self {
        self.update(|d| {
            assert!(
                from >= 1 && from < d.schema,
                "upcast {} from a version between 1 and {}, not {from}",
                self.event_type,
                d.schema - 1
            );
            d.steps.insert(from, Arc::new(step));
        });
        self
    }
}

/// How a command's payload is kept: which fields go to the vault. Made with
/// [`Domain::command`]; its methods return it, so they chain:
/// `shop_domain.command("Register").personal(&["email"], |_, id| id.to_owned()).secret(&["password"]);`.
/// Clients that submit these commands need the domain: `kernable.with_domain(&shop_domain)`.
pub struct CommandSchema {
    domain: Domain,
    command_type: String,
}

impl CommandSchema {
    fn update(&self, f: impl FnOnce(&mut VaultFields)) {
        let mut commands = self.domain.inner.commands.write().unwrap();
        f(commands
            .get_mut(&self.command_type)
            .expect("declared by Domain::command"));
    }

    /// Keeps these top-level fields in the vault as personal data of `subject`; see
    /// [`EventSchema::personal`].
    pub fn personal(
        self,
        fields: &[&str],
        subject: impl Fn(&Value, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        let fields = fields.iter().map(|f| (*f).to_owned()).collect();
        self.update(|v| v.personal.push((fields, Arc::new(subject))));
        self
    }

    /// Keeps these top-level fields (passwords, one-time tokens) in the vault only until the
    /// command completed. The handler gets them in clear; the stored command keeps a reference.
    pub fn secret(self, fields: &[&str]) -> Self {
        self.update(|v| v.secret.extend(fields.iter().map(|f| (*f).to_owned())));
        self
    }
}

/// An event a process cannot read: its schema is newer than known, or a step is missing.
#[derive(Debug)]
pub(crate) struct SchemaError(pub String);

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaError {}

/// `data` of a `domain.event_type` event stored at `schema`, upcast to the version `known`
/// declares (1 if it declares none).
pub(crate) fn upcast(
    known: Option<&Domain>,
    domain: &str,
    event_type: &str,
    schema: u32,
    mut data: Value,
) -> Result<Value, SchemaError> {
    let def = known.and_then(|d| d.event_def(event_type));
    let current = def.as_ref().map_or(1, |d| d.schema);
    if schema > current {
        return Err(SchemaError(format!(
            "{domain}.{event_type} has schema {schema}, but this process knows it only up to {current}: update it"
        )));
    }
    for v in schema..current {
        let step = def.as_ref().and_then(|d| d.steps.get(&v)).ok_or_else(|| {
            SchemaError(format!(
                "no upcast for {domain}.{event_type} from schema {v}"
            ))
        })?;
        data = step(data);
    }
    Ok(data)
}

/// An event returned by a command handler; `data` is stored as JSON. Make it with
/// [`NewEvent::new`]: `NewEvent::new("ItemAdded", &item)`.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEvent {
    /// The event type within the aggregate's domain, e.g. `ItemAdded`.
    pub event_type: String,
    /// The payload.
    pub data: Value,
}

impl NewEvent {
    /// An `event_type` event with `data`; `()` stands for no data.
    ///
    /// # Panics
    ///
    /// If `data` cannot be represented as JSON (a map with non-string keys).
    pub fn new(event_type: &str, data: impl Serialize) -> NewEvent {
        NewEvent {
            event_type: event_type.to_owned(),
            data: serde_json::to_value(data).expect("event data is JSON"),
        }
    }
}

/// What `apply` gets about an event besides its payload. Only fields known before the kernel
/// stores the event, so a snapshot and a replay compute the same state; put a time into the
/// data (`ctx.now()` in the command handler) if the state needs one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventMeta {
    pub event_type: String,
    /// The aggregate's version after this event: 1 for its first event.
    pub version: u64,
    pub tenant: String,
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// The command that caused the event.
    pub command_id: String,
}

/// What `apply` gets besides state, data and the event's meta. No clock and no IDs: `apply`
/// must compute the same state every time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApplyContext {
    /// The request context of the command that caused the event.
    pub request: RequestContext,
}

/// What a command handler gets about the command besides its payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandMeta {
    /// The command's ID: given with [`crate::Submit::id`], else a new UUID.
    pub command_id: String,
    /// The command type within the domain.
    pub command_type: String,
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub tenant: String,
}

/// What a command handler gets besides state, data and the command's meta: the request
/// context, and the command's time and IDs.
#[derive(Clone, Debug)]
pub struct CommandContext {
    /// Who sends the command: check permissions here.
    pub request: RequestContext,
    command_id: String,
    received_at: Timestamp,
    deadline: Timestamp,
    ids: u64,
}

impl CommandContext {
    /// A context for calling a command handler directly, e.g. in a unit test with
    /// [`Aggregate::handle`]; its deadline is 30 s after `received_at`.
    pub fn new(
        command_id: &str,
        request: RequestContext,
        received_at: Timestamp,
    ) -> CommandContext {
        CommandContext {
            command_id: command_id.to_owned(),
            request,
            received_at,
            deadline: Timestamp(received_at.0 + 30_000),
            ids: 0,
        }
    }

    /// When the kernel received the command: the same every time the command runs. Use it
    /// instead of the clock.
    pub fn now(&self) -> Timestamp {
        self.received_at
    }

    /// A new ID that is the same every time the command runs (1st call, 2nd call, ...). Use it
    /// instead of random IDs.
    pub fn id(&mut self) -> String {
        self.ids += 1;
        signing::derived_id(&self.command_id, self.ids - 1)
    }

    /// When the kernel hands the command to another worker if this one has not answered.
    pub fn deadline(&self) -> Timestamp {
        self.deadline
    }
}

/// An event to fold with [`Aggregate::fold`]: type, payload version and data, and what `apply`
/// learns about it (empty fields if unknown). `FoldEvent::new("ItemAdded", json!({ "qty": 2 }))`
/// is enough for most tests.
#[derive(Clone, Debug, Default)]
pub struct FoldEvent {
    pub event_type: String,
    pub schema: u32,
    pub data: Value,
    pub meta: EventMeta,
    /// The request context of the command that caused the event: `apply` gets it in its
    /// [`ApplyContext`].
    pub request: RequestContext,
}

impl FoldEvent {
    /// An `event_type` event at version 1 of its payload.
    pub fn new(event_type: &str, data: impl Serialize) -> FoldEvent {
        FoldEvent {
            event_type: event_type.to_owned(),
            schema: 1,
            data: serde_json::to_value(data).expect("event data is JSON"),
            meta: EventMeta::default(),
            request: RequestContext::default(),
        }
    }
}

/// What an aggregate's state must be: copied, and stored as JSON in snapshots. Every type that
/// is `Clone + Serialize + DeserializeOwned + Send + Sync + 'static` is one, typically a struct
/// with `#[derive(Clone, Default, Serialize, Deserialize)]`.
pub trait State: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> State for S {}

type Apply<S> =
    Arc<dyn Fn(&mut S, Value, &EventMeta, &ApplyContext) -> Result<(), BoxError> + Send + Sync>;
type Handle<S> = Arc<
    dyn Fn(&S, Value, &CommandMeta, &mut CommandContext) -> Result<Vec<NewEvent>, Failure>
        + Send
        + Sync,
>;

type Fact<S> = Arc<dyn Fn(&S) -> bool + Send + Sync>;
type KeyValues<S> = Arc<dyn Fn(&S) -> Vec<String> + Send + Sync>;
type Requires<S> = Arc<dyn Fn(&S, Value) -> Result<Vec<Requirement>, Failure> + Send + Sync>;

/// A unique key as declared: its values in a state, its scope and whose data the values are.
struct KeyDef<S> {
    name: String,
    values: KeyValues<S>,
    global: bool,
    personal: Option<KeySubject<S>>,
}

impl<S> Clone for KeyDef<S> {
    fn clone(&self) -> Self {
        KeyDef {
            name: self.name.clone(),
            values: Arc::clone(&self.values),
            global: self.global,
            personal: self.personal.clone(),
        }
    }
}

struct AggregateInner<S> {
    domain: Domain,
    name: String,
    initial: S,
    /// Snapshot every this many events, and the state format.
    settings: Mutex<(u64, String)>,
    appliers: RwLock<HashMap<String, Apply<S>>>,
    handlers: RwLock<HashMap<String, Handle<S>>>,
    /// Unique keys and facts, in the order declared.
    keys: RwLock<Vec<KeyDef<S>>>,
    facts: RwLock<Vec<(String, Fact<S>)>>,
    /// Command type -> the facts of other aggregates its commands require.
    requires: RwLock<HashMap<String, Requires<S>>>,
}

/// An aggregate: how events change its state (`apply`) and which events a command produces
/// (`command`). Handlers must be deterministic and free of side effects; they may run again.
/// Create it with `domain(...).aggregate(...)`; cloning gives another handle to the same
/// aggregate. The registering methods return the aggregate, so they chain.
///
/// `apply` handlers change the state in place:
/// `cart_aggregate.apply("ItemAdded", |state, data: Item, _evt, _ctx| state.items += data.qty);`.
/// `command` handlers return the new events or a [`Rejected`]:
/// `cart_aggregate.command("AddItem", |state, data: Item, _cmd, _ctx| { if state.checked_out {
/// return Err(Rejected::new("Cart is checked out")); } Ok(vec![NewEvent::new("ItemAdded", data)]) });`.
/// The type of `data` (here `Item`, any `Deserialize` type, or `serde_json::Value`) says how
/// the payload is read; a payload that does not fit fails the command with `handler_error`.
/// Handlers are synchronous and must not block.
///
/// Unique keys, facts and requirements reach across aggregates; the kernel checks them
/// atomically when a command commits. `unique` declares a key computed from the state, which
/// no two aggregates hold with the same value: `customer_aggregate.unique("email", |s|
/// s.email.clone(), Unique::tenant());`. `fact` declares a condition of the state that other
/// aggregates' commands may require: `vehicle_aggregate.fact("open", |s| !s.wound_up);`.
/// `requires` names the facts a command needs:
/// `account_aggregate.requires("OpenAccount", |_state, data: Open| [Requirement::new("funds",
/// "vehicle", &data.vehicle_id, "open")]);`. After every command, the worker sends the keys and
/// facts of the new state, so an aggregate always holds exactly what its state yields.
///
/// Test without a kernel: `cart_aggregate.fold([FoldEvent::new("ItemAdded", json!({ "qty": 2 }))], None)?`
/// returns the state, `cart_aggregate.handle(&state, "AddItem", json!({ "qty": 1 }), &mut ctx)`
/// runs a command handler with a [`CommandContext::new`], and `cart_aggregate.index(&state)?`
/// and `cart_aggregate.requirements(&state, "AddItem", json!({ "qty": 1 }))?` compute the keys,
/// facts and requirements.
pub struct Aggregate<S> {
    inner: Arc<AggregateInner<S>>,
}

impl<S> Clone for Aggregate<S> {
    fn clone(&self) -> Self {
        Aggregate {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Why a handler failed: a business rejection, or anything else (`handler_error`).
#[derive(Debug)]
pub(crate) enum Failure {
    Rejected(Rejected),
    Error(String),
    /// This process cannot read the aggregate's events: not the command's fault.
    Schema(String),
}

impl Failure {
    /// As a test helper reports it.
    fn into_error(self) -> Error {
        match self {
            Failure::Rejected(r) => Error::Rejected(r),
            Failure::Error(m) | Failure::Schema(m) => Error::Config(m),
        }
    }
}

impl From<SchemaError> for Failure {
    fn from(e: SchemaError) -> Failure {
        Failure::Schema(e.0)
    }
}

impl<S: State> Aggregate<S> {
    /// The domain the aggregate belongs to.
    pub fn domain(&self) -> &Domain {
        &self.inner.domain
    }

    /// The aggregate's name within its domain, e.g. `cart`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The state before the first event.
    pub fn initial(&self) -> &S {
        &self.inner.initial
    }

    /// Sends a snapshot of the state every `events` events (default 100; 0 turns them off).
    pub fn snapshot_every(&self, events: u64) -> Self {
        self.inner.settings.lock().unwrap().0 = events;
        self.clone()
    }

    /// Version of the state format (default "1"). Change it when `apply` produces differently
    /// shaped state; older snapshots are then ignored and the state is rebuilt from the events.
    pub fn schema(&self, schema: &str) -> Self {
        self.inner.settings.lock().unwrap().1 = schema.to_owned();
        self.clone()
    }

    /// How an event of `event_type` changes the state: `f(state, data, evt, ctx)` changes
    /// `state` in place, or replaces it with `*state = …`. `data` is the event's payload as `D`,
    /// upcast to the current version; `evt` is an [`EventMeta`], `ctx` an [`ApplyContext`].
    /// See [`Aggregate`] for an example.
    pub fn apply<D: DeserializeOwned>(
        &self,
        event_type: &str,
        f: impl Fn(&mut S, D, &EventMeta, &ApplyContext) + Send + Sync + 'static,
    ) -> Self {
        let apply: Apply<S> = Arc::new(move |state, data, meta, ctx| {
            let data = serde_json::from_value(data)
                .map_err(|e| format!("{} data: {e}", meta.event_type))?;
            f(state, data, meta, ctx);
            Ok(())
        });
        self.inner
            .appliers
            .write()
            .unwrap()
            .insert(event_type.to_owned(), apply);
        self.clone()
    }

    /// Which events a `command_type` command produces: `f(state, data, cmd, ctx)` returns
    /// them as `Ok(vec![NewEvent::new(…)])`, or `Err(Rejected::new(…))`. `data` is the
    /// command's payload as `D`; `cmd` is a [`CommandMeta`], `ctx` a [`CommandContext`] with
    /// the request context and deterministic `now()` and `id()`. See [`Aggregate`] for an
    /// example.
    ///
    /// # Panics
    ///
    /// If `command_type` starts with `_reindex_`: those are reserved for the built-in command
    /// of aggregates with unique keys or facts (see [`crate::reindex_type`]), and an aggregate
    /// cannot see the other aggregate types of its domain.
    pub fn command<D: DeserializeOwned>(
        &self,
        command_type: &str,
        f: impl Fn(&S, D, &CommandMeta, &mut CommandContext) -> Result<Vec<NewEvent>, Rejected>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        assert!(
            !command_type.starts_with("_reindex_"),
            "{command_type}: command types starting with _reindex_ are reserved for the built-in \
             command of aggregates with unique keys or facts; name yours otherwise"
        );
        let name = command_type.to_owned();
        let handle: Handle<S> = Arc::new(move |state, data, cmd, ctx| {
            let data = serde_json::from_value(data)
                .map_err(|e| Failure::Error(format!("{name} data: {e}")))?;
            f(state, data, cmd, ctx).map_err(Failure::Rejected)
        });
        self.inner
            .handlers
            .write()
            .unwrap()
            .insert(command_type.to_owned(), handle);
        self.clone()
    }

    /// Declares a unique key named `name`: `values(state)` returns the values the aggregate
    /// holds in that state, as anything that iterates over `String`s (values are strings by
    /// type; format numbers yourself): `None` (holds nothing),
    /// `Some(value)` (one value) or a `Vec` (several; repeated ones count once). No two
    /// aggregates of this type hold the same value of the key at the same time; a command that
    /// would make its aggregate hold a value another one holds is rejected with `unique_taken`.
    /// The value is compared as is: normalize it in `values` (e.g. lowercase an email); an empty
    /// string is a value like any other. `options` says where it is unique (within the tenant,
    /// or [`Unique::global`]) and whether the value is personal ([`Unique::personal`]). Example:
    /// `customer_aggregate.unique("email", |s| s.email.as_ref().map(|e| e.to_lowercase()),
    /// Unique::tenant());`.
    ///
    /// The worker computes the keys from the state after every command, also one without
    /// events; `values` must be deterministic like `apply`, and one that panics fails the command
    /// with `handler_error`. An aggregate that had events before the key was declared holds
    /// none of its values until it runs a command: [`crate::Kernable::reindex`] runs one.
    ///
    /// # Panics
    ///
    /// If `name` is empty, longer than 128 bytes or holds `.` or `*`, or the aggregate declares
    /// a key named `name` already (a fact may have the same name).
    pub fn unique<I: IntoIterator<Item = String>>(
        &self,
        name: &str,
        values: impl Fn(&S) -> I + Send + Sync + 'static,
        options: Unique<S>,
    ) -> Self {
        let taken = self
            .inner
            .keys
            .read()
            .unwrap()
            .iter()
            .any(|k| k.name == name);
        self.declarable("key", name, taken);
        self.inner.keys.write().unwrap().push(KeyDef {
            name: name.to_owned(),
            values: Arc::new(move |state| values(state).into_iter().collect()),
            global: options.global,
            personal: options.personal,
        });
        self.clone()
    }

    /// Declares a fact named `name`, which the aggregate holds while `holds(state)` is true.
    /// Commands of other aggregates of the tenant may require it (see [`Aggregate::requires`]).
    /// Computed like the keys (see [`Aggregate::unique`]). Example:
    /// `vehicle_aggregate.fact("open", |s| !s.wound_up);`.
    ///
    /// # Panics
    ///
    /// If `name` is empty, longer than 128 bytes or holds `.` or `*`, or the aggregate declares
    /// a fact named `name` already (a key may have the same name).
    pub fn fact(&self, name: &str, holds: impl Fn(&S) -> bool + Send + Sync + 'static) -> Self {
        let taken = self
            .inner
            .facts
            .read()
            .unwrap()
            .iter()
            .any(|(f, _)| f == name);
        self.declarable("fact", name, taken);
        self.inner
            .facts
            .write()
            .unwrap()
            .push((name.to_owned(), Arc::new(holds)));
        self.clone()
    }

    /// Which facts of other aggregates of the tenant a `command_type` command requires:
    /// `f(state, data)` returns [`Requirement`]s (anything that iterates over them), from the
    /// state before the command and its payload as `D`. The command commits only if each
    /// named aggregate holds its fact at that moment, otherwise it is rejected with
    /// `requirement_missing`; nothing is locked in between. Example:
    /// `account_aggregate.requires("OpenAccount", |_state, data: Open| [Requirement::new("funds",
    /// "vehicle", &data.vehicle_id, "open")]);`. A payload that is not a `D`, or an `f` that
    /// panics, fails the command with `handler_error`.
    ///
    /// # Panics
    ///
    /// If the aggregate has no `command_type` handler (declare it with
    /// [`Aggregate::command`] first).
    pub fn requires<D: DeserializeOwned, I: IntoIterator<Item = Requirement>>(
        &self,
        command_type: &str,
        f: impl Fn(&S, D) -> I + Send + Sync + 'static,
    ) -> Self {
        assert!(
            self.handler(command_type).is_some(),
            "{} has no command {command_type} to require facts for: declare it first",
            self.inner.name
        );
        let name = command_type.to_owned();
        let requires: Requires<S> = Arc::new(move |state, data| {
            let data = serde_json::from_value(data)
                .map_err(|e| Failure::Error(format!("{name} data: {e}")))?;
            Ok(f(state, data).into_iter().collect())
        });
        self.inner
            .requires
            .write()
            .unwrap()
            .insert(command_type.to_owned(), requires);
        self.clone()
    }

    /// Panics unless `name` can name a new key or fact (`kind`) of this aggregate; `taken`:
    /// one of that kind has the name already.
    fn declarable(&self, kind: &str, name: &str, taken: bool) {
        let aggregate = format!("{}.{}", self.inner.domain.name(), self.inner.name);
        assert!(
            unqualified(name) && name.len() <= 128,
            "{aggregate}: a {kind} name is 1 to 128 bytes without `.` or `*`, not {name:?}"
        );
        assert!(!taken, "{aggregate} declares a {kind} {name} already");
    }

    /// Whether the aggregate declares unique keys or facts.
    fn indexed(&self) -> bool {
        !self.inner.keys.read().unwrap().is_empty() || !self.inner.facts.read().unwrap().is_empty()
    }

    /// The unique keys and facts the aggregate holds in `state`, as the worker sends them after
    /// a command that leaves it in `state`; for tests without a kernel. [`Error::Config`] if a
    /// key or fact function panics.
    pub fn index(&self, state: &S) -> Result<Index, Error> {
        self.index_of(state).map_err(Failure::into_error)
    }

    /// The requirements of a `command_type` command with `data` on `state` (none if it declares
    /// none, see [`Aggregate::requires`]); for tests without a kernel. [`Error::Config`] if
    /// `data` is not the function's `D` or it panics.
    pub fn requirements(
        &self,
        state: &S,
        command_type: &str,
        data: impl Serialize,
    ) -> Result<Vec<Requirement>, Error> {
        let data = serde_json::to_value(data)?;
        self.requirements_of(state, command_type, data)
            .map_err(Failure::into_error)
    }

    fn index_of(&self, state: &S) -> Result<Index, Failure> {
        let keys = self.inner.keys.read().unwrap().clone();
        let facts = self.inner.facts.read().unwrap().clone();
        caught(|| {
            let mut index = Index::default();
            for key in &keys {
                let subject = match &key.personal {
                    None => None,
                    Some(subject) => match subject(state) {
                        Some(s) if !s.is_empty() => Some(s),
                        _ => continue,
                    },
                };
                let mut values: Vec<String> = Vec::new();
                for value in (key.values)(state) {
                    if !values.contains(&value) {
                        values.push(value);
                    }
                }
                index.keys.extend(values.into_iter().map(|value| UniqueKey {
                    name: key.name.clone(),
                    value,
                    global: key.global,
                    subject: subject.clone(),
                }));
            }
            index.facts = facts
                .iter()
                .filter(|(_, holds)| holds(state))
                .map(|(name, _)| name.clone())
                .collect();
            index
        })
    }

    fn requirements_of(
        &self,
        state: &S,
        command_type: &str,
        data: Value,
    ) -> Result<Vec<Requirement>, Failure> {
        let requires = self
            .inner
            .requires
            .read()
            .unwrap()
            .get(command_type)
            .cloned();
        match requires {
            Some(requires) => caught(|| requires(state, data))?,
            None => Ok(Vec::new()),
        }
    }

    /// State after `history`, starting from `from` (default: the initial state). Events are
    /// upcast first; events without an `apply` are skipped. For tests without a kernel.
    /// [`Error::Config`] if an `apply` fails (also a payload that is not its `D`, or a panic)
    /// or an event's schema is newer than known.
    pub fn fold(
        &self,
        history: impl IntoIterator<Item = FoldEvent>,
        from: Option<S>,
    ) -> Result<S, Error> {
        self.fold_inner(history, from).map_err(|f| match f {
            Failure::Rejected(r) => Error::Rejected(r),
            Failure::Error(m) | Failure::Schema(m) => Error::Config(m),
        })
    }

    fn fold_inner(
        &self,
        history: impl IntoIterator<Item = FoldEvent>,
        from: Option<S>,
    ) -> Result<S, Failure> {
        let mut state = from.unwrap_or_else(|| self.inner.initial.clone());
        let appliers = self.inner.appliers.read().unwrap();
        for e in history {
            let Some(apply) = appliers.get(&e.event_type) else {
                continue;
            };
            let data = self.inner.domain.upcast(&e.event_type, e.schema, e.data)?;
            let meta = EventMeta {
                event_type: e.event_type,
                domain: if e.meta.domain.is_empty() {
                    self.inner.domain.name().to_owned()
                } else {
                    e.meta.domain
                },
                aggregate_type: if e.meta.aggregate_type.is_empty() {
                    self.inner.name.clone()
                } else {
                    e.meta.aggregate_type
                },
                ..e.meta
            };
            let ctx = ApplyContext { request: e.request };
            caught(|| apply(&mut state, data, &meta, &ctx))?
                .map_err(|e| Failure::Error(e.to_string()))?;
        }
        Ok(state)
    }

    /// Runs the `command_type` handler on `state`, e.g. in a unit test. The handler's
    /// [`CommandMeta`] has the command ID of `ctx`, this aggregate's domain and name, and an empty
    /// aggregate ID and tenant. [`Error::Rejected`] if the handler rejects the command,
    /// [`Error::Config`] if there is no handler for `command_type` or it fails otherwise.
    pub fn handle(
        &self,
        state: &S,
        command_type: &str,
        data: impl Serialize,
        ctx: &mut CommandContext,
    ) -> Result<Vec<NewEvent>, Error> {
        let handler = self.handler(command_type).ok_or_else(|| {
            Error::Config(format!(
                "no handler for {command_type} on {}",
                self.inner.name
            ))
        })?;
        let data = serde_json::to_value(data)?;
        let cmd = CommandMeta {
            command_id: ctx.command_id.clone(),
            command_type: command_type.to_owned(),
            domain: self.inner.domain.name().to_owned(),
            aggregate_type: self.inner.name.clone(),
            ..CommandMeta::default()
        };
        match caught(|| handler(state, data, &cmd, ctx)).and_then(|r| r) {
            Ok(events) => Ok(events),
            Err(Failure::Rejected(r)) => Err(Error::Rejected(r)),
            Err(Failure::Error(m) | Failure::Schema(m)) => Err(Error::Config(m)),
        }
    }

    fn handler(&self, command_type: &str) -> Option<Handle<S>> {
        self.inner
            .handlers
            .read()
            .unwrap()
            .get(command_type)
            .cloned()
    }

    /// A rejection (`personal_in_clear`) if an event holds, in a field the event type does not
    /// declare personal, a non-null value under the name of a personal field of the command:
    /// it would be stored in the signed log in clear, for good.
    fn personal_in_clear(&self, command_type: &str, events: &[NewEvent]) -> Result<(), Failure> {
        let domain = &self.inner.domain;
        let Some(command) = domain.command_vault(command_type) else {
            return Ok(());
        };
        for e in events {
            let def = domain.event_def(&e.event_type);
            let declared = |field: &String| {
                def.as_ref()
                    .is_some_and(|d| d.vault.personal.iter().any(|(f, _)| f.contains(field)))
            };
            for field in command.personal.iter().flat_map(|(fields, _)| fields) {
                let held = e.data.get(field).is_some_and(|v| !v.is_null());
                if held && !declared(field) {
                    return Err(Failure::Rejected(
                        Rejected::new(format!(
                            "{command_type} declares {field} personal, but {event} does not: it \
                             would be stored in clear (declare it with Domain.event({event:?}, \
                             …).personal)",
                            event = e.event_type,
                        ))
                        .code("personal_in_clear"),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// `f()`, with a panic turned into an error.
fn caught<T>(f: impl FnOnce() -> T) -> Result<T, Failure> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|panic| Failure::Error(panic_text(panic.as_ref())))
}

/// What a panic said, as far as it can be shown.
pub(crate) fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "handler panicked".into())
}

/// What a worker sends back for a command it executed.
pub(crate) struct Executed {
    pub events: Vec<proto::NewEvent>,
    pub vault: Vec<proto::VaultValue>,
    /// Version, state format and state, if the events cross a multiple of `snapshot_every`.
    pub snapshot: Option<(u64, String, Vec<u8>)>,
    /// The unique keys and facts of the state after the events, and the facts the command
    /// requires of other aggregates.
    pub keys: Vec<proto::UniqueKey>,
    pub facts: Vec<String>,
    pub requires: Vec<proto::Requirement>,
}

/// An aggregate as a worker runs it, whatever its state type.
pub(crate) trait Runnable: Send + Sync {
    fn domain(&self) -> &Domain;
    fn name(&self) -> &str;
    fn command_types(&self) -> Vec<String>;
    /// The event types it knows: applied ones and the domain's declared ones.
    fn event_types(&self) -> Vec<String>;
    fn execute(&self, x: &proto::Execute) -> Result<Executed, Failure>;
}

impl<S: State> Runnable for Aggregate<S> {
    fn domain(&self) -> &Domain {
        &self.inner.domain
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn command_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self
            .inner
            .handlers
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        if self.indexed() {
            types.push(reindex_type(&self.inner.name));
        }
        types
    }

    fn event_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self
            .inner
            .appliers
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        types.extend(self.inner.domain.event_types());
        types.sort();
        types.dedup();
        types
    }

    fn execute(&self, x: &proto::Execute) -> Result<Executed, Failure> {
        let cmd = x.command.clone().unwrap_or_default();
        let (snapshot_every, schema) = self.inner.settings.lock().unwrap().clone();
        let from = match &x.snapshot {
            Some(s) if s.schema != schema => {
                return Err(Failure::Rejected(
                    Rejected::new(format!("snapshot schema {} is not {schema}", s.schema))
                        .code("snapshot_stale"),
                ));
            }
            Some(s) => Some(
                serde_json::from_slice(&s.state)
                    .map_err(|e| Failure::Error(format!("snapshot of {}: {e}", self.inner.name)))?,
            ),
            None => None,
        };
        let history = x.history.iter().map(|e| FoldEvent {
            event_type: e.event_type.clone(),
            schema: e.schema,
            data: vault::unseal(decode(&e.payload), &x.vault).0,
            meta: meta_of(&e.event_type, e.version, &cmd, &e.command_id),
            request: RequestContext::from_wire(e.context.as_ref()),
        });
        let state = self.fold_inner(history.collect::<Vec<_>>(), from)?;
        let request = RequestContext::from_wire(cmd.context.as_ref());
        let meta = CommandMeta {
            command_id: cmd.command_id.clone(),
            command_type: cmd.command_type.clone(),
            domain: cmd.domain.clone(),
            aggregate_type: cmd.aggregate_type.clone(),
            aggregate_id: cmd.aggregate_id.clone(),
            tenant: cmd.tenant_id.clone(),
        };
        let mut ctx = CommandContext {
            command_id: cmd.command_id.clone(),
            request: request.clone(),
            received_at: Timestamp(x.received_at),
            deadline: Timestamp(x.deadline),
            ids: 0,
        };
        let indexed = self.indexed();
        let handler = self.handler(&cmd.command_type);
        let data = vault::unseal(decode(&cmd.payload), &x.vault).0;
        let produced = match handler {
            Some(handler) => caught(|| handler(&state, data.clone(), &meta, &mut ctx))??,
            // Produces nothing, so the result carries the keys and facts of the current state.
            None if indexed && cmd.command_type == reindex_type(&self.inner.name) => Vec::new(),
            None => {
                return Err(Failure::Rejected(
                    Rejected::new(format!(
                        "no handler for {} on {}.{}",
                        cmd.command_type, cmd.domain, cmd.aggregate_type
                    ))
                    .code("unknown_command"),
                ));
            }
        };
        self.personal_in_clear(&cmd.command_type, &produced)?;
        let requires = self.requirements_of(&state, &cmd.command_type, data)?;

        let mut events = Vec::with_capacity(produced.len());
        let mut values = Vec::new();
        for e in &produced {
            let def = self.inner.domain.event_def(&e.event_type);
            let sealed = vault::seal(&e.data, def.as_ref().map(|d| &d.vault), &cmd.aggregate_id)
                .map_err(Failure::Error)?;
            values.extend(sealed.values);
            let erases = def
                .as_ref()
                .and_then(|d| d.eraser.as_ref())
                .map(|subject| subject(&e.data, &cmd.aggregate_id))
                .unwrap_or_default();
            events.push(proto::NewEvent {
                event_type: e.event_type.clone(),
                schema: def.as_ref().map_or(1, |d| d.schema),
                payload: sealed.payload,
                vault_refs: sealed.refs,
                erases,
            });
        }

        let before = x.expected_version;
        let version = before + produced.len() as u64;
        let snapshot_due = snapshot_every != 0
            && !produced.is_empty()
            && version / snapshot_every != before / snapshot_every;
        let after = if produced.is_empty() || !(snapshot_due || indexed) {
            state
        } else {
            // Apply the events as they will be read back (a JSON round trip), so the snapshot
            // and the keys equal those of a replay.
            let read = produced.iter().enumerate().map(|(i, e)| FoldEvent {
                event_type: e.event_type.clone(),
                schema: self.inner.domain.schema_of(&e.event_type),
                data: decode(&encode(&e.data)),
                meta: meta_of(&e.event_type, before + i as u64 + 1, &cmd, &cmd.command_id),
                request: request.clone(),
            });
            self.fold_inner(read.collect::<Vec<_>>(), Some(state))?
        };
        let snapshot = if snapshot_due {
            let state = serde_json::to_vec(&after).map_err(|e| Failure::Error(e.to_string()))?;
            Some((version, schema, state))
        } else {
            None
        };
        let index = if indexed {
            self.index_of(&after)?
        } else {
            Index::default()
        };
        Ok(Executed {
            events,
            vault: values,
            snapshot,
            keys: index.keys.iter().map(UniqueKey::to_wire).collect(),
            facts: index.facts,
            requires: requires.iter().map(|q| q.to_wire(&cmd.domain)).collect(),
        })
    }
}

/// What `apply` gets about an event of `cmd`'s aggregate, caused by command `command_id`.
fn meta_of(event_type: &str, version: u64, cmd: &proto::Command, command_id: &str) -> EventMeta {
    EventMeta {
        event_type: event_type.to_owned(),
        version,
        tenant: cmd.tenant_id.clone(),
        domain: cmd.domain.clone(),
        aggregate_type: cmd.aggregate_type.clone(),
        aggregate_id: cmd.aggregate_id.clone(),
        command_id: command_id.to_owned(),
    }
}

/// JSON bytes of `data`; no data is `{}`.
pub(crate) fn encode(data: &Value) -> Vec<u8> {
    if data.is_null() {
        return b"{}".to_vec();
    }
    serde_json::to_vec(data).expect("a JSON value encodes")
}

/// The JSON value of `payload`; an empty payload is `{}`.
pub(crate) fn decode(payload: &[u8]) -> Value {
    if payload.is_empty() {
        return Value::Object(Map::new());
    }
    serde_json::from_slice(payload).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_says_what_it_said_as_far_as_it_can() {
        let text = |f: fn()| panic_text(catch_unwind(f).unwrap_err().as_ref());
        std::panic::set_hook(Box::new(|_| {}));
        assert_eq!(text(|| panic!("plain")), "plain");
        assert_eq!(text(|| panic!("formatted {}", 7)), "formatted 7");
        assert_eq!(text(|| std::panic::panic_any(7)), "handler panicked");
        let _ = std::panic::take_hook();
        assert_eq!(SchemaError("too new".into()).to_string(), "too new");
    }
}
