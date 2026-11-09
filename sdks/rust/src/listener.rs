//! Listeners (read models and reactors), their queries and middleware, and subscriptions.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kernable_core::{
    PROTOCOL,
    proto::{
        self, Ack, ChallengeResponse, Hello, ListenerMessage, Middleware, Parked, QueryAnswer,
        SubscribeRequest, Verdict, event_service_client::EventServiceClient,
        listener_delivery::Msg as Delivery, listener_message::Msg as Reply,
        listener_service_client::ListenerServiceClient, middleware_call, query_answer,
        query_result,
    },
    signing,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    BoxError, Client, Domain, Error, HANDLING, Kernable, Outcome, Query, QueryOutcome, Rejected,
    RequestContext, Store, StoredEvent, Submit, Timestamp,
    context::issued_now,
    domain::{decode, upcast},
    log,
    messages::{Key, TypeKey},
    vault,
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type EventHandler =
    Arc<dyn Fn(StoredEvent, ListenerContext) -> BoxFuture<Result<(), BoxError>> + Send + Sync>;
type QueryHandler =
    Arc<dyn Fn(Value, QueryMeta, QueryContext) -> BoxFuture<Result<Value, BoxError>> + Send + Sync>;
type CheckHandler = Arc<dyn Fn(Check) -> BoxFuture<Result<(), BoxError>> + Send + Sync>;

/// What a listener handler gets about an event besides its payload (the `evt` of
/// [`Listener::on`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoredEventMeta {
    pub event_type: String,
    /// The aggregate's version after this event: 1 for its first event.
    pub version: u64,
    pub tenant: String,
    /// Always the domain of the command that caused the event.
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// The command that caused the event.
    pub command_id: String,
    /// Global position of the event.
    pub position: u64,
    /// When the kernel committed the event.
    pub created_at: Timestamp,
    /// Version of the payload format as stored; the handler gets the payload upcast to the
    /// version it knows, unless it is raw.
    pub schema: u32,
    /// Fields kept in the vault whose value is not in the payload (`null`): erased, or not
    /// readable with this key (granted `vault_read`).
    pub redacted: Vec<String>,
}

impl StoredEventMeta {
    fn of(e: &StoredEvent) -> StoredEventMeta {
        StoredEventMeta {
            event_type: e.event_type.clone(),
            version: e.version,
            tenant: e.tenant.clone(),
            domain: e.domain.clone(),
            aggregate_type: e.aggregate_type.clone(),
            aggregate_id: e.aggregate_id.clone(),
            command_id: e.command_id.clone(),
            position: e.position,
            created_at: e.created_at,
            schema: e.schema,
            redacted: e.redacted.clone(),
        }
    }
}

/// What a listener handler gets besides the event's payload and meta (the `ctx` of
/// [`Listener::on`]).
#[derive(Clone)]
pub struct ListenerContext {
    /// The event was committed before this connection began: a read model rebuilding from the
    /// start, and equally the backlog a reactor catches up on after a restart or a lost
    /// connection. A reactor must not skip side effects because of it (those events are new to
    /// it); submit with fixed command IDs instead, so they are safe to repeat. Use it to skip
    /// work that only matters live, e.g. a notification about something long past in a
    /// listener that starts at 0.
    pub replaying: bool,
    /// The listener's store, if it has one ([`Listener::store`]).
    pub store: Option<Store>,
    /// The request context of the command that caused the event.
    pub request: RequestContext,
    command_id: String,
    /// The event's tenant: the default for follow-up commands and queries.
    tenant: String,
    kernable: Kernable,
}

impl ListenerContext {
    /// A request context for commands caused by this event: same sender and correlation, this
    /// event's command as the cause, without an expected version or privileges. Pass it with
    /// [`crate::Submit::context`]; [`ListenerContext::submit`] does.
    pub fn follow(&self) -> RequestContext {
        self.request.follow(&self.command_id)
    }

    /// Submits a command this event causes, on the listener's connection, and returns its
    /// events. It needs a fixed ID ([`Submit::id`], e.g. `confirm-<order>`), so a redelivered
    /// event runs it once; without one it is [`Error::Config`]. Unless the submit sets them,
    /// the context is [`ListenerContext::follow`] and the tenant the event's.
    ///
    /// A rejection is [`Error::Rejected`]: returned from the handler (`?`), it parks the event
    /// at once in a reactor (see [`Listener::park_after`]). Match it to accept one: `match
    /// ctx.submit(s).await { Err(Error::Rejected(r)) if r.code == "unique_taken" => {} r => {
    /// r?; } }`. Other errors as for [`Kernable::submit`].
    pub async fn submit(&self, submit: Submit) -> Result<Vec<StoredEvent>, Error> {
        if submit.id.is_none() {
            return Err(Error::Config(format!(
                "{}.{}: a follow-up command needs a fixed ID (Submit::id), so a redelivered \
                 event runs it once",
                submit.domain, submit.command_type
            )));
        }
        let submit = Submit {
            tenant: submit.tenant.or_else(|| Some(self.tenant.clone())),
            context: submit.context.or_else(|| Some(self.follow())),
            ..submit
        };
        match self.kernable.submit(submit).await? {
            Outcome::Committed(events) => Ok(events),
            Outcome::Rejected(r) => Err(Error::Rejected(r)),
        }
    }

    /// Asks a query on the listener's connection, like [`Kernable::query`]; unless the query
    /// sets it, for the event's tenant. It is the listener's own read: it keeps the
    /// connection's context, not the event's sender.
    pub async fn query<T: DeserializeOwned>(&self, query: Query) -> Result<T, Error> {
        let query = Query {
            tenant: query.tenant.or_else(|| Some(self.tenant.clone())),
            ..query
        };
        self.kernable.query(query).await
    }
}

/// What a query handler gets about the query besides its data (the `qry` of
/// [`Listener::query`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryMeta {
    /// The query's ID, a new UUID per query.
    pub query_id: String,
    /// The query type within the domain.
    pub query_type: String,
    pub domain: String,
    pub tenant: String,
}

/// What a query handler gets besides the query's data and meta (the `ctx` of
/// [`Listener::query`]).
#[derive(Clone)]
pub struct QueryContext {
    /// Who asks: check permissions here.
    pub request: RequestContext,
    /// The listener's store, if it has one ([`Listener::store`]).
    pub store: Option<Store>,
}

/// Whether a middleware checks a command, a query, a file request or a cache request
/// ([`Check::kind`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckKind {
    /// A command: [`Check::aggregate_type`] and [`Check::aggregate_id`] are set.
    Command,
    /// A query.
    Query,
    /// A file request ([`crate::Files`]): [`Check::type_name`] is `PutData`, `GetData` or
    /// `DeleteData`, [`Check::key`] is set.
    Data,
    /// A cache request ([`crate::Cache`]): [`Check::type_name`] is `SetCache`, `GetCache` or
    /// `DeleteCache`, [`Check::key`] is set.
    Cache,
}

/// What came of the request a middleware after it checks: an [`Outcome`] for a command, a
/// [`QueryOutcome`] for a query.
#[derive(Clone, Debug, PartialEq)]
pub enum CheckOutcome {
    /// What became of a command.
    Command(Outcome),
    /// What became of a query.
    Query(QueryOutcome),
}

/// A command, query, file or cache request a middleware checks, before the kernel accepts or
/// routes it or after (file and cache requests: before only): what the handler of
/// [`Listener::middleware`] gets.
#[derive(Clone, Debug)]
pub struct Check {
    pub kind: CheckKind,
    /// The command or query ID; for a file or cache request, the kernel's call ID.
    pub id: String,
    pub tenant: String,
    pub domain: String,
    /// The command or query type within the domain; for a file or cache request `PutData`,
    /// `GetData`, `DeleteData`, `SetCache`, `GetCache` or `DeleteCache`.
    pub type_name: String,
    /// Commands only.
    pub aggregate_type: Option<String>,
    /// Commands only.
    pub aggregate_id: Option<String>,
    /// The aggregate the request is about, its type qualified: a command's aggregate
    /// (`shop.order`, `o-17`), or the one a query declares ([`crate::query`]'s `target`,
    /// [`crate::QuerySchema::target`]). `None` for queries that declare none, and for file and
    /// cache requests.
    pub target: Option<Target>,
    /// File and cache requests only: the file's or value's key.
    pub key: Option<String>,
    /// The request's payload; vault fields are `null` unless the key is granted `vault_read`.
    /// `Value::Null` for a file or cache request: the middleware never sees the file or value.
    pub data: Value,
    /// Who sends it: check permissions here.
    pub request: RequestContext,
    /// After it, what came of the request; before it, `None`.
    pub outcome: Option<CheckOutcome>,
    /// The listener's store, if it has one ([`Listener::store`]): what the middleware learned
    /// from events, to decide with.
    pub store: Option<Store>,
}

/// The aggregate a request is about ([`Check::target`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    /// Qualified: `shop.order`.
    pub aggregate_type: String,
    pub aggregate_id: String,
}

/// When a middleware runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum When {
    /// Checks a request before the kernel accepts or routes it; a rejection refuses it
    /// (default).
    #[default]
    Before,
    /// Gets the request's outcome too. A rejection refuses a query's response; a command's
    /// outcome, and a rejected query, it only observes, best effort (it misses what happens
    /// while it is away, and nobody waits for it).
    After,
}

impl When {
    fn wire(self) -> proto::When {
        match self {
            When::Before => proto::When::Before,
            When::After => proto::When::After,
        }
    }
}

impl fmt::Display for When {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            When::Before => "before",
            When::After => "after",
        })
    }
}

/// What a middleware checks: `MiddlewareOptions::new(&["shop.*"])`, with
/// `MiddlewareOptions { order: 5, ..MiddlewareOptions::new(&["*"]) }` for another order and
/// `.after()` to run after requests.
#[derive(Clone, Debug)]
pub struct MiddlewareOptions {
    /// Command and query types: `*` (all), a type of the listener's domain (`Place`), of another
    /// domain (`orders.Place`) or a whole domain (`orders.*`). File and cache requests are
    /// types too: `PutData`, `GetData`, `DeleteData`, `SetCache`, `GetCache`, `DeleteCache`
    /// (e.g. `shop.PutData`); `*` and `orders.*` cover them.
    pub types: Vec<String>,
    /// Position in the chain: lower runs first, equal orders by listener name (default 0).
    /// Negative orders are reserved for kernable's kits (a grant of `reserved_order`).
    pub order: i32,
    /// Before (default) or after requests; see [`When`].
    pub when: When,
}

impl MiddlewareOptions {
    /// Checks `types` (see [`MiddlewareOptions::types`]) before requests, at order 0.
    pub fn new(types: &[&str]) -> MiddlewareOptions {
        MiddlewareOptions {
            types: types.iter().map(|t| (*t).to_owned()).collect(),
            order: 0,
            when: When::Before,
        }
    }

    /// Runs after requests instead of before: the handler gets their outcome too.
    #[must_use]
    pub fn after(mut self) -> MiddlewareOptions {
        self.when = When::After;
        self
    }
}

struct Settings {
    order: i32,
    /// Position to start after on the first connect; `None`: the kernel's stored one, or the
    /// newest event for a new listener.
    start: Option<u64>,
    /// Not registered with the kernel; see [`Listener::live`].
    live: bool,
    reconnect: Duration,
    store: Option<String>,
    schema: String,
    /// Attempts before an event whose handler fails is parked; 0: never.
    park_after: u32,
    /// The instance ID; `None`: `KERNABLE_INSTANCE`, else none.
    instance: Option<String>,
    /// Whether the instances take turns; see [`Listener::single`].
    single: bool,
}

struct ListenerInner {
    domain: Domain,
    name: String,
    settings: Mutex<Settings>,
    /// Qualified event type or `Domain.*` -> handler and whether it is raw.
    handlers: RwLock<HashMap<String, (EventHandler, bool)>>,
    /// The keys of `handlers` and `queries` in definition order.
    event_types: RwLock<Vec<String>>,
    query_types: RwLock<Vec<String>>,
    domains: RwLock<HashMap<String, Domain>>,
    /// Qualified query type -> handler.
    queries: RwLock<HashMap<String, QueryHandler>>,
    /// Middleware options with qualified types, and the handler, before and after requests.
    middlewares: RwLock<BTreeMap<When, (MiddlewareOptions, CheckHandler)>>,
}

/// A durable, named event listener of a domain: a read model ([`Domain::read_model`]) or a
/// reactor ([`Domain::reactor`]). It defines what to handle; [`crate::Kernable::listener`] runs
/// it. Events arrive in order, one at a time, after every listener with a lower order has
/// processed them. Delivery is at least once: if a handler fails, the listener reconnects and
/// the event is delivered again, until a reactor gives up on it and parks it
/// ([`Listener::park_after`]).
///
/// Types without a domain (`ItemAdded`) are the listener's own domain's; other domains' are
/// qualified (`identity.AccountRegistered`). Handlers get event payloads upcast to the versions
/// of the listener's domain and of the domains it is given with `with_domain`. An event with a
/// newer schema than known stops the listener (it retries and logs) until it is updated, so it
/// never misreads data. Cloning gives another handle to the same definition, and every
/// configuring method returns one, so they chain.
///
/// Handlers are closures that return a future: `Fn(data, meta, ctx) -> impl Future<Output =
/// Result<_, BoxError>>`. They are called again for every event, so they cannot own mutable
/// state: share it as `Arc<Mutex<…>>`, clone the `Arc` into each closure with `move`, and
/// lock it before the `async` block (do not hold a `std::sync::MutexGuard` across `.await`):
/// `let rows = Arc::new(Mutex::new(HashMap::<String, u32>::new())); let r = Arc::clone(&rows);
/// carts_readmodel.on("ItemAdded", move |data: ItemAdded, evt, _ctx| {
/// *r.lock().unwrap().entry(evt.aggregate_id).or_default() += data.qty; async { Ok(()) } });`
///
/// A query handler returns its answer (any `Serialize`) or a [`Rejected`] as a [`BoxError`]:
/// `carts_readmodel.query("GetCart", move |q: GetCart, _qry, _ctx| { let row = r.lock().unwrap().get(&q.id).copied(); async move { Ok(row.ok_or_else(|| Rejected::new("no such cart").code("not_found"))?) } });`
#[derive(Clone)]
pub struct Listener {
    inner: Arc<ListenerInner>,
}

/// Adds `t` to `types` unless it is there already.
fn remember(types: &RwLock<Vec<String>>, t: &str) {
    let mut types = types.write().unwrap();
    if !types.iter().any(|known| known == t) {
        types.push(t.to_owned());
    }
}

fn qualified(t: &str, wildcard: bool) -> bool {
    let Some((domain, name)) = t.split_once('.') else {
        return false;
    };
    let domain_ok = domain.starts_with(|c: char| c.is_ascii_alphabetic())
        && domain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    let name_ok = (wildcard && name == "*") || (!name.is_empty() && !name.contains(['.', '*']));
    domain_ok && name_ok
}

impl Listener {
    pub(crate) fn new(
        domain: &Domain,
        name: &str,
        order: i32,
        start: Option<u64>,
        park_after: u32,
        single: bool,
    ) -> Listener {
        let domains = HashMap::from([(domain.name().to_owned(), domain.clone())]);
        Listener {
            inner: Arc::new(ListenerInner {
                domain: domain.clone(),
                name: name.to_owned(),
                settings: Mutex::new(Settings {
                    order,
                    start,
                    live: false,
                    reconnect: Duration::from_secs(1),
                    store: None,
                    schema: "1".into(),
                    park_after,
                    instance: None,
                    single,
                }),
                handlers: RwLock::default(),
                event_types: RwLock::default(),
                query_types: RwLock::default(),
                domains: RwLock::new(domains),
                queries: RwLock::default(),
                middlewares: RwLock::default(),
            }),
        }
    }

    /// The listener's name: the kernel keeps its position under it.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The domain the listener belongs to: types without a domain are its.
    pub fn domain(&self) -> &Domain {
        &self.inner.domain
    }

    /// `t` qualified: a type without a domain is the listener's domain's. `None` if `t` is
    /// neither a type nor qualified (`domain.Type`, or `domain.*` with `wildcard`).
    /// The type as this listener names it; a message type's schema declared on its domain,
    /// which the listener then upcasts its events with.
    fn learn(&self, key: Key) -> String {
        let name = key.qualified_for(&self.inner.domain);
        if let (Some(own), Some(declare)) = (key.domain, key.declare) {
            if own == self.inner.domain.name() {
                self.inner.domain.declare_once(&key.name, declare);
            } else {
                let mut domains = self.inner.domains.write().unwrap();
                domains
                    .entry(own.to_owned())
                    .or_insert_with(|| crate::domain(own))
                    .declare_once(&key.name, declare);
            }
        }
        name
    }

    #[cfg(test)]
    pub(crate) fn domain_of(&self, name: &str) -> Option<Domain> {
        self.inner.domains.read().unwrap().get(name).cloned()
    }

    fn qualify(&self, t: &str, wildcard: bool) -> Option<String> {
        if t.contains('.') {
            qualified(t, wildcard).then(|| t.to_owned())
        } else {
            (!t.is_empty() && !t.contains('*')).then(|| format!("{}.{t}", self.inner.domain.name()))
        }
    }

    /// Listeners with a lower order process each event first; equal orders run in parallel.
    /// Defaults: 0 for read models, 10 for reactors. Negative orders are reserved for kernable's kits (a grant of `reserved_order`).
    pub fn order(&self, order: i32) -> Listener {
        self.inner.settings.lock().unwrap().order = order;
        self.clone()
    }

    /// Position to start after on the first connect: 0 rebuilds from the beginning (default
    /// for read models). Reactors resume after the last acknowledged event unless given one,
    /// and a new one starts at the newest event, so it never reacts to what happened before it
    /// existed. Reconnects always resume after the last handled event.
    pub fn start(&self, position: u64) -> Listener {
        self.inner.settings.lock().unwrap().start = Some(position);
        self.clone()
    }

    /// Not registered with the kernel: no stored position, always starts at the newest event,
    /// nobody waits for it, and it is forgotten when it disconnects. For reactions that only
    /// matter while the process runs, e.g. pushing to connected browsers. Make it live before
    /// running it.
    ///
    /// # Panics
    ///
    /// If the listener is a middleware: a live listener cannot be one.
    pub fn live(&self) -> Listener {
        assert!(
            !self.is_middleware(),
            "{} is a middleware: a live listener cannot be one",
            self.inner.name
        );
        self.inner.settings.lock().unwrap().live = true;
        self.clone()
    }

    /// Runs this process's listener as the instance `id` of a group under the listener's name,
    /// e.g. one per node of a pair: 1 to 128 letters, digits, `-`, `_` and `.`. Default:
    /// `KERNABLE_INSTANCE`, which `kernable node` sets; without either, the name has one
    /// connection. Read models are replicated: every instance gets every event and answers
    /// queries. Reactors are single (see [`Listener::single`]).
    pub fn instance(&self, id: &str) -> Listener {
        self.inner.settings.lock().unwrap().instance = Some(id.to_owned());
        self.clone()
    }

    /// Whether the instances take turns (default for reactors): one gets events, the others
    /// wait and the first of them takes over when it leaves, so a side effect happens once.
    /// `false` (default for read models): every instance gets every event, from its own
    /// position; such a listener cannot park events. Live listeners are never single.
    pub fn single(&self, single: bool) -> Listener {
        self.inner.settings.lock().unwrap().single = single;
        self.clone()
    }

    /// The instance ID it connects with, and whether its group is single; ("", false) without
    /// an instance.
    pub(crate) fn instance_of(&self) -> (String, bool) {
        let s = self.inner.settings.lock().unwrap();
        let instance = s
            .instance
            .clone()
            .or_else(|| std::env::var("KERNABLE_INSTANCE").ok())
            .unwrap_or_default();
        let single = !instance.is_empty() && s.single && !s.live;
        (instance, single)
    }

    pub(crate) fn is_live(&self) -> bool {
        self.inner.settings.lock().unwrap().live
    }

    /// Whether it checks requests, before or after.
    pub(crate) fn is_middleware(&self) -> bool {
        !self.inner.middlewares.read().unwrap().is_empty()
    }

    /// The qualified event types (or `domain.*`) it reads, in definition order.
    pub(crate) fn event_types(&self) -> Vec<String> {
        self.inner.event_types.read().unwrap().clone()
    }

    /// The qualified query types it answers, in definition order.
    pub(crate) fn query_types(&self) -> Vec<String> {
        self.inner.query_types.read().unwrap().clone()
    }

    /// The types it checks before and after requests, if it is a middleware.
    pub(crate) fn checks(&self) -> Vec<(When, Vec<String>)> {
        self.inner
            .middlewares
            .read()
            .unwrap()
            .iter()
            .map(|(when, (options, _))| (*when, options.types.clone()))
            .collect()
    }

    /// What it does from `position`, for the log; `name` shortens the types.
    pub(crate) fn details(&self, position: u64, name: &dyn Fn(&str) -> String) -> Vec<String> {
        log::listener_details(
            position,
            &self.event_types(),
            &self.query_types(),
            &self.checks(),
            name,
        )
    }

    /// Delay before reconnecting after the stream drops or a handler fails (default 1 s); twice
    /// as long after each further failed attempt, at most 30 s.
    pub fn reconnect(&self, delay: Duration) -> Listener {
        self.inner.settings.lock().unwrap().reconnect = delay;
        self.clone()
    }

    /// How often a handler may fail on one event before the listener parks it and moves on
    /// (default 5 for reactors, 0 for read models; 0: never, the listener keeps getting the
    /// event and everything behind it waits). A [`Rejected`] (also a rejected
    /// [`ListenerContext::submit`]) parks at once; the kernel being unreachable or busy
    /// (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, [`Error::NotSettled`]) or the
    /// store failing never does: the listener waits and tries again. The kernel keeps parked
    /// events with the rejection's code and message (for other errors `handler_error` and
    /// `handler failed`; the log has the whole error) until an operator retries or dismisses
    /// them (`kernable listeners retry`). A read model should not park: it would miss the
    /// event's change. With a store, a parked event's writes are dropped, the rest of its
    /// batch kept. Live listeners never park, and no listener parks an event that erases a
    /// subject: its handler must get past it, so no personal data stays behind.
    pub fn park_after(&self, attempts: u32) -> Listener {
        self.inner.settings.lock().unwrap().park_after = attempts;
        self.clone()
    }

    /// Keeps the listener's state in a store (`sqlite:<path>` or `memory:`) instead of
    /// memory: its data and position are written together per batch, and a restart continues
    /// after the stored position. `schema` is the version of the stored data's format: change
    /// it when the handlers store data differently, and the store is cleared and rebuilt.
    /// Handlers get the store as `ctx.store` ([`ListenerContext::store`],
    /// [`QueryContext::store`]). Example: `shop_domain.read_model("carts").store("sqlite:carts.db", "1")`.
    pub fn store(&self, url: &str, schema: &str) -> Listener {
        let mut settings = self.inner.settings.lock().unwrap();
        settings.store = Some(url.to_owned());
        settings.schema = schema.to_owned();
        drop(settings);
        self.clone()
    }

    /// Upcasts the events of another `domain` with its event schemas (see [`Domain::event`]);
    /// the listener's own domain's apply without it.
    pub fn with_domain(&self, domain: &Domain) -> Listener {
        self.inner
            .domains
            .write()
            .unwrap()
            .insert(domain.name().to_owned(), domain.clone());
        self.clone()
    }

    /// Handles `event_type` events: a type of the listener's domain (`ItemAdded`), of another
    /// domain (`orders.OrderPlaced`), a whole domain (`orders.*`), or an event type's constant
    /// (see [`crate::event`]; `query` takes a query type's likewise). `handler(data, evt, ctx)`
    /// gets the payload as `D` (`serde_json::Value` for any), a [`StoredEventMeta`] and a
    /// [`ListenerContext`], and returns a future of `Result<(), BoxError>`. A payload that is
    /// not a `D` or an `Err` fails the batch: the listener reconnects and the event is
    /// delivered again, so handlers must be idempotent. See [`Listener`] for an example.
    ///
    /// # Panics
    ///
    /// If `event_type` is none of these.
    pub fn on<D, F, Fut>(&self, event_type: impl TypeKey<D>, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let event_type = self.learn(event_type.key());
        self.handle(&event_type, handler, false)
    }

    /// Like [`on`](Self::on), but gets events as stored, without upcasting, whatever their
    /// schema (for archives and exports).
    pub fn on_raw<D, F, Fut>(&self, event_type: impl TypeKey<D>, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let event_type = self.learn(event_type.key());
        self.handle(&event_type, handler, true)
    }

    /// Like [`on`](Self::on), but keeps a handler registered for the type already: this one runs
    /// after it, in the same delivery (a failing first one skips it). For code that adds to a
    /// listener it does not own, e.g. the kit's `visibility`; register it after your own handlers.
    pub fn also_on<D, F, Fut>(&self, event_type: impl TypeKey<D>, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let event_type = self.learn(event_type.key());
        let Some(qualified) = self.qualify(&event_type, true) else {
            panic!(
                "listen to a type (Type or domain.Type) or a domain (domain.*), not {event_type:?}"
            );
        };
        let before = self.inner.handlers.read().unwrap().get(&qualified).cloned();
        self.handle(&event_type, handler, false);
        if let Some((first, raw)) = before {
            let mut handlers = self.inner.handlers.write().unwrap();
            let then = handlers
                .get(&qualified)
                .map(|(h, _)| Arc::clone(h))
                .expect("registered just now");
            let both: EventHandler = Arc::new(move |e, ctx| {
                let (first, then) = (Arc::clone(&first), Arc::clone(&then));
                Box::pin(async move {
                    first(e.clone(), ctx.clone()).await?;
                    then(e, ctx).await
                })
            });
            handlers.insert(qualified, (both, raw));
        }
        self.clone()
    }

    fn handle<D, F, Fut>(&self, event_type: &str, handler: F, raw: bool) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let Some(event_type) = self.qualify(event_type, true) else {
            panic!(
                "listen to a type (Type or domain.Type) or a domain (domain.*), not {event_type:?}"
            );
        };
        let handler = Arc::new(handler);
        let handler: EventHandler = Arc::new(move |e, ctx| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let meta = StoredEventMeta::of(&e);
                let data: D = serde_json::from_value(e.data)
                    .map_err(|err| format!("{}.{} data: {err}", meta.domain, meta.event_type))?;
                handler(data, meta, ctx).await
            })
        });
        remember(&self.inner.event_types, &event_type);
        self.inner
            .handlers
            .write()
            .unwrap()
            .insert(event_type, (handler, raw));
        self.clone()
    }

    /// Answers `query_type` queries: a type of the listener's domain (`GetOrder`) or of another
    /// one (`orders.GetOrder`), from this listener's state: `handler(data, qry, ctx)` gets the
    /// query's data as `D`, a [`QueryMeta`] and a [`QueryContext`], and returns a future of
    /// the answer `R` (any `Serialize`). The kernel delivers queries in order with the events,
    /// so an answer reflects every event handled before it. Return a [`Rejected`] (as a
    /// [`BoxError`]) to refuse; any other error refuses with `handler_error`. A handler cannot
    /// query its own listener (the answer would wait behind itself): the kernel refuses it. See
    /// [`Listener`] for an example.
    ///
    /// # Panics
    ///
    /// If `query_type` is neither.
    pub fn query<D, R, F, Fut>(&self, query_type: impl TypeKey<D>, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        R: Serialize,
        F: Fn(D, QueryMeta, QueryContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, BoxError>> + Send + 'static,
    {
        let query_type = self.learn(query_type.key());
        let Some(query_type) = self.qualify(&query_type, false) else {
            panic!("answer a query type (Query or Domain.Query), not {query_type:?}");
        };
        let handler = Arc::new(handler);
        let query: QueryHandler = Arc::new(move |data, qry, ctx| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let data: D = serde_json::from_value(data)?;
                let answer = handler(data, qry, ctx).await?;
                Ok(serde_json::to_value(answer)?)
            })
        });
        remember(&self.inner.query_types, &query_type);
        self.inner
            .queries
            .write()
            .unwrap()
            .insert(query_type, query);
        self.clone()
    }

    /// Makes this listener a middleware: the kernel asks `handler` about every command, query,
    /// file and cache request of `options.types` (see [`MiddlewareOptions::types`]), before accepting or routing it (or after, see [`When`]), in
    /// chain order, and only once this listener has caught up with the events. Return a
    /// [`Rejected`] to refuse; any other error refuses too. After a request, the check carries
    /// its outcome ([`Check::outcome`]). The registration is stored: while this listener is
    /// away, the kernel refuses those requests (`kernable listeners remove` unregisters it).
    /// A listener has at most one middleware before and one after. The handler gets a
    /// [`Check`] and returns a future of `Result<(), BoxError>`: `Ok(())` lets the request
    /// pass. Example: `MiddlewareOptions::new(&["shop.*"])` with a handler that returns
    /// `Err(Rejected::new("not allowed").code("forbidden").into())` unless
    /// `check.request.sender_identity` may send `check.type_name`.
    ///
    /// # Panics
    ///
    /// If a type is not `*`, a type, a qualified type or a domain, the listener has a
    /// middleware at `options.when` already, or it is live.
    pub fn middleware<F, Fut>(&self, options: MiddlewareOptions, handler: F) -> Listener
    where
        F: Fn(Check) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        assert!(
            !self.is_live(),
            "{} is live: a live listener cannot be a middleware",
            self.inner.name
        );
        let types = options
            .types
            .iter()
            .map(|t| {
                let qualified = if t == "*" {
                    Some(t.clone())
                } else {
                    self.qualify(t, true)
                };
                qualified.unwrap_or_else(|| {
                    panic!(
                        "check `*`, a type (Type or domain.Type) or a domain (domain.*), not {t:?}"
                    )
                })
            })
            .collect();
        let when = options.when;
        let taken = self.inner.middlewares.read().unwrap().contains_key(&when);
        assert!(
            !taken,
            "{} has a middleware {when} already",
            self.inner.name
        );
        let options = MiddlewareOptions { types, ..options };
        let handler: CheckHandler = Arc::new(move |c| Box::pin(handler(c)));
        self.inner
            .middlewares
            .write()
            .unwrap()
            .insert(when, (options, handler));
        self.clone()
    }
}

struct HandleInner {
    client: Client,
    /// For handlers' follow-up commands and queries.
    kernable: Kernable,
    /// How often the handler failed on an event, by position, until it gets past it.
    attempts: Mutex<HashMap<u64, u32>>,
    listener: Listener,
    /// The service it belongs to (`Options::service`); empty if none.
    service: String,
    position: AtomicU64,
    store: Mutex<Option<Store>>,
    stopped: watch::Sender<bool>,
    ready: watch::Sender<bool>,
    /// Whether the first connect gets its own line; an app prints it in its overview instead.
    announce: AtomicBool,
    /// Connects the kernel accepted so far.
    connects: AtomicU64,
    /// Position after which the kernel delivered on the first connect.
    started_at: AtomicU64,
    /// As a partition's stream: its partition, whose position the store keeps apart.
    part: String,
    /// As a partition's stream: the store is the parent's, which closes it.
    shared: bool,
    /// As a partition's stream: the handle that runs it.
    parent: Option<Arc<HandleInner>>,
    /// As that handle: how far each partition's stream has handled, for the order across
    /// moves (S4); `moved` counts the changes.
    progress: Mutex<HashMap<String, u64>>,
    moved: watch::Sender<u64>,
    /// As a partition's stream: tenants it read the map again for.
    checked: Mutex<HashSet<String>>,
}

impl HandleInner {
    /// On the parent: partition `part`'s stream has handled everything up to `position`.
    fn advanced(&self, part: &str, position: u64) {
        let mut progress = self.progress.lock().unwrap();
        if progress.get(part).is_none_or(|at| *at < position) {
            progress.insert(part.to_owned(), position);
            self.moved.send_modify(|n| *n += 1);
        }
    }

    /// On the parent: whether partition `part`'s stream has handled up to `position`.
    fn passed(&self, part: &str, position: u64) -> bool {
        self.progress
            .lock()
            .unwrap()
            .get(part)
            .is_some_and(|at| *at >= position)
    }
}

/// A [`Listener`] running on a connection; see [`crate::Kernable::listener`]. Cloning gives
/// another handle to the same running listener.
///
/// `run().await` processes events until `stop()`. To run several listeners in one process,
/// spawn each (`tokio::spawn(async move { handle.run().await })`) and await `ready()` where
/// the next step needs them connected, or use an app ([`crate::Kernable::app`]).
#[derive(Clone)]
pub struct ListenerHandle {
    inner: Arc<HandleInner>,
}

impl ListenerHandle {
    pub(crate) fn new(kernable: &Kernable, listener: &Listener, service: String) -> ListenerHandle {
        ListenerHandle {
            inner: Arc::new(HandleInner {
                client: kernable.client.clone(),
                kernable: kernable.clone(),
                attempts: Mutex::default(),
                listener: listener.clone(),
                service,
                position: AtomicU64::new(0),
                store: Mutex::default(),
                stopped: watch::Sender::new(false),
                ready: watch::Sender::new(false),
                announce: AtomicBool::new(true),
                connects: AtomicU64::new(0),
                started_at: AtomicU64::new(0),
                part: String::new(),
                shared: false,
                parent: None,
                progress: Mutex::default(),
                moved: watch::Sender::new(0),
                checked: Mutex::default(),
            }),
        }
    }

    /// The same listener on another client: one partition's stream, sharing `store`.
    fn on(&self, client: Client, part: String, store: Option<Store>) -> ListenerHandle {
        let inner = &self.inner;
        ListenerHandle {
            inner: Arc::new(HandleInner {
                client,
                kernable: inner.kernable.clone(),
                attempts: Mutex::default(),
                listener: inner.listener.clone(),
                service: inner.service.clone(),
                position: AtomicU64::new(0),
                store: Mutex::new(store),
                stopped: watch::Sender::new(false),
                ready: watch::Sender::new(false),
                announce: AtomicBool::new(inner.announce.load(Ordering::SeqCst)),
                connects: AtomicU64::new(0),
                started_at: AtomicU64::new(0),
                part,
                shared: true,
                parent: Some(Arc::clone(inner)),
                progress: Mutex::default(),
                moved: watch::Sender::new(0),
                checked: Mutex::default(),
            }),
        }
    }

    /// Whether the first connect gets its own line (default); an app prints it in its overview.
    pub(crate) fn announce(&self, announce: bool) {
        self.inner.announce.store(announce, Ordering::SeqCst);
    }

    /// Position after which the kernel delivered on the first connect.
    pub(crate) fn started_at(&self) -> u64 {
        self.inner.started_at.load(Ordering::SeqCst)
    }

    /// The listener's definition.
    pub fn listener(&self) -> &Listener {
        &self.inner.listener
    }

    /// Global position of the last handled event (or skipped, for other event types).
    pub fn position(&self) -> u64 {
        self.inner.position.load(Ordering::SeqCst)
    }

    /// The listener's store, while `run()` has it open.
    pub fn opened_store(&self) -> Option<Store> {
        self.inner.store.lock().unwrap().clone()
    }

    /// Returns once the kernel accepted the listener.
    pub async fn ready(&self) {
        let _ = self.inner.ready.subscribe().wait_for(|r| *r).await;
    }

    /// Ends `run`.
    pub fn stop(&self) {
        self.inner.stopped.send_replace(true);
    }

    fn def(&self) -> &ListenerInner {
        &self.inner.listener.inner
    }

    /// Processes events, queries and checks until `stop()`, reconnecting when the stream drops
    /// or a handler fails. With a store, opens it first and continues after its position; if
    /// it cannot be opened (a full disk), logs the error and tries again, backing off like a
    /// reconnect. When it ends, it closes the store ([`Store::close`]); a later run opens it
    /// again.
    pub async fn run(&self) {
        // Also when the caller drops the future (a timeout, an aborted task).
        struct Closing<'a>(&'a HandleInner);
        impl Drop for Closing<'_> {
            fn drop(&mut self) {
                if self.0.shared {
                    return; // the parent's store
                }
                if let Some(store) = self.0.store.lock().unwrap().take() {
                    store.close();
                }
            }
        }
        let _closing = Closing(&self.inner);
        let mut stopped = self.inner.stopped.subscribe();
        let what = format!("listener {}", self.def().name);
        let base = self.def().settings.lock().unwrap().reconnect;
        let Some(parts) = crate::partitions_of(&self.inner.client, &what, base, &mut stopped).await
        else {
            return;
        };
        if parts.len() > 1 || !parts[0].0.is_empty() {
            // A stream per partition, each from its own position in the shared store.
            if !self.open_store(&what, &mut stopped).await {
                return;
            }
            let store = self.opened_store();
            let children: Vec<ListenerHandle> = parts
                .into_iter()
                .map(|(name, client)| self.on(client, name, store.clone()))
                .collect();
            let (this, all) = (self.clone(), children.clone());
            tokio::spawn(async move {
                for child in &all {
                    child.ready().await;
                }
                let first = all
                    .iter()
                    .map(ListenerHandle::started_at)
                    .min()
                    .unwrap_or(0);
                this.inner.started_at.store(first, Ordering::SeqCst);
                this.inner.ready.send_replace(true);
            });
            let stopping = {
                let (this, all) = (self.clone(), children.clone());
                tokio::spawn(async move {
                    let _ = this.inner.stopped.subscribe().wait_for(|s| *s).await;
                    for child in &all {
                        child.stop();
                    }
                })
            };
            let running: Vec<_> = children
                .into_iter()
                .map(|child| tokio::spawn(async move { child.run_one().await }))
                .collect();
            for run in running {
                let _ = run.await;
            }
            stopping.abort();
            return;
        }
        self.run_one().await;
    }

    /// Opens the listener's store, if it has one and it is not open; tries again while it
    /// cannot be opened. False once stopped.
    async fn open_store(&self, what: &str, stopped: &mut watch::Receiver<bool>) -> bool {
        let log = &self.inner.client.log;
        let store = {
            let s = self.def().settings.lock().unwrap();
            s.store.clone().map(|url| (url, s.schema.clone()))
        };
        let Some((url, schema)) = store else {
            return true;
        };
        if self.opened_store().is_some() {
            return true;
        }
        let mut attempt = 0;
        let store = loop {
            match Store::open(&url, &schema) {
                Ok(store) => break store,
                Err(e) => {
                    attempt += 1;
                    let base = self.def().settings.lock().unwrap().reconnect;
                    let delay = log::backoff(base, attempt);
                    log.error(what, || {
                        format!("store: {e}; trying again in {} s", delay.as_secs_f64())
                    });
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = stopped.wait_for(|s| *s) => return false,
                    }
                }
            }
        };
        *self.inner.store.lock().unwrap() = Some(store);
        true
    }

    /// The stream loop on this listener's one kernel (or one partition).
    async fn run_one(&self) {
        let mut stopped = self.inner.stopped.subscribe();
        let log = &self.inner.client.log;
        let what = format!("listener {}", self.def().name);
        let (mut start, store) = {
            let s = self.def().settings.lock().unwrap();
            (s.start, s.store.clone().map(|url| (url, s.schema.clone())))
        };
        if self.inner.shared
            && let Some(store) = self.opened_store()
        {
            let at = store.committed_for(&self.inner.part).unwrap_or(0);
            self.inner.position.store(at, Ordering::SeqCst);
            if at > 0 {
                start = Some(at);
            }
        }
        if let Some((url, schema)) = store
            && !self.inner.shared
            && self.opened_store().is_none()
        {
            let mut attempt = 0;
            let store = loop {
                match Store::open(&url, &schema) {
                    Ok(store) => break store,
                    Err(e) => {
                        attempt += 1;
                        let base = self.def().settings.lock().unwrap().reconnect;
                        let delay = log::backoff(base, attempt);
                        log.error(&what, || {
                            format!("store: {e}; trying again in {} s", delay.as_secs_f64())
                        });
                        tokio::select! {
                            () = tokio::time::sleep(delay) => {}
                            _ = stopped.wait_for(|s| *s) => return,
                        }
                    }
                }
            };
            self.inner
                .position
                .store(store.committed(), Ordering::SeqCst);
            // A new (or cleared) store says nothing: the listener's own start applies (a
            // reactor starts at the newest event).
            if store.committed() > 0 {
                start = Some(store.committed());
            }
            *self.inner.store.lock().unwrap() = Some(store);
        }
        let mut attempt = 0;
        while !*stopped.borrow() {
            let connects = self.inner.connects.load(Ordering::SeqCst);
            let result = tokio::select! {
                result = self.listen(start) => result,
                _ = stopped.wait_for(|s| *s) => return,
            };
            attempt = if self.inner.connects.load(Ordering::SeqCst) > connects {
                1
            } else {
                attempt + 1
            };
            // Resume after the last handled event, once there is one: before the first ready,
            // the position says nothing (a new reactor would replay everything from 0).
            if self.inner.connects.load(Ordering::SeqCst) > 0 {
                start = Some(self.position());
            }
            let base = self.def().settings.lock().unwrap().reconnect;
            let delay = log::reconnecting(log, &what, result.err().as_ref(), base, attempt);
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = stopped.wait_for(|s| *s) => return,
            }
        }
    }

    async fn listen(&self, start: Option<u64>) -> Result<(), BoxError> {
        let inner = &self.inner;
        let def = self.def();
        let client = &inner.client;
        let (tx, rx) = mpsc::channel(1024);
        let msg = |m: Reply| ListenerMessage { msg: Some(m) };
        let (order, live) = {
            let s = def.settings.lock().unwrap();
            (s.order, s.live)
        };
        let middlewares: Vec<Middleware> = def
            .middlewares
            .read()
            .unwrap()
            .values()
            .map(|(o, _)| Middleware {
                order: o.order,
                types: o.types.clone(),
                when: o.when.wire().into(),
            })
            .collect();
        let event_types = self.inner.listener.event_types();
        let query_types = self.inner.listener.query_types();
        let what = format!("listener {}", def.name);
        let (instance, single) = self.inner.listener.instance_of();
        let hello = Hello {
            name: def.name.clone(),
            key_id: client.key_id.clone(),
            order,
            event_types: event_types.clone(),
            from_position: start,
            query_types: query_types.clone(),
            middlewares,
            protocol: PROTOCOL,
            service: inner.service.clone(),
            live,
            instance,
            single,
        };
        tx.send(msg(Reply::Hello(hello))).await?;
        let (at, channel) = client.channel();
        let ended = |status: tonic::Status| {
            client.ended(at, &status);
            status
        };
        let mut inbound = ListenerServiceClient::new(channel)
            .listen(client.request(ReceiverStream::new(rx)))
            .await
            .map_err(ended)?
            .into_inner();
        let mut replay_up_to = 0;
        while let Some(m) = inbound.message().await.map_err(ended)? {
            // Requests sent from the handlers name this listener, so the kernel never makes
            // them wait for it (it handles one message at a time).
            let reply = match m.msg {
                Some(Delivery::Challenge(c)) => Reply::ChallengeResponse(ChallengeResponse {
                    sig: client.sign(&signing::challenge_message(&c.nonce)).sig,
                }),
                Some(Delivery::Ready(r)) => {
                    inner.position.store(r.after_position, Ordering::SeqCst);
                    if let Some(parent) = &inner.parent {
                        parent.advanced(&inner.part, r.after_position);
                    }
                    replay_up_to = r.head_position;
                    if inner.connects.fetch_add(1, Ordering::SeqCst) > 0 {
                        client.log.info(&what, || {
                            format!("connected again at position {}", r.after_position)
                        });
                    } else {
                        inner.started_at.store(r.after_position, Ordering::SeqCst);
                        if inner.announce.load(Ordering::SeqCst) {
                            client.log.info(&what, || {
                                let details =
                                    self.inner.listener.details(r.after_position, &|t| t.into());
                                log::listener_connected(&client.log.peer, &details)
                            });
                        }
                    }
                    inner.ready.send_replace(true);
                    continue;
                }
                Some(Delivery::Waiting(w)) => {
                    // Another instance runs the group: this one is ready to take over.
                    client.log.info(&what, || {
                        format!("waiting: instance {} is active", w.active)
                    });
                    inner.ready.send_replace(true);
                    continue;
                }
                Some(Delivery::Batch(batch)) => {
                    let up_to = batch.up_to;
                    let erased = batch
                        .events
                        .iter()
                        .chain(&batch.retries)
                        .any(|e| !e.erases.is_empty());
                    // A kernel that goes back (a bug, or not the kernel) must not make a store
                    // apply events twice: the session ends, and the next resumes after what
                    // was handled.
                    let at = inner.position.load(Ordering::SeqCst);
                    if up_to < at {
                        return Err(format!(
                            "the kernel sent a batch up to {up_to}, behind position {at}"
                        )
                        .into());
                    }
                    self.in_order(&batch.events, &what).await?;
                    let store = self.opened_store();
                    // With a store, the batch's data and position are committed together,
                    // before the ack.
                    // The partitions' batches take turns on a shared store.
                    let _turn = match &store {
                        Some(s) => Some(s.turn().await),
                        None => None,
                    };
                    if let Some(s) = &store {
                        s.begin()?;
                    }
                    let handled = log::watched(
                        &client.log,
                        &what,
                        Duration::from_secs(30),
                        || stuck(&format!("the batch up to {up_to}"), 30),
                        unwinding(HANDLING.scope(
                            def.name.clone(),
                            self.handle_batch(batch, replay_up_to, store.clone()),
                        )),
                    )
                    .await
                    .unwrap_or_else(|panic| Err(panic.into()))
                    .and_then(|parked| match &store {
                        Some(s) => {
                            Ok(s.commit_part(up_to, erased, &inner.part).map(|()| parked)?)
                        }
                        None => Ok(parked),
                    });
                    let parked = match handled {
                        Ok(parked) => parked,
                        Err(e) => {
                            if let Some(s) = &store {
                                s.rollback();
                                let at = s.committed_for(&inner.part).unwrap_or(0);
                                inner.position.store(at, Ordering::SeqCst);
                            }
                            return Err(e);
                        }
                    };
                    inner.position.store(up_to, Ordering::SeqCst);
                    if let Some(parent) = &inner.parent {
                        parent.advanced(&inner.part, up_to);
                    }
                    Reply::Ack(Ack {
                        position: up_to,
                        parked,
                    })
                }
                Some(Delivery::Query(q)) => {
                    let query = q.query.unwrap_or_default();
                    let query_type = format!("{}.{}", query.domain, query.query_type);
                    let asked = format!("the query {query_type}");
                    let answer = log::watched(
                        &client.log,
                        &what,
                        Duration::from_secs(5),
                        || stuck(&asked, 5),
                        unwinding(HANDLING.scope(def.name.clone(), self.answer(query))),
                    )
                    .await
                    .unwrap_or_else(|panic| {
                        let rejected = Rejected::new(panic).code("handler_error");
                        query_answer::Outcome::Rejected(rejected.to_wire())
                    });
                    client.log.debug(&what, || {
                        let rejected = match &answer {
                            query_answer::Outcome::Rejected(r) => Some(r.code.as_str()),
                            _ => None,
                        };
                        log::query_done(&query_type, rejected)
                    });
                    Reply::Answer(QueryAnswer {
                        call_id: q.call_id,
                        outcome: Some(answer),
                    })
                }
                Some(Delivery::Check(c)) => {
                    let call_id = c.call_id.clone();
                    let rejected = log::watched(
                        &client.log,
                        &what,
                        Duration::from_secs(5),
                        || stuck("a middleware check", 5),
                        unwinding(HANDLING.scope(def.name.clone(), self.verdict(c))),
                    )
                    .await
                    .unwrap_or_else(|panic| {
                        Some(Rejected::new(panic).code("middleware_error").to_wire())
                    });
                    Reply::Verdict(Verdict { call_id, rejected })
                }
                None => continue,
            };
            tx.send(msg(reply)).await?;
        }
        Ok(())
    }

    /// Before a batch of a partition's stream: the events of a tenant that moved here
    /// after its move wait until the stream of the partition it came from has passed the
    /// tenant's last event there, so a tenant's events keep their order across the move. Waits
    /// at most 30 s (a partition that is down), then goes on with a warning.
    async fn in_order(&self, events: &[proto::Event], what: &str) -> Result<(), BoxError> {
        let inner = &self.inner;
        let Some(parent) = &inner.parent else {
            return Ok(());
        };
        if events.is_empty() {
            return Ok(());
        }
        let client = &inner.client;
        let Some(mut map) = client.partition_map(false).await? else {
            return Ok(());
        };
        let part = inner.part.as_str();
        let known = |m: &proto::PartitionMap, tenant: &str| {
            crate::partition_of(m, tenant) == part
                || m.moved
                    .iter()
                    .any(|x| x.tenant == tenant && (x.to == part || x.from == part))
        };
        for e in events {
            if !known(&map, &e.tenant_id)
                && inner.checked.lock().unwrap().insert(e.tenant_id.clone())
            {
                // The tenant came here after the map was read; another kernel may take a moment
                // to hold the new one. This stream's partition holds it already.
                for i in 0..3 {
                    if known(&map, &e.tenant_id) {
                        break;
                    }
                    if i > 0 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    if let Some(m) = client.partition_map(true).await? {
                        map = m;
                    }
                }
            }
            for m in &map.moved {
                if m.tenant != e.tenant_id
                    || m.to != part
                    || e.global_position <= m.target_after
                    || !map.partitions.iter().any(|p| p.name == m.from)
                {
                    continue;
                }
                let mut moved = parent.moved.subscribe();
                let mut stopped = inner.stopped.subscribe();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                while !parent.passed(&m.from, m.source_head) {
                    tokio::select! {
                        _ = moved.changed() => {}
                        _ = stopped.wait_for(|s| *s) => return Err("stopped".into()),
                        () = tokio::time::sleep_until(deadline) => {
                            client.log.warn(what, || format!(
                                "tenant {} moved here from partition {}, whose stream has not passed position {} in 30 s; going on without its order",
                                e.tenant_id, m.from, m.source_head
                            ));
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Handles a batch: first the retries, then the events after the position. Returns the
    /// events it parked; an error if a handler failed on one it does not park.
    async fn handle_batch(
        &self,
        batch: proto::Batch,
        replay_up_to: u64,
        store: Option<Store>,
    ) -> Result<Vec<Parked>, BoxError> {
        let def = self.def();
        let (park_after, live) = {
            let s = def.settings.lock().unwrap();
            (s.park_after, s.live)
        };
        let parking = park_after > 0 && !live;
        let mut parked = Vec::new();
        let retries = batch.retries.into_iter().map(|e| (e, true));
        for (e, retry) in retries.chain(batch.events.into_iter().map(|e| (e, false))) {
            let position = e.global_position;
            if !retry && position <= self.position() {
                continue; // handled already
            }
            let entry = {
                let handlers = def.handlers.read().unwrap();
                handlers
                    .get(&format!("{}.{}", e.domain, e.event_type))
                    .or_else(|| handlers.get(&format!("{}.*", e.domain)))
                    .cloned()
            };
            if let Some((handler, raw)) = entry {
                let erasing = !e.erases.is_empty();
                let mut event = StoredEvent::from_wire(e, &batch.vault);
                if !raw {
                    let known = def.domains.read().unwrap().get(&event.domain).cloned();
                    event.data = upcast(
                        known.as_ref(),
                        &event.domain,
                        &event.event_type,
                        event.schema,
                        event.data,
                    )?;
                }
                let ctx = ListenerContext {
                    replaying: position <= replay_up_to,
                    store: store.clone(),
                    request: event.request.clone(),
                    command_id: event.command_id.clone(),
                    tenant: event.tenant.clone(),
                    kernable: self.inner.kernable.clone(),
                };
                let event_type = format!("{}.{}", event.domain, event.event_type);
                let saved = match &store {
                    Some(s) if parking => {
                        s.savepoint()?;
                        true
                    }
                    _ => false,
                };
                let result = unwinding(handler(event, ctx))
                    .await
                    .unwrap_or_else(|panic| Err(panic.into()));
                match result {
                    Ok(()) => {
                        if let (true, Some(s)) = (saved, &store) {
                            s.release()?;
                        }
                        self.inner.attempts.lock().unwrap().remove(&position);
                        self.inner
                            .client
                            .log
                            .debug(&format!("listener {}", def.name), || {
                                log::event_processed(&event_type, position)
                            });
                    }
                    Err(err) => {
                        // An erasing event is never parked: its handler must get past it, or
                        // what it removes would stay behind.
                        let p = self.give_up(
                            &event_type,
                            position,
                            err,
                            park_after,
                            parking && !erasing,
                        )?;
                        if let (true, Some(s)) = (saved, &store) {
                            s.rollback_event()?;
                        }
                        parked.push(p);
                    }
                }
            }
            if !retry {
                self.inner.position.store(position, Ordering::SeqCst);
            }
        }
        Ok(parked)
    }

    /// What to do after the handler failed on the event at `position`: park it (a rejection at
    /// once, another error after `park_after` attempts), or fail the batch with `err`, so the
    /// listener gets the event again (the kernel or the store away, or attempts left).
    fn give_up(
        &self,
        event_type: &str,
        position: u64,
        err: BoxError,
        park_after: u32,
        parking: bool,
    ) -> Result<Parked, BoxError> {
        if !parking || transient(&err) {
            return Err(err);
        }
        let attempts = {
            let mut counts = self.inner.attempts.lock().unwrap();
            let n = counts.entry(position).or_default();
            *n += 1;
            *n
        };
        let rejected = rejection(&err);
        if rejected.is_none() && attempts < park_after {
            return Err(err);
        }
        self.inner.attempts.lock().unwrap().remove(&position);
        let what = format!("listener {}", self.def().name);
        let (code, message) = match rejected {
            Some(r) => {
                let (code, message) = (r.code.clone(), r.message.clone());
                self.inner.client.log.warn(&what, || {
                    format!("{event_type} at {position} rejected ({code}), parked: {message}")
                });
                (code, message)
            }
            None => {
                self.inner.client.log.error(&what, || {
                    format!("{event_type} at {position} failed {attempts} times, parked: {err}")
                });
                ("handler_error".to_owned(), "handler failed".to_owned())
            }
        };
        Ok(Parked {
            position,
            code,
            message,
            attempts,
        })
    }

    async fn answer(&self, q: proto::QueryRequest) -> query_answer::Outcome {
        let handler = self
            .def()
            .queries
            .read()
            .unwrap()
            .get(&format!("{}.{}", q.domain, q.query_type))
            .cloned();
        let Some(handler) = handler else {
            let reason = Rejected::new(format!("no handler for {}.{}", q.domain, q.query_type))
                .code("unknown_query");
            return query_answer::Outcome::Rejected(reason.to_wire());
        };
        let qry = QueryMeta {
            query_id: q.query_id,
            query_type: q.query_type,
            domain: q.domain,
            tenant: q.tenant_id,
        };
        let ctx = QueryContext {
            request: RequestContext::from_wire(q.context.as_ref()),
            store: self.opened_store(),
        };
        match handler(decode(&q.payload), qry, ctx).await {
            // `None` or `()` is answered as `null`: only commands and events send `{}` for no
            // data.
            Ok(answer) => query_answer::Outcome::Response(
                serde_json::to_vec(&answer).expect("a JSON value encodes"),
            ),
            Err(e) => query_answer::Outcome::Rejected(Rejected::of(&e, "handler_error").to_wire()),
        }
    }

    async fn verdict(&self, call: proto::MiddlewareCall) -> Option<proto::Rejected> {
        let when = match call.when() {
            proto::When::Before => When::Before,
            proto::When::After => When::After,
        };
        let checker = self
            .def()
            .middlewares
            .read()
            .unwrap()
            .get(&when)
            .map(|(_, h)| Arc::clone(h));
        let Some(checker) = checker else {
            return Some(
                Rejected::new(format!("no middleware {when}"))
                    .code("not_a_middleware")
                    .to_wire(),
            );
        };
        // After calls carry no vault values.
        let outcome = match (when, call.outcome) {
            (When::Before, _) => None,
            (When::After, Some(outcome)) => Some(check_outcome(outcome)),
            (When::After, None) => {
                return Some(
                    Rejected::new("no outcome to check")
                        .code("middleware_error")
                        .to_wire(),
                );
            }
        };
        let vault = call.vault;
        let store = self.opened_store();
        let check = match call.request {
            Some(middleware_call::Request::Command(x)) => Check {
                kind: CheckKind::Command,
                id: x.command_id,
                tenant: x.tenant_id,
                target: Some(Target {
                    aggregate_type: format!("{}.{}", x.domain, x.aggregate_type),
                    aggregate_id: x.aggregate_id.clone(),
                }),
                domain: x.domain,
                type_name: x.command_type,
                aggregate_type: Some(x.aggregate_type),
                aggregate_id: Some(x.aggregate_id),
                key: None,
                data: vault::unseal(decode(&x.payload), &vault).0,
                request: RequestContext::from_wire(x.context.as_ref()),
                outcome,
                store: store.clone(),
            },
            Some(middleware_call::Request::Query(q)) => Check {
                kind: CheckKind::Query,
                id: q.query_id,
                tenant: q.tenant_id,
                domain: q.domain,
                type_name: q.query_type,
                target: q
                    .target
                    .filter(|t| !t.aggregate_id.is_empty())
                    .map(|t| Target {
                        aggregate_type: t.aggregate_type,
                        aggregate_id: t.aggregate_id,
                    }),
                aggregate_type: None,
                aggregate_id: None,
                key: None,
                data: decode(&q.payload),
                request: RequestContext::from_wire(q.context.as_ref()),
                outcome,
                store: store.clone(),
            },
            Some(middleware_call::Request::Data(d)) => Check {
                kind: CheckKind::Data,
                type_name: type_within(&call.r#type, &d.domain),
                id: call.call_id,
                tenant: d.tenant_id,
                domain: d.domain,
                aggregate_type: None,
                aggregate_id: None,
                target: None,
                key: Some(d.key),
                data: Value::Null,
                request: RequestContext::from_wire(d.context.as_ref()),
                outcome,
                store: store.clone(),
            },
            Some(middleware_call::Request::Cache(c)) => Check {
                kind: CheckKind::Cache,
                type_name: type_within(&call.r#type, &c.domain),
                id: call.call_id,
                tenant: c.tenant_id,
                domain: c.domain,
                aggregate_type: None,
                aggregate_id: None,
                target: None,
                key: Some(c.key),
                data: Value::Null,
                request: RequestContext::from_wire(c.context.as_ref()),
                outcome,
                store: store.clone(),
            },
            None => {
                return Some(
                    Rejected::new("nothing to check")
                        .code("middleware_error")
                        .to_wire(),
                );
            }
        };
        match checker(check).await {
            Ok(()) => None,
            Err(e) => Some(Rejected::of(&e, "middleware_error").to_wire()),
        }
    }
}

/// The type within its domain of a qualified one: `shop.PutData` → `PutData`.
fn type_within(qualified: &str, domain: &str) -> String {
    qualified
        .strip_prefix(domain)
        .and_then(|t| t.strip_prefix('.'))
        .unwrap_or(qualified)
        .to_owned()
}

/// What came of the request a middleware after it checks.
fn check_outcome(outcome: middleware_call::Outcome) -> CheckOutcome {
    match outcome {
        middleware_call::Outcome::CommandResult(r) => CheckOutcome::Command(match r.outcome {
            Some(proto::command_result::Outcome::Committed(c)) => Outcome::Committed(
                c.events
                    .into_iter()
                    .map(|e| StoredEvent::from_wire(e, &[]))
                    .collect(),
            ),
            Some(proto::command_result::Outcome::Rejected(r)) => {
                Outcome::Rejected(Rejected::from_wire(r))
            }
            // Accepted only queues a command: an after call never carries it.
            _ => Outcome::Rejected(Rejected::new("no outcome")),
        }),
        middleware_call::Outcome::QueryResult(r) => CheckOutcome::Query(match r.outcome {
            Some(query_result::Outcome::Response(data)) => QueryOutcome::Answered(decode(&data)),
            Some(query_result::Outcome::Rejected(r)) => {
                QueryOutcome::Rejected(Rejected::from_wire(r))
            }
            None => QueryOutcome::Rejected(Rejected::new("no outcome")),
        }),
    }
}

/// Where a subscription starts and what it reads, for [`crate::Kernable::subscribe`]:
/// `SubscribeOptions { after: last_seen, types: vec!["shop.*".into()], ..Default::default() }`.
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    /// Global position of the last event already handled; 0: from the beginning.
    pub after: u64,
    /// Qualified event types (`orders.OrderPlaced`) or domains (`orders.*`); empty: all the key
    /// may read.
    pub types: Vec<String>,
    /// Delay before reconnecting after the stream drops or the handler fails (default 1 s);
    /// twice as long after each further failed attempt, at most 30 s.
    pub reconnect: Duration,
}

impl Default for SubscribeOptions {
    fn default() -> SubscribeOptions {
        SubscribeOptions {
            after: 0,
            types: Vec::new(),
            reconnect: Duration::from_secs(1),
        }
    }
}

/// A running subscription; see [`crate::Kernable::subscribe`]. Dropping it ends the
/// subscription too; [`Subscription::stop`] also waits until it has ended.
pub struct Subscription {
    position: Arc<AtomicU64>,
    stopped: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Subscription {
    pub(crate) fn start<F, Fut>(
        client: Client,
        options: SubscribeOptions,
        handler: F,
    ) -> Subscription
    where
        F: Fn(StoredEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let position = Arc::new(AtomicU64::new(options.after));
        let stopped = watch::Sender::new(false);
        let task = tokio::spawn(subscriptions(
            client,
            options,
            handler,
            Arc::clone(&position),
            stopped.subscribe(),
        ));
        Subscription {
            position,
            stopped,
            task,
        }
    }

    /// Global position of the last handled event.
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::SeqCst)
    }

    /// Ends the subscription and waits until it has.
    pub async fn stop(self) {
        self.stopped.send_replace(true);
        let _ = self.task.await;
    }
}

/// With partitions: a stream per partition, each from the start (`after` names a
/// position of one kernel); the handler takes one event at a time, in order within each
/// partition. Without: one stream, as before.
async fn subscriptions<F, Fut>(
    client: Client,
    options: SubscribeOptions,
    handler: F,
    position: Arc<AtomicU64>,
    mut stopped: watch::Receiver<bool>,
) where
    F: Fn(StoredEvent) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    let Some(parts) =
        crate::partitions_of(&client, "subscription", options.reconnect, &mut stopped).await
    else {
        return;
    };
    if parts.len() == 1 && parts[0].0.is_empty() {
        return subscription(client, options, handler, position, stopped).await;
    }
    let handler = Arc::new(handler);
    let turns = Arc::new(tokio::sync::Mutex::new(()));
    let running: Vec<_> = parts
        .into_iter()
        .map(|(_, part)| {
            let (handler, turns) = (Arc::clone(&handler), Arc::clone(&turns));
            let in_turn = move |e: StoredEvent| {
                let (handler, turns) = (Arc::clone(&handler), Arc::clone(&turns));
                async move {
                    let _turn = turns.lock().await;
                    handler(e).await
                }
            };
            let options = SubscribeOptions {
                after: 0,
                ..options.clone()
            };
            let at = Arc::new(AtomicU64::new(0));
            tokio::spawn(subscription(part, options, in_turn, at, stopped.clone()))
        })
        .collect();
    for run in running {
        let _ = run.await;
    }
}

async fn subscription<F, Fut>(
    client: Client,
    options: SubscribeOptions,
    handler: F,
    position: Arc<AtomicU64>,
    mut stopped: watch::Receiver<bool>,
) where
    F: Fn(StoredEvent) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    let read = async |position: &AtomicU64| -> Result<(), BoxError> {
        let after = position.load(Ordering::SeqCst);
        let issued_at = issued_now();
        let sig = client.sign(&signing::subscribe_message(
            after,
            &options.types,
            issued_at,
        ));
        let request = SubscribeRequest {
            after_position: after,
            event_types: options.types.clone(),
            issued_at,
            client_sig: Some(sig),
            protocol: PROTOCOL,
        };
        let (at, channel) = client.channel();
        let ended = |status: tonic::Status| {
            client.ended(at, &status);
            status
        };
        let mut stream = EventServiceClient::new(channel)
            .subscribe(client.request(request))
            .await
            .map_err(ended)?
            .into_inner();
        while let Some(e) = stream.message().await.map_err(ended)? {
            let at = e.global_position;
            handler(StoredEvent::from_wire(e, &[])).await?;
            position.store(at, Ordering::SeqCst);
        }
        Ok(())
    };
    let mut attempt = 0;
    while !*stopped.borrow() {
        let from = position.load(Ordering::SeqCst);
        let result = tokio::select! {
            result = read(&position) => result,
            _ = stopped.wait_for(|s| *s) => return,
        };
        attempt = if position.load(Ordering::SeqCst) > from {
            1
        } else {
            attempt + 1
        };
        let delay = log::reconnecting(
            &client.log,
            "subscription",
            result.err().as_ref(),
            options.reconnect,
            attempt,
        );
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = stopped.wait_for(|s| *s) => return,
        }
    }
}

/// `f`, with a panic in it as an error: a handler that panics fails its batch, query or check
/// instead of ending the listener, or an app's process.
async fn unwinding<T>(f: impl Future<Output = T>) -> Result<T, String> {
    let mut f = std::pin::pin!(f);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(v)) => std::task::Poll::Ready(Ok(v)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(panic) => std::task::Poll::Ready(Err(crate::domain::panic_text(panic.as_ref()))),
        }
    })
    .await
}

/// Attempts before a reactor parks an event its handler keeps failing on.
pub(crate) const PARK_AFTER: u32 = 5;

/// The rejection a failed handler returned, also one a submit or query inside it got.
fn rejection(err: &BoxError) -> Option<&Rejected> {
    err.downcast_ref::<Rejected>()
        .or_else(|| match err.downcast_ref::<Error>() {
            Some(Error::Rejected(r)) => Some(r),
            _ => None,
        })
}

/// Whether a failed handler should simply try again: the kernel was unreachable or busy, a
/// submit did not settle in time, or the store failed. Never parks.
fn transient(err: &BoxError) -> bool {
    let busy = |s: &tonic::Status| {
        matches!(
            s.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
                | tonic::Code::Cancelled
                | tonic::Code::Aborted
                | tonic::Code::Internal
        )
    };
    match err.downcast_ref::<Error>() {
        Some(Error::Status(s)) => busy(s),
        Some(Error::NotSettled(_) | Error::Store(_)) => true,
        _ => err.downcast_ref::<tonic::Status>().is_some_and(busy),
    }
}

/// What a listener logs when it still handles `what` after `seconds`: a query or check after 5 s
/// (the kernel's default `--query-timeout-ms` and `--middleware-timeout-ms`, after which the
/// request has failed), a batch after 30 s (everything behind it waits).
fn stuck(what: &str, seconds: u64) -> String {
    format!(
        "still handling {what} after {seconds} s: everything behind it waits; is a handler stuck?"
    )
}
