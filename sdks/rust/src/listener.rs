//! Listeners (read models and reactors), their queries and middleware, and subscriptions.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kernable_core::{
    PROTOCOL,
    proto::{
        self, Ack, ChallengeResponse, Hello, ListenerMessage, Middleware, QueryAnswer,
        SubscribeRequest, Verdict, event_service_client::EventServiceClient,
        listener_delivery::Msg as Delivery, listener_message::Msg as Reply,
        listener_service_client::ListenerServiceClient, middleware_call, query_answer,
    },
    signing,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    BoxError, Client, Domain, HANDLING, Rejected, RequestContext, Store, StoredEvent,
    context::now_ms,
    domain::{decode, encode, upcast},
    log, vault,
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type EventHandler =
    Arc<dyn Fn(StoredEvent, ListenerContext) -> BoxFuture<Result<(), BoxError>> + Send + Sync>;
type QueryHandler =
    Arc<dyn Fn(Value, QueryMeta, QueryContext) -> BoxFuture<Result<Value, BoxError>> + Send + Sync>;
type CheckHandler = Arc<dyn Fn(Check) -> BoxFuture<Result<(), BoxError>> + Send + Sync>;

/// What a listener handler gets about an event besides its payload.
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
            schema: e.schema,
            redacted: e.redacted.clone(),
        }
    }
}

/// What a listener handler gets besides the event's payload and meta.
#[derive(Clone)]
pub struct ListenerContext {
    /// The event was committed before the listener connected: it is being replayed.
    pub replaying: bool,
    /// The listener's store, if it has one ([`Listener::store`]).
    pub store: Option<Store>,
    /// The request context of the command that caused the event.
    pub request: RequestContext,
    command_id: String,
}

impl ListenerContext {
    /// A request context for commands caused by this event: same sender and correlation.
    pub fn follow(&self) -> RequestContext {
        self.request.follow(&self.command_id)
    }
}

/// What a query handler gets about the query besides its data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryMeta {
    pub query_id: String,
    /// The query type within the domain.
    pub query_type: String,
    pub domain: String,
    pub tenant: String,
}

/// What a query handler gets besides the query's data and meta.
#[derive(Clone)]
pub struct QueryContext {
    /// Who asks: check permissions here.
    pub request: RequestContext,
    /// The listener's store, if it has one.
    pub store: Option<Store>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckKind {
    Command,
    Query,
}

/// A command or query a middleware checks before the kernel accepts or routes it.
#[derive(Clone, Debug)]
pub struct Check {
    pub kind: CheckKind,
    /// The command or query ID.
    pub id: String,
    pub tenant: String,
    pub domain: String,
    /// The command or query type within the domain.
    pub type_name: String,
    /// Commands only.
    pub aggregate_type: Option<String>,
    pub aggregate_id: Option<String>,
    pub data: Value,
    /// Who sends it: check permissions here.
    pub request: RequestContext,
}

/// What a middleware checks.
#[derive(Clone, Debug)]
pub struct MiddlewareOptions {
    /// Command and query types: `*` (all), a type of the listener's domain (`Place`), of another
    /// domain (`orders.Place`) or a whole domain (`orders.*`).
    pub types: Vec<String>,
    /// Position in the chain: lower runs first, equal orders by listener name (default 0).
    /// Negative orders are reserved for kernable.
    pub order: i32,
}

impl MiddlewareOptions {
    pub fn new(types: &[&str]) -> MiddlewareOptions {
        MiddlewareOptions {
            types: types.iter().map(|t| (*t).to_owned()).collect(),
            order: 0,
        }
    }
}

struct Settings {
    order: i32,
    /// Position to start after on the first connect; `None`: the kernel's stored one.
    start: Option<u64>,
    reconnect: Duration,
    store: Option<String>,
    schema: String,
}

struct ListenerInner {
    domain: Domain,
    name: String,
    settings: Mutex<Settings>,
    /// Qualified event type or `Domain.*` -> handler and whether it is raw.
    handlers: RwLock<HashMap<String, (EventHandler, bool)>>,
    domains: RwLock<HashMap<String, Domain>>,
    /// Qualified query type -> handler.
    queries: RwLock<HashMap<String, QueryHandler>>,
    /// Middleware options with qualified types, and the handler.
    checker: RwLock<Option<(MiddlewareOptions, CheckHandler)>>,
}

/// A durable, named event listener of a domain: a read model ([`Domain::read_model`]) or a
/// reactor ([`Domain::reactor`]). It defines what to handle; [`crate::Kernable::listen`] runs
/// it. Events arrive in order, one at a time, after every listener with a lower order has
/// processed them. Delivery is at least once: if a handler fails, the listener reconnects and
/// the event is delivered again.
///
/// Types without a domain (`ItemAdded`) are the listener's own domain's; other domains' are
/// qualified (`identity.AccountRegistered`). Handlers get event payloads upcast to the versions
/// of the listener's domain and of the domains it is given with `with_domain`. An event with a
/// newer schema than known stops the listener (it retries and logs) until it is updated, so it
/// never misreads data. Cloning gives another handle to the same definition.
#[derive(Clone)]
pub struct Listener {
    inner: Arc<ListenerInner>,
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
    pub(crate) fn new(domain: &Domain, name: &str, order: i32, start: Option<u64>) -> Listener {
        let domains = HashMap::from([(domain.name().to_owned(), domain.clone())]);
        Listener {
            inner: Arc::new(ListenerInner {
                domain: domain.clone(),
                name: name.to_owned(),
                settings: Mutex::new(Settings {
                    order,
                    start,
                    reconnect: Duration::from_secs(1),
                    store: None,
                    schema: "1".into(),
                }),
                handlers: RwLock::default(),
                domains: RwLock::new(domains),
                queries: RwLock::default(),
                checker: RwLock::default(),
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The domain the listener belongs to: types without a domain are its.
    pub fn domain(&self) -> &Domain {
        &self.inner.domain
    }

    /// `t` qualified: a type without a domain is the listener's domain's. `None` if `t` is
    /// neither a type nor qualified (`domain.Type`, or `domain.*` with `wildcard`).
    fn qualify(&self, t: &str, wildcard: bool) -> Option<String> {
        if t.contains('.') {
            qualified(t, wildcard).then(|| t.to_owned())
        } else {
            (!t.is_empty() && !t.contains('*')).then(|| format!("{}.{t}", self.inner.domain.name()))
        }
    }

    /// Listeners with a lower order process each event first; equal orders run in parallel.
    /// Defaults: 0 for read models, 10 for reactors. Negative orders are reserved for kernable.
    pub fn order(&self, order: i32) -> Listener {
        self.inner.settings.lock().unwrap().order = order;
        self.clone()
    }

    /// Position to start after on the first connect: 0 rebuilds from the beginning (default
    /// for read models). Reactors resume after the last acknowledged event unless given one.
    /// Reconnects always resume after the last handled event.
    pub fn start(&self, position: u64) -> Listener {
        self.inner.settings.lock().unwrap().start = Some(position);
        self.clone()
    }

    /// Delay before reconnecting after the stream drops (default 1 s).
    pub fn reconnect(&self, delay: Duration) -> Listener {
        self.inner.settings.lock().unwrap().reconnect = delay;
        self.clone()
    }

    /// Keeps the listener's state in a store (`sqlite:<path>` or `memory:`) instead of
    /// memory: its data and position are written together per batch, and a restart continues
    /// after the stored position. `schema` is the version of the stored data's format: change
    /// it when the handlers store data differently, and the store is cleared and rebuilt.
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
    /// domain (`orders.OrderPlaced`) or a whole domain (`orders.*`). `handler(data, evt, ctx)`
    /// gets the payload as `D` (`serde_json::Value` for any); a payload that is not a `D` fails
    /// the batch, so it is delivered again.
    ///
    /// # Panics
    ///
    /// If `event_type` is none of these.
    pub fn on<D, F, Fut>(&self, event_type: &str, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.handle(event_type, handler, false)
    }

    /// Like [`on`](Self::on), but gets events as stored, without upcasting, whatever their
    /// schema (for archives and exports).
    pub fn on_raw<D, F, Fut>(&self, event_type: &str, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        F: Fn(D, StoredEventMeta, ListenerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.handle(event_type, handler, true)
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
        self.inner
            .handlers
            .write()
            .unwrap()
            .insert(event_type, (handler, raw));
        self.clone()
    }

    /// Answers `query_type` queries: a type of the listener's domain (`GetOrder`) or of another
    /// one (`orders.GetOrder`), from this listener's state: `handler(data, qry, ctx)` gets the
    /// query's data as `D`. The kernel delivers queries in order with the events, so an answer
    /// reflects every event handled before it. Return a [`Rejected`] to refuse. A handler
    /// cannot query its own listener (the answer would wait behind itself): the kernel refuses
    /// it.
    ///
    /// # Panics
    ///
    /// If `query_type` is neither.
    pub fn query<D, R, F, Fut>(&self, query_type: &str, handler: F) -> Listener
    where
        D: DeserializeOwned + Send + 'static,
        R: Serialize,
        F: Fn(D, QueryMeta, QueryContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, BoxError>> + Send + 'static,
    {
        let Some(query_type) = self.qualify(query_type, false) else {
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
        self.inner
            .queries
            .write()
            .unwrap()
            .insert(query_type, query);
        self.clone()
    }

    /// Makes this listener a middleware: the kernel asks `handler` about every command and
    /// query of `options.types` before accepting or routing it, in chain order, and only once
    /// this listener has caught up with the events. Return a [`Rejected`] to refuse; any other
    /// error refuses too. The registration is stored: while this listener is away, the kernel
    /// refuses those requests (`kernable listeners remove` unregisters it).
    ///
    /// # Panics
    ///
    /// If a type is not `*`, a type, a qualified type or a domain.
    pub fn middleware<F, Fut>(&self, options: MiddlewareOptions, handler: F) -> Listener
    where
        F: Fn(Check) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
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
        let options = MiddlewareOptions { types, ..options };
        let handler: CheckHandler = Arc::new(move |c| Box::pin(handler(c)));
        *self.inner.checker.write().unwrap() = Some((options, handler));
        self.clone()
    }
}

struct HandleInner {
    client: Client,
    listener: Listener,
    position: AtomicU64,
    store: Mutex<Option<Store>>,
    stopped: watch::Sender<bool>,
    ready: watch::Sender<bool>,
}

/// A [`Listener`] running on a connection; see [`crate::Kernable::listen`]. Cloning gives
/// another handle to the same running listener.
#[derive(Clone)]
pub struct ListenerHandle {
    inner: Arc<HandleInner>,
}

impl ListenerHandle {
    pub(crate) fn new(client: Client, listener: &Listener) -> ListenerHandle {
        ListenerHandle {
            inner: Arc::new(HandleInner {
                client,
                listener: listener.clone(),
                position: AtomicU64::new(0),
                store: Mutex::default(),
                stopped: watch::Sender::new(false),
                ready: watch::Sender::new(false),
            }),
        }
    }

    /// The listener's definition.
    pub fn listener(&self) -> &Listener {
        &self.inner.listener
    }

    /// Global position of the last handled event (or skipped, for other event types).
    pub fn position(&self) -> u64 {
        self.inner.position.load(Ordering::SeqCst)
    }

    /// The listener's store, once `run()` opened it.
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

    /// Processes events until `stop()`, reconnecting when the stream drops.
    pub async fn run(&self) {
        let mut stopped = self.inner.stopped.subscribe();
        let log = &self.inner.client.log;
        let what = format!("listener {}", self.def().name);
        let (mut start, store) = {
            let s = self.def().settings.lock().unwrap();
            (s.start, s.store.clone().map(|url| (url, s.schema.clone())))
        };
        if let Some((url, schema)) = store
            && self.opened_store().is_none()
        {
            match Store::open(&url, &schema) {
                Ok(store) => {
                    self.inner
                        .position
                        .store(store.committed(), Ordering::SeqCst);
                    start = Some(store.committed());
                    *self.inner.store.lock().unwrap() = Some(store);
                }
                Err(e) => {
                    log.error(&what, || e.to_string());
                    return;
                }
            }
        }
        while !*stopped.borrow() {
            let result = tokio::select! {
                result = self.listen(start) => result,
                _ = stopped.wait_for(|s| *s) => return,
            };
            start = Some(self.position());
            let delay = self.def().settings.lock().unwrap().reconnect;
            let error = log::reason(result.err().as_ref());
            log.warn(&what, || log::connection_lost(&error, delay));
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
        let order = def.settings.lock().unwrap().order;
        let middleware = def
            .checker
            .read()
            .unwrap()
            .as_ref()
            .map(|(o, _)| Middleware {
                order: o.order,
                types: o.types.clone(),
            });
        let event_types: Vec<String> = def.handlers.read().unwrap().keys().cloned().collect();
        let query_types: Vec<String> = def.queries.read().unwrap().keys().cloned().collect();
        let what = format!("listener {}", def.name);
        let checks = middleware.as_ref().map(|m| m.types.clone());
        let hello = Hello {
            name: def.name.clone(),
            key_id: client.key_id.clone(),
            order,
            event_types: event_types.clone(),
            from_position: start,
            query_types: query_types.clone(),
            middleware,
            protocol: PROTOCOL,
        };
        tx.send(msg(Reply::Hello(hello))).await?;
        let mut service = ListenerServiceClient::new(client.channel.clone());
        let mut inbound = service
            .listen(client.request(ReceiverStream::new(rx)))
            .await?
            .into_inner();
        let mut replay_up_to = 0;
        while let Some(m) = inbound.message().await? {
            // Requests sent from the handlers name this listener, so the kernel never makes
            // them wait for it (it handles one message at a time).
            let reply = match m.msg {
                Some(Delivery::Challenge(c)) => Reply::ChallengeResponse(ChallengeResponse {
                    sig: client.sign(&signing::challenge_message(&c.nonce)).sig,
                }),
                Some(Delivery::Ready(r)) => {
                    inner.position.store(r.after_position, Ordering::SeqCst);
                    replay_up_to = r.head_position;
                    client.log.info(&what, || {
                        log::listener_connected(
                            &client.log.connected(&client.key_id),
                            r.after_position,
                            &event_types,
                            &query_types,
                            checks.as_deref(),
                        )
                    });
                    inner.ready.send_replace(true);
                    continue;
                }
                Some(Delivery::Batch(batch)) => {
                    let up_to = batch.up_to;
                    let store = self.opened_store();
                    // With a store, the batch's data and position are committed together,
                    // before the ack.
                    if let Some(s) = &store {
                        s.begin()?;
                    }
                    let handled = HANDLING
                        .scope(
                            def.name.clone(),
                            self.handle_batch(batch, replay_up_to, store.clone()),
                        )
                        .await;
                    if let Err(e) = handled {
                        if let Some(s) = &store {
                            s.rollback();
                            inner.position.store(s.committed(), Ordering::SeqCst);
                        }
                        return Err(e);
                    }
                    if let Some(s) = &store {
                        s.commit(up_to)?;
                    }
                    inner.position.store(up_to, Ordering::SeqCst);
                    Reply::Ack(Ack { position: up_to })
                }
                Some(Delivery::Query(q)) => {
                    let query = q.query.unwrap_or_default();
                    let query_type = format!("{}.{}", query.domain, query.query_type);
                    let answer = HANDLING.scope(def.name.clone(), self.answer(query)).await;
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
                    let rejected = HANDLING
                        .scope(def.name.clone(), self.verdict(c.request, c.vault))
                        .await;
                    Reply::Verdict(Verdict {
                        call_id: c.call_id,
                        rejected,
                    })
                }
                None => continue,
            };
            tx.send(msg(reply)).await?;
        }
        Ok(())
    }

    async fn handle_batch(
        &self,
        batch: proto::Batch,
        replay_up_to: u64,
        store: Option<Store>,
    ) -> Result<(), BoxError> {
        let def = self.def();
        for e in batch.events {
            let position = e.global_position;
            let entry = {
                let handlers = def.handlers.read().unwrap();
                handlers
                    .get(&format!("{}.{}", e.domain, e.event_type))
                    .or_else(|| handlers.get(&format!("{}.*", e.domain)))
                    .cloned()
            };
            if let Some((handler, raw)) = entry {
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
                };
                let event_type = format!("{}.{}", event.domain, event.event_type);
                handler(event, ctx).await?;
                self.inner
                    .client
                    .log
                    .debug(&format!("listener {}", def.name), || {
                        log::event_processed(&event_type, position)
                    });
            }
            self.inner.position.store(position, Ordering::SeqCst);
        }
        Ok(())
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
            Ok(answer) => query_answer::Outcome::Response(encode(&answer)),
            Err(e) => query_answer::Outcome::Rejected(Rejected::of(&e, "handler_error").to_wire()),
        }
    }

    async fn verdict(
        &self,
        request: Option<middleware_call::Request>,
        vault: Vec<proto::VaultValue>,
    ) -> Option<proto::Rejected> {
        let checker = self
            .def()
            .checker
            .read()
            .unwrap()
            .as_ref()
            .map(|(_, h)| Arc::clone(h));
        let Some(checker) = checker else {
            return Some(
                Rejected::new("not a middleware")
                    .code("not_a_middleware")
                    .to_wire(),
            );
        };
        let check = match request {
            Some(middleware_call::Request::Command(x)) => Check {
                kind: CheckKind::Command,
                id: x.command_id,
                tenant: x.tenant_id,
                domain: x.domain,
                type_name: x.command_type,
                aggregate_type: Some(x.aggregate_type),
                aggregate_id: Some(x.aggregate_id),
                data: vault::unseal(decode(&x.payload), &vault).0,
                request: RequestContext::from_wire(x.context.as_ref()),
            },
            Some(middleware_call::Request::Query(q)) => Check {
                kind: CheckKind::Query,
                id: q.query_id,
                tenant: q.tenant_id,
                domain: q.domain,
                type_name: q.query_type,
                aggregate_type: None,
                aggregate_id: None,
                data: decode(&q.payload),
                request: RequestContext::from_wire(q.context.as_ref()),
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

/// Where a subscription starts and what it reads.
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    /// Global position of the last event already handled; 0: from the beginning.
    pub after: u64,
    /// Qualified event types (`orders.OrderPlaced`) or domains (`orders.*`); empty: all the key
    /// may read.
    pub types: Vec<String>,
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

/// A running subscription; see [`crate::Kernable::subscribe`].
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
        let task = tokio::spawn(subscription(
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
        let issued_at = now_ms();
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
        let mut events = EventServiceClient::new(client.channel.clone());
        let mut stream = events
            .subscribe(client.request(request))
            .await?
            .into_inner();
        while let Some(e) = stream.message().await? {
            let at = e.global_position;
            handler(StoredEvent::from_wire(e, &[])).await?;
            position.store(at, Ordering::SeqCst);
        }
        Ok(())
    };
    while !*stopped.borrow() {
        let result = tokio::select! {
            result = read(&position) => result,
            _ = stopped.wait_for(|s| *s) => return,
        };
        let error = log::reason(result.err().as_ref());
        client.log.warn("subscription", || {
            log::connection_lost(&error, options.reconnect)
        });
        tokio::select! {
            () = tokio::time::sleep(options.reconnect) => {}
            _ = stopped.wait_for(|s| *s) => return,
        }
    }
}
