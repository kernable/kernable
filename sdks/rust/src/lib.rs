//! kernable: write event-sourced business logic in Rust; the kernel does the rest.
//!
//! ```no_run
//! use kernable::{Kernable, NewEvent, Options, Rejected, domain};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Default, Serialize, Deserialize)]
//! struct Cart { items: u32, checked_out: bool }
//!
//! #[derive(Serialize, Deserialize)]
//! struct AddItem { sku: String, qty: u32 }
//!
//! # async fn run() -> Result<(), kernable::BoxError> {
//! let shop_domain = domain("shop");
//! let cart_aggregate = shop_domain.aggregate("cart", Cart::default());
//! cart_aggregate.apply("ItemAdded", |state, data: AddItem, _evt, _ctx| state.items += data.qty);
//! cart_aggregate.command("AddItem", |state, data: AddItem, _cmd, _ctx| {
//!     if state.checked_out {
//!         return Err(Rejected::new("Cart is checked out"));
//!     }
//!     Ok(vec![NewEvent::new("ItemAdded", data)])
//! });
//! Kernable::new("app.key")?.worker(&cart_aggregate).run().await;
//! # Ok(())
//! # }
//! ```

mod app;
mod context;
mod data;
mod domain;
mod error;
mod listener;
mod log;
mod store;
mod vault;
mod worker;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    PROTOCOL,
    proto::{
        self, ApplyRequest, Attribute, Membership, SubmitRequest, Wait as WireWait, command_result,
        command_service_client::CommandServiceClient,
        membership_service_client::MembershipServiceClient, query_result,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tonic::{metadata::MetadataValue, transport::Channel};

pub use app::{App, AppHandle};
pub use context::{RequestContext, Timestamp};
pub use data::{Cache, Data, Files};
pub use domain::{
    Aggregate, ApplyContext, CommandContext, CommandMeta, CommandSchema, Domain, EventMeta,
    EventSchema, FoldEvent, NewEvent, State, domain,
};
pub use error::{BoxError, Error, Rejected};
pub use kernable_core::transport::Tls;
pub use listener::{
    Check, CheckKind, CheckOutcome, Listener, ListenerContext, ListenerHandle, MiddlewareOptions,
    QueryContext, QueryMeta, StoredEventMeta, SubscribeOptions, Subscription, When,
};
pub use store::Store;
pub use worker::Worker;

use context::{now_ms, random_id};
use domain::{decode, encode};

tokio::task_local! {
    /// The name of the listener whose handler is running, for requests sent from it.
    static HANDLING: String;
}

/// The listener whose handler runs this task, if any.
fn handling() -> String {
    HANDLING.try_with(Clone::clone).unwrap_or_default()
}

/// How to reach the kernel and as whom.
#[derive(Clone, Debug)]
pub struct Options {
    url: Option<String>,
    key: Option<PathBuf>,
    grant: Option<PathBuf>,
    tenant: String,
    service: Option<String>,
    context: RequestContext,
    tls: Option<Tls>,
}

/// Every option at its default: the key from `KERNABLE_KEY`, else `app.key` in the current
/// directory; the service from `KERNABLE_SERVICE`, if set; TLS from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set.
impl Default for Options {
    fn default() -> Options {
        Options {
            url: None,
            key: None,
            grant: None,
            tenant: "default".into(),
            service: None,
            context: RequestContext::default(),
            tls: None,
        }
    }
}

impl Options {
    /// Signs with the key in the `.key` file at `key`.
    pub fn new(key: impl AsRef<Path>) -> Options {
        Options {
            key: Some(key.as_ref().to_owned()),
            ..Options::default()
        }
    }

    /// Kernel address: `http://host:port` (`host:port` works too) or, on Unix,
    /// `unix:/path/to.sock`. Default: `KERNABLE_URL`, else `http://127.0.0.1:7233`, where
    /// `kernable start` listens.
    pub fn url(mut self, url: impl Into<String>) -> Options {
        self.url = Some(url.into());
        self
    }

    /// The key's grant (`kernable grant`), sent with every call. Default: `KERNABLE_GRANT`, else
    /// the `.grant` file next to the key file, if there is one.
    pub fn grant(mut self, path: impl AsRef<Path>) -> Options {
        self.grant = Some(path.as_ref().to_owned());
        self
    }

    /// Default tenant for `submit`, `query`, files and the cache (default `default`).
    pub fn tenant(mut self, tenant: impl Into<String>) -> Options {
        self.tenant = tenant.into();
        self
    }

    /// The service this process's workers and listeners belong to. The service's membership
    /// (see [`Kernable::apply`]) decides what the kernel waits for: what it lists counts, the
    /// service's other workers and listeners are dynamic, and only listeners it lists as
    /// middlewares may check requests. Default: `KERNABLE_SERVICE`, else none (everything
    /// registered counts).
    pub fn service(mut self, service: impl Into<String>) -> Options {
        self.service = Some(service.into());
        self
    }

    /// Default request context, e.g. this service's identity; a request's own context replaces it.
    pub fn context(mut self, context: RequestContext) -> Options {
        self.context = context;
        self
    }

    /// Mutual TLS for `host:port` URLs: the CA certificate and this process's certificate and key.
    /// Default: `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set.
    pub fn tls(mut self, tls: Tls) -> Options {
        self.tls = Some(tls);
        self
    }
}

/// The options for the key file at a path, with every other option at its default.
impl From<&str> for Options {
    fn from(key: &str) -> Options {
        Options::new(key)
    }
}

impl From<String> for Options {
    fn from(key: String) -> Options {
        Options::new(key)
    }
}

impl From<&Path> for Options {
    fn from(key: &Path) -> Options {
        Options::new(key)
    }
}

impl From<PathBuf> for Options {
    fn from(key: PathBuf) -> Options {
        Options::new(key)
    }
}

/// Channels to the kernel, this process's key and its grant: what every part needs to call.
#[derive(Clone)]
pub(crate) struct Client {
    channel: Channel,
    key: Arc<SigningKey>,
    key_id: String,
    grant: Option<MetadataValue<tonic::metadata::Ascii>>,
    log: log::Log,
}

impl Client {
    /// `message` as a request that carries the grant.
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if let Some(grant) = &self.grant {
            request
                .metadata_mut()
                .insert("kernable-grant", grant.clone());
        }
        request
    }

    fn sign(&self, message: &[u8]) -> proto::Signature {
        signing::sign(&self.key, message)
    }
}

/// A connection to the kernel: submits commands, asks queries, and runs workers and listeners.
/// Cloning gives another handle to the same connection.
#[derive(Clone)]
pub struct Kernable {
    client: Client,
    tenant: String,
    context: RequestContext,
    domains: Arc<RwLock<HashMap<String, Domain>>>,
    /// The service of this process's workers and listeners; empty if none.
    service: String,
}

impl Kernable {
    /// A client for the kernel `options` names, or with only a key file's path, for the default
    /// kernel; `Options::default()` takes everything from the environment. It connects on first
    /// use and again after the connection drops. Needs a tokio runtime.
    pub fn new(options: impl Into<Options>) -> Result<Kernable, Error> {
        let options = options.into();
        let url = options
            .url
            .or_else(|| std::env::var("KERNABLE_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:7233".into());
        let key_file = options
            .key
            .or_else(|| std::env::var_os("KERNABLE_KEY").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("app.key"));
        let key = signing::read_signing_key(&key_file)
            .map_err(|e| Error::Config(format!("key {}: {e}", key_file.display())))?;
        let explicit = options
            .grant
            .clone()
            .or_else(|| std::env::var_os("KERNABLE_GRANT").map(PathBuf::from));
        let grant_file = explicit
            .clone()
            .unwrap_or_else(|| key_file.with_extension("grant"));
        let grant =
            if explicit.is_some() || grant_file.exists() {
                let hex = std::fs::read_to_string(&grant_file)
                    .map_err(|e| Error::Config(format!("grant {}: {e}", grant_file.display())))?;
                Some(hex.trim().parse().map_err(|_| {
                    Error::Config(format!("grant {} is not hex", grant_file.display()))
                })?)
            } else {
                None
            };
        let tls = match options.tls {
            Some(tls) => Some(tls),
            None => tls_from_env()?,
        };
        let channel = transport::connect_lazy(&url, tls.as_ref())
            .map_err(|e| Error::Config(format!("kernel {url}: {e}")))?;
        let service = options
            .service
            .or_else(|| std::env::var("KERNABLE_SERVICE").ok())
            .unwrap_or_default();
        let key_id = signing::key_id(&key.verifying_key());
        let log = log::Log::new(&url, &key_file, &key_id);
        Ok(Kernable {
            client: Client {
                channel,
                key_id,
                key: Arc::new(key),
                grant,
                log,
            },
            tenant: options.tenant,
            context: options.context,
            domains: Arc::default(),
            service,
        })
    }

    /// Seals the vault fields of this domain's commands when submitting them; see
    /// [`Domain::command`].
    pub fn with_domain(&self, domain: &Domain) -> Kernable {
        self.domains
            .write()
            .unwrap()
            .insert(domain.name().to_owned(), domain.clone());
        self.clone()
    }

    /// A worker that runs the commands of `aggregate`; add more with [`Worker::aggregate`]. It
    /// belongs to this connection's service (see [`Options::service`]).
    pub fn worker<S: State>(&self, aggregate: &Aggregate<S>) -> Worker {
        Worker::new(self.client.clone(), self.service.clone()).aggregate(aggregate)
    }

    /// Runs `listener` (a read model or reactor of a domain) on this connection: the handle
    /// processes its events with `run()`. Each call gives a new handle. The listener belongs to
    /// this connection's service (see [`Options::service`]).
    pub fn listener(&self, listener: &Listener) -> ListenerHandle {
        ListenerHandle::new(self.client.clone(), listener, self.service.clone())
    }

    /// Files of the default tenant: bytes up to 16 MiB per key, kept by the kernel outside the
    /// signed event log; see [`Files`].
    pub fn data(&self) -> Files {
        Files::new(self.client.clone(), self.tenant.clone())
    }

    /// The kernel's cache for the default tenant: values up to 1 MiB per key, optionally with a
    /// TTL, lost when the kernel restarts; see [`Cache`].
    pub fn cache(&self) -> Cache {
        Cache::new(self.client.clone(), self.tenant.clone())
    }

    /// Replaces the membership of a service (default: this connection's, see
    /// [`Options::service`]): the workers, listeners and middlewares the kernel waits for, plus
    /// free metadata. Apply it once per deploy (e.g. from a deploy script), not from every
    /// process: the latest apply wins. The key needs the `manage` grant for the service. A
    /// middleware the new membership no longer lists stops checking.
    pub async fn apply(&self, manifest: Manifest) -> Result<(), Error> {
        let service = manifest.service.unwrap_or_else(|| self.service.clone());
        if service.is_empty() {
            return Err(Error::Config(
                "apply needs a service: set it with Manifest::service or Options::service".into(),
            ));
        }
        let membership = Membership {
            service,
            workers: manifest.workers,
            listeners: manifest.listeners,
            middlewares: manifest.middlewares,
            metadata: manifest
                .metadata
                .into_iter()
                .map(|(key, value)| Attribute { key, value })
                .collect(),
            issued_at: now_ms(),
        };
        let sig = self.client.sign(&signing::membership_message(&membership));
        let request = ApplyRequest {
            membership: Some(membership),
            sig: Some(sig),
            protocol: PROTOCOL,
        };
        let mut memberships = MembershipServiceClient::new(self.client.channel.clone());
        memberships.apply(self.client.request(request)).await?;
        Ok(())
    }

    /// An app made of whole domains: [`AppHandle::run`] applies the service's membership (the
    /// domains' aggregates as workers, their read models and reactors as listeners, those that
    /// check requests as middlewares; live ones are not listed, and the metadata is `version`
    /// and `description`, when given, then the app's other metadata), then runs one worker for
    /// all the aggregates and every listener, live ones too, all in the app's service.
    ///
    /// One service is one app: two processes with the same service would replace each other's
    /// membership. [`Error::Config`] if neither the app nor the connection names a service.
    pub fn app(&self, app: App) -> Result<AppHandle, Error> {
        AppHandle::new(self, app)
    }

    /// Runs a command and returns its outcome; see [`Submit::wait`].
    pub async fn submit(&self, submit: Submit) -> Result<Outcome, Error> {
        let wait = submit.wait;
        let res = self.send(submit, wait.wire()).await?;
        match res.outcome {
            Some(command_result::Outcome::Rejected(r)) => {
                Ok(Outcome::Rejected(Rejected::from_wire(r)))
            }
            Some(command_result::Outcome::Committed(c)) => {
                // Events come back with references only: the submitter is no vault reader.
                let events: Vec<StoredEvent> = c
                    .events
                    .into_iter()
                    .map(|e| StoredEvent::from_wire(e, &[]))
                    .collect();
                if wait == Wait::Settled && !c.settled {
                    return Err(Error::NotSettled(events));
                }
                Ok(Outcome::Committed(events))
            }
            _ => Err(Error::Status(tonic::Status::internal("no outcome"))),
        }
    }

    /// Stores the command in the kernel's queue and returns its ID without waiting for it to
    /// run. Submit it again with the same `id` to get the outcome.
    pub async fn enqueue(&self, submit: Submit) -> Result<String, Error> {
        Ok(self.send(submit, WireWait::Accepted).await?.command_id)
    }

    async fn send(&self, s: Submit, wait: WireWait) -> Result<proto::CommandResult, Error> {
        let command_id = s.id.unwrap_or_else(random_id);
        let vault = self
            .domains
            .read()
            .unwrap()
            .get(&s.domain)
            .and_then(|d| d.command_vault(&s.command_type));
        let sealed =
            vault::seal(&s.data, vault.as_ref(), &s.aggregate_id).map_err(Error::Config)?;
        let mut context = s.context.unwrap_or_else(|| self.context.clone());
        if context.correlation_id.is_empty() {
            context.correlation_id = command_id.clone();
        }
        let mut cmd = proto::Command {
            command_id,
            tenant_id: s.tenant.unwrap_or_else(|| self.tenant.clone()),
            domain: s.domain,
            aggregate_type: s.aggregate_type,
            aggregate_id: s.aggregate_id,
            command_type: s.command_type,
            payload: sealed.payload,
            client_sig: None,
            context: Some(context.to_wire()),
            vault_refs: sealed.refs,
        };
        cmd.client_sig = Some(self.client.sign(&signing::command_message(&cmd)));
        let request = SubmitRequest {
            command: Some(cmd),
            wait: wait.into(),
            protocol: PROTOCOL,
            vault: sealed.values,
            listener: handling(),
        };
        let mut commands = CommandServiceClient::new(self.client.channel.clone());
        Ok(commands
            .submit(self.client.request(request))
            .await?
            .into_inner())
    }

    /// Asks the read model that answers `domain.type` and returns its answer. Queries always go
    /// through the kernel, also within one process. A refusal is [`Error::Rejected`].
    pub async fn query<T: DeserializeOwned>(&self, query: Query) -> Result<T, Error> {
        let mut request = proto::QueryRequest {
            query_id: random_id(),
            tenant_id: query.tenant.unwrap_or_else(|| self.tenant.clone()),
            domain: query.domain,
            query_type: query.query_type,
            payload: encode(&query.data),
            context: Some(
                query
                    .context
                    .unwrap_or_else(|| self.context.clone())
                    .to_wire(),
            ),
            min_position: query.min_position,
            issued_at: now_ms(),
            client_sig: None,
            protocol: PROTOCOL,
            listener: String::new(),
        };
        request.client_sig = Some(self.client.sign(&signing::query_message(&request)));
        request.listener = handling();
        let mut queries = QueryServiceClient::new(self.client.channel.clone());
        let res = queries
            .query(self.client.request(request))
            .await?
            .into_inner();
        match res.outcome {
            Some(query_result::Outcome::Response(r)) => Ok(serde_json::from_value(decode(&r))?),
            Some(query_result::Outcome::Rejected(r)) => {
                Err(Error::Rejected(Rejected::from_wire(r)))
            }
            None => Err(Error::Status(tonic::Status::internal("no outcome"))),
        }
    }

    /// Streams committed events after `options.after` (backlog first, then live) to `handler`,
    /// one at a time. Reconnects and resumes after the last handled event; if the handler
    /// fails, that event is delivered again. Persist the position yourself to resume across
    /// restarts.
    pub fn subscribe<F, Fut>(&self, options: SubscribeOptions, handler: F) -> Subscription
    where
        F: Fn(StoredEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        Subscription::start(self.client.clone(), options, handler)
    }
}

/// How long `submit` waits before it responds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Wait {
    /// Until every listener has processed the events, so read models already show them
    /// (default); [`Error::NotSettled`] on timeout. From a listener's handler, only the
    /// listeners with a lower order count.
    #[default]
    Settled,
    /// Until the events are stored.
    Committed,
}

impl Wait {
    fn wire(self) -> WireWait {
        match self {
            Wait::Settled => WireWait::Settled,
            Wait::Committed => WireWait::Committed,
        }
    }
}

/// Mutual TLS from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`: all or none.
fn tls_from_env() -> Result<Option<Tls>, Error> {
    let var = |name| std::env::var_os(name).map(PathBuf::from);
    match (
        var("KERNABLE_TLS_CA"),
        var("KERNABLE_TLS_CERT"),
        var("KERNABLE_TLS_KEY"),
    ) {
        (None, None, None) => Ok(None),
        (Some(ca), Some(cert), Some(key)) => Ok(Some(Tls { ca, cert, key })),
        _ => Err(Error::Config(
            "set all of KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY, or none".into(),
        )),
    }
}

/// What a service consists of, for [`Kernable::apply`]; see `Membership` in kernable.proto.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    service: Option<String>,
    workers: Vec<String>,
    listeners: Vec<String>,
    middlewares: Vec<String>,
    metadata: Vec<(String, String)>,
}

impl Manifest {
    /// An empty manifest for the connection's service.
    pub fn new() -> Manifest {
        Manifest::default()
    }

    /// The service it is for. Default: the connection's ([`Options::service`]).
    pub fn service(mut self, service: impl Into<String>) -> Manifest {
        self.service = Some(service.into());
        self
    }

    /// An aggregate the service runs: the kernel waits for a worker of it and accepts its
    /// commands even before a worker registered it.
    pub fn worker<S: State>(mut self, aggregate: &Aggregate<S>) -> Manifest {
        self.workers.push(format!(
            "{}.{}",
            aggregate.domain().name(),
            aggregate.name()
        ));
        self
    }

    /// A listener the kernel waits for; the service's other listeners count for nobody.
    pub fn listener(mut self, listener: &Listener) -> Manifest {
        self.listeners.push(listener.name().to_owned());
        self
    }

    /// A listener of the service that may check requests; it counts like a listener.
    pub fn middleware(mut self, listener: &Listener) -> Manifest {
        self.middlewares.push(listener.name().to_owned());
        self
    }

    /// Free metadata, e.g. `("version", "1.0.0")`; kept in the order given.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Manifest {
        self.metadata.push((key.into(), value.into()));
        self
    }
}

/// A command to submit.
#[derive(Clone, Debug)]
pub struct Submit {
    domain: String,
    aggregate_type: String,
    aggregate_id: String,
    command_type: String,
    data: Value,
    tenant: Option<String>,
    id: Option<String>,
    context: Option<RequestContext>,
    wait: Wait,
}

impl Submit {
    /// A `command_type` command of `domain` for the aggregate `aggregate_type` with ID
    /// `aggregate_id`.
    pub fn new(
        domain: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        command_type: &str,
    ) -> Submit {
        Submit {
            domain: domain.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            command_type: command_type.to_owned(),
            data: Value::Null,
            tenant: None,
            id: None,
            context: None,
            wait: Wait::Settled,
        }
    }

    /// The command's data.
    ///
    /// # Panics
    ///
    /// If `data` cannot be represented as JSON.
    pub fn data(mut self, data: impl Serialize) -> Submit {
        self.data = serde_json::to_value(data).expect("command data is JSON");
        self
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Submit {
        self.tenant = Some(tenant.into());
        self
    }

    /// Idempotency key; resubmitting the same ID returns the original outcome.
    pub fn id(mut self, id: impl Into<String>) -> Submit {
        self.id = Some(id.into());
        self
    }

    /// Replaces the client's default context; inside a listener, pass `ctx.follow()`.
    pub fn context(mut self, context: RequestContext) -> Submit {
        self.context = Some(context);
        self
    }

    pub fn wait(mut self, wait: Wait) -> Submit {
        self.wait = wait;
        self
    }
}

/// A query to ask.
#[derive(Clone, Debug)]
pub struct Query {
    domain: String,
    query_type: String,
    data: Value,
    tenant: Option<String>,
    context: Option<RequestContext>,
    min_position: u64,
}

impl Query {
    /// A `query_type` query of `domain`.
    pub fn new(domain: &str, query_type: &str) -> Query {
        Query {
            domain: domain.to_owned(),
            query_type: query_type.to_owned(),
            data: Value::Null,
            tenant: None,
            context: None,
            min_position: 0,
        }
    }

    /// The query's data.
    ///
    /// # Panics
    ///
    /// If `data` cannot be represented as JSON.
    pub fn data(mut self, data: impl Serialize) -> Query {
        self.data = serde_json::to_value(data).expect("query data is JSON");
        self
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Query {
        self.tenant = Some(tenant.into());
        self
    }

    /// Replaces the client's default context.
    pub fn context(mut self, context: RequestContext) -> Query {
        self.context = Some(context);
        self
    }

    /// Answer only after the read model has processed this global position (e.g. of a
    /// committed submit).
    pub fn min_position(mut self, position: u64) -> Query {
        self.min_position = position;
        self
    }
}

/// What became of a command.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Committed(Vec<StoredEvent>),
    Rejected(Rejected),
}

impl Outcome {
    pub fn is_committed(&self) -> bool {
        matches!(self, Outcome::Committed(_))
    }

    /// The committed events; none if the command was rejected.
    pub fn events(&self) -> &[StoredEvent] {
        match self {
            Outcome::Committed(events) => events,
            Outcome::Rejected(_) => &[],
        }
    }
}

/// What became of a query: its response data or why it was rejected.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryOutcome {
    Answered(Value),
    Rejected(Rejected),
}

impl QueryOutcome {
    pub fn is_answered(&self) -> bool {
        matches!(self, QueryOutcome::Answered(_))
    }
}

/// An event as stored by the kernel.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub position: u64,
    pub tenant: String,
    /// Always the domain of the command that caused the event.
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
    pub event_type: String,
    /// Version of the payload format. Listeners get `data` upcast to the version they know
    /// (see [`Domain::event`]), unless the handler is raw.
    pub schema: u32,
    pub data: Value,
    /// Fields kept in the vault whose value is not here (`null` in `data`): erased, or not
    /// readable with this key (granted `vault_read`).
    pub redacted: Vec<String>,
    pub command_id: String,
    /// The request context of the command that caused the event.
    pub request: RequestContext,
}

impl StoredEvent {
    /// `data` as a `D`.
    pub fn parse<D: DeserializeOwned>(&self) -> Result<D, serde_json::Error> {
        serde_json::from_value(self.data.clone())
    }

    pub(crate) fn from_wire(e: proto::Event, vault: &[proto::VaultValue]) -> StoredEvent {
        let (data, redacted) = vault::unseal(decode(&e.payload), vault);
        StoredEvent {
            position: e.global_position,
            tenant: e.tenant_id,
            domain: e.domain,
            aggregate_type: e.aggregate_type,
            aggregate_id: e.aggregate_id,
            version: e.version,
            event_type: e.event_type,
            schema: e.schema,
            data,
            redacted,
            command_id: e.command_id,
            request: RequestContext::from_wire(e.context.as_ref()),
        }
    }
}
