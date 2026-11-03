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
mod index;
mod listener;
mod log;
mod store;
mod vault;
mod worker;

use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
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
pub use index::{Index, Reindex, Requirement, Unique, UniqueKey, reindex_type};
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

/// How to reach the kernel and as whom: the options of [`Kernable::new`], set with builder
/// methods that each return the changed options.
///
/// `Options::default()` takes everything from the environment, else the defaults: the key from
/// `KERNABLE_KEY`, else `app.key` in the current directory; the kernel from `KERNABLE_URL`,
/// else `http://127.0.0.1:7233`; the grant from `KERNABLE_GRANT`, else the `.grant` file next
/// to the key; the service from `KERNABLE_SERVICE`; TLS from `KERNABLE_TLS_CA`,
/// `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`; the tenant `default`. A key file's path (`&str`,
/// `String`, `&Path`, `PathBuf`) converts into options with only the key set, so
/// `Kernable::new("app.key")` works too. Example:
/// `Kernable::new(Options::new("shop.key").url("http://kernel:7233").service("shop"))?`.
#[derive(Clone, Debug)]
pub struct Options {
    url: Option<String>,
    key: Option<PathBuf>,
    grant: Option<PathBuf>,
    tenant: String,
    service: Option<String>,
    context: RequestContext,
    tls: Option<Tls>,
    insecure: Option<bool>,
    timeout: Duration,
}

/// How long a request waits, for the connection and the answer, unless told otherwise: above
/// the kernel's own submit and settle timeouts.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

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
            insecure: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl Options {
    /// Options that sign with the key in the `.key` file at `key` (made with
    /// `kernable keys new`), every other option at its default.
    pub fn new(key: impl AsRef<Path>) -> Options {
        Options {
            key: Some(key.as_ref().to_owned()),
            ..Options::default()
        }
    }

    /// Kernel address: `http://host:port` (`host:port` works too) or, on Unix,
    /// `unix:/path/to.sock`. For a pair of kernels with a standby, both, comma-separated
    /// (`http://a:7233,http://b:7233`): the client turns to the other one when a kernel answers
    /// `UNAVAILABLE` (a standby says `not_primary`) or cannot be reached. Default:
    /// `KERNABLE_URL`, else `http://127.0.0.1:7233`, where `kernable start` listens.
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

    /// Mutual TLS for `host:port` URLs: the CA certificate and this process's certificate and key,
    /// as paths of PEM files (`Tls { ca, cert, key }`, re-exported from the kernel's crate).
    /// Default: `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set (all three
    /// or none).
    pub fn tls(mut self, tls: Tls) -> Options {
        self.tls = Some(tls);
        self
    }

    /// Allows plain text to a kernel that is not on this machine. Without TLS, grants, signed
    /// requests and vault values cross the network readable, so [`Kernable::new`] refuses such
    /// a URL unless this is set. Default: `KERNABLE_INSECURE=1`, else not allowed.
    pub fn insecure(mut self) -> Options {
        self.insecure = Some(true);
        self
    }

    /// How long a request (`submit`, `query`, files, cache, `apply`) waits in all: for the
    /// connection, e.g. while the kernel restarts, and for its answer. Default 60 s. A
    /// connection the network dropped silently is noticed within 40 s by keepalive pings.
    pub fn timeout(mut self, timeout: Duration) -> Options {
        self.timeout = timeout;
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

/// The channels to the kernels at the URL: one, or the two kernels of a pair. Calls go to the
/// current one; one that answers `UNAVAILABLE` (a standby says `not_primary`) or cannot be
/// reached hands over to the next.
pub(crate) struct Channels {
    all: Vec<Channel>,
    current: std::sync::atomic::AtomicUsize,
}

/// Channels to the kernel, this process's key and its grant: what every part needs to call.
#[derive(Clone)]
pub(crate) struct Client {
    channels: Arc<Channels>,
    key: Arc<SigningKey>,
    key_id: String,
    grant: Option<MetadataValue<tonic::metadata::Ascii>>,
    log: log::Log,
    timeout: Duration,
}

impl Client {
    /// The current channel, with its index for [`Client::ended`].
    pub(crate) fn channel(&self) -> (usize, Channel) {
        let at = self
            .channels
            .current
            .load(std::sync::atomic::Ordering::SeqCst);
        (at, self.channels.all[at].clone())
    }

    /// A call or stream on channel `at` ended with `status`: a kernel that is not the primary,
    /// or cannot be reached, hands over to the next address.
    pub(crate) fn ended(&self, at: usize, status: &tonic::Status) -> bool {
        let n = self.channels.all.len();
        let away = status.code() == tonic::Code::Unavailable || not_connected(status);
        if n > 1 && away {
            let _ = self.channels.current.compare_exchange(
                at,
                (at + 1) % n,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        n > 1 && away
    }

    /// A unary call of `message` (sent with the grant) through `call`. While the kernel cannot
    /// be reached it waits and tries again, but only after failures to connect: the request
    /// never left, so a write cannot run twice. With several addresses, it also tries the next
    /// after `UNAVAILABLE` (a standby's `not_primary`; a resubmit is safe, as commands are
    /// idempotent by ID). Gives up after `timeout` (default: the client's) in all.
    async fn call<T: Clone, R, Fut>(
        &self,
        timeout: Option<Duration>,
        message: T,
        mut call: impl FnMut(Channel, tonic::Request<T>) -> Fut,
    ) -> Result<R, tonic::Status>
    where
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
    {
        let timeout = timeout.unwrap_or(self.timeout);
        let deadline = Instant::now() + timeout;
        let mut pause = Duration::from_millis(50);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let mut request = self.request(message.clone());
            request.set_timeout(left);
            let (at, channel) = self.channel();
            let result = match tokio::time::timeout(left, call(channel, request)).await {
                Ok(result) => result,
                Err(_) => Err(tonic::Status::deadline_exceeded(format!(
                    "no answer from the kernel within {} ms",
                    timeout.as_millis()
                ))),
            };
            match result {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) if Instant::now() + pause < deadline && self.ended(at, &status) => {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(1));
                }
                Err(status) if not_connected(&status) && Instant::now() + pause < deadline => {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(1));
                }
                Err(status) if not_connected(&status) => {
                    return Err(tonic::Status::deadline_exceeded(format!(
                        "no connection to the kernel within {} ms: {}",
                        timeout.as_millis(),
                        status.message()
                    )));
                }
                Err(status) => return Err(status),
            }
        }
    }

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
///
/// The calls to the kernel are `async` and need a Tokio runtime (`#[tokio::main]`). Workers,
/// listeners and apps run until stopped: `run().await` them, or `tokio::spawn` them and wait
/// for `ready()`. Workers and listeners print connection lines to stderr up to the level
/// `KERNABLE_LOG` names (`off`, `error`, `warn`, `info` (default), `debug`). Example:
/// `let kernable = Kernable::new("app.key")?; let outcome =
/// kernable.submit(Submit::new("shop", "cart", "c1", "AddItem").data(json!({ "qty": 2 }))).await?;`
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
    /// kernel; `Options::default()` takes everything from the environment (see [`Options`]). It
    /// connects on first use and again after the connection drops. Needs a Tokio runtime.
    /// [`Error::Config`] if the key, grant or TLS files cannot be read, only some of the TLS
    /// variables are set, or the URL is invalid. Example: `Kernable::new("app.key")?`.
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
        let insecure = options
            .insecure
            .unwrap_or_else(|| std::env::var("KERNABLE_INSECURE").is_ok_and(|v| v == "1"));
        if tls.is_none() && !insecure && !local(&url) {
            return Err(Error::Config(format!(
                "{url} is not on this machine: connect with TLS (Options::tls), or allow plain \
                 text with Options::insecure"
            )));
        }
        // A pair of kernels: several addresses, comma-separated.
        let all = url
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(|u| {
                transport::connect_lazy(u, tls.as_ref())
                    .map_err(|e| Error::Config(format!("kernel {u}: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if all.is_empty() {
            return Err(Error::Config("no kernel address".into()));
        }
        let channels = Arc::new(Channels {
            all,
            current: Default::default(),
        });
        let service = options
            .service
            .or_else(|| std::env::var("KERNABLE_SERVICE").ok())
            .unwrap_or_default();
        let key_id = signing::key_id(&key.verifying_key());
        let log = log::Log::new(&url, &key_file, &key_id);
        // Whoever can read the key file can sign as this process.
        #[cfg(unix)]
        if let Ok(meta) = std::fs::metadata(&key_file) {
            let mode = std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o777;
            if mode & 0o077 != 0 {
                let path = key_file.display();
                log.warn("key", || {
                    format!("{path} can be read by other users (mode {mode:o}): chmod 600 {path}")
                });
            }
        }
        Ok(Kernable {
            client: Client {
                channels,
                key_id,
                key: Arc::new(key),
                grant,
                log,
                timeout: options.timeout,
            },
            tenant: options.tenant,
            context: options.context,
            domains: Arc::default(),
            service,
        })
    }

    /// Seals the vault fields of this domain's commands when submitting them; see
    /// [`Domain::command`]. Returns another handle to this connection, so it chains:
    /// `Kernable::new("app.key")?.with_domain(&shop_domain)`.
    pub fn with_domain(&self, domain: &Domain) -> Kernable {
        self.domains
            .write()
            .unwrap()
            .insert(domain.name().to_owned(), domain.clone());
        self.clone()
    }

    /// A worker that runs the commands of `aggregate`; add more with [`Worker::aggregate`]. It
    /// belongs to this connection's service (see [`Options::service`]). Nothing runs until
    /// [`Worker::run`] is awaited: `kernable.worker(&cart_aggregate).run().await`.
    pub fn worker<S: State>(&self, aggregate: &Aggregate<S>) -> Worker {
        Worker::new(self.client.clone(), self.service.clone()).aggregate(aggregate)
    }

    /// Runs `listener` (a read model or reactor of a domain) on this connection: the handle
    /// processes its events with `run()`. Each call gives a new handle. The listener belongs to
    /// this connection's service (see [`Options::service`]). Example:
    /// `kernable.listener(&carts_readmodel).run().await`.
    pub fn listener(&self, listener: &Listener) -> ListenerHandle {
        ListenerHandle::new(self, listener, self.service.clone())
    }

    /// Files of the default tenant: bytes up to 16 MiB per key, kept by the kernel outside the
    /// signed event log; see [`Files`]. Example:
    /// `kernable.data().put("shop", "invoices/c1.pdf", bytes, "application/pdf").await?`.
    pub fn data(&self) -> Files {
        Files::new(
            self.client.clone(),
            self.tenant.clone(),
            self.context.clone(),
        )
    }

    /// The kernel's cache for the default tenant: values up to 1 MiB per key, optionally with a
    /// TTL, lost when the kernel restarts; see [`Cache`]. Example:
    /// `kernable.cache().set("shop", "rate:c1", "3", Some(Duration::from_secs(60))).await?`.
    pub fn cache(&self) -> Cache {
        Cache::new(
            self.client.clone(),
            self.tenant.clone(),
            self.context.clone(),
        )
    }

    /// Replaces the membership of a service (default: this connection's, see
    /// [`Options::service`]): the workers, listeners and middlewares the kernel waits for, plus
    /// free metadata. Apply it once per deploy (e.g. from a deploy script), not from every
    /// process: the newest apply wins, and an older one that arrives late is refused. The key needs the `manage` grant for the service. A
    /// middleware the new membership no longer lists stops checking. Example:
    /// `kernable.apply(Manifest::new().service("shop").worker(&cart_aggregate).listener(&carts_readmodel)).await?`.
    /// [`Error::Config`] if neither the manifest nor the connection names a service,
    /// [`Error::Status`] if the kernel refuses it.
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
        self.client
            .call(None, request, |channel, r| async move {
                MembershipServiceClient::new(channel).apply(r).await
            })
            .await?;
        Ok(())
    }

    /// An app made of whole domains: [`AppHandle::run`] applies the service's membership (the
    /// domains' aggregates as workers, their read models and reactors as listeners, those that
    /// check requests as middlewares; live ones are not listed, and the metadata is `version`
    /// and `description`, when given, then the app's other metadata), then runs one worker for
    /// all the aggregates and every listener, live ones too, all in the app's service.
    ///
    /// Example: `kernable.app(App::new().service("shop").version("1.0.0").domain(&shop_domain))?.run().await?`.
    /// Every start applies the membership, so the key needs the `manage` grant for the service.
    /// One service is one app: two processes with the same service would replace each other's
    /// membership. [`Error::Config`] if neither the app nor the connection names a service.
    pub fn app(&self, app: App) -> Result<AppHandle, Error> {
        AppHandle::new(self, app)
    }

    /// Runs a command and returns its outcome: [`Outcome::Committed`] with the new events, or
    /// [`Outcome::Rejected`] if a handler or middleware rejected it (a rejection is `Ok`, not
    /// an error). By default it waits until every listener that counts has processed the
    /// events; see [`Submit::wait`]. [`Error::NotSettled`] if they did not in time (the
    /// command is committed), [`Error::Status`] if the kernel refuses the call (no grant,
    /// unknown aggregate type, timeout). Example: `kernable.submit(Submit::new("shop", "cart",
    /// "c1", "AddItem").data(json!({ "sku": "apple", "qty": 2 }))).await?`.
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
    /// run. Submit it again with the same `id` ([`Submit::id`]) to get the outcome; its `wait`
    /// is ignored.
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
        Ok(self
            .client
            .call(s.timeout, request, |channel, r| async move {
                CommandServiceClient::new(channel).submit(r).await
            })
            .await?)
    }

    /// Asks the read model that answers `domain.type` and returns its answer, deserialized as
    /// `T` (`serde_json::Value` for any). Queries always go through the kernel, also within one
    /// process. A refusal by the read model or a middleware is [`Error::Rejected`]; the
    /// kernel's own (no grant, no read model connected) is [`Error::Status`]; an answer that is
    /// not a `T` is [`Error::Json`]. Example: `let cart: Value =
    /// kernable.query(Query::new("shop", "GetCart").data(json!({ "id": "c1" }))).await?;`
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
        let res = self
            .client
            .call(query.timeout, request, |channel, r| async move {
                QueryServiceClient::new(channel).query(r).await
            })
            .await?;
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
    /// restarts. Needs the `subscribe` grant for the types. Unlike a listener, a subscription is
    /// not registered with the kernel: nobody waits for it, and its events carry no vault values
    /// (vault fields are `null` and listed in `redacted`). It runs in a Tokio task from the
    /// call on; [`Subscription::stop`] ends it.
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
    /// Until the events are stored. To read them back, ask with [`Query::min_position`] set to
    /// the last event's position.
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
/// Built with methods that each return the changed manifest:
/// `Manifest::new().service("shop").worker(&cart_aggregate).listener(&carts_readmodel).metadata("version", "1.0.0")`.
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

/// A command to submit with [`Kernable::submit`] or [`Kernable::enqueue`], built with methods
/// that each return the changed command:
/// `Submit::new("shop", "cart", "c1", "AddItem").data(json!({ "qty": 2 })).wait(Wait::Committed)`.
/// Without `data`, the payload is an empty object (`{}`).
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
    timeout: Option<Duration>,
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
            timeout: None,
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

    /// The tenant to submit to. Default: the connection's ([`Options::tenant`]).
    pub fn tenant(mut self, tenant: impl Into<String>) -> Submit {
        self.tenant = Some(tenant.into());
        self
    }

    /// Idempotency key (default: a new UUID); resubmitting the same ID returns the original
    /// outcome without running the command again.
    pub fn id(mut self, id: impl Into<String>) -> Submit {
        self.id = Some(id.into());
        self
    }

    /// Replaces the client's default context; inside a listener, pass `ctx.follow()`.
    pub fn context(mut self, context: RequestContext) -> Submit {
        self.context = Some(context);
        self
    }

    /// How long [`Kernable::submit`] waits: [`Wait::Settled`] (default) or [`Wait::Committed`].
    pub fn wait(mut self, wait: Wait) -> Submit {
        self.wait = wait;
        self
    }

    /// How long to wait for the connection and the outcome; default [`Options::timeout`].
    pub fn timeout(mut self, timeout: Duration) -> Submit {
        self.timeout = Some(timeout);
        self
    }
}

/// A query to ask with [`Kernable::query`], built with methods that each return the changed
/// query: `Query::new("shop", "GetCart").data(json!({ "id": "c1" }))`. Without `data`, the
/// query's data is an empty object (`{}`).
#[derive(Clone, Debug)]
pub struct Query {
    domain: String,
    query_type: String,
    data: Value,
    tenant: Option<String>,
    context: Option<RequestContext>,
    min_position: u64,
    timeout: Option<Duration>,
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
            timeout: None,
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

    /// The tenant to ask in. Default: the connection's ([`Options::tenant`]).
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

    /// How long to wait for the connection and the answer; default [`Options::timeout`].
    pub fn timeout(mut self, timeout: Duration) -> Query {
        self.timeout = Some(timeout);
        self
    }
}

/// What became of a command, returned by [`Kernable::submit`].
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// The command's events are stored, in order. Vault fields in them are `null` (see
    /// [`StoredEvent::redacted`]).
    Committed(Vec<StoredEvent>),
    /// A handler, a middleware or the kernel rejected the command; nothing was stored but the
    /// rejection.
    Rejected(Rejected),
}

impl Outcome {
    /// Whether the command was committed.
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

/// What became of a query, as a middleware that runs after requests gets it
/// ([`CheckOutcome::Query`]): its response data or why it was rejected.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryOutcome {
    /// The read model's answer.
    Answered(Value),
    /// The read model or a middleware rejected the query.
    Rejected(Rejected),
}

impl QueryOutcome {
    /// Whether the read model answered.
    pub fn is_answered(&self) -> bool {
        matches!(self, QueryOutcome::Answered(_))
    }
}

/// An event as stored by the kernel: in an [`Outcome`] and in a [`Kernable::subscribe`]
/// handler.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    /// Global position of the event in the kernel's log.
    pub position: u64,
    pub tenant: String,
    /// Always the domain of the command that caused the event.
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// The aggregate's version after this event: 1 for its first event.
    pub version: u64,
    /// The event type within the domain, e.g. `ItemAdded`.
    pub event_type: String,
    /// Version of the payload format. Listeners get `data` upcast to the version they know
    /// (see [`Domain::event`]), unless the handler is raw.
    pub schema: u32,
    /// The payload; [`StoredEvent::parse`] deserializes it.
    pub data: Value,
    /// Fields kept in the vault whose value is not here (`null` in `data`): erased, or not
    /// readable with this key (granted `vault_read`).
    pub redacted: Vec<String>,
    /// The command that caused the event.
    pub command_id: String,
    /// When the kernel committed the event.
    pub created_at: Timestamp,
    /// The request context of the command that caused the event.
    pub request: RequestContext,
}

impl StoredEvent {
    /// `data` as a `D`, e.g. `let item: ItemAdded = event.parse()?;`.
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
            created_at: Timestamp(e.created_at),
            request: RequestContext::from_wire(e.context.as_ref()),
        }
    }
}

/// Whether `status` says the connection could not be made: the request was not sent.
fn not_connected(status: &tonic::Status) -> bool {
    use std::io::ErrorKind::{AddrNotAvailable, ConnectionRefused, NotFound, TimedOut};
    let mut source = std::error::Error::source(status);
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return matches!(
                io.kind(),
                ConnectionRefused | NotFound | AddrNotAvailable | TimedOut
            );
        }
        if e.to_string().contains("error trying to connect") {
            return true;
        }
        source = e.source();
    }
    false
}

/// Whether `url` (every address of a comma-separated list) is a Unix socket or a loopback
/// address: plain text never leaves the machine.
fn local(url: &str) -> bool {
    if url.contains(',') {
        return url.split(',').all(|u| local(u.trim()));
    }
    if url.starts_with("unix:") {
        return true;
    }
    let host = transport::tcp_addr(url);
    let host = host.rsplit_once(':').map_or(host, |(host, port)| {
        if port.chars().all(|c| c.is_ascii_digit()) {
            host
        } else {
            url
        }
    });
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host == "localhost"
        || host == "::1"
        || host.strip_prefix("127.").is_some_and(|rest| {
            rest.split('.').count() == 3 && rest.split('.').all(|p| p.parse::<u8>().is_ok())
        })
}
