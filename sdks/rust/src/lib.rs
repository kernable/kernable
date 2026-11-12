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
mod messages;
pub mod process;
mod store;
mod vault;
mod worker;

use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    PROTOCOL,
    proto::{
        self, ApplyRequest, Attribute, CancelRequest, Membership, ProcessActing,
        ProcessRegistration, SubmitRequest, Wait as WireWait, cancel_result, command_result,
        command_service_client::CommandServiceClient,
        membership_service_client::MembershipServiceClient,
        partition_service_client::PartitionServiceClient,
        process_service_client::ProcessServiceClient, query_result,
        query_service_client::QueryServiceClient,
    },
    signing, transport,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tonic::{metadata::MetadataValue, transport::Channel};

pub use app::{App, AppHandle};
pub use context::{RequestContext, Timestamp};
pub use data::{Cache, Data, Files, Listed, Listing};
pub use domain::{
    Aggregate, ApplyContext, CommandContext, CommandMeta, CommandSchema, Domain, EventMeta,
    EventSchema, FoldEvent, NewEvent, QuerySchema, QueryTarget, State, domain,
};
pub use error::{BoxError, Error, Rejected};
pub use index::{Index, Reindex, Requirement, Unique, UniqueKey, reindex_type};
pub use kernable_core::transport::Tls;
pub use kernable_sdk_macros::{command, event, query};
pub use listener::{
    Check, CheckKind, CheckOutcome, Listener, ListenerContext, ListenerHandle, MiddlewareOptions,
    QueryContext, QueryMeta, StoredEventMeta, SubscribeOptions, Subscription, Target, When,
};
#[doc(hidden)]
pub use messages::__private;
pub use messages::{CommandType, EventType, Message, Of, QueryType, TypeKey};
pub use process::Process;
pub use store::Store;

// The attributes name the crate `::kernable`; within it too (its tests).
extern crate self as kernable;
pub use worker::Worker;

use context::{issued_now, random_id};
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
/// to the key; the service from `KERNABLE_SERVICE`; the deployment from `KERNABLE_DEPLOYMENT`;
/// TLS from `KERNABLE_TLS_CA`,
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
    deployment: Option<String>,
    context: RequestContext,
    tls: Option<Tls>,
    insecure: Option<bool>,
    timeout: Duration,
}

/// How long a request waits, for the connection and the answer, unless told otherwise: above
/// the kernel's own submit and settle timeouts.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Every option at its default: the key from `KERNABLE_KEY`, else `app.key` in the current
/// directory; the service from `KERNABLE_SERVICE`, if set; the deployment from
/// `KERNABLE_DEPLOYMENT`, if set; TLS from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and
/// `KERNABLE_TLS_KEY`, if set.
impl Default for Options {
    fn default() -> Options {
        Options {
            url: None,
            key: None,
            grant: None,
            tenant: "default".into(),
            service: None,
            deployment: None,
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

    /// The deployment of this process: the version of its code, e.g. `2026-11-11.1` or a commit
    /// hash; 1 to 128 letters, digits, `-`, `_` and `.`. Its workers write it into every event
    /// and get commands only while the service's membership names it (or names none); its read
    /// models and middlewares run beside those of other deployments, each from its own
    /// position, and its queries go to its own read models first. [`Kernable::apply`] names it
    /// in the membership, so a rollback is starting the earlier code and applying again.
    /// Default: `KERNABLE_DEPLOYMENT`, else none. An invalid one is [`Error::Config`] in
    /// [`Kernable::new`].
    pub fn deployment(mut self, deployment: impl Into<String>) -> Options {
        self.deployment = Some(deployment.into());
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
    /// A submit (by its command ID), a cancel, a query or a data or cache read (get, list) that
    /// a kernel answers `UNAVAILABLE` (shutting down, handing over, a middleware or read model
    /// not connected) is asked again with backoff within this time; any other request (a
    /// write) may have run, and fails at once.
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
    /// The newest position this client's own commands committed at on these kernels (one
    /// partition): its queries wait for it, on whichever copy answers (read your writes).
    seen: std::sync::atomic::AtomicU64,
}

/// What a client knows of the partitions: the map, read once (and again after a
/// `wrong_partition`), and the channels to each partition.
pub(crate) struct Partitions {
    /// `None`: not read yet; `Some(None)`: the kernel serves every tenant.
    map: tokio::sync::Mutex<Option<Option<proto::PartitionMap>>>,
    channels: std::sync::Mutex<HashMap<String, Arc<Channels>>>,
    tls: Option<Tls>,
}

/// The partitions of `client`, read again (backing off like a reconnect) while no kernel
/// answers; `None` once `stopped`.
pub(crate) async fn partitions_of(
    client: &Client,
    what: &str,
    base: Duration,
    stopped: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<Vec<(String, Client)>> {
    let mut attempt = 0;
    loop {
        let read = tokio::select! {
            read = client.partitions() => read,
            _ = stopped.wait_for(|s| *s) => return None,
        };
        match read {
            Ok(parts) => return Some(parts),
            Err(status) => {
                attempt += 1;
                let error: BoxError = status.into();
                let delay = log::reconnecting(&client.log, what, Some(&error), base, attempt);
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    _ = stopped.wait_for(|s| *s) => return None,
                }
            }
        }
    }
}

/// The partition `map` assigns `tenant` to.
pub(crate) fn partition_of<'a>(map: &'a proto::PartitionMap, tenant: &str) -> &'a str {
    map.tenants
        .iter()
        .find(|t| t.tenant == tenant)
        .map_or(map.default.as_str(), |t| t.partition.as_str())
}

/// Whether a kernel refused a call because its tenant is moving to another partition.
pub(crate) fn moving(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable && status.message().starts_with("moving")
}

/// How long a call waits for its tenant's move to end.
const MOVE_WAIT: Duration = Duration::from_secs(30);

/// Whether a kernel refused a call as one for a tenant of another partition.
pub(crate) fn wrong_partition(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with("wrong_partition")
}

/// Channels to the kernel, this process's key and its grant: what every part needs to call.
#[derive(Clone)]
pub(crate) struct Client {
    channels: Arc<Channels>,
    parts: Arc<Partitions>,
    key: Arc<SigningKey>,
    key_id: String,
    grant: Option<MetadataValue<tonic::metadata::Ascii>>,
    log: log::Log,
    timeout: Duration,
    /// The deployment of this process ([`Options::deployment`]); empty if none.
    deployment: String,
}

impl Client {
    /// The partition map the first kernel holds, read once and again after `refresh`;
    /// `None` if it serves every tenant.
    pub(crate) async fn partition_map(
        &self,
        refresh: bool,
    ) -> Result<Option<proto::PartitionMap>, tonic::Status> {
        let mut held = self.parts.map.lock().await;
        if refresh {
            *held = None;
        }
        if let Some(map) = held.as_ref() {
            return Ok(map.clone());
        }
        let request = proto::MapRequest { protocol: PROTOCOL };
        let map = match self
            .call_with(None, request, true, |channel, r| async move {
                PartitionServiceClient::new(channel).map(r).await
            })
            .await
        {
            Ok(map) => Some(map),
            Err(s) if matches!(s.code(), tonic::Code::NotFound | tonic::Code::Unimplemented) => {
                None
            }
            Err(s) => return Err(s),
        };
        *held = Some(map.clone());
        Ok(map)
    }

    /// This client on partition `name` of `map`.
    fn on_partition(&self, map: &proto::PartitionMap, name: &str) -> Result<Client, tonic::Status> {
        let partition = map
            .partitions
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| {
                tonic::Status::internal(format!("the partition map names no partition {name}"))
            })?;
        let addresses = partition.addresses.join(",");
        let mut cached = self.parts.channels.lock().unwrap();
        let channels = match cached.get(&addresses) {
            Some(channels) => Arc::clone(channels),
            None => {
                let all = partition
                    .addresses
                    .iter()
                    .map(|a| {
                        transport::connect_lazy(a, self.parts.tls.as_ref()).map_err(|e| {
                            tonic::Status::internal(format!("partition {name}, {a}: {e}"))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let channels = Arc::new(Channels {
                    all,
                    current: Default::default(),
                    seen: Default::default(),
                });
                cached.insert(addresses, Arc::clone(&channels));
                channels
            }
        };
        Ok(Client {
            channels,
            ..self.clone()
        })
    }

    /// This client on `tenant`'s partition.
    pub(crate) async fn for_tenant(&self, tenant: &str) -> Result<Client, tonic::Status> {
        match self.partition_map(false).await? {
            None => Ok(self.clone()),
            Some(map) => {
                let name = partition_of(&map, tenant).to_owned();
                self.on_partition(&map, &name)
            }
        }
    }

    /// This client on the control partition, which takes memberships.
    pub(crate) async fn control(&self) -> Result<Client, tonic::Status> {
        match self.partition_map(false).await? {
            None => Ok(self.clone()),
            Some(map) => self.on_partition(&map, &map.control),
        }
    }

    /// This client on every partition, by name; one, named `""`, without partitions.
    pub(crate) async fn partitions(&self) -> Result<Vec<(String, Client)>, tonic::Status> {
        match self.partition_map(false).await? {
            None => Ok(vec![(String::new(), self.clone())]),
            Some(map) => map
                .partitions
                .iter()
                .map(|p| Ok((p.name.clone(), self.on_partition(&map, &p.name)?)))
                .collect(),
        }
    }

    /// `call` on `tenant`'s partition, sent anew after reading the map again on
    /// `wrong_partition`, and while the tenant moves (`moving`, refused before it took effect)
    /// for up to 30 s.
    pub(crate) async fn routed<R, Fut>(
        &self,
        tenant: &str,
        mut call: impl FnMut(Client) -> Fut,
    ) -> Result<R, tonic::Status>
    where
        Fut: Future<Output = Result<R, tonic::Status>>,
    {
        let mut attempt = 0;
        let deadline = Instant::now() + MOVE_WAIT;
        let mut pause = Duration::from_millis(50);
        loop {
            let client = self.for_tenant(tenant).await?;
            match call(client).await {
                Err(status) if moving(&status) && Instant::now() + pause < deadline => {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(1));
                }
                Err(status) if attempt < 3 && wrong_partition(&status) => {
                    attempt += 1;
                    self.partition_map(true).await?;
                }
                result => return result,
            }
        }
    }

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
    /// idempotent by ID). Gives up after `timeout` (default: the client's) in all. For calls
    /// that are safe to repeat, use [`call_with`](Self::call_with).
    async fn call<T: Clone, R, Fut>(
        &self,
        timeout: Option<Duration>,
        message: T,
        call: impl FnMut(Channel, tonic::Request<T>) -> Fut,
    ) -> Result<R, tonic::Status>
    where
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
    {
        self.call_with(timeout, message, false, call).await
    }

    /// Like [`call`](Self::call); `idempotent` (a command, by its ID: the kernel answers a
    /// resubmission with the stored result; a cancel; a query or another read), it also tries
    /// again, with backoff until the deadline, when a kernel answers `UNAVAILABLE`, and for a
    /// short while when the connection dropped with the request under way: whether it ran is
    /// unknown, and running it again is safe.
    async fn call_with<T: Clone, R, Fut>(
        &self,
        timeout: Option<Duration>,
        message: T,
        idempotent: bool,
        mut call: impl FnMut(Channel, tonic::Request<T>) -> Fut,
    ) -> Result<R, tonic::Status>
    where
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
    {
        let timeout = timeout.unwrap_or(self.timeout);
        let deadline = Instant::now() + timeout;
        let mut pause = Duration::from_millis(50);
        // Since when the request may have reached a kernel that then went away: from then on it
        // is tried again for a short while only (a handover, a switchover), not until the
        // deadline (a crash the caller should hear of).
        let mut dropped_at: Option<Instant> = None;
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
                    pause = (pause * 2).min(MAX_PAUSE);
                }
                // A kernel that answers UNAVAILABLE (shutting down, handing over, a read model
                // or middleware not connected yet) is asked again until the deadline, also at a
                // single address; only an idempotent call, so a request that ran is never run a
                // second time. A tenant that moves is waited for by `routed`, a dropped
                // connection below.
                Err(status)
                    if idempotent
                        && status.code() == tonic::Code::Unavailable
                        && !moving(&status)
                        && !dropped(&status)
                        && !not_connected(&status)
                        && Instant::now() + pause < deadline =>
                {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(MAX_PAUSE);
                }
                Err(status)
                    if (not_connected(&status) || (idempotent && dropped(&status)))
                        && Instant::now() + pause < deadline
                        && dropped_at.is_none_or(|at| at.elapsed() < DROPPED_RETRY) =>
                {
                    if dropped(&status) {
                        dropped_at.get_or_insert_with(Instant::now);
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(MAX_PAUSE);
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
    /// variables are set, or the URL or the deployment is invalid. Example: `Kernable::new("app.key")?`.
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
            seen: Default::default(),
        });
        let service = options
            .service
            .or_else(|| std::env::var("KERNABLE_SERVICE").ok())
            .unwrap_or_default();
        let deployment = options
            .deployment
            .or_else(|| std::env::var("KERNABLE_DEPLOYMENT").ok())
            .unwrap_or_default();
        check_deployment(&deployment)?;
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
                parts: Arc::new(Partitions {
                    map: tokio::sync::Mutex::default(),
                    channels: Default::default(),
                    tls: tls.clone(),
                }),
                key_id,
                key: Arc::new(key),
                grant,
                log,
                timeout: options.timeout,
                deployment,
            },
            tenant: options.tenant,
            context: options.context,
            domains: Arc::default(),
            service,
        })
    }

    /// Seals the vault fields of this domain's commands when submitting them (see
    /// [`Domain::command`]) and sends the target of its queries asked by name (see
    /// [`Domain::query`]). Returns another handle to this connection, so it chains:
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
    /// middleware the new membership no longer lists stops checking. It names the deployment
    /// that serves the service ([`Manifest::deployment`], default this connection's, see
    /// [`Options::deployment`]): its workers run the commands and its read models answer. A
    /// membership that names another deployment than the serving one switches to it as
    /// [`Manifest::switch`] says: by default only once that deployment has caught up, refused
    /// before with `not_caught_up`, and never back to one that served before (`served_before`;
    /// [`Error::waits_for_switch`]). Example:
    /// `kernable.apply(Manifest::new().service("shop").worker(&cart_aggregate).listener(&carts_readmodel)).await?`.
    /// [`Error::Config`] if neither the manifest nor the connection names a service, or the
    /// deployment is invalid, [`Error::Status`] if the kernel refuses it.
    pub async fn apply(&self, manifest: Manifest) -> Result<(), Error> {
        let service = manifest
            .service
            .clone()
            .unwrap_or_else(|| self.service.clone());
        if service.is_empty() {
            return Err(Error::Config(
                "apply needs a service: set it with Manifest::service or Options::service".into(),
            ));
        }
        let deployment = manifest
            .deployment
            .clone()
            .unwrap_or_else(|| self.client.deployment.clone());
        check_deployment(&deployment)?;
        let manifest_switch = manifest.switch;
        let membership = membership(service, deployment, manifest, issued_now());
        let sig = self.client.sign(&signing::membership_message(&membership));
        let request = ApplyRequest {
            membership: Some(membership),
            sig: Some(sig),
            protocol: PROTOCOL,
            force: manifest_switch == Switch::Now,
            no_switch: manifest_switch == Switch::Never,
            no_rollback: manifest_switch == Switch::CaughtUp,
        };
        self.client
            .control()
            .await?
            .call(None, request, |channel, r| async move {
                MembershipServiceClient::new(channel).apply(r).await
            })
            .await?;
        Ok(())
    }

    /// Registers a business process with the kernel (see [`process`]): its model, signed. The
    /// kernel numbers the versions: a model equal to the newest version registers nothing
    /// ([`process::Registered::new`] is `false`), a different one becomes the next version;
    /// running cases finish on the version they started with. An app registers its domains'
    /// processes itself ([`Kernable::app`]). The key needs the `process` grant for the process,
    /// and `submit`, `query` and `listen` for the commands, queries and events it names.
    /// [`Error::Config`] if the model is invalid ([`Process::model`]), [`Error::Status`] if the
    /// kernel refuses it (`INVALID_ARGUMENT` names the step and why).
    pub async fn register_process(&self, process: &Process) -> Result<process::Registered, Error> {
        let model = serde_json::to_vec(&process.model()?)?;
        let issued_at = issued_now();
        let sig = self
            .client
            .sign(&signing::process_message(&model, issued_at));
        let request = ProcessRegistration {
            model,
            issued_at,
            sig: Some(sig),
            protocol: PROTOCOL,
        };
        let registered = self
            .client
            .control()
            .await?
            .call(None, request, |channel, r| async move {
                ProcessServiceClient::new(channel).register(r).await
            })
            .await?;
        Ok(process::Registered {
            name: registered.name,
            version: registered.version,
            new: registered.new,
        })
    }

    /// Sets as whom the process `name` (qualified: `shop.fulfilment`) acts: the request context
    /// its commands and queries carry (sender tenant, identity, session), instead of
    /// `process:<name>`. The kernel keeps it with the process, replicated to every partition.
    /// The kernable kit calls it for every process it gives an identity and an API key. Returns
    /// whether it changed (`false`: it acted so already). The key needs the `process` grant for
    /// the process; [`Error::Status`] if the kernel refuses (`NOT_FOUND`: no such process).
    pub async fn process_act(&self, name: &str, context: RequestContext) -> Result<bool, Error> {
        let context = context.to_wire();
        let issued_at = issued_now();
        let sig = self.client.sign(&signing::process_act_message(
            name,
            Some(&context),
            issued_at,
        ));
        let request = ProcessActing {
            name: name.to_owned(),
            context: Some(context),
            issued_at,
            sig: Some(sig),
            protocol: PROTOCOL,
        };
        let acted = self
            .client
            .control()
            .await?
            .call(None, request, |channel, r| async move {
                ProcessServiceClient::new(channel).act(r).await
            })
            .await?;
        Ok(acted.changed)
    }

    /// An app made of whole domains: [`AppHandle::run`] applies the service's membership (the
    /// domains' aggregates as workers, their read models and reactors as listeners, those that
    /// check requests as middlewares; live ones are not listed, and the metadata is `version`
    /// and `description`, when given, then the app's other metadata; the domains' business
    /// processes), then runs one worker for all the aggregates and every listener, live ones
    /// too, all in the app's service, and registers the processes once the worker is ready
    /// ([`Kernable::register_process`]; the key then needs the `process` grant for them).
    ///
    /// Example: `kernable.app(App::new().service("shop").version("1.0.0").domain(&shop_domain))?.run().await?`.
    /// Every start applies the membership, so the key needs the `manage` grant for the service.
    /// One service is one app: two processes with the same service would replace each other's
    /// membership. [`Error::Config`] if neither the app nor the connection names a service.
    pub fn app(&self, app: App) -> Result<AppHandle, Error> {
        AppHandle::new(self, app)
    }

    /// Runs a command and returns its outcome: [`Outcome::Committed`] with the new events, or
    /// [`Outcome::Rejected`] if a handler, a middleware or the kernel rejected it (a rejection
    /// is `Ok`, not an error); the kernel rejects with `unknown_history` a command whose
    /// aggregate holds events the serving deployment's worker does not know (after a
    /// rollback, see [`Manifest::deployment`]). By default it waits until every listener that counts has processed the
    /// events; see [`Submit::wait`]. [`Error::NotSettled`] if they did not in time (the
    /// command is committed), [`Error::Status`] if the kernel refuses the call (no grant,
    /// unknown aggregate type, timeout). Example: `kernable.submit(Submit::new("shop", "cart",
    /// "c1", "AddItem").data(json!({ "sku": "apple", "qty": 2 }))).await?`.
    ///
    /// A scheduled command ([`Submit::at`]) has no outcome yet: enqueue it
    /// ([`Kernable::enqueue`]); submitting it is [`Error::Config`]. Submitted again by its ID
    /// without `at`, it waits for the scheduled command's outcome; [`Error::NotDue`] if the
    /// kernel answers that it is not due yet.
    pub async fn submit(&self, submit: Submit) -> Result<Outcome, Error> {
        if submit.at.is_some() {
            return Err(Error::Config(
                "a scheduled command (`at`) has no outcome yet: use `enqueue`".into(),
            ));
        }
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
            Some(command_result::Outcome::Accepted(_)) => Err(Error::NotDue(res.command_id)),
            None => Err(Error::Status(tonic::Status::internal("no outcome"))),
        }
    }

    /// Stores the command in the kernel's queue and returns its ID without waiting for it to
    /// run. Submit it again with the same `id` ([`Submit::id`]) to get the outcome; its `wait`
    /// is ignored. With [`Submit::at`], a scheduled command: it runs at that time, e.g. a
    /// reminder in 24 hours, `kernable.enqueue(Submit::command(Remind {}, "o1")
    /// .at(SystemTime::now() + Duration::from_secs(86_400))).await?`; cancel it with
    /// [`Kernable::cancel`].
    pub async fn enqueue(&self, submit: Submit) -> Result<String, Error> {
        Ok(self.send(submit, WireWait::Accepted).await?.command_id)
    }

    /// Cancels a scheduled command ([`Submit::at`]) before it is due: it completes as rejected
    /// `cancelled`, which a submit of its ID answers from then on. Needs the `submit` grant for
    /// the command's type; middlewares check it as a cancel of that type ([`CheckKind::Cancel`]).
    /// Returns [`CancelOutcome::Cancelled`] (also when it was cancelled before), or
    /// [`CancelOutcome::Rejected`] with `too_late` (due, running or done already), `not_found`,
    /// or a middleware's code. Example: `kernable.cancel(Cancel::new(&id)).await?`.
    pub async fn cancel(&self, cancel: Cancel) -> Result<CancelOutcome, Error> {
        let mut request = CancelRequest {
            command_id: cancel.command_id,
            tenant_id: cancel.tenant.unwrap_or_else(|| self.tenant.clone()),
            context: Some(
                cancel
                    .context
                    .unwrap_or_else(|| self.context.clone())
                    .to_wire(),
            ),
            issued_at: issued_now(),
            client_sig: None,
            protocol: PROTOCOL,
        };
        request.client_sig = Some(self.client.sign(&signing::cancel_message(&request)));
        let tenant = request.tenant_id.clone();
        let timeout = cancel.timeout;
        let res = self
            .client
            .routed(&tenant, |client| {
                let request = request.clone();
                async move {
                    // Cancelling twice answers the same: safe to send again.
                    client
                        .call_with(timeout, request, true, |channel, r| async move {
                            CommandServiceClient::new(channel).cancel(r).await
                        })
                        .await
                }
            })
            .await?;
        match res.outcome {
            Some(cancel_result::Outcome::Cancelled(_)) => Ok(CancelOutcome::Cancelled),
            Some(cancel_result::Outcome::Rejected(r)) => {
                Ok(CancelOutcome::Rejected(Rejected::from_wire(r)))
            }
            None => Err(Error::Status(tonic::Status::internal("no outcome"))),
        }
    }

    async fn send(&self, s: Submit, wait: WireWait) -> Result<proto::CommandResult, Error> {
        if s.until.is_some() && s.at.is_none() {
            return Err(Error::Config("`until` needs `at`".into()));
        }
        let command_id = s.id.unwrap_or_else(random_id);
        let vault = s.vault.clone().or_else(|| {
            self.domains
                .read()
                .unwrap()
                .get(&s.domain)
                .and_then(|d| d.command_vault(&s.command_type))
        });
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
            deliver_at: s.at.unwrap_or(0),
            deliver_until: s.until.unwrap_or(0),
        };
        cmd.client_sig = Some(self.client.sign(&signing::command_message(&cmd)));
        let request = SubmitRequest {
            command: Some(cmd),
            wait: wait.into(),
            protocol: PROTOCOL,
            vault: sealed.values,
            listener: handling(),
        };
        let tenant = request
            .command
            .as_ref()
            .map(|c| c.tenant_id.clone())
            .unwrap_or_default();
        let timeout = s.timeout;
        Ok(self
            .client
            .routed(&tenant, |client| {
                let request = request.clone();
                async move {
                    let result = client
                        .call_with(timeout, request, true, |channel, r| async move {
                            CommandServiceClient::new(channel).submit(r).await
                        })
                        .await?;
                    if let Some(command_result::Outcome::Committed(c)) = &result.outcome {
                        let last = c.events.last().map_or(0, |e| e.global_position);
                        client
                            .channels
                            .seen
                            .fetch_max(c.position.max(last), std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok(result)
                }
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
        let declared = query.target.clone().or_else(|| {
            self.domains
                .read()
                .unwrap()
                .get(&query.domain)
                .and_then(|d| d.query_target(&query.query_type))
        });
        let target = declared.and_then(|t| t.wire(&query.domain, &query.data));
        let mut request = proto::QueryRequest {
            target,
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
            issued_at: issued_now(),
            client_sig: None,
            protocol: PROTOCOL,
            listener: String::new(),
            wait_only: false,
            deployment: self.client.deployment.clone(),
        };
        let timeout = query.timeout;
        if query.all_partitions {
            self.caught_up_everywhere(&request, timeout).await?;
            request.issued_at = issued_now();
        }
        let listener = handling();
        let tenant = request.tenant_id.clone();
        let res = self
            .client
            .routed(&tenant, |client| {
                let mut request = request.clone();
                // Read your writes, on whichever copy answers: the query waits for this client's
                // own newest command. Not from a handler, which must not wait behind its own
                // listener.
                if listener.is_empty() {
                    request.min_position = request.min_position.max(
                        client
                            .channels
                            .seen
                            .load(std::sync::atomic::Ordering::SeqCst),
                    );
                }
                request.client_sig = Some(client.sign(&signing::query_message(&request)));
                request.listener = listener.clone();
                async move {
                    client
                        .call_with(timeout, request, true, |channel, r| async move {
                            QueryServiceClient::new(channel).query(r).await
                        })
                        .await
                }
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

    /// Asks a query of a message type and returns its typed answer: `kernable.ask(GetOrder {
    /// id }, |q| q.tenant(t)).await?`; `options` sets what [`Query`]'s methods set.
    pub async fn ask<Q: QueryType>(
        &self,
        query: Q,
        options: impl FnOnce(Query) -> Query,
    ) -> Result<Q::Answer, Error> {
        self.query(options(Query::of(query))).await
    }

    /// Returns once a read model that answers `query` has processed every partition's head, as
    /// of the call: a wait-only query to each partition, at the same time.
    async fn caught_up_everywhere(
        &self,
        query: &proto::QueryRequest,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        let mut wait = proto::QueryRequest {
            wait_only: true,
            min_position: 0,
            payload: Vec::new(),
            ..query.clone()
        };
        wait.client_sig = Some(self.client.sign(&signing::query_message(&wait)));
        wait.listener = handling();
        let mut waits = tokio::task::JoinSet::new();
        for (_, client) in self.client.partitions().await? {
            let wait = wait.clone();
            waits.spawn(async move {
                client
                    .call_with(timeout, wait, true, |channel, r| async move {
                        QueryServiceClient::new(channel).query(r).await
                    })
                    .await
            });
        }
        while let Some(waited) = waits.join_next().await {
            waited.map_err(|e| Error::Status(tonic::Status::internal(e.to_string())))??;
        }
        Ok(())
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

/// The membership `manifest` describes, for `service` and `deployment`.
fn membership(
    service: String,
    deployment: String,
    manifest: Manifest,
    issued_at: i64,
) -> Membership {
    Membership {
        service,
        workers: manifest.workers,
        listeners: manifest.listeners,
        middlewares: manifest.middlewares,
        metadata: manifest
            .metadata
            .into_iter()
            .map(|(key, value)| Attribute { key, value })
            .collect(),
        issued_at,
        processes: manifest.processes,
        deployment,
    }
}

/// [`Error::Config`] unless `deployment` is empty or 1 to 128 letters, digits, `-`, `_` and `.`.
fn check_deployment(deployment: &str) -> Result<(), Error> {
    let valid = deployment.len() <= 128
        && deployment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if valid {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "deployment {deployment:?}: use 1 to 128 letters, digits, `-`, `_` and `.`"
        )))
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
    processes: Vec<String>,
    deployment: Option<String>,
    switch: Switch,
}

/// When a membership that names another deployment than the one serving its service switches
/// to it ([`Manifest::switch`], [`App::switch`]). See the how-to "Deployments and rollbacks".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Switch {
    /// Once the deployment has caught up (the default), and only to a deployment that never
    /// served before: its listed workers are connected and its read models within the kernel's
    /// `--deployment-switch-lag` of the newest event. Until then the kernel refuses it
    /// (`not_caught_up`); an app runs beside the serving deployment meanwhile and applies again
    /// until it switches. A deployment that served before (it wrote events) is never switched
    /// back to this way (`served_before`): its app runs beside, as with [`Switch::Never`],
    /// until an operator rolls back (`kernable apply --deployment`). So an older deployment's
    /// app that restarts during a rolling deploy does not take the service back.
    #[default]
    CaughtUp,
    /// At once, caught up or not (`kernable apply --force`).
    Now,
    /// Never by itself: the membership applies only while its deployment serves already (or
    /// the service has none yet); otherwise the kernel refuses it (`not_serving`). An app runs
    /// beside the serving deployment until an operator switches (`kernable apply --deployment`).
    Never,
}

impl Switch {
    /// `KERNABLE_SWITCH`: `caught-up` (default), `now` or `never`.
    pub fn from_env() -> Result<Switch, Error> {
        match std::env::var("KERNABLE_SWITCH").as_deref() {
            Err(_) | Ok("" | "caught-up") => Ok(Switch::CaughtUp),
            Ok("now") => Ok(Switch::Now),
            Ok("never") => Ok(Switch::Never),
            Ok(other) => Err(Error::Config(format!(
                "KERNABLE_SWITCH={other}: caught-up, now or never"
            ))),
        }
    }
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

    /// A business process of the service ([`Domain::process`]): the kernel starts new cases of
    /// it while a membership lists it; one no longer listed starts none, its running cases
    /// finish. Register it too ([`Kernable::register_process`]); an app does both.
    pub fn process(mut self, process: &Process) -> Manifest {
        self.processes.push(process.name());
        self
    }

    /// Free metadata, e.g. `("version", "1.0.0")`; kept in the order given.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Manifest {
        self.metadata.push((key.into(), value.into()));
        self
    }

    /// The deployment that serves the service: its workers run the commands, its listeners
    /// count and its read models answer queries first; the service's workers of other
    /// deployments get no commands, their read models keep catching up. A rollback applies a
    /// membership naming the earlier deployment. Default: the connection's
    /// ([`Options::deployment`]); empty: every deployment serves.
    pub fn deployment(mut self, deployment: impl Into<String>) -> Manifest {
        self.deployment = Some(deployment.into());
        self
    }

    /// When it switches the service to its deployment if another one serves: see [`Switch`].
    /// Default [`Switch::CaughtUp`].
    pub fn switch(mut self, switch: Switch) -> Manifest {
        self.switch = switch;
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
    /// A message type's vault fields, from its declaration.
    vault: Option<vault::VaultFields>,
    /// A scheduled command's delivery time and expiry, ms since epoch.
    at: Option<i64>,
    until: Option<i64>,
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
            vault: None,
            at: None,
            until: None,
        }
    }

    /// A command of a message type (see [`command`]) for the aggregate with ID `aggregate_id`:
    /// its type says the domain, the aggregate type and the vault fields.
    /// `Submit::command(PlaceOrder { customer_id }, "o1").tenant(t)`.
    ///
    /// # Panics
    ///
    /// If `command` cannot be represented as JSON.
    pub fn command<C: CommandType>(command: C, aggregate_id: &str) -> Submit {
        let vault = messages::domain_of::<C>().command_vault(C::TYPE);
        Submit {
            vault,
            ..Submit::new(C::DOMAIN, C::AGGREGATE, aggregate_id, C::TYPE).data(command)
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

    /// [`Kernable::enqueue`] only: a scheduled command, run at `time` and not before. The kernel
    /// stores it at once; the aggregate's other commands pass meanwhile, and middlewares check
    /// it now and again when it is due. At most the kernel's `--schedule-max-days` (default 366)
    /// ahead. Cancel it with [`Kernable::cancel`].
    pub fn at(mut self, time: SystemTime) -> Submit {
        self.at = Some(millis(time));
        self
    }

    /// With [`Submit::at`]: if the command cannot run by `time` (after an outage, say), it is
    /// rejected `expired` instead of running late. Without `at`, submitting or enqueueing it is
    /// [`Error::Config`].
    pub fn until(mut self, time: SystemTime) -> Submit {
        self.until = Some(millis(time));
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

/// Milliseconds since the epoch; 0 before it.
fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// A cancel of a scheduled command for [`Kernable::cancel`], built with methods that each
/// return the changed cancel: `Cancel::new(&id).tenant("t1")`.
#[derive(Clone, Debug)]
pub struct Cancel {
    command_id: String,
    tenant: Option<String>,
    context: Option<RequestContext>,
    timeout: Option<Duration>,
}

impl Cancel {
    /// A cancel of the scheduled command `command_id` (the ID [`Kernable::enqueue`] returned).
    pub fn new(command_id: &str) -> Cancel {
        Cancel {
            command_id: command_id.to_owned(),
            tenant: None,
            context: None,
            timeout: None,
        }
    }

    /// The command's tenant. Default: the connection's ([`Options::tenant`]).
    pub fn tenant(mut self, tenant: impl Into<String>) -> Cancel {
        self.tenant = Some(tenant.into());
        self
    }

    /// Replaces the client's default context: who cancels, for the middlewares.
    pub fn context(mut self, context: RequestContext) -> Cancel {
        self.context = Some(context);
        self
    }

    /// How long to wait for the connection and the answer; default [`Options::timeout`].
    pub fn timeout(mut self, timeout: Duration) -> Cancel {
        self.timeout = Some(timeout);
        self
    }
}

/// What became of a cancel, returned by [`Kernable::cancel`].
#[derive(Clone, Debug, PartialEq)]
pub enum CancelOutcome {
    /// The scheduled command will not run: it completed as rejected `cancelled`.
    Cancelled,
    /// Not cancelled: `too_late` (due, running or done already), `not_found`, or a
    /// middleware's rejection.
    Rejected(Rejected),
}

impl CancelOutcome {
    /// Whether the command was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, CancelOutcome::Cancelled)
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
    all_partitions: bool,
    /// A message type's target, from its declaration.
    target: Option<QueryTarget>,
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
            all_partitions: false,
            target: None,
        }
    }

    /// A query of a message type (see [`query`]); its type says the domain and the aggregate it
    /// is about. Ask it with [`Kernable::ask`] for its typed answer.
    ///
    /// # Panics
    ///
    /// If `query` cannot be represented as JSON.
    pub fn of<Q: QueryType>(query: Q) -> Query {
        let target = messages::domain_of::<Q>().query_target(Q::TYPE);
        Query {
            target,
            ..Query::new(Q::DOMAIN, Q::TYPE).data(query)
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

    /// For a read model across tenants with partitions: answer only once the read model has
    /// processed every partition's events up to the query, not only those of the tenant's
    /// partition. Costs a wait-only query to each partition first.
    pub fn all_partitions(mut self) -> Query {
        self.all_partitions = true;
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
    /// The code version of the worker that wrote it ([`Options::deployment`]); empty if none.
    pub deployment: String,
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
            deployment: e.deployment,
        }
    }
}

/// Whether `status` says the connection could not be made: the request was not sent.
/// Whether the connection dropped with the request under way (a kernel that stopped or handed
/// its stores over, a proxy that closed): unknown whether it ran.
fn dropped(status: &tonic::Status) -> bool {
    use std::io::ErrorKind::{BrokenPipe, ConnectionAborted, ConnectionReset, UnexpectedEof};
    if !matches!(
        status.code(),
        tonic::Code::Unknown | tonic::Code::Unavailable | tonic::Code::Internal
    ) {
        return false;
    }
    let mut source = std::error::Error::source(status);
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof
            )
        {
            return true;
        }
        source = e.source();
    }
    let message = status.message();
    message.contains("transport error")
        || message.contains("connection closed")
        || message.contains("error reading a body from connection")
        || message.contains("h2 protocol error")
}

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

/// The longest pause between tries of a call while the kernel is away: a pair's switchover or a
/// kernel update takes a few hundred milliseconds, and the client finds the kernel serving again
/// within this much of it.
const MAX_PAUSE: Duration = Duration::from_millis(200);

/// How long a submit whose connection dropped under way is tried again: long enough for a
/// kernel handing its stores to a new binary or its role to a standby (a few hundred
/// milliseconds), short enough that a crash reaches the caller.
const DROPPED_RETRY: Duration = Duration::from_secs(5);

#[cfg(test)]
mod dropped_tests {
    use super::*;

    #[test]
    fn a_connection_that_dropped_under_way_counts_as_dropped_and_nothing_else_does() {
        assert!(dropped(&tonic::Status::unknown("transport error")));
        assert!(dropped(&tonic::Status::unavailable(
            "h2 protocol error: error reading a body from connection"
        )));
        assert!(!dropped(&tonic::Status::unknown("a worker's error")));
        assert!(!dropped(&tonic::Status::invalid_argument(
            "transport error"
        )));
        assert!(!dropped(&tonic::Status::deadline_exceeded(
            "transport error"
        )));
    }
}

#[cfg(test)]
mod deployment_tests {
    use serde_json::Value;

    use super::*;

    /// A membership with a deployment is signed as the shared test vector
    /// `membership_deployment` says.
    #[test]
    fn a_membership_with_a_deployment_is_signed_as_the_test_vectors_say() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../proto/test-vectors/signing.json"
        );
        let vectors: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let vector = &vectors["membership_deployment"];
        let input = &vector["input"];
        let texts = |v: &Value| -> Vec<String> {
            v.as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_owned())
                .collect()
        };
        let manifest = Manifest {
            service: None,
            workers: texts(&input["workers"]),
            listeners: texts(&input["listeners"]),
            middlewares: texts(&input["middlewares"]),
            metadata: input["metadata"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| {
                    let text = |k: &str| a[k].as_str().unwrap().to_owned();
                    (text("key"), text("value"))
                })
                .collect(),
            processes: texts(&input["processes"]),
            deployment: None,
            switch: Switch::default(),
        };
        let membership = membership(
            input["service"].as_str().unwrap().to_owned(),
            input["deployment"].as_str().unwrap().to_owned(),
            manifest,
            input["issued_at"].as_i64().unwrap(),
        );
        let message = signing::membership_message(&membership);
        assert_eq!(hex::encode(&message), vector["message"].as_str().unwrap());
        let key = signing::signing_key_from_hex(vectors["key"]["seed"].as_str().unwrap()).unwrap();
        assert_eq!(
            hex::encode(signing::sign(&key, &message).sig),
            vector["signature"].as_str().unwrap()
        );
    }

    #[test]
    fn a_deployment_is_letters_digits_dashes_underscores_and_dots() {
        for valid in ["", "v1", "2026-11-11.1", "a_b", &"x".repeat(128)] {
            assert!(check_deployment(valid).is_ok(), "{valid}");
        }
        for invalid in ["v 1", "v1@x", "ü", "a/b", &"x".repeat(129)] {
            assert!(
                matches!(check_deployment(invalid), Err(Error::Config(m)) if m.contains("1 to 128")),
                "{invalid}"
            );
        }
    }

    /// An app keeps applying (it runs beside) while its deployment has not caught up, may not
    /// switch, or served before; other refusals end it.
    #[test]
    fn an_app_waits_beside_while_its_deployment_has_not_caught_up_or_served_before() {
        let refused = Error::Status;
        for details in [
            "not_caught_up: deployment v2 has not caught up",
            "not_serving: shop is served by v1",
            "served_before: deployment v1 served shop before and v2 serves it now",
        ] {
            assert!(
                refused(tonic::Status::failed_precondition(details)).waits_for_switch(),
                "{details}"
            );
        }
        assert!(
            !refused(tonic::Status::failed_precondition(
                "key may not manage shop"
            ))
            .waits_for_switch()
        );
        assert!(!refused(tonic::Status::unavailable("served_before")).waits_for_switch());
    }
}
