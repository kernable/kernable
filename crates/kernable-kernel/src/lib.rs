//! The kernable kernel: accepts signed commands, leases them to authenticated workers,
//! verifies their signed results and commits them to the event store.
//!
//! Workers pull work: each session runs its own dispatch loop that leases commands of the
//! types the worker registered, up to its `max_concurrency`, from the queue in kernel memory
//! (one command per aggregate at a time, see [`queue`]).
//!
//! By default a submit responds once every registered listener has processed the command's
//! events (see [`listeners`]), so a query right after it sees the new state.
//!
//! Before a command is accepted or a query routed, every middleware covering its type checks
//! it, in chain order; the first rejection refuses the request. A missing middleware refuses
//! too: the chain fails closed. Refused commands are not stored, like any other request the
//! kernel does not accept. Middleware after a request sees its outcome: it may still refuse a
//! query's response (failing closed as well), and it observes, best effort, every completed
//! command and every rejected query.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod backup;
mod cache;
pub mod config;
mod connections;
pub mod crash;
pub mod demo;
pub mod export;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
pub mod global_keys;
pub mod grants;
mod http;
mod index;
mod listeners;
mod memberships;
mod metrics;
pub mod moves;
pub mod node;
pub mod partitions;
mod queue;
mod replays;
mod replication;
pub use replication::{refuse_if_handed_over, switchover_keeper};
pub mod standby;
pub mod supervisor;
mod trend;
mod ui;
pub mod witness;
mod workers;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, RwLock,
        atomic::{
            AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed, Ordering::SeqCst,
        },
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Accepted, CacheRequest, CacheResult, Challenge, Command, CommandResult, Committed,
        DataEntry, DataList, DataRequest, DataResult, Event, Execute, ExecuteMore, Grant,
        KernelMessage, ListenerDelivery, ListenerMessage, MiddlewareCall, NewEvent, QueryRequest,
        QueryResult, Register, Registered, Rejected, RetryRequest, RetryResult, RevocationRecord,
        RevokeRequest, RevokeResponse, Signature, Snapshot, SnapshotRecord, SubmitRequest,
        SubscribeRequest, VaultValue, Wait, When, WorkerMessage,
        cache_service_server::{CacheService, CacheServiceServer},
        command_result::Outcome,
        command_service_server::{CommandService, CommandServiceServer},
        data_service_server::{DataService, DataServiceServer},
        event_service_server::{EventService, EventServiceServer},
        export_service_server::ExportServiceServer,
        global_key_service_server::GlobalKeyServiceServer,
        kernel_message::Msg as Out,
        listener_service_server::{ListenerService, ListenerServiceServer},
        membership_service_server::MembershipServiceServer,
        middleware_call,
        move_service_server::MoveServiceServer,
        partition_service_server::PartitionServiceServer,
        query_answer, query_result,
        query_service_server::{QueryService, QueryServiceServer},
        replication_service_server::ReplicationServiceServer,
        trust_service_server::{TrustService, TrustServiceServer},
        trust_sync_service_server::TrustSyncServiceServer,
        worker_gateway_server::{WorkerGateway, WorkerGatewayServer},
        worker_message::Msg as In,
    },
    signing, transport,
};
use kernable_store::{
    Enqueued, Error as StoreError, Store, StreamId,
    data::{Data, DataKey, DataStore},
    snapshots::SnapshotStore,
    vault::{self, Owner, Vault},
};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    metadata::MetadataMap,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

pub use config::Config;
pub use grants::Permissions;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Re-executions after a version conflict before the command is rejected.
const MAX_CONFLICT_RETRIES: u32 = 5;
/// Rejection code of a worker that cannot read the snapshot it got.
const SNAPSHOT_STALE: &str = "snapshot_stale";
/// How often idle workers look for expired leases.
const POLL: Duration = Duration::from_millis(100);
/// The longest pause before a command whose commit failed runs again.
const COMMIT_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Accepted clock difference for signed subscribe requests.
const MAX_REQUEST_AGE_MS: i64 = 60_000;

/// The largest file of the data service, cache value and key.
const MAX_CACHE_BYTES: usize = 1 << 20;

/// Files in one page of `DataService.ListData`.
const DATA_PAGE: usize = 1000;
const MAX_KEY_BYTES: usize = 512;
/// Events read from the store per subscription round trip.
const SUBSCRIBE_BATCH: u32 = 500;
/// The metadata entry that carries a request's grant: the hex-encoded `Grant`.
pub const GRANT_HEADER: &str = "kernable-grant";

/// How often the kernel pings an idle HTTP/2 connection, and how long it waits for the answer
/// before it drops the connection: a peer that vanished without closing it frees its slot.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bytes a message adds around an `Execute` or `Batch` (field tag and length).
const ENVELOPE_BYTES: usize = 16;

/// Requests or streams in progress per key, each counted until its [`Slot`] drops.
#[derive(Default)]
struct PerKey(Arc<Mutex<HashMap<String, usize>>>);

/// A token bucket per key: `rate` tokens a second, at most `rate` saved up.
#[derive(Default)]
struct Rates(Mutex<HashMap<String, (f64, Instant)>>);

impl Rates {
    /// Takes a token for `key_id`; false if it has none left.
    fn take(&self, key_id: &str, rate: u64) -> bool {
        let rate = rate as f64;
        let now = Instant::now();
        let mut buckets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        // Full buckets say nothing: dropped now and then, so idle keys do not pile up.
        if buckets.len() > 10_000 {
            buckets.retain(|_, (tokens, at)| *tokens + at.elapsed().as_secs_f64() * rate < rate);
        }
        let (tokens, at) = buckets.entry(key_id.to_owned()).or_insert((rate, now));
        *tokens = (*tokens + now.duration_since(*at).as_secs_f64() * rate).min(rate);
        *at = now;
        if *tokens < 1.0 {
            return false;
        }
        *tokens -= 1.0;
        true
    }
}

/// One counted request or stream of a key.
struct Slot(Arc<Mutex<HashMap<String, usize>>>, String);

impl PerKey {
    /// A slot for `key_id`, unless it holds `max` already.
    fn take(&self, key_id: &str, max: usize) -> Option<Slot> {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let held = counts.get(key_id).copied().unwrap_or(0);
        if held >= max {
            return None;
        }
        counts.insert(key_id.to_owned(), held + 1);
        Some(Slot(Arc::clone(&self.0), key_id.to_owned()))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(held) = counts.get_mut(&self.1) {
            *held -= 1;
            if *held == 0 {
                counts.remove(&self.1);
            }
        }
    }
}

/// A submit or query in progress, see [`Kernel::request`].
struct Running<'a>(&'a Kernel);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.requests.fetch_sub(1, SeqCst);
        self.0.idle.notify_waiters();
    }
}

/// The protocol versions this kernel accepts; see "Protocol versions" in `kernable.proto`.
pub const PROTOCOLS: std::ops::RangeInclusive<u32> =
    kernable_core::PROTOCOL..=kernable_core::PROTOCOL;

/// Refuses a request or stream of a protocol version this kernel does not speak.
pub(crate) fn protocol(version: u32) -> Result<(), Status> {
    if PROTOCOLS.contains(&version) {
        return Ok(());
    }
    let (min, max) = (PROTOCOLS.start(), PROTOCOLS.end());
    let advice = if version < *min {
        "update the SDK"
    } else {
        "update the kernel"
    };
    Err(Status::failed_precondition(format!(
        "protocol {version} is not supported; this kernel speaks {min} to {max}: {advice}"
    )))
}

pub struct Kernel {
    config: Arc<Config>,
    /// The newest verified grant per key ID.
    grants: Mutex<HashMap<String, Grant>>,
    /// Derived from `grants` and `revoked`; replaced whenever either changes.
    permissions: RwLock<Arc<Permissions>>,
    store: Box<dyn Store>,
    /// Signs checkpoints and revocations; read again on reload (SIGHUP).
    key: Mutex<SigningKey>,
    /// Revoked key ID -> since when (ms since epoch), from the store.
    revoked: Mutex<HashMap<String, i64>>,
    /// Bumped whenever permissions change, so connected sessions check their key again.
    changes: watch::Sender<u64>,
    /// Submit calls waiting for a command's result.
    waiters: Mutex<HashMap<String, Vec<oneshot::Sender<CommandResult>>>>,
    /// Wakes idle dispatch loops when new commands arrive.
    work: Notify,
    /// Commits that failed in a row (a full disk): their commands wait longer each time.
    failed_commits: AtomicU32,
    /// Wakes subscriptions and listeners when events were committed.
    appended: Notify,
    listeners: listeners::Registry,
    workers: workers::Registry,
    memberships: memberships::Memberships,
    queue: queue::Queue,
    metrics: metrics::Metrics,
    /// Listener positions over the last hour, for the UI.
    trend: trend::Trend,
    /// The highest payload version stored per qualified event type; never goes down. Held
    /// through every commit.
    schemas: Mutex<HashMap<String, u32>>,
    /// The unique keys and facts every aggregate holds. Locked after `schemas`, never without
    /// it, so the two serialise commits in one order.
    index: Mutex<index::Index>,
    /// The latest snapshot per aggregate, apart from the log.
    snapshots: Box<dyn SnapshotStore>,
    /// Personal and secret values; `None` without `[vault]`.
    vault: Option<Vault>,
    /// Files of the data service.
    data: Box<dyn DataStore>,
    /// Values of the cache service.
    cache: cache::Cache,
    /// Set on shutdown: new submits and queries are refused while running ones finish.
    draining: AtomicBool,
    /// A switchover is under way: new requests are refused, as while draining.
    pub(crate) switching: AtomicBool,
    /// Submits and queries being handled, and a wake-up when one ends.
    requests: AtomicUsize,
    idle: Notify,
    /// Submits, queries, file and cache requests in progress per key.
    requests_by_key: PerKey,
    /// Open subscriptions per key.
    subscriptions_by_key: PerKey,
    /// The newest global position known to be on the disk: listeners, subscribers and workers
    /// never get an event beyond it, so nobody acts on what a power loss could take back.
    durable: AtomicU64,
    /// Data and cache writes seen within their time window.
    replays: replays::Replays,
    /// The standby, with `--replication sync`: writes count once it holds them.
    replication: Option<Arc<replication::Primary>>,
    /// The backup follower, with `--backup on`.
    backup: replication::Backup,
    /// The primary of a pair takes switchover requests here; None otherwise.
    switchover: Mutex<Option<tokio::sync::mpsc::Sender<node::SwitchoverAsk>>>,
    /// Bytes of the event store, the vault and the files, measured every 10 s.
    stored_bytes: AtomicU64,
    /// Requests per key and second.
    rates: Rates,
    /// The partition map, with `--partition` (S2).
    maps: partitions::Maps,
    /// Global keys of commits in flight on this partition (S3).
    in_flight_keys: global_keys::InFlight,
    /// On the key owner: one decision about global keys at a time.
    global_owner: tokio::sync::Mutex<()>,
}

impl Kernel {
    pub fn open(config: Config) -> Result<Arc<Self>, BoxError> {
        let kernel = taking_over(config.take_over_ms, || {
            kernable_store::open_with(&config.store, config.durability)
                .map_err(BoxError::from)
                .and_then(|store| Self::open_on(config.clone(), store))
        })?;
        // It follows no primary (any more): the standby's note goes.
        if let Some(m) = standby::marker(&kernel.config().store, "following") {
            let _ = std::fs::remove_file(m);
        }
        Ok(kernel)
    }

    /// Like [`Kernel::open`], on a store opened already (`config.store` is not opened): for
    /// tests that wrap the store, e.g. in one that fails on purpose.
    pub fn open_on(config: Config, store: Box<dyn Store>) -> Result<Arc<Self>, BoxError> {
        let key = config.read_signing_key(&config.kernel_key)?;
        let revoked: HashMap<String, i64> = store
            .revocations()?
            .into_iter()
            .map(|r| (r.key_id, r.since))
            .collect();
        let memberships = memberships::Memberships::load(store.memberships()?);
        let listeners = listeners::Registry::load(store.listeners()?, &memberships);
        let workers = workers::Registry::load(store.workers()?);
        let schemas = store.event_schemas()?.into_iter().collect();
        let snapshots = kernable_store::snapshots::open(&config.snapshots)?;
        let vault = match &config.vault {
            Some(v) => {
                let key = config.read_secret(&v.key).map_err(|e| match e.kind() {
                    std::io::ErrorKind::InvalidData => {
                        "the vault key must be 32 bytes, hex encoded".into()
                    }
                    _ => BoxError::from(e),
                })?;
                let vault = Vault::open(&v.store, &key)?;
                let sealed = vault.sealed()?;
                if sealed > kernable_store::vault::MAX_SEALED / 2 {
                    tracing::warn!(
                        sealed,
                        limit = kernable_store::vault::MAX_SEALED,
                        "the vault key is past half its limit of values; \
                         plan a `kernable vault rekey`"
                    );
                }
                recover_vault(store.as_ref(), &vault, snapshots.as_ref())?;
                Some(vault)
            }
            None => None,
        };
        // The index is read as commits need it; personal keys need the vault for that.
        if vault.is_none() && store.holds_personal_keys()? {
            return Err(index::NO_VAULT.into());
        }
        let index = index::Index::default();
        let data = kernable_store::data::open(&config.data)?;
        let cache = cache::Cache::open(&config.cache, config.limits.cache_bytes)?;
        let durable = AtomicU64::new(store.head_position()?);
        // A standby and a backup follower continue the record log (design/replication.md);
        // without either it is not kept.
        store.keep_record_log(config.replication.sync || config.replication.backup)?;
        let replication = config.replication.sync.then(|| {
            Arc::new(match config.replication.pair {
                Some(_) => replication::Primary::new(),
                None => replication::Primary::fixed(&config.store),
            })
        });
        let maps = partitions::Maps::new(store.partition_map()?);
        let queue = queue::Queue::default();
        // Commands of a tenant moving here are copies a stopped copy left pending: the next
        // copy completes them, no worker runs them (S4).
        let arriving: Vec<String> = match (&config.partition, maps.get()) {
            (Some(p), Some(map)) => map
                .moving
                .iter()
                .filter(|m| m.to == p.name)
                .map(|m| m.tenant.clone())
                .collect(),
            _ => Vec::new(),
        };
        for pending in store.pending()? {
            if !arriving.contains(&pending.command.tenant_id) {
                queue.push(pending.command, pending.received_at);
            }
        }
        let kernel = Arc::new(Kernel {
            config: Arc::new(config),
            grants: Mutex::default(),
            permissions: RwLock::default(),
            store,
            key: Mutex::new(key),
            revoked: Mutex::new(revoked),
            changes: watch::channel(0).0,
            waiters: Mutex::default(),
            work: Notify::new(),
            failed_commits: AtomicU32::new(0),
            appended: Notify::new(),
            backup: replication::Backup::default(),
            switchover: Mutex::default(),
            stored_bytes: AtomicU64::new(0),
            rates: Rates::default(),
            listeners,
            workers,
            memberships,
            queue,
            metrics: metrics::Metrics::default(),
            trend: trend::Trend::default(),
            schemas: Mutex::new(schemas),
            index: Mutex::new(index),
            snapshots,
            vault,
            data,
            cache,
            draining: Default::default(),
            switching: Default::default(),
            requests: Default::default(),
            idle: Notify::new(),
            requests_by_key: PerKey::default(),
            subscriptions_by_key: PerKey::default(),
            durable,
            replays: replays::Replays::new(now_ms()),
            replication,
            maps,
            in_flight_keys: Default::default(),
            global_owner: Default::default(),
        });
        // The newest grants it had, checked again: a root key may be gone or revoked since.
        for grant in kernel.store.grants()? {
            let key_id = grant.key_id.clone();
            if let Err(e) = kernel
                .check_grant(&grant)
                .and_then(|()| kernel.keep(grant, false))
            {
                tracing::warn!(key = %key_id, error = %e.message(), "stored grant no longer valid");
            }
        }
        Ok(kernel)
    }

    /// The configuration in effect.
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }

    /// What every key may do, by the grants presented so far.
    pub fn permissions(&self) -> Arc<Permissions> {
        Arc::clone(
            &self
                .permissions
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    fn key(&self) -> SigningKey {
        self.key
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Since when `key_id` is revoked (ms since epoch), if it is.
    /// Whether `key_id` is revoked.
    pub(crate) fn revoked(&self, key_id: &str) -> bool {
        self.revoked_since(key_id).is_some()
    }

    fn revoked_since(&self, key_id: &str) -> Option<i64> {
        let revoked = self.revoked.lock().unwrap_or_else(PoisonError::into_inner);
        revoked.get(key_id).copied()
    }

    /// Notified whenever permissions change: sessions then check their key again.
    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// Reads the kernel key again (on SIGHUP), so a new key signs from now on.
    pub fn reload(&self) -> Result<(), BoxError> {
        config::forget_unwrapped_keys();
        let key = self.config().read_signing_key(&self.config().kernel_key)?;
        *self.key.lock().unwrap_or_else(PoisonError::into_inner) = key;
        Ok(())
    }

    /// Whether `sig` was made by a root key that is not revoked.
    fn by_root(&self, sig: &Signature, msg: &[u8]) -> Result<(), Status> {
        if !self.config.roots.contains(&sig.key_id) {
            return Err(Status::permission_denied("not signed by a root key"));
        }
        if self.revoked_since(&sig.key_id).is_some() {
            return Err(Status::permission_denied("the root key was revoked"));
        }
        signing::verify(sig, msg).map_err(|_| Status::unauthenticated("invalid root signature"))
    }

    /// Takes a grant into effect: checks its root signature and keeps it if it is the newest
    /// for its key, in the store too, so that an older grant stays refused after a restart
    ///. Sessions are checked again when it replaces another grant.
    pub fn grant(&self, grant: Grant) -> Result<(), Status> {
        self.check_grant(&grant)?;
        self.keep(grant, true)
    }

    fn check_grant(&self, grant: &Grant) -> Result<(), Status> {
        let sig = grant
            .root_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("grant is not signed"))?;
        self.by_root(sig, &signing::grant_message(grant))?;
        if let Some(reason) = grants::malformed(grant) {
            return Err(Status::invalid_argument(format!("grant: {reason}")));
        }
        if self.revoked_since(&grant.key_id).is_some() {
            return Err(Status::permission_denied("key was revoked"));
        }
        Ok(())
    }

    /// Keeps a checked grant unless one of its key issued at the same time or later is held;
    /// `store`: also in the store, before it takes effect.
    fn keep(&self, grant: Grant, store: bool) -> Result<(), Status> {
        self.keep_with(grant, store, true)
    }

    /// Like [`Kernel::keep`]; `standby`: a stored grant also waits for the standby. The
    /// standby's own grant cannot: it arrives before the standby follows.
    fn keep_with(&self, grant: Grant, store: bool, standby: bool) -> Result<(), Status> {
        let held = |grants: &HashMap<String, Grant>| {
            grants
                .get(&grant.key_id)
                .is_some_and(|held| held.issued_at >= grant.issued_at)
        };
        if held(&self.grants.lock().unwrap_or_else(PoisonError::into_inner)) {
            return Ok(());
        }
        // Stored and on the disk (and on the standby) before it takes effect, without the lock:
        // waiting for the standby under it would stall every request that presents a grant,
        // and the stream to the standby with them. Requests presenting the same grant meanwhile
        // store it too (a no-op) and wait as well.
        if store {
            self.store.save_grant(&grant).map_err(internal)?;
            match standby {
                true => self.sync(0),
                false => self.sync_disk(0),
            }
        }
        let replaced = {
            let mut grants = self.grants.lock().unwrap_or_else(PoisonError::into_inner);
            if held(&grants) {
                return Ok(());
            }
            let replaced = grants.contains_key(&grant.key_id);
            grants.insert(grant.key_id.clone(), grant);
            // Before the lock is released: a request presenting the same grant meanwhile finds
            // it held and goes on, so the permissions must already include it.
            self.set_permissions(&grants);
            replaced
        };
        if replaced {
            self.changes.send_modify(|n| *n += 1);
        }
        Ok(())
    }

    /// Takes the grant a request carries into effect, if it carries one; it must be for the key
    /// that signed the request.
    pub(crate) fn present(&self, metadata: &MetadataMap, key_id: &str) -> Result<(), Status> {
        match self.presented(metadata, key_id)? {
            Some(grant) => self.grant(grant),
            None => Ok(()),
        }
    }

    /// Like [`Kernel::present`], for the standby's own stream: its grant is kept without
    /// waiting for a standby (it is the standby), and reaches it with the records.
    pub(crate) fn present_standby(
        &self,
        metadata: &MetadataMap,
        key_id: &str,
    ) -> Result<(), Status> {
        match self.presented(metadata, key_id)? {
            Some(grant) => {
                self.check_grant(&grant)?;
                self.keep_with(grant, true, false)
            }
            None => Ok(()),
        }
    }

    /// The grant a request carries, for `key_id`, if it carries one.
    fn presented(&self, metadata: &MetadataMap, key_id: &str) -> Result<Option<Grant>, Status> {
        let Some(value) = metadata.get(GRANT_HEADER) else {
            return Ok(None);
        };
        let grant = value
            .to_str()
            .ok()
            .and_then(|v| hex::decode(v.trim()).ok())
            .and_then(|bytes| Grant::decode(bytes.as_slice()).ok())
            .ok_or_else(|| Status::invalid_argument("kernable-grant is not a hex-encoded Grant"))?;
        if grant.key_id != key_id {
            return Err(Status::permission_denied("the grant is for another key"));
        }
        Ok(Some(grant))
    }

    /// Revokes `key_id` from `since` on: stores the revocation signed by the kernel, refuses the
    /// key from now on and ends its sessions. Never undone; a second revocation is ignored.
    pub fn revoke(&self, key_id: &str, since: i64) -> Result<(), StoreError> {
        if self.revoked_since(key_id).is_some() {
            return Ok(());
        }
        let created_at = now_ms();
        let msg = signing::revocation_message(key_id, since, created_at);
        self.store.revoke(&RevocationRecord {
            key_id: key_id.to_owned(),
            since,
            created_at,
            kernel_sig: Some(signing::sign(&self.key(), &msg)),
        })?;
        self.sync(0);
        crash::point("revocation.stored");
        tracing::warn!(key = %key_id, since, "key revoked");
        self.revoked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key_id.to_owned(), since);
        self.update_permissions();
        self.changes.send_modify(|n| *n += 1);
        self.work.notify_waiters();
        Ok(())
    }

    fn update_permissions(&self) {
        let grants = self.grants.lock().unwrap_or_else(PoisonError::into_inner);
        self.set_permissions(&grants);
    }

    /// Rebuilds the permissions from `grants` (held locked by the caller) and the revocations.
    fn set_permissions(&self, grants: &HashMap<String, Grant>) {
        let permissions = {
            let revoked = self.revoked.lock().unwrap_or_else(PoisonError::into_inner);
            Permissions::new(grants, &revoked)
        };
        *self
            .permissions
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(permissions);
    }

    /// The vault, or a refusal for requests that need one.
    fn vault(&self) -> Result<&Vault, Status> {
        self.vault.as_ref().ok_or_else(|| {
            Status::failed_precondition("this kernel has no vault (kernable start --vault-key)")
        })
    }

    /// The values of `refs` that the vault still holds (erased ones are missing).
    fn values_of(&self, refs: &[String]) -> Vec<VaultValue> {
        let Some(vault) = &self.vault else {
            return Vec::new();
        };
        vault.get(refs).unwrap_or_else(|e| {
            tracing::error!(error = %e, "reading the vault failed");
            Vec::new()
        })
    }

    /// Whether the listener or middleware key `key_id` gets the vault values of `name`.
    fn reads_vault(&self, key_id: &str, name: &str) -> bool {
        self.vault.is_some() && config::allowed(&self.permissions().vault_read, key_id, name)
    }

    /// Keeps a result's vault values, before the events are committed, and returns the subjects
    /// its events erase: the kernel erases them once the events are committed, so the log
    /// always records an erasure that happened.
    fn vault_result(
        &self,
        worker_key: &str,
        cmd: &Command,
        events: &[NewEvent],
        values: &[VaultValue],
    ) -> Result<Vec<String>, Rejected> {
        let refused = |code: &str, message: String| Rejected {
            code: code.into(),
            message,
            ..Default::default()
        };
        let refs: Vec<String> = events.iter().flat_map(|e| e.vault_refs.clone()).collect();
        let erasing: Vec<&NewEvent> = events.iter().filter(|e| !e.erases.is_empty()).collect();
        if refs.is_empty() && erasing.is_empty() && values.is_empty() {
            return Ok(Vec::new());
        }
        let Some(vault) = &self.vault else {
            return Err(refused("no_vault", "this kernel has no vault".into()));
        };
        for e in &erasing {
            let name = config::qualified(&cmd.domain, &e.event_type);
            let allowed = config::allowed(&self.permissions().vault_erase, worker_key, &name);
            if !allowed {
                return Err(refused(
                    "erase_not_allowed",
                    format!("worker key may not erase with {name}"),
                ));
            }
        }
        let stream = StreamId {
            tenant_id: cmd.tenant_id.clone(),
            domain: cmd.domain.clone(),
            aggregate_type: cmd.aggregate_type.clone(),
            aggregate_id: cmd.aggregate_id.clone(),
        };
        let owner = Owner {
            command_id: &cmd.command_id,
            tenant: &cmd.tenant_id,
            stream: Some(&stream),
        };
        vault
            .put(values, &refs, &owner, now_ms())
            .map_err(|e| refused("invalid_vault_values", e.to_string()))?;
        Ok(erasing.iter().map(|e| e.erases.clone()).collect())
    }

    /// Erases the subjects of a committed command's erasing events; see [`erase_subjects`].
    fn erase(&self, cmd: &Command, subjects: &[String], own: &StreamId) -> Vec<StreamId> {
        match &self.vault {
            Some(vault) => erase_subjects(
                vault,
                self.snapshots.as_ref(),
                &cmd.tenant_id,
                &cmd.command_id,
                subjects,
                own,
            ),
            None => Vec::new(),
        }
    }

    /// Deletes a completed command's secrets.
    /// Notes a changed file in the record log; with a standby, it holds the note before the
    /// client is told.
    fn data_changed(&self, r: &DataRequest) -> Result<(), Status> {
        self.store
            .data_changed(&r.tenant_id, &r.domain, &r.key)
            .map_err(store_error)?;
        if self.replication.is_some() {
            self.sync(0);
        }
        Ok(())
    }

    fn drop_secrets(&self, command_id: &str) {
        if let Some(vault) = &self.vault
            && let Err(e) = vault.drop_secrets(command_id)
        {
            tracing::error!(error = %e, "deleting secrets failed");
        }
    }

    /// Whether the kernel serves everything: not shutting down, and every registered listener
    /// connected and caught up. Otherwise why not.
    pub fn readiness(&self) -> Result<(), String> {
        if self.draining.load(SeqCst) {
            return Err("shutting down".into());
        }
        if let Some(primary) = &self.replication
            && !primary.in_sync()
        {
            return Err("waiting for a standby in sync (--replication sync)".into());
        }
        let (workers, listeners) = self.memberships.listed();
        let missing = self.workers.missing(&workers);
        if !missing.is_empty() {
            return Err(format!("waiting for workers: {}", missing.join(", ")));
        }
        match self.listeners.behind(&listeners).as_slice() {
            [] => Ok(()),
            behind => Err(format!("waiting for listeners: {}", behind.join(", "))),
        }
    }

    /// Refuses new submits and queries, and waits up to `drain_timeout_ms` for running ones
    /// to finish. Returns whether they all did. Workers and listeners stay connected meanwhile,
    /// so running commands can complete and settle.
    pub async fn drain(&self) -> bool {
        self.draining.store(true, SeqCst);
        self.idle_within(Duration::from_millis(self.config().drain_timeout_ms))
            .await
    }

    /// Waits up to `timeout` until no submit or query runs; whether none does.
    pub(crate) async fn idle_within(&self, timeout: Duration) -> bool {
        let idle = async {
            loop {
                let ended = self.idle.notified();
                if self.requests.load(SeqCst) == 0 {
                    return;
                }
                ended.await;
            }
        };
        tokio::time::timeout(timeout, idle).await.is_ok()
    }

    /// Puts every write so far on the disk, shared with concurrent callers (group commit), and
    /// records that the log is durable up to `position`. Called before anyone is told of a
    /// write. If the disk fails, the kernel stops: only a restart, reading what is on the disk,
    /// knows what was kept, and answering on would promise what may be lost.
    fn sync(&self, position: u64) {
        self.sync_disk(0);
        if let Some(primary) = &self.replication {
            match self.store.record_head() {
                Ok(head) => primary.wait(head.seq),
                Err(e) => {
                    tracing::error!(error = %e, "the record log cannot be read: stopping");
                    eprintln!("kernable: the record log cannot be read ({e}); stopping");
                    std::process::exit(70);
                }
            }
        }
        self.durable.fetch_max(position, SeqCst);
    }

    /// Like [`Kernel::sync`], without waiting for the standby: for what the standby itself needs
    /// before it can follow.
    fn sync_disk(&self, position: u64) {
        if let Err(e) = self.store.sync() {
            tracing::error!(error = %e, "the store could not put a write on the disk: stopping");
            eprintln!("kernable: the store could not put a write on the disk ({e}); stopping");
            std::process::exit(70);
        }
        if self.replication.is_none() {
            self.durable.fetch_max(position, SeqCst);
        }
    }

    /// A slot for one more request of `key_id`, or RESOURCE_EXHAUSTED at `--max-requests-per-key`
    /// or `--max-requests-per-second-per-key`.
    fn request_slot(&self, key_id: &str) -> Result<Slot, Status> {
        let rate = self.config.limits.requests_per_second_per_key;
        if rate > 0 && !self.rates.take(key_id, rate) {
            self.metrics.limited("rate");
            return Err(Status::resource_exhausted(format!(
                "rate: the key started {rate} requests in the last second \
                 (--max-requests-per-second-per-key); retry later"
            )));
        }
        let max = self.config.limits.requests_per_key;
        self.requests_by_key.take(key_id, max).ok_or_else(|| {
            self.metrics.limited("requests_per_key");
            Status::resource_exhausted(format!(
                "the key has {max} requests in progress (--max-requests-per-key); retry later"
            ))
        })
    }

    /// RESOURCE_EXHAUSTED `quota_storage` if the stores reached `--max-store-bytes` and `name` (a
    /// qualified command or file request type) is not exempt.
    fn quota(&self, name: &str) -> Result<(), Status> {
        let limits = &self.config.limits;
        let held = self.stored_bytes.load(SeqCst);
        if limits.store_bytes == 0
            || held < limits.store_bytes
            || limits.quota_exempt.iter().any(|p| config::covers(p, name))
        {
            return Ok(());
        }
        self.metrics.limited("quota_storage");
        Err(Status::resource_exhausted(format!(
            "quota_storage: the kernel holds {held} of its {} bytes (--max-store-bytes); \
             delete files or erase data, or raise the limit",
            limits.store_bytes
        )))
    }

    /// The bytes of the event store, the vault and the files on the disk now.
    fn measure_store(&self) -> u64 {
        let config = self.config();
        let mut urls = vec![config.store.clone(), config.data.clone()];
        if let Some(v) = &config.vault {
            urls.push(v.store.clone());
        }
        urls.iter().map(|u| bytes_of(u)).sum()
    }

    /// The next message of a worker's or listener's handshake, within `--handshake-timeout-ms`.
    async fn handshake<T>(
        &self,
        next: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let timeout = Duration::from_millis(self.config.limits.handshake_timeout_ms);
        tokio::time::timeout(timeout, next)
            .await
            .unwrap_or_else(|_| {
                self.metrics.limited("handshake_timeout");
                Err(Status::deadline_exceeded(
                    "the stream did not complete its handshake in time (--handshake-timeout-ms)",
                ))
            })
    }

    /// Checks a data or cache request: the signature over `msg`, its age, a valid domain and
    /// key, and that the key may do `action` (`rules`) in the domain. Returns the request's slot.
    #[allow(clippy::too_many_arguments)]
    fn authorize_store(
        &self,
        metadata: &MetadataMap,
        sig: Option<&Signature>,
        msg: &[u8],
        issued_at: i64,
        domain: &str,
        key: Option<&str>,
        action: &str,
        rules: fn(&Permissions) -> &grants::Rules,
    ) -> Result<Slot, Status> {
        let sig = sig.ok_or_else(|| Status::unauthenticated("request is not signed"))?;
        signing::verify(sig, msg)
            .map_err(|_| Status::unauthenticated("invalid request signature"))?;
        self.present(metadata, &sig.key_id)?;
        let slot = self.request_slot(&sig.key_id)?;
        if (now_ms() - issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }
        if !config::valid_domain(domain) {
            return Err(Status::invalid_argument("a domain name is needed"));
        }
        // `None`: a listing's prefix, which may be empty.
        if key.is_some_and(str::is_empty) || key.unwrap_or_default().len() > MAX_KEY_BYTES {
            return Err(Status::invalid_argument(format!(
                "a key has 1 to {MAX_KEY_BYTES} bytes"
            )));
        }
        let permissions = self.permissions();
        if !config::allowed(rules(&permissions), &sig.key_id, &format!("{domain}.*")) {
            return Err(Status::permission_denied(format!(
                "key may not {action} in {domain}"
            )));
        }
        Ok(slot)
    }

    /// Claims a data or cache write, authorized already, so that it runs at most once.
    fn claim(&self, sig: Option<&Signature>, issued_at: i64) -> Result<replays::Claim<'_>, Status> {
        let sig = sig.map_or(&[][..], |s| s.sig.as_slice());
        self.replays.claim(sig, issued_at, now_ms())
    }

    /// Counts a submit or query until the returned guard drops; refused while draining.
    fn request(&self) -> Result<Running<'_>, Status> {
        self.requests.fetch_add(1, SeqCst);
        let running = Running(self);
        if self.draining.load(SeqCst) {
            return Err(Status::unavailable("the kernel is shutting down; retry"));
        }
        if self.switching.load(SeqCst) {
            return Err(Status::unavailable(
                "the kernel hands its role over to its standby; retry",
            ));
        }
        Ok(running)
    }

    fn schemas(&self) -> MutexGuard<'_, HashMap<String, u32>> {
        self.schemas.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index of keys and facts; lock [`Kernel::schemas`] first.
    fn index(&self) -> MutexGuard<'_, index::Index> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn waiters(&self) -> MutexGuard<'_, HashMap<String, Vec<oneshot::Sender<CommandResult>>>> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes a final checkpoint and the listener positions. Leases are not persisted:
    /// after a restart, every unfinished command is delivered again right away.
    pub fn shutdown(&self) -> Result<(), BoxError> {
        // Handed over, it writes nothing more: its log stays the new primary's beginning, so it
        // follows it without an image. The new primary holds every position acknowledged.
        if self.replication.as_ref().is_some_and(|p| p.handed_over()) {
            return Ok(());
        }
        self.store.checkpoint(&self.key(), now_ms())?;
        self.listeners.seen_now();
        listeners::flush(self)?;
        self.store.save_workers(&self.workers.seen_now())?;
        Ok(())
    }

    fn complete(self: &Arc<Self>, command: &Command, result: CommandResult) {
        self.observe(command, &result);
        // The aggregate is free again: its next queued command can be leased now.
        self.work.notify_waiters();
        for waiter in self
            .waiters()
            .remove(&result.command_id)
            .unwrap_or_default()
        {
            let _ = waiter.send(result.clone());
        }
    }

    /// Runs `request` (of the qualified type `name`) through the middleware chain at `when`,
    /// with the request's `outcome` after it. `Ok(Some(reason))` if a middleware rejected it;
    /// an error if one is missing or too slow.
    async fn check(
        &self,
        name: &str,
        id: &str,
        when: When,
        request: middleware_call::Request,
        values: &[VaultValue],
        outcome: Option<middleware_call::Outcome>,
    ) -> Result<Option<Rejected>, Status> {
        let chain = self.listeners.chain(name, when);
        if chain.is_empty() {
            return Ok(None);
        }
        let started = Instant::now();
        let call = MiddlewareCall {
            call_id: String::new(),
            request: Some(request),
            vault: Vec::new(),
            when: when.into(),
            outcome,
            r#type: name.to_owned(),
        };
        let outcome = self.run_chain(&chain, name, id, call, values).await;
        let counter = match &outcome {
            Ok(None) => &self.metrics.middleware_passed,
            Ok(Some(_)) => &self.metrics.middleware_rejected,
            Err(_) => &self.metrics.middleware_failed,
        };
        counter.fetch_add(1, Relaxed);
        self.metrics.middleware.record(started.elapsed());
        outcome
    }

    async fn run_chain(
        &self,
        chain: &[String],
        name: &str,
        id: &str,
        call: MiddlewareCall,
        values: &[VaultValue],
    ) -> Result<Option<Rejected>, Status> {
        let timeout = Duration::from_millis(self.config().middleware_timeout_ms);
        for middleware in chain {
            let key = self.listeners.key(middleware).unwrap_or_default();
            let call = MiddlewareCall {
                vault: if self.reads_vault(&key, name) {
                    values.to_vec()
                } else {
                    Vec::new()
                },
                ..call.clone()
            };
            let checked = tokio::time::timeout(timeout, async {
                // Wait until the middleware has caught up with what it missed while away.
                let dispatched = loop {
                    let progressed = self.listeners.progress.notified();
                    if let Some(d) = self.listeners.check(middleware, id, call.clone())? {
                        break d;
                    }
                    progressed.await;
                };
                dispatched.call().await
            })
            .await
            .map_err(|_| {
                Status::deadline_exceeded(format!("middleware {middleware} did not answer in time"))
            })??;
            let listeners::Reply::Verdict(verdict) = checked else {
                return Err(Status::internal(format!(
                    "middleware {middleware} sent no verdict"
                )));
            };
            if let Some(mut reason) = verdict.rejected {
                tracing::info!(%middleware, code = %reason.code, "middleware rejected a request");
                // Only the kernel names a key or requirement.
                reason.key.clear();
                reason.requirement = None;
                return Ok(Some(reason));
            }
        }
        Ok(None)
    }

    /// Tells the middleware after `command` its result, in the background: nobody waits for
    /// it, and a missing or failing middleware is only logged.
    fn observe(self: &Arc<Self>, command: &Command, result: &CommandResult) {
        let name = config::qualified(&command.domain, &command.command_type);
        if self.listeners.chain(&name, When::After).is_empty() {
            return;
        }
        let kernel = Arc::clone(self);
        let (command, result) = (command.clone(), result.clone());
        tokio::spawn(async move {
            let id = command.command_id.clone();
            let request = middleware_call::Request::Command(command);
            let outcome = middleware_call::Outcome::CommandResult(result);
            let checked = kernel
                .check(&name, &id, When::After, request, &[], Some(outcome))
                .await;
            if let Err(e) = checked {
                tracing::warn!(command = %id, error = %e.message(), "middleware after a command missed it");
            }
        });
    }

    /// Tells the middleware after `query` that it was rejected, like [`Kernel::observe`].
    fn observe_query(self: &Arc<Self>, name: &str, query: &QueryRequest, reason: &Rejected) {
        if self.listeners.chain(name, When::After).is_empty() {
            return;
        }
        let kernel = Arc::clone(self);
        let name = name.to_owned();
        let result = QueryResult {
            query_id: query.query_id.clone(),
            outcome: Some(query_result::Outcome::Rejected(reason.clone())),
        };
        let query = query.clone();
        tokio::spawn(async move {
            let id = query.query_id.clone();
            let request = middleware_call::Request::Query(query);
            let outcome = middleware_call::Outcome::QueryResult(result);
            let checked = kernel
                .check(&name, &id, When::After, request, &[], Some(outcome))
                .await;
            if let Err(e) = checked {
                tracing::warn!(query = %id, error = %e.message(), "middleware after a query missed it");
            }
        });
    }

    /// The listener `name` that sent a request from its handler (none if empty), and a refusal
    /// if the request (of the qualified type `request_type`) would have to wait for it.
    fn caller(
        &self,
        name: &str,
        request_type: &str,
        waits_after: bool,
    ) -> Result<Option<listeners::Caller>, Status> {
        if name.is_empty() {
            return Ok(None);
        }
        if !config::valid_name(name) {
            return Err(Status::invalid_argument(
                "a listener name has 1 to 128 letters, digits, `-`, `_` and `.`",
            ));
        }
        let caller = self
            .listeners
            .caller(name)
            .ok_or_else(|| Status::invalid_argument(format!("no listener {name}")))?;
        let checks_itself = self
            .listeners
            .chain(request_type, When::Before)
            .contains(&caller.name)
            || waits_after
                && self
                    .listeners
                    .chain(request_type, When::After)
                    .contains(&caller.name);
        if checks_itself {
            return Err(Status::failed_precondition(format!(
                "middleware {name} checks {request_type} itself: a request from its own handler \
                 would wait behind itself"
            )));
        }
        Ok(Some(caller))
    }

    /// Waits until every registered listener (for a `caller`, every listener before it) has
    /// acknowledged the result's events. Returns whether they did within `settle_timeout_ms`.
    async fn settle(&self, result: &CommandResult, caller: Option<&listeners::Caller>) -> bool {
        let Some(Outcome::Committed(c)) = &result.outcome else {
            return true;
        };
        let Some(last) = c.events.last().map(|e| e.global_position) else {
            return true;
        };
        let settled = async {
            loop {
                let progressed = self.listeners.progress.notified();
                if self.listeners.settled(last, caller) {
                    return;
                }
                progressed.await;
            }
        };
        let timeout = Duration::from_millis(self.config().settle_timeout_ms);
        tokio::time::timeout(timeout, settled).await.is_ok()
    }
}

/// Whether the TCP address `host:port` only accepts connections from this machine.
/// A kernel's socket: its owner and group may connect, others only through an ACL that grants
/// them (the default mode would let others read it but not connect, and leave an ACL's write
/// masked off).
#[cfg(unix)]
pub fn socket_mode(path: &str) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))
}

fn loopback(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Opens with `open`; while another process holds the stores, tries again for up to
/// `take_over_ms`: a kernel update on the same machine, where the old kernel lets go of its
/// stores once its running requests finished, and this one opens them the moment it does.
pub(crate) fn taking_over<T>(
    take_over_ms: u64,
    mut open: impl FnMut() -> Result<T, BoxError>,
) -> Result<T, BoxError> {
    let deadline = std::time::Instant::now() + Duration::from_millis(take_over_ms);
    let mut waited = false;
    loop {
        match open() {
            Err(e) if in_use(&e) && std::time::Instant::now() < deadline => {
                if !waited {
                    tracing::info!("another kernel holds the stores: waiting to take them over");
                    waited = true;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(opened) => {
                if waited {
                    tracing::warn!("took the stores over from the kernel before");
                }
                return Ok(opened);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether `e` says another process holds a store (its lock), as a kernel being replaced does.
fn in_use(e: &BoxError) -> bool {
    e.to_string().contains("in use by another process")
}

/// Serves the gRPC API on `config.listen` and writes checkpoints until dropped.
pub async fn serve(kernel: Arc<Kernel>) -> Result<(), BoxError> {
    let api = Api(Arc::clone(&kernel));
    let mut builder = Server::builder();
    let listen = kernel.config().listen.clone();
    if kernel.config().tls.is_none()
        && transport::unix_path(&listen).is_none()
        && !loopback(&listen)
    {
        return Err(format!(
            "{listen} is reachable from other machines: plain TCP only on a loopback address \
             (127.0.0.1:7233); elsewhere, use mutual TLS (--tls-cert, --tls-key, --tls-client-ca)"
        )
        .into());
    }
    if let Some(tls) = &kernel.config().tls {
        if transport::unix_path(&kernel.config().listen).is_some() {
            return Err(
                "TLS applies to TCP only; drop the --tls flags or listen on host:port".into(),
            );
        }
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let client_ca = Certificate::from_pem(std::fs::read(&tls.client_ca)?);
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )?;
    }
    let limits = kernel.config().limits.clone();
    let max = limits.message_bytes;
    let data_max = max.max(kernel.config.limits.data_bytes + (64 << 10));
    let cache_max = max.max(MAX_CACHE_BYTES + (64 << 10));
    let router = builder
        .max_concurrent_streams(Some(limits.streams_per_connection))
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(
            CommandServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            EventServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            ListenerServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            QueryServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            TrustServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            MembershipServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        )
        .add_service(
            DataServiceServer::new(api.clone())
                .max_decoding_message_size(data_max)
                .max_encoding_message_size(data_max),
        )
        .add_service(
            CacheServiceServer::new(api.clone())
                .max_decoding_message_size(cache_max)
                .max_encoding_message_size(cache_max),
        )
        .add_service(PartitionServiceServer::new(api.clone()))
        .add_service(GlobalKeyServiceServer::new(api.clone()))
        .add_service(
            MoveServiceServer::new(api.clone())
                .max_encoding_message_size(replication::MAX_MESSAGE_BYTES),
        )
        .add_service(TrustSyncServiceServer::new(api.clone()))
        .add_service(
            ExportServiceServer::new(api.clone())
                .max_encoding_message_size(replication::MAX_MESSAGE_BYTES),
        )
        .add_service(
            ReplicationServiceServer::new(api.clone())
                .max_decoding_message_size(max)
                .max_encoding_message_size(replication::MAX_MESSAGE_BYTES),
        )
        .add_service(
            WorkerGatewayServer::new(api)
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
        );

    let server = async {
        match transport::unix_path(&listen) {
            #[cfg(unix)]
            Some(path) => {
                use std::os::unix::fs::FileTypeExt;
                // Replace a stale socket from a previous run, but never any other file.
                if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                    std::fs::remove_file(path)?;
                }
                let listener = tokio::net::UnixListener::bind(path)?;
                socket_mode(path)?;
                let incoming = connections::limited(Arc::clone(&kernel), listener);
                router.serve_with_incoming(incoming).await?;
            }
            #[cfg(not(unix))]
            Some(_) => return Err("unix: sockets need Unix; listen on host:port".into()),
            None => {
                let listener = TcpListener::bind(&listen).await?;
                let incoming = connections::limited(Arc::clone(&kernel), listener);
                router.serve_with_incoming(incoming).await?;
            }
        }
        Ok(())
    };
    tokio::select! {
        result = server => result,
        _ = checkpoints(&kernel) => unreachable!(),
        _ = measure_stores(&kernel) => unreachable!(),
        _ = flush_listeners(&kernel) => unreachable!(),
        _ = sample_trend(&kernel) => unreachable!(),
        () = partitions::follow_control(&kernel) => unreachable!(),
        () = moves::follow_moves(&kernel) => unreachable!(),
        result = serve_metrics(&kernel) => result,
        result = serve_ui(&kernel) => result,
    }
}

async fn serve_ui(kernel: &Arc<Kernel>) -> Result<(), BoxError> {
    match &kernel.config().ui_listen {
        Some(addr) if !loopback(addr) => Err(format!(
            "--ui-listen {addr}: the UI has no login, so it listens on a loopback address only (127.0.0.1:7234)"
        )
        .into()),
        // The UI is a convenience: if its address is taken (another dev kernel), the kernel
        // runs on without it.
        Some(addr) => {
            if let Err(e) = ui::serve(Arc::clone(kernel), addr).await {
                tracing::warn!(%addr, error = %e, "the UI is off");
            }
            std::future::pending().await
        }
        None => std::future::pending().await,
    }
}

async fn serve_metrics(kernel: &Arc<Kernel>) -> Result<(), BoxError> {
    match &kernel.config().metrics_listen {
        Some(addr) => metrics::serve(Arc::clone(kernel), addr).await,
        None => std::future::pending().await,
    }
}

async fn flush_listeners(kernel: &Kernel) {
    let mut tick = tokio::time::interval(listeners::FLUSH);
    loop {
        tick.tick().await;
        if let Err(e) = listeners::flush(kernel) {
            tracing::error!(error = %e, "saving listener positions failed");
        }
    }
}

async fn sample_trend(kernel: &Kernel) {
    let mut tick = tokio::time::interval(trend::EVERY);
    loop {
        tick.tick().await;
        match kernel.store.head_position() {
            Ok(head) => kernel
                .trend
                .record(now_ms(), head, &kernel.listeners.snapshot()),
            Err(e) => tracing::warn!(error = %e, "sampling listener positions failed"),
        }
    }
}

/// The bytes a store at `url` takes on the disk: a SQLite file with its journal, a file log, or
/// a directory of files.
fn bytes_of(url: &str) -> u64 {
    fn walk(path: &std::path::Path) -> u64 {
        match std::fs::symlink_metadata(path) {
            Ok(m) if m.is_dir() => std::fs::read_dir(path)
                .map(|entries| entries.flatten().map(|e| walk(&e.path())).sum())
                .unwrap_or(0),
            Ok(m) => m.len(),
            Err(_) => 0,
        }
    }
    match kernable_store::backend(url) {
        ("memory", _) => 0,
        ("sqlite", path) => ["", "-wal", "-shm"]
            .iter()
            .map(|suffix| walk(std::path::Path::new(&format!("{path}{suffix}"))))
            .sum(),
        (_, path) => walk(std::path::Path::new(path)),
    }
}

/// Measures the stores every 10 s, and warns once they pass 80 % of `--max-store-bytes`.
async fn measure_stores(kernel: &Arc<Kernel>) {
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    let mut warned = false;
    loop {
        tick.tick().await;
        let k = Arc::clone(kernel);
        let Ok(held) = tokio::task::spawn_blocking(move || k.measure_store()).await else {
            continue;
        };
        kernel.stored_bytes.store(held, SeqCst);
        let max = kernel.config().limits.store_bytes;
        if max == 0 {
            continue;
        }
        let high = held.saturating_mul(10) >= max.saturating_mul(8);
        if high && !warned {
            tracing::warn!(
                held,
                max,
                "the stores passed 80 % of --max-store-bytes; at the limit, new commands and file puts are refused"
            );
        }
        warned = high;
    }
}

async fn checkpoints(kernel: &Kernel) {
    let mut tick = tokio::time::interval(Duration::from_millis(
        kernel.config().checkpoint_interval_ms,
    ));
    loop {
        tick.tick().await;
        match kernel.store.checkpoint(&kernel.key(), now_ms()) {
            Ok(Some(_)) => crash::point("checkpoint.stored"),
            Ok(None) => {}
            Err(e) => tracing::error!(error = %e, "checkpoint failed"),
        }
    }
}

#[derive(Clone)]
struct Api(Arc<Kernel>);

#[tonic::async_trait]
impl TrustService for Api {
    async fn revoke(
        &self,
        request: Request<RevokeRequest>,
    ) -> Result<Response<RevokeResponse>, Status> {
        let kernel = &self.0;
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .root_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("revocation is not signed"))?;
        kernel.by_root(
            sig,
            &signing::revoke_message(&req.key_id, req.since, req.issued_at),
        )?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "revocation expired; check the clock",
            ));
        }
        if signing::verifying_key_from_hex(&req.key_id).is_err() {
            return Err(Status::invalid_argument("not a key ID"));
        }
        kernel.revoke(&req.key_id, req.since).map_err(internal)?;
        Ok(Response::new(RevokeResponse {}))
    }
}

#[tonic::async_trait]
impl CommandService for Api {
    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<CommandResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        protocol(request.protocol)?;
        let _running = kernel.request()?;
        let wait = request.wait();
        let listener = request.listener;
        let values = request.vault;
        let cmd = request
            .command
            .ok_or_else(|| Status::invalid_argument("no command"))?;
        kernel.owns(&cmd.tenant_id)?;
        let sig = cmd
            .client_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("command is not signed"))?;
        signing::verify(sig, &signing::command_message(&cmd))
            .map_err(|_| Status::unauthenticated("invalid command signature"))?;
        kernel.present(&metadata, &sig.key_id)?;
        let _slot = kernel.request_slot(&sig.key_id)?;
        let permissions = kernel.permissions();
        if !config::valid_domain(&cmd.domain) || !config::valid_type(&cmd.command_type) {
            return Err(Status::invalid_argument(
                "a command needs a domain name and a command type without dots",
            ));
        }
        let name = config::qualified(&cmd.domain, &cmd.command_type);
        if !config::allowed(&permissions.submit, &sig.key_id, &name) {
            return Err(Status::permission_denied(format!(
                "key may not submit {name}"
            )));
        }
        let bypass = cmd.context.as_ref().is_some_and(|c| c.bypass_authorization);
        if bypass && !config::allowed(&permissions.privileged, &sig.key_id, "bypass_authorization")
        {
            return Err(Status::permission_denied(
                "key may not set bypass_authorization",
            ));
        }
        let caller = kernel.caller(&listener, &name, false)?;
        if !cmd.vault_refs.is_empty() || !values.is_empty() {
            kernel.vault()?;
            vault::check(&values, &cmd.vault_refs)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        let aggregate = config::qualified(&cmd.domain, &cmd.aggregate_type);
        if !kernel.workers.runs(kernel, &aggregate, &name) {
            return Err(Status::not_found(format!(
                "no worker registered {name} for {aggregate}"
            )));
        }
        // A full store refuses new commands; a stored one is always answered.
        if let Err(status) = kernel.quota(&name)
            && kernel
                .store
                .command(&cmd.command_id)
                .map_err(internal)?
                .is_none()
        {
            return Err(status);
        }
        // A full queue refuses new commands before anything is stored; a stored one is always
        // answered.
        if let Err(reason) = kernel.queue.room(&cmd, &kernel.config().limits)
            && kernel
                .store
                .command(&cmd.command_id)
                .map_err(internal)?
                .is_none()
        {
            kernel.metrics.limited(reason);
            return Err(Status::resource_exhausted(format!(
                "the command queue is full ({reason}); retry later"
            )));
        }
        let request = middleware_call::Request::Command(cmd.clone());
        if let Some(reason) = kernel
            .check(&name, &cmd.command_id, When::Before, request, &values, None)
            .await?
        {
            let result = CommandResult {
                command_id: cmd.command_id.clone(),
                outcome: Some(Outcome::Rejected(reason)),
            };
            kernel.observe(&cmd, &result);
            return Ok(Response::new(result));
        }

        if let Some(vault) = &kernel.vault
            && !cmd.vault_refs.is_empty()
        {
            // A resubmitted command that already completed needs no values: its secrets are
            // gone, and it only gets its stored result. Checked again on a failure, in case it
            // completed in between.
            let completed = || {
                kernel
                    .store
                    .result(&cmd.command_id)
                    .map(|r| r.is_some())
                    .map_err(internal)
            };
            let owner = Owner {
                command_id: &cmd.command_id,
                tenant: &cmd.tenant_id,
                stream: None,
            };
            if !completed()? {
                match vault.put(&values, &cmd.vault_refs, &owner, now_ms()) {
                    Ok(()) => crash::point("vault.stored"),
                    Err(e) if !completed()? => {
                        return Err(Status::invalid_argument(e.to_string()));
                    }
                    Err(_) => {}
                }
            }
        }

        // Register before enqueueing, so a fast completion cannot be missed. Waiters of
        // earlier submits that gave up are dropped here.
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = kernel.waiters();
            let waiting = waiters.entry(cmd.command_id.clone()).or_default();
            waiting.retain(|w| !w.is_closed());
            waiting.push(tx);
        }
        let received_at = now_ms();
        let enqueued = match kernel.store.enqueue(&cmd, received_at) {
            Ok(enqueued) => enqueued,
            Err(e) => {
                // Not answered: our waiter goes, and the entry with it if it was the last.
                drop(rx);
                let mut waiters = kernel.waiters();
                if let Some(waiting) = waiters.get_mut(&cmd.command_id) {
                    waiting.retain(|w| !w.is_closed());
                    if waiting.is_empty() {
                        waiters.remove(&cmd.command_id);
                    }
                }
                return Err(internal(e));
            }
        };
        let done = match enqueued {
            Enqueued::Duplicate {
                result: Some(result),
            } => {
                kernel.sync(last_position(&result));
                kernel.waiters().remove(&cmd.command_id);
                kernel.drop_secrets(&cmd.command_id); // a resubmission brought them again
                Some(result)
            }
            Enqueued::Duplicate { result: None } => {
                // Stored and pending, but perhaps not queued: an enqueue that failed after it
                // took effect. Queue it as stored (a queued one is not queued twice).
                if let Some(stored) = kernel.store.command(&cmd.command_id).map_err(internal)?
                    && kernel.queue.push(stored.command, stored.received_at)
                {
                    kernel.work.notify_waiters();
                }
                None
            }
            Enqueued::New => {
                crash::point("submit.enqueued");
                kernel.queue.push(cmd.clone(), received_at);
                kernel.work.notify_waiters();
                None
            }
        };
        let mut result = match done {
            Some(result) => result,
            None if wait == Wait::Accepted => {
                // Told it is accepted, the client may never send it again: on the disk first.
                kernel.sync(0);
                kernel.waiters().remove(&cmd.command_id);
                return Ok(Response::new(CommandResult {
                    command_id: cmd.command_id,
                    outcome: Some(Outcome::Accepted(Accepted {})),
                }));
            }
            None => {
                let timeout = Duration::from_millis(kernel.config().submit_timeout_ms);
                match tokio::time::timeout(timeout, rx).await {
                    Ok(Ok(result)) => result,
                    _ => {
                        return Err(Status::deadline_exceeded(
                            "command accepted but not completed yet; resubmit it to get the result",
                        ));
                    }
                }
            }
        };
        if wait == Wait::Settled {
            let started = Instant::now();
            if kernel.settle(&result, caller.as_ref()).await
                && let Some(Outcome::Committed(c)) = &mut result.outcome
            {
                c.settled = true;
                kernel.metrics.settle.record(started.elapsed());
                crash::point("settle.done");
            }
        }
        Ok(Response::new(result))
    }
}

#[tonic::async_trait]
impl EventService for Api {
    type SubscribeStream = ReceiverStream<Result<Event, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let kernel = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        if kernel.draining.load(SeqCst) {
            return Err(Status::unavailable("the kernel is shutting down; retry"));
        }
        let sig = req
            .client_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("request is not signed"))?;
        let msg = signing::subscribe_message(req.after_position, &req.event_types, req.issued_at);
        signing::verify(sig, &msg)
            .map_err(|_| Status::unauthenticated("invalid request signature"))?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }

        kernel.present(&metadata, &sig.key_id)?;
        config::readable(
            &kernel.permissions().subscribe,
            &sig.key_id,
            &req.event_types,
        )
        .map_err(Status::permission_denied)?;
        let max = kernel.config.limits.subscriptions_per_key;
        let slot = kernel
            .subscriptions_by_key
            .take(&sig.key_id, max)
            .ok_or_else(|| {
                kernel.metrics.limited("subscriptions_per_key");
                Status::resource_exhausted(format!(
                    "the key has {max} open subscriptions (--max-subscriptions-per-key)"
                ))
            })?;

        let (tx, rx) = mpsc::channel(SUBSCRIBE_BATCH as usize);
        let reader = (sig.key_id.clone(), req.event_types.clone());
        tokio::spawn(async move {
            let _slot = slot;
            stream_events(kernel, req.after_position, reader, tx).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Sends stored events after `after` (backlog first, then live) until the subscriber leaves.
/// Always reads from the store, so a subscriber can never miss or reorder events.
async fn stream_events(
    kernel: Arc<Kernel>,
    mut after: u64,
    (key_id, requested): (String, Vec<String>),
    tx: mpsc::Sender<Result<Event, Status>>,
) {
    loop {
        // The subscriber's key may have been revoked or lost its rights to a newer grant; a
        // subscription to every type gets what the grant allows now.
        let types = match config::readable(&kernel.permissions().subscribe, &key_id, &requested) {
            Ok(types) => types,
            Err(_) => {
                let status = Status::permission_denied("subscriber key is no longer allowed");
                let _ = tx.send(Err(status)).await;
                return;
            }
        };
        let appended = kernel.appended.notified();
        let durable = kernel.durable.load(SeqCst);
        let batch = if after < durable {
            match kernel.store.read_all(after, SUBSCRIBE_BATCH) {
                Ok(mut batch) => {
                    batch.retain(|e| e.global_position <= durable);
                    batch
                }
                Err(e) => {
                    let _ = tx.send(Err(internal(e))).await;
                    return;
                }
            }
        } else {
            Vec::new()
        };
        if batch.is_empty() {
            tokio::select! {
                _ = appended => {}
                _ = tx.closed() => return,
            }
            continue;
        }
        for event in batch {
            after = event.global_position;
            let wanted = wanted(types.as_deref(), &event);
            if wanted && tx.send(Ok(event)).await.is_err() {
                return;
            }
        }
    }
}

fn data_key(r: &DataRequest) -> DataKey {
    DataKey {
        tenant_id: r.tenant_id.clone(),
        domain: r.domain.clone(),
        key: r.key.clone(),
    }
}

fn store_error(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

impl Api {
    /// Runs the middleware chain on a file or cache request: the middlewares that check
    /// `<domain>.<type_name>` decide, before the request runs. A refusal is PERMISSION_DENIED.
    async fn check_store(
        &self,
        domain: &str,
        type_name: &str,
        request: middleware_call::Request,
    ) -> Result<(), Status> {
        let name = config::qualified(domain, type_name);
        let id = format!("{name}@{}", now_ms());
        match self
            .0
            .check(&name, &id, When::Before, request, &[], None)
            .await?
        {
            Some(reason) => Err(Status::permission_denied(format!(
                "{}: {}",
                reason.code, reason.message
            ))),
            None => Ok(()),
        }
    }

    /// Checks a data request for `op` (`put`, `get`, `delete`): signature, grant, middleware.
    async fn data_request(
        &self,
        request: Request<DataRequest>,
        op: &str,
    ) -> Result<(DataRequest, Slot), Status> {
        let metadata = request.metadata().clone();
        let r = request.into_inner();
        protocol(r.protocol)?;
        self.0.owns(&r.tenant_id)?;
        let (action, rules): (_, fn(&Permissions) -> &grants::Rules) = match op {
            "get" | "list" => ("read files", |p| &p.data_read),
            _ => ("write files", |p| &p.data_write),
        };
        let msg = signing::data_message(op, &r);
        let slot = self.0.authorize_store(
            &metadata,
            r.client_sig.as_ref(),
            &msg,
            r.issued_at,
            &r.domain,
            (op != "list").then_some(r.key.as_str()),
            action,
            rules,
        )?;
        let type_name = match op {
            "put" => "PutData",
            "get" => "GetData",
            "list" => "ListData",
            _ => "DeleteData",
        };
        // The middleware decides on who asks for which file, not on its content.
        let checked = DataRequest {
            data: Vec::new(),
            ..r.clone()
        };
        self.check_store(
            &r.domain,
            type_name,
            middleware_call::Request::Data(checked),
        )
        .await?;
        Ok((r, slot))
    }

    /// Checks a cache request for `op` (`set`, `get`, `delete`): signature, grant, middleware.
    async fn cache_request(
        &self,
        request: Request<CacheRequest>,
        op: &str,
    ) -> Result<(CacheRequest, Slot), Status> {
        let metadata = request.metadata().clone();
        let r = request.into_inner();
        protocol(r.protocol)?;
        self.0.owns(&r.tenant_id)?;
        let msg = signing::cache_message(op, &r);
        let slot = self.0.authorize_store(
            &metadata,
            r.client_sig.as_ref(),
            &msg,
            r.issued_at,
            &r.domain,
            Some(&r.key),
            "use the cache",
            |p| &p.cache,
        )?;
        let type_name = match op {
            "set" => "SetCache",
            "get" => "GetCache",
            _ => "DeleteCache",
        };
        let checked = CacheRequest {
            value: Vec::new(),
            ..r.clone()
        };
        self.check_store(
            &r.domain,
            type_name,
            middleware_call::Request::Cache(checked),
        )
        .await?;
        Ok((r, slot))
    }
}

#[tonic::async_trait]
impl DataService for Api {
    async fn put_data(
        &self,
        request: Request<DataRequest>,
    ) -> Result<Response<DataResult>, Status> {
        let (r, _slot) = self.data_request(request, "put").await?;
        let max = self.0.config.limits.data_bytes;
        if r.data.len() > max {
            return Err(Status::invalid_argument(format!(
                "a file has at most {max} bytes (--max-data-bytes)"
            )));
        }
        let claim = self.0.claim(r.client_sig.as_ref(), r.issued_at)?;
        self.0.quota(&config::qualified(&r.domain, "PutData"))?;
        let data = Data {
            content_type: r.content_type.clone(),
            data: r.data.clone(),
        };
        let stored = self.0.data.put(&data_key(&r), &data);
        // Tried: it may have taken effect even if it failed, so its signature stays used and a
        // captured copy can never undo a newer write. A resend is signed anew.
        claim.done();
        stored.map_err(store_error)?;
        self.0.data_changed(&r)?;
        Ok(Response::new(DataResult {
            found: true,
            sha256: Sha256::digest(&r.data).to_vec(),
            ..Default::default()
        }))
    }

    async fn get_data(
        &self,
        request: Request<DataRequest>,
    ) -> Result<Response<DataResult>, Status> {
        let (r, _slot) = self.data_request(request, "get").await?;
        let result = match self.0.data.get(&data_key(&r)).map_err(store_error)? {
            Some(d) => DataResult {
                found: true,
                sha256: Sha256::digest(&d.data).to_vec(),
                data: d.data,
                content_type: d.content_type,
            },
            None => DataResult::default(),
        };
        Ok(Response::new(result))
    }

    async fn list_data(&self, request: Request<DataRequest>) -> Result<Response<DataList>, Status> {
        let (r, _slot) = self.data_request(request, "list").await?;
        let k = Arc::clone(&self.0);
        let files = tokio::task::spawn_blocking(move || {
            k.data
                .list(&r.tenant_id, &r.domain, &r.key, &r.after, DATA_PAGE + 1)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(store_error)?;
        let more = files.len() > DATA_PAGE;
        let files: Vec<DataEntry> = files
            .into_iter()
            .take(DATA_PAGE)
            .map(|f| DataEntry {
                key: f.key,
                content_type: f.content_type,
                size: f.size,
            })
            .collect();
        let next = if more {
            files.last().map(|f| f.key.clone()).unwrap_or_default()
        } else {
            String::new()
        };
        Ok(Response::new(DataList { files, next }))
    }

    async fn delete_data(
        &self,
        request: Request<DataRequest>,
    ) -> Result<Response<DataResult>, Status> {
        let (r, _slot) = self.data_request(request, "delete").await?;
        let claim = self.0.claim(r.client_sig.as_ref(), r.issued_at)?;
        let deleted = self.0.data.delete(&data_key(&r));
        // Tried: as for a put, its signature stays used even if it failed.
        claim.done();
        let found = deleted.map_err(store_error)?;
        self.0.data_changed(&r)?;
        Ok(Response::new(DataResult {
            found,
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl CacheService for Api {
    async fn set_cache(
        &self,
        request: Request<CacheRequest>,
    ) -> Result<Response<CacheResult>, Status> {
        let (r, _slot) = self.cache_request(request, "set").await?;
        if r.value.len() > MAX_CACHE_BYTES {
            return Err(Status::invalid_argument(format!(
                "a cache value has at most {} MiB",
                MAX_CACHE_BYTES >> 20
            )));
        }
        let claim = self.0.claim(r.client_sig.as_ref(), r.issued_at)?;
        let kept = self
            .0
            .cache
            .set((r.tenant_id, r.domain, r.key), r.value, r.ttl_ms);
        if !kept {
            self.0.metrics.limited("cache_bytes");
            return Err(Status::resource_exhausted(
                "the value is larger than the whole cache (--max-cache-bytes)",
            ));
        }
        claim.done();
        Ok(Response::new(CacheResult {
            found: true,
            value: Vec::new(),
        }))
    }

    async fn get_cache(
        &self,
        request: Request<CacheRequest>,
    ) -> Result<Response<CacheResult>, Status> {
        let (r, _slot) = self.cache_request(request, "get").await?;
        let value = self.0.cache.get((r.tenant_id, r.domain, r.key));
        Ok(Response::new(CacheResult {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    async fn delete_cache(
        &self,
        request: Request<CacheRequest>,
    ) -> Result<Response<CacheResult>, Status> {
        let (r, _slot) = self.cache_request(request, "delete").await?;
        let claim = self.0.claim(r.client_sig.as_ref(), r.issued_at)?;
        let found = self.0.cache.delete((r.tenant_id, r.domain, r.key));
        claim.done();
        Ok(Response::new(CacheResult {
            found,
            value: Vec::new(),
        }))
    }
}

/// A wait-only query (`QueryRequest.wait_only`): an empty response once a read model that
/// answers `name` has processed this kernel's head as of now (and `min_position`). It reads
/// nothing, so no middleware checks it.
async fn wait_only(
    kernel: &Kernel,
    name: &str,
    query: QueryRequest,
    caller: Option<&listeners::Caller>,
) -> Result<Response<QueryResult>, Status> {
    let target = query.min_position.max(kernel.durable.load(SeqCst));
    let probe = QueryRequest {
        min_position: target,
        ..query.clone()
    };
    if !kernel.listeners.answers(name, &probe, caller) {
        return Err(Status::unavailable(format!(
            "no read model answers {name}{}",
            if caller.is_some() {
                " without waiting behind the listener that asks"
            } else {
                ""
            }
        )));
    }
    let timeout = Duration::from_millis(kernel.config().query_timeout_ms);
    tokio::time::timeout(timeout, async {
        loop {
            let progressed = kernel.listeners.progress.notified();
            if kernel.listeners.caught_up(name, &probe, caller) {
                return;
            }
            progressed.await;
        }
    })
    .await
    .map_err(|_| {
        Status::deadline_exceeded(format!(
            "no read model that answers {name} reached position {target} in time"
        ))
    })?;
    Ok(Response::new(QueryResult {
        query_id: query.query_id,
        outcome: Some(query_result::Outcome::Response(Vec::new())),
    }))
}

#[tonic::async_trait]
impl QueryService for Api {
    async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let query = request.into_inner();
        protocol(query.protocol)?;
        if !query.wait_only {
            // A wait-only query reads nothing of its tenant: every partition takes it.
            kernel.owns(&query.tenant_id)?;
        }
        let _running = kernel.request()?;
        let sig = query
            .client_sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("query is not signed"))?;
        signing::verify(sig, &signing::query_message(&query))
            .map_err(|_| Status::unauthenticated("invalid query signature"))?;
        kernel.present(&metadata, &sig.key_id)?;
        let _slot = kernel.request_slot(&sig.key_id)?;
        let permissions = kernel.permissions();
        if (now_ms() - query.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("query expired; check the clock"));
        }
        if !config::valid_domain(&query.domain) || !config::valid_type(&query.query_type) {
            return Err(Status::invalid_argument(
                "a query needs a domain name and a query type without dots",
            ));
        }
        let name = config::qualified(&query.domain, &query.query_type);
        if !config::allowed(&permissions.query, &sig.key_id, &name) {
            return Err(Status::permission_denied(format!("key may not ask {name}")));
        }
        let bypass = query
            .context
            .as_ref()
            .is_some_and(|c| c.bypass_authorization);
        if bypass && !config::allowed(&permissions.privileged, &sig.key_id, "bypass_authorization")
        {
            return Err(Status::permission_denied(
                "key may not set bypass_authorization",
            ));
        }
        let caller = kernel.caller(&query.listener, &name, true)?;
        if query.wait_only {
            return wait_only(kernel, &name, query, caller.as_ref()).await;
        }
        let request = middleware_call::Request::Query(query.clone());
        if let Some(reason) = kernel
            .check(&name, &query.query_id, When::Before, request, &[], None)
            .await?
        {
            kernel.metrics.queries_rejected.fetch_add(1, Relaxed);
            kernel.observe_query(&name, &query, &reason);
            return Ok(Response::new(QueryResult {
                query_id: query.query_id,
                outcome: Some(query_result::Outcome::Rejected(reason)),
            }));
        }
        if !kernel.listeners.answers(&name, &query, caller.as_ref()) {
            kernel.metrics.queries_failed.fetch_add(1, Relaxed);
            if let Some(c) = &caller
                && kernel.listeners.answers(&name, &query, None)
            {
                return Err(Status::failed_precondition(format!(
                    "{name} is answered only by {} itself or by listeners after it: a query \
                     from its handler would wait behind itself",
                    c.name
                )));
            }
            return Err(Status::unavailable(format!("no read model answers {name}")));
        }

        let started = Instant::now();
        let timeout = Duration::from_millis(kernel.config().query_timeout_ms);
        let answered = tokio::time::timeout(timeout, async {
            // Wait until a read model that answers the query has caught up.
            let dispatched = loop {
                let progressed = kernel.listeners.progress.notified();
                if let Some(d) = kernel.listeners.dispatch(&name, &query, caller.as_ref()) {
                    break d;
                }
                progressed.await;
            };
            dispatched.call().await
        })
        .await;
        let answer = match answered {
            Ok(Ok(listeners::Reply::Answer(answer))) => answer,
            Ok(Ok(listeners::Reply::Verdict(_))) => {
                return Err(Status::internal(
                    "the read model sent a verdict, not an answer",
                ));
            }
            Ok(Err(status)) => {
                kernel.metrics.queries_failed.fetch_add(1, Relaxed);
                return Err(status);
            }
            Err(_) => {
                kernel.metrics.queries_failed.fetch_add(1, Relaxed);
                return Err(Status::deadline_exceeded(
                    "no read model answered in time (or none has caught up to min_position)",
                ));
            }
        };
        kernel.metrics.query.record(started.elapsed());
        let outcome = match answer.outcome {
            Some(query_answer::Outcome::Response(r)) => {
                // The middleware after it may still refuse the response; it fails closed.
                let result = QueryResult {
                    query_id: query.query_id.clone(),
                    outcome: Some(query_result::Outcome::Response(r.clone())),
                };
                let request = middleware_call::Request::Query(query.clone());
                let outcome = middleware_call::Outcome::QueryResult(result);
                let checked = kernel
                    .check(
                        &name,
                        &query.query_id,
                        When::After,
                        request,
                        &[],
                        Some(outcome),
                    )
                    .await?;
                if let Some(reason) = checked {
                    kernel.metrics.queries_rejected.fetch_add(1, Relaxed);
                    query_result::Outcome::Rejected(reason)
                } else {
                    kernel.metrics.queries_answered.fetch_add(1, Relaxed);
                    query_result::Outcome::Response(r)
                }
            }
            Some(query_answer::Outcome::Rejected(r)) => {
                kernel.metrics.queries_rejected.fetch_add(1, Relaxed);
                kernel.observe_query(&name, &query, &r);
                query_result::Outcome::Rejected(r)
            }
            None => return Err(Status::internal("empty answer")),
        };
        Ok(Response::new(QueryResult {
            query_id: query.query_id,
            outcome: Some(outcome),
        }))
    }
}

#[tonic::async_trait]
impl ListenerService for Api {
    type ListenStream = ReceiverStream<Result<ListenerDelivery, Status>>;

    async fn listen(
        &self,
        request: Request<Streaming<ListenerMessage>>,
    ) -> Result<Response<Self::ListenStream>, Status> {
        let (out, rx) = mpsc::channel(4);
        let kernel = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = listeners::run(kernel, &metadata, &mut inbound, &out).await {
                let _ = out.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn retry(&self, request: Request<RetryRequest>) -> Result<Response<RetryResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("retry request is not signed"))?;
        let message =
            signing::retry_message(&req.listener, &req.positions, req.dismiss, req.issued_at);
        signing::verify(&sig, &message)
            .map_err(|_| Status::unauthenticated("invalid retry request signature"))?;
        kernel.present(&metadata, &sig.key_id)?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "retry request expired; check the clock",
            ));
        }
        let service = kernel
            .listeners
            .service(&req.listener)
            .ok_or_else(|| Status::not_found(format!("no listener {}", req.listener)))?;
        let manages = if service.is_empty() { "*" } else { &service };
        if !config::allowed(&kernel.permissions().manage, &sig.key_id, manages) {
            return Err(Status::permission_denied(format!(
                "key may not manage {manages}"
            )));
        }
        let positions = kernel
            .listeners
            .retry(&req.listener, &req.positions, req.dismiss)
            .ok_or_else(|| Status::not_found(format!("no listener {}", req.listener)))?;
        // Stored before the answer, so a retry or dismissal that was confirmed holds.
        listeners::flush(kernel).map_err(|e| Status::internal(e.to_string()))?;
        let what = if req.dismiss { "dismissed" } else { "retrying" };
        tracing::info!(listener = %req.listener, key = %sig.key_id, events = positions.len(), "{what} parked events");
        Ok(Response::new(RetryResult { positions }))
    }
}

#[tonic::async_trait]
impl WorkerGateway for Api {
    type SessionStream = ReceiverStream<Result<KernelMessage, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let (out, rx) = mpsc::channel(64);
        let kernel = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = run_session(kernel, &metadata, &mut inbound, &out).await {
                let _ = out.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

type Outbox = mpsc::Sender<Result<KernelMessage, Status>>;

/// What a commit changes in the index: what the aggregate holds from then on, the vault
/// values of its personal keys (those `added` are new), and the record the store keeps.
struct IndexChange {
    entry: index::Entry,
    /// Vault values put for this commit; removed if it does not happen.
    added: Vec<String>,
    /// Vault values of personal keys the aggregate no longer holds; removed once it committed.
    dropped: Vec<String>,
    stored: kernable_core::proto::AggregateIndex,
}

/// Why the index refuses a result: the command's fault, or reading the index failed (the
/// command goes back to the queue).
enum Refusal {
    Rejected(Rejected),
    Failed(BoxError),
}

impl From<Rejected> for Refusal {
    fn from(r: Rejected) -> Self {
        Refusal::Rejected(r)
    }
}

impl From<BoxError> for Refusal {
    fn from(e: BoxError) -> Self {
        Refusal::Failed(e)
    }
}

/// A command currently executed by the session's worker.
struct InFlight {
    command: Command,
    received_at: i64,
    leased_at: Instant,
    expected_version: u64,
    /// The command was sent with a snapshot.
    with_snapshot: bool,
    attempts: u32,
    permit: OwnedSemaphorePermit,
}

struct Session {
    kernel: Arc<Kernel>,
    key_id: String,
    /// The payload version the worker writes per qualified event type; others at 1.
    schemas: HashMap<String, u32>,
    out: Outbox,
    in_flight: Mutex<HashMap<String, InFlight>>,
}

/// Hands the session's in-flight commands back to the queue when it ends, however it ends,
/// so another worker gets them without waiting for the leases to expire.
struct ReleaseOnDrop(Arc<Session>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let leases: Vec<String> = self
            .0
            .in_flight()
            .drain()
            .map(|(lease_id, _)| lease_id)
            .collect();
        for lease_id in leases {
            self.0.kernel.queue.release(&lease_id);
        }
        self.0.kernel.work.notify_waiters();
    }
}

/// Counts a connected worker session and logs it, until dropped.
struct SessionCount(Arc<Kernel>, String);

impl SessionCount {
    fn new(kernel: &Arc<Kernel>, worker_id: &str) -> Self {
        kernel.metrics.worker_sessions.fetch_add(1, Relaxed);
        tracing::info!(worker = worker_id, "worker connected");
        SessionCount(Arc::clone(kernel), worker_id.to_owned())
    }
}

impl Drop for SessionCount {
    fn drop(&mut self) {
        self.0.metrics.worker_sessions.fetch_sub(1, Relaxed);
        tracing::info!(worker = %self.1, "worker disconnected");
    }
}

/// Tells the worker registry when a session ends, however it ends.
struct DisconnectOnDrop(Arc<Kernel>, u64, Vec<String>);

impl Drop for DisconnectOnDrop {
    fn drop(&mut self) {
        self.0.workers.disconnect(&self.0, self.1, &self.2);
    }
}

/// Aborts a session's background task when the session ends, however it ends.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn next(inbound: &mut Streaming<WorkerMessage>) -> Result<Option<In>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

async fn send(out: &Outbox, msg: Out) -> Result<(), Status> {
    out.send(Ok(KernelMessage { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("worker disconnected"))
}

/// `execute` as an Execute and the ExecuteMore parts after it, each at most `max`
/// bytes on the wire, with the vault values of its events in each part and those of the command
/// (`command_refs`) in the first. `None` if the command with its snapshot, or an event with its
/// values, does not fit one message.
fn in_parts(
    mut execute: Execute,
    command_refs: &[String],
    max: usize,
) -> Option<(Execute, Vec<ExecuteMore>)> {
    // A length-delimited field: its tag, its length and itself.
    fn field(len: usize) -> usize {
        1 + prost::encoding::encoded_len_varint(len as u64) + len
    }
    let history = std::mem::take(&mut execute.history);
    let values: HashMap<String, VaultValue> = std::mem::take(&mut execute.vault)
        .into_iter()
        .map(|v| (v.r#ref.clone(), v))
        .collect();
    execute.vault = command_refs
        .iter()
        .filter_map(|r| values.get(r).cloned())
        .collect();
    execute.more = true;
    let first_size = execute.encoded_len() + ENVELOPE_BYTES;
    if first_size > max {
        return None;
    }
    let empty = ExecuteMore {
        lease_id: execute.lease_id.clone(),
        more: true,
        ..Default::default()
    };
    let part_size = empty.encoded_len() + ENVELOPE_BYTES;
    // Each part: its events, its values, its size, and which values it holds.
    let mut parts = vec![(Vec::new(), Vec::new(), first_size, HashSet::new())];
    for event in history {
        let fresh = |held: &HashSet<String>| -> Vec<&VaultValue> {
            let mut seen = HashSet::new();
            event
                .vault_refs
                .iter()
                .filter(|r| !held.contains(*r) && seen.insert(*r))
                .filter_map(|r| values.get(r))
                .collect()
        };
        let cost = |held: &HashSet<String>| {
            field(event.encoded_len())
                + fresh(held)
                    .iter()
                    .map(|v| field(v.encoded_len()))
                    .sum::<usize>()
        };
        let last = parts.last().expect("one part at least");
        if last.2 + cost(&last.3) > max {
            if part_size + cost(&HashSet::new()) > max {
                return None;
            }
            parts.push((Vec::new(), Vec::new(), part_size, HashSet::new()));
        }
        let part = parts.last_mut().expect("one part at least");
        let added: Vec<VaultValue> = fresh(&part.3).into_iter().cloned().collect();
        part.2 += cost(&part.3);
        for v in added {
            part.3.insert(v.r#ref.clone());
            part.1.push(v);
        }
        part.0.push(event);
    }
    let count = parts.len();
    let mut parts = parts.into_iter().enumerate();
    let (_, (history, vault, _, _)) = parts.next().expect("one part at least");
    execute.history = history;
    execute.vault.extend(vault);
    execute.more = count > 1;
    let rest = parts
        .map(|(i, (history, vault, _, _))| ExecuteMore {
            lease_id: execute.lease_id.clone(),
            history,
            vault,
            more: i + 1 < count,
        })
        .collect();
    Some((execute, rest))
}

async fn run_session(
    kernel: Arc<Kernel>,
    metadata: &MetadataMap,
    inbound: &mut Streaming<WorkerMessage>,
    out: &Outbox,
) -> Result<(), Status> {
    // Handshake: Register -> Challenge -> ChallengeResponse -> Registered.
    let Some(In::Register(reg)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected Register"));
    };
    protocol(reg.protocol)?;
    workers::validate(&reg)?;
    kernel.present(metadata, &reg.key_id)?;
    let permissions = kernel.permissions();
    let authorized = reg
        .aggregates
        .iter()
        .flat_map(|a| &a.command_types)
        .all(|t| config::allowed(&permissions.run, &reg.key_id, t));
    if !authorized {
        return Err(Status::permission_denied(
            "worker key may not handle these command types",
        ));
    }
    let schemas = declared_schemas(&kernel, &reg)?;

    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    send(
        out,
        Out::Challenge(Challenge {
            nonce: nonce.to_vec(),
        }),
    )
    .await?;
    let Some(In::ChallengeResponse(resp)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: reg.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;
    let (command_types, session) = kernel.workers.connect(&kernel, &reg, &permissions)?;
    let _unregister = DisconnectOnDrop(
        Arc::clone(&kernel),
        session,
        reg.aggregates
            .iter()
            .map(|a| a.aggregate_type.clone())
            .collect(),
    );
    send(
        out,
        Out::Registered(Registered {
            session_id: reg.worker_id.clone(),
        }),
    )
    .await?;

    let _counted = SessionCount::new(&kernel, &reg.worker_id);
    let session = Arc::new(Session {
        kernel,
        key_id: reg.key_id,
        schemas,
        out: out.clone(),
        in_flight: Mutex::default(),
    });
    let permits = Arc::new(Semaphore::new(reg.max_concurrency.max(1) as usize));
    let mut changes = session.kernel.changes();
    // Declared first, dropped last: stop dispatching, then release what is in flight.
    let _release = ReleaseOnDrop(Arc::clone(&session));
    let _dispatch = AbortOnDrop(tokio::spawn(dispatch(
        Arc::clone(&session),
        command_types.clone(),
        permits,
    )));

    loop {
        let message = tokio::select! {
            message = next(inbound) => message?,
            Ok(()) = changes.changed() => {
                // Permissions changed: end the session if the key may no longer run its commands.
                let permissions = session.kernel.permissions();
                if !command_types.iter().all(|t| config::allowed(&permissions.run, &session.key_id, t)) {
                    return Err(Status::permission_denied("worker key is no longer allowed or was revoked"));
                }
                continue;
            }
        };
        match message {
            None => return Ok(()),
            Some(In::Result(r)) => session.on_result(r).await?,
            Some(In::Rejection(r)) => session.on_rejection(r).await?,
            Some(_) => return Err(Status::invalid_argument("unexpected message")),
        }
    }
}

/// Checks the payload versions a registering worker declares: a worker that would write an
/// older version of an event type than the store holds is outdated and refused.
fn declared_schemas(kernel: &Kernel, reg: &Register) -> Result<HashMap<String, u32>, Status> {
    let domains: Vec<&str> = reg
        .aggregates
        .iter()
        .filter_map(|a| a.aggregate_type.split_once('.').map(|(domain, _)| domain))
        .collect();
    let stored = kernel.schemas();
    let mut declared = HashMap::new();
    for s in &reg.event_schemas {
        let domain = s.event_type.split_once('.').map(|(domain, _)| domain);
        if !config::valid_qualified(&s.event_type) || s.schema == 0 {
            return Err(Status::invalid_argument(
                "declare event schemas as a qualified type (domain.EventType) and a version from 1",
            ));
        }
        if !domain.is_some_and(|d| domains.contains(&d)) {
            return Err(Status::invalid_argument(format!(
                "{} is not of a domain whose commands this worker handles",
                s.event_type
            )));
        }
        if let Some(newer) = stored.get(&s.event_type).filter(|v| **v > s.schema) {
            return Err(Status::failed_precondition(format!(
                "outdated worker: the store holds {} at schema {newer}, this worker writes {}",
                s.event_type, s.schema
            )));
        }
        declared.insert(s.event_type.clone(), s.schema);
    }
    Ok(declared)
}

async fn dispatch(session: Arc<Session>, command_types: Vec<String>, permits: Arc<Semaphore>) {
    let types: Vec<&str> = command_types.iter().map(String::as_str).collect();
    let kernel = &session.kernel;
    let ttl = Duration::from_millis(kernel.config().lease_ttl_ms.max(0) as u64);
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (lease_id, command, received_at) = loop {
            let notified = kernel.work.notified();
            // Not one it still runs itself: a lease that ran out under it goes to another
            // worker first, so a worker too slow for its lease cannot keep a command forever.
            let busy: std::collections::HashSet<String> = session
                .in_flight()
                .values()
                .map(|f| f.command.command_id.clone())
                .collect();
            if let Some(lease) = kernel.queue.lease(&types, ttl, &busy) {
                break lease;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(POLL) => {}
            }
        };
        if !session
            .execute(lease_id, command, received_at, 0, permit)
            .await
        {
            return;
        }
    }
}

impl Session {
    fn in_flight(&self) -> MutexGuard<'_, HashMap<String, InFlight>> {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends the command with the latest snapshot and the events after it.
    /// Returns false once the worker is gone.
    async fn execute(
        &self,
        lease_id: String,
        command: Command,
        received_at: i64,
        attempts: u32,
        permit: OwnedSemaphorePermit,
    ) -> bool {
        let (tenant, domain, aggregate_type, id) = (
            &command.tenant_id,
            &command.domain,
            &command.aggregate_type,
            &command.aggregate_id,
        );
        let store = &self.kernel.store;
        let stream = StreamId {
            tenant_id: tenant.clone(),
            domain: domain.clone(),
            aggregate_type: aggregate_type.clone(),
            aggregate_id: id.clone(),
        };
        let loaded = self.kernel.snapshots.get(&stream).and_then(|record| {
            let snapshot = record.and_then(|r| r.snapshot);
            let after = snapshot.as_ref().map_or(0, |s| s.version);
            let history = store.read_stream_after(tenant, domain, aggregate_type, id, after)?;
            Ok((snapshot, history))
        });
        let (snapshot, history) = match loaded {
            Ok(loaded) => loaded,
            Err(e) => {
                tracing::error!(error = %e, "reading the stream failed");
                self.kernel.queue.release(&lease_id);
                self.kernel.work.notify_waiters();
                return true;
            }
        };
        let expected_version = history
            .last()
            .map(|e| e.version)
            .or(snapshot.as_ref().map(|s| s.version))
            .unwrap_or(0);
        let client = command.client_sig.as_ref().map(|sig| sig.key_id.as_str());
        if let Some(since) = client.and_then(|key| self.kernel.revoked_since(key))
            && received_at >= since
        {
            // Accepted after its client's key was stolen, or before the kernel knew.
            let reason = Rejected {
                code: "key_revoked".into(),
                message: "the command was signed with a key revoked since then".into(),
                ..Default::default()
            };
            drop(permit);
            self.reject(&lease_id, command, reason);
            return true;
        }
        let required = command.context.as_ref().and_then(|c| c.expected_version);
        if let Some(required) = required.filter(|v| *v != expected_version) {
            // The client based the command on a state that has changed since.
            let reason = Rejected {
                code: "version_mismatch".into(),
                message: format!(
                    "expected version {required}, the aggregate is at {expected_version}"
                ),
                ..Default::default()
            };
            drop(permit);
            self.reject(&lease_id, command, reason);
            return true;
        }
        self.in_flight().insert(
            lease_id.clone(),
            InFlight {
                command: command.clone(),
                received_at,
                leased_at: Instant::now(),
                expected_version,
                with_snapshot: snapshot.is_some(),
                attempts,
                permit,
            },
        );
        let refs: Vec<String> = command
            .vault_refs
            .iter()
            .chain(history.iter().flat_map(|e| &e.vault_refs))
            .cloned()
            .collect();
        let vault = self.kernel.values_of(&refs);
        let execute = Execute {
            vault,
            lease_id,
            command: Some(command),
            history,
            expected_version,
            snapshot,
            received_at,
            deadline: now_ms() + self.kernel.config().lease_ttl_ms,
            more: false,
        };
        let size = execute.encoded_len() + ENVELOPE_BYTES;
        let max = self.kernel.config.limits.message_bytes;
        if size <= max {
            let sent = send(&self.out, Out::Execute(execute)).await.is_ok();
            crash::point("execute.sent");
            return sent;
        }
        // Too long for one message: in parts, to a worker that reads them.
        let events = execute.history.len();
        let lease_id = execute.lease_id.clone();
        let command = execute.command.clone().unwrap_or_default();
        let Some((first, rest)) = in_parts(execute, &command.vault_refs, max) else {
            // No worker could receive it: a final rejection instead of a retry forever.
            let in_flight = self.in_flight().remove(&lease_id);
            drop(in_flight);
            self.kernel.metrics.limited("too_large");
            let reason = Rejected {
                code: "too_large".into(),
                message: format!(
                    "the command with its snapshot, or one of the {events} events after it, is \
                     more than --max-message-bytes {max}: raise the limit in the kernel and the SDKs"
                ),
                ..Default::default()
            };
            self.reject(&lease_id, command, reason);
            return true;
        };
        let mut sent = send(&self.out, Out::Execute(first)).await.is_ok();
        for part in rest {
            sent = sent && send(&self.out, Out::ExecuteMore(part)).await.is_ok();
        }
        crash::point("execute.sent");
        sent
    }

    async fn on_result(&self, r: kernable_core::proto::Result) -> Result<(), Status> {
        let Some(f) = self.in_flight().remove(&r.lease_id) else {
            return Ok(()); // unknown or already handled lease
        };
        let sig = r.worker_sig.clone().unwrap_or_default();
        let msg = signing::result_message(&f.command, f.expected_version, &r.events);
        let snapshot_ok = r
            .snapshot
            .as_ref()
            .is_none_or(|s| self.valid_snapshot(s, &f, r.events.len()));
        let index_ok = self.valid_index(&r, &f);
        if sig.key_id != self.key_id
            || signing::verify(&sig, &msg).is_err()
            || !snapshot_ok
            || !index_ok
        {
            // Put it back, so it is released when the session ends.
            self.in_flight().insert(r.lease_id, f);
            return Err(Status::unauthenticated(
                "invalid result, index or snapshot signature",
            ));
        }
        if !self.kernel.queue.holds(&r.lease_id) {
            return Ok(()); // the lease expired; the command was handed to another worker
        }
        if !r.events.iter().all(|e| config::valid_type(&e.event_type)) {
            // Events always belong to the command's domain: a qualified or empty type is a bug.
            let reason = Rejected {
                code: "invalid_event_type".into(),
                message: "event types must be non-empty and without dots".into(),
                ..Default::default()
            };
            self.reject(&r.lease_id, f.command, reason);
            return Ok(());
        }

        if r.events
            .iter()
            .any(|e| !e.origin_partition.is_empty() || e.origin_position != 0)
        {
            // Only the kernel sets where an event came from, when its tenant moves (S4).
            let reason = Rejected {
                code: "invalid_event_origin".into(),
                message: "a worker's events have no origin; the kernel sets it on a move".into(),
                ..Default::default()
            };
            self.reject(&r.lease_id, f.command, reason);
            return Ok(());
        }

        let name = |e: &NewEvent| config::qualified(&f.command.domain, &e.event_type);
        if let Some(e) = r
            .events
            .iter()
            .find(|e| e.schema != self.schemas.get(&name(e)).copied().unwrap_or(1))
        {
            let reason = Rejected {
                code: "invalid_event_schema".into(),
                message: format!(
                    "{} written at schema {}, not at the declared one",
                    name(e),
                    e.schema
                ),
                ..Default::default()
            };
            self.reject(&r.lease_id, f.command, reason);
            return Ok(());
        }

        let stream = StreamId {
            tenant_id: f.command.tenant_id.clone(),
            domain: f.command.domain.clone(),
            aggregate_type: f.command.aggregate_type.clone(),
            aggregate_id: f.command.aggregate_id.clone(),
        };
        if let Err(reason) = index::validate(&r) {
            self.reject(&r.lease_id, f.command, reason);
            return Ok(());
        }
        if r.keys.iter().any(|k| !k.subject.is_empty()) && self.kernel.vault.is_none() {
            let reason = Rejected {
                code: "no_vault".into(),
                message: "this kernel has no vault".into(),
                ..Default::default()
            };
            self.reject(&r.lease_id, f.command, reason);
            return Ok(());
        }
        // An aggregate without events does not exist: whatever its initial state would
        // hold, it holds nothing (a reindex of an ID never written).
        let exists = f.expected_version > 0 || !r.events.is_empty();
        let wanted = index::Held {
            keys: r
                .keys
                .iter()
                .filter(|_| exists)
                .map(|k| index::Key {
                    name: k.name.clone(),
                    global: k.global,
                    id: signing::unique_key_id(&f.command, k),
                    subject: k.subject.clone(),
                })
                .collect(),
            facts: r.facts.iter().filter(|_| exists).cloned().collect(),
        };
        // Global keys across partitions (S3): reserved with the key owner first, and held as
        // in flight until this commit is done.
        let global: Vec<(&str, [u8; 32])> = wanted
            .keys
            .iter()
            .filter(|k| k.global && k.subject.is_empty())
            .map(|k| (k.name.as_str(), k.id))
            .collect();
        let ids: Vec<[u8; 32]> = global.iter().map(|(_, id)| *id).collect();
        let _flying = self.kernel.in_flight_keys.hold(&ids);
        if !ids.is_empty() && self.kernel.config.partition.is_some() {
            match self.kernel.reserve_global(&ids).await {
                Ok(granted) => {
                    if let Some(i) = granted.iter().position(|g| !g) {
                        let reason = Rejected {
                            code: "unique_taken".into(),
                            message: format!(
                                "{}.{} key {} is taken",
                                f.command.domain, f.command.aggregate_type, global[i].0
                            ),
                            key: global[i].0.to_owned(),
                            ..Default::default()
                        };
                        self.reject(&r.lease_id, f.command, reason);
                        return Ok(());
                    }
                }
                Err(status) => {
                    // Ends the session; the command goes back to the queue.
                    tracing::warn!(error = %status.message(), "reserving global keys failed");
                    self.in_flight().insert(r.lease_id, f);
                    return Err(Status::unavailable("reserving global keys failed"));
                }
            }
        }

        let erasing = match self
            .kernel
            .vault_result(&self.key_id, &f.command, &r.events, &r.vault)
        {
            Ok(erasing) => erasing,
            Err(reason) => {
                self.reject(&r.lease_id, f.command, reason);
                return Ok(());
            }
        };

        let committed = {
            // Held through the commit, so an outdated worker cannot write next to a newer one,
            // and with the index, so that two commits never take one key and a requirement
            // never sees a fact that a commit is removing.
            let mut stored = self.kernel.schemas();
            let mut index = self.kernel.index();
            let outdated = r.events.iter().find_map(|e| {
                let newer = stored.get(&name(e)).filter(|v| **v > e.schema)?;
                Some(format!(
                    "outdated worker: the store holds {} at schema {newer}, this worker writes {}",
                    name(e),
                    e.schema
                ))
            });
            if let Some(message) = outdated {
                // Ends the session; the command goes back to the queue for a current worker.
                tracing::warn!(%message, "worker refused");
                self.in_flight().insert(r.lease_id, f);
                return Err(Status::failed_precondition(message));
            }
            let change = match self.index_change(&mut index, &f.command, &stream, &wanted, &r) {
                Ok(change) => change,
                Err(Refusal::Rejected(reason)) => {
                    drop((index, stored));
                    self.reject(&r.lease_id, f.command, reason);
                    return Ok(());
                }
                Err(Refusal::Failed(e)) => {
                    // Ends the session; the command goes back to the queue.
                    tracing::error!(error = %e, "reading the index failed");
                    self.in_flight().insert(r.lease_id, f);
                    return Err(Status::unavailable("reading the index failed"));
                }
            };
            // The erasures are noted first: a crash after the commit still carries them out.
            if let Some(vault) = &self.kernel.vault {
                let noted = erasing.iter().try_for_each(|subject| {
                    vault.intend_erase(&f.command.tenant_id, subject, &f.command.command_id)
                });
                if let Err(e) = noted {
                    drop((index, stored));
                    let reason = Rejected {
                        code: "vault_error".into(),
                        message: e.to_string(),
                        ..Default::default()
                    };
                    self.reject(&r.lease_id, f.command, reason);
                    return Ok(());
                }
                if !erasing.is_empty() {
                    crash::point("erasure.intended");
                }
            }
            let committed = self.kernel.store.commit(
                &f.command.command_id,
                f.expected_version,
                &r.events,
                &sig,
                change.as_ref().map(|c| &c.stored),
                now_ms(),
            );
            if committed.is_ok() {
                for e in &r.events {
                    let max = stored.entry(name(e)).or_default();
                    *max = (*max).max(e.schema);
                }
            }
            // A write that failed may have taken effect all the same (see `completed_earlier`).
            let took_effect = match &committed {
                Ok(_) => true,
                Err(StoreError::VersionConflict { .. } | StoreError::NotPending) => false,
                Err(_) => matches!(
                    self.kernel.store.result(&f.command.command_id),
                    Ok(Some(CommandResult {
                        outcome: Some(Outcome::Committed(_)),
                        ..
                    }))
                ),
            };
            if let Some(change) = change {
                if took_effect {
                    index.set(&stream, change.entry);
                    if let Some(vault) = &self.kernel.vault
                        && let Err(e) = vault.remove(&change.dropped)
                    {
                        // Harmless: a value the stream no longer refers to holds no key.
                        tracing::error!(error = %e, "dropping the values of released keys failed");
                    }
                } else if let Some(vault) = &self.kernel.vault
                    && let Err(e) = vault.remove(&change.added)
                {
                    tracing::error!(error = %e, "dropping the vault values of a failed commit failed");
                }
            }
            if let Some(vault) = &self.kernel.vault
                && !erasing.is_empty()
            {
                if took_effect {
                    crash::point("erasure.pending");
                    let erased = self.kernel.erase(&f.command, &erasing, &stream);
                    // Personal keys whose values are gone are free.
                    index.erased(&erased);
                    crash::point("erasure.done");
                } else if let Err(e) = vault.drop_intents(&f.command.command_id) {
                    // Harmless: the next start sees the command uncommitted and drops them.
                    tracing::error!(error = %e, "dropping the erasures of a failed commit failed");
                }
            }
            committed
        };
        match committed {
            Ok(events) => {
                crash::point("commit.stored");
                self.kernel.failed_commits.store(0, Relaxed);
                // The flush and what follows it run apart from the session, so the next results
                // commit meanwhile and share the flush (group commit). The lease and the
                // worker's permit are held until then: the aggregate's next command waits for
                // its history to be on the disk.
                let kernel = Arc::clone(&self.kernel);
                let snapshot = r.snapshot.filter(|_| erasing.is_empty());
                let lease_id = r.lease_id;
                tokio::spawn(async move {
                    let last = events.last().map_or(0, |e| e.global_position);
                    let syncing = Arc::clone(&kernel);
                    let _ = tokio::task::spawn_blocking(move || syncing.sync(last)).await;
                    kernel.drop_secrets(&f.command.command_id);
                    // After an erasure the worker's state may still hold erased values: no
                    // snapshot.
                    if let (Some(snapshot), Some(last)) = (snapshot, events.last()) {
                        let record = SnapshotRecord {
                            tenant_id: last.tenant_id.clone(),
                            domain: last.domain.clone(),
                            aggregate_type: last.aggregate_type.clone(),
                            aggregate_id: last.aggregate_id.clone(),
                            snapshot: Some(snapshot),
                            event_hash: last.hash.clone(),
                        };
                        // A snapshot is only a cache: losing one costs time, not data.
                        match kernel.snapshots.save(&record) {
                            Ok(()) => crash::point("snapshot.stored"),
                            Err(e) => tracing::warn!(error = %e, "saving snapshot failed"),
                        }
                    }
                    kernel.queue.done(&lease_id);
                    kernel.metrics.committed.fetch_add(1, Relaxed);
                    kernel.metrics.execution.record(f.leased_at.elapsed());
                    kernel.appended.notify_waiters();
                    let result = CommandResult {
                        command_id: f.command.command_id.clone(),
                        outcome: Some(Outcome::Committed(Committed {
                            events,
                            settled: false,
                        })),
                    };
                    kernel.complete(&f.command, result);
                });
            }
            Err(StoreError::VersionConflict { .. }) if f.attempts < MAX_CONFLICT_RETRIES => {
                self.execute(
                    r.lease_id,
                    f.command,
                    f.received_at,
                    f.attempts + 1,
                    f.permit,
                )
                .await;
            }
            Err(StoreError::VersionConflict { .. }) => {
                let reason = Rejected {
                    code: "concurrency_conflict".into(),
                    message: "aggregate kept changing; retry the command".into(),
                    ..Default::default()
                };
                self.reject(&r.lease_id, f.command, reason);
            }
            Err(StoreError::NotPending) => {
                if !self.completed_earlier(&r.lease_id, &f.command) {
                    self.done(&r.lease_id);
                }
            }
            Err(e) => {
                // The command runs again after a pause that doubles with every failure in a
                // row, up to 5 s: a store that cannot write (a full disk) is not hammered, and
                // the log gets a line at the 1st, 2nd, 4th, 8th, ... failure.
                let failed = self.kernel.failed_commits.fetch_add(1, Relaxed) + 1;
                let delay =
                    Duration::from_millis(50 << (failed - 1).min(7)).min(COMMIT_BACKOFF_MAX);
                if failed.is_power_of_two() {
                    tracing::error!(error = %e, failed, "commit failed; trying again after a pause");
                }
                if !self.completed_earlier(&r.lease_id, &f.command) {
                    self.kernel.queue.release_after(&r.lease_id, delay);
                }
            }
        }
        Ok(())
    }

    async fn on_rejection(&self, r: kernable_core::proto::Rejection) -> Result<(), Status> {
        let Some(f) = self.in_flight().remove(&r.lease_id) else {
            return Ok(());
        };
        let mut reason = r.reason.unwrap_or_default();
        let sig = r.worker_sig.unwrap_or_default();
        let msg = signing::rejection_message(&f.command.command_id, &reason);
        if sig.key_id != self.key_id || signing::verify(&sig, &msg).is_err() {
            self.in_flight().insert(r.lease_id, f);
            return Err(Status::unauthenticated("invalid rejection signature"));
        }
        // Only the kernel names a key or requirement: its index decides those.
        reason.key.clear();
        reason.requirement = None;
        if reason.code == SNAPSHOT_STALE && f.with_snapshot {
            // The worker cannot read the snapshot: drop it and send all events instead.
            let c = &f.command;
            let stream = StreamId {
                tenant_id: c.tenant_id.clone(),
                domain: c.domain.clone(),
                aggregate_type: c.aggregate_type.clone(),
                aggregate_id: c.aggregate_id.clone(),
            };
            if let Err(e) = self.kernel.snapshots.remove(&stream) {
                tracing::error!(error = %e, "removing snapshot failed");
                self.release(&r.lease_id);
                return Ok(());
            }
            self.execute(
                r.lease_id,
                f.command,
                f.received_at,
                f.attempts + 1,
                f.permit,
            )
            .await;
            return Ok(());
        }
        self.reject(&r.lease_id, f.command, reason);
        Ok(())
    }

    /// A snapshot must describe the state right after the result's events and be signed
    /// by this session's worker.
    fn valid_snapshot(&self, s: &Snapshot, f: &InFlight, event_count: usize) -> bool {
        let msg = signing::snapshot_message(&f.command, s.version, &s.schema, &s.state);
        event_count > 0
            && s.version == f.expected_version + event_count as u64
            && s.worker_sig
                .as_ref()
                .is_some_and(|sig| sig.key_id == self.key_id && signing::verify(sig, &msg).is_ok())
    }

    /// A result's keys, facts and requirements must be signed by this session's worker, if it
    /// has any.
    fn valid_index(&self, r: &kernable_core::proto::Result, f: &InFlight) -> bool {
        if r.keys.is_empty() && r.facts.is_empty() && r.requires.is_empty() {
            return true;
        }
        let msg = signing::index_message(
            &f.command,
            f.expected_version,
            &r.keys,
            &r.facts,
            &r.requires,
        );
        r.index_sig
            .as_ref()
            .is_some_and(|sig| sig.key_id == self.key_id && signing::verify(sig, &msg).is_ok())
    }

    /// Checks `wanted`, what the command's aggregate is to hold, and the result's requirements
    /// against the index before the command; `None` if the aggregate holds `wanted` already.
    /// New personal keys get their vault values here, before the commit.
    fn index_change(
        &self,
        index: &mut index::Index,
        cmd: &Command,
        stream: &StreamId,
        wanted: &index::Held,
        r: &kernable_core::proto::Result,
    ) -> Result<Option<IndexChange>, Refusal> {
        let store = self.kernel.store.as_ref();
        let vault = self.kernel.vault.as_ref();
        for q in &r.requires {
            // By key: the aggregate of the type that holds it, tenant or global (a key name has
            // one scope per type), and only in the command's tenant, like one named by ID: a
            // global key held in another tenant names no aggregate here. None holds no fact.
            let other = if q.key.is_empty() {
                Some(StreamId {
                    tenant_id: cmd.tenant_id.clone(),
                    domain: q.domain.clone(),
                    aggregate_type: q.aggregate_type.clone(),
                    aggregate_id: q.aggregate_id.clone(),
                })
            } else {
                let id = |tenant: &str| {
                    signing::unique_key_id_of(
                        tenant,
                        &q.domain,
                        &q.aggregate_type,
                        &q.key,
                        &q.value_hash,
                    )
                };
                let holder = match index.holder(store, vault, &id(&cmd.tenant_id))? {
                    Some(holder) => Some(holder),
                    None => index.holder(store, vault, &id(""))?,
                };
                holder.filter(|holder| holder.tenant_id == cmd.tenant_id)
            };
            let holds = match other {
                Some(o) => index.holds(store, vault, &o, &q.fact)?,
                None => false,
            };
            if holds == q.absent {
                // Never the value, nor its hash: the key's name says which.
                let which = if q.key.is_empty() {
                    q.aggregate_id.clone()
                } else {
                    format!("with key {}", q.key)
                };
                let verb = if q.absent { "holds" } else { "does not hold" };
                return Err(Rejected {
                    code: "requirement_missing".into(),
                    message: format!(
                        "{}.{} {which} {verb} {}",
                        q.domain, q.aggregate_type, q.fact
                    ),
                    requirement: Some(Box::new(kernable_core::proto::Requirement {
                        value_hash: Vec::new(),
                        ..q.clone()
                    })),
                    ..Default::default()
                }
                .into());
            }
        }
        let current = index.held(store, vault, stream)?;
        // Never the value: the message tells the client which key, not whose. A key the
        // aggregate holds already needs no lookup.
        for k in wanted
            .keys
            .iter()
            .filter(|k| !current.held.keys.contains(k))
        {
            // A stream of a tenant that moved away holds nothing here any more (S4).
            if index
                .holder(store, vault, &k.id)?
                .is_some_and(|holder| holder != *stream && self.kernel.serves(&holder.tenant_id))
            {
                return Err(Rejected {
                    code: "unique_taken".into(),
                    message: format!(
                        "{}.{} key {} is taken",
                        cmd.domain, cmd.aggregate_type, k.name
                    ),
                    key: k.name.clone(),
                    ..Default::default()
                }
                .into());
            }
        }
        if current.held == *wanted {
            return Ok(None);
        }
        let mut change = IndexChange {
            entry: index::Entry {
                held: wanted.clone(),
                refs: HashMap::new(),
            },
            added: Vec::new(),
            dropped: current
                .held
                .keys
                .iter()
                .filter(|k| !wanted.keys.contains(*k))
                .filter_map(|k| current.refs.get(&k.id).cloned())
                .collect(),
            stored: Default::default(),
        };
        for k in wanted.keys.iter().filter(|k| !k.subject.is_empty()) {
            let kept = current
                .held
                .keys
                .contains(k)
                .then(|| current.refs.get(&k.id))
                .flatten();
            let r = match (kept, vault) {
                (Some(r), _) => r.clone(),
                (None, Some(vault)) => {
                    match index::put_personal(vault, cmd, stream, &k.id, &k.subject, now_ms()) {
                        Ok(r) => {
                            change.added.push(r.clone());
                            r
                        }
                        Err(e) => {
                            if let Err(e) = vault.remove(&change.added) {
                                tracing::error!(error = %e, "dropping vault values failed");
                            }
                            return Err(Rejected {
                                code: "vault_error".into(),
                                message: e.to_string(),
                                ..Default::default()
                            }
                            .into());
                        }
                    }
                }
                (None, None) => {
                    return Err(Rejected {
                        code: "no_vault".into(),
                        message: "this kernel has no vault".into(),
                        ..Default::default()
                    }
                    .into());
                }
            };
            change.entry.refs.insert(k.id, r);
        }
        change.stored = index::stored(&change.entry.held, &change.entry.refs);
        Ok(Some(change))
    }

    fn reject(&self, lease_id: &str, command: Command, reason: Rejected) {
        if !self.kernel.queue.holds(lease_id) {
            return;
        }
        let command_id = command.command_id.clone();
        match self.kernel.store.reject(&command_id, reason.clone()) {
            Ok(()) => {
                crash::point("reject.stored");
                self.kernel.sync(0);
                self.kernel.drop_secrets(&command_id);
                self.kernel.queue.done(lease_id);
                self.kernel.metrics.rejected.fetch_add(1, Relaxed);
                let result = CommandResult {
                    command_id,
                    outcome: Some(Outcome::Rejected(reason)),
                };
                self.kernel.complete(&command, result);
            }
            Err(StoreError::NotPending) => {
                if !self.completed_earlier(lease_id, &command) {
                    self.done(lease_id);
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "reject failed");
                if !self.completed_earlier(lease_id, &command) {
                    self.release(lease_id);
                }
            }
        }
    }

    /// Whether `command` is completed in the store already, by an earlier attempt whose write
    /// took effect although it reported a failure; if so, it is answered with what is stored.
    fn completed_earlier(&self, lease_id: &str, command: &Command) -> bool {
        let Ok(Some(result)) = self.kernel.store.result(&command.command_id) else {
            return false;
        };
        tracing::warn!(command = %command.command_id, "a failed write had taken effect: answering with the stored result");
        self.kernel.sync(last_position(&result));
        self.kernel.queue.done(lease_id);
        self.kernel.drop_secrets(&command.command_id);
        self.kernel.appended.notify_waiters();
        self.kernel.complete(command, result);
        true
    }

    /// The command was completed under an earlier lease: drop it from the queue.
    fn done(&self, lease_id: &str) {
        self.kernel.queue.done(lease_id);
        self.kernel.work.notify_waiters();
    }

    /// Hands the command back for another attempt.
    fn release(&self, lease_id: &str) {
        self.kernel.queue.release(lease_id);
        self.kernel.work.notify_waiters();
    }
}

/// Whether `event` matches the type patterns of a subscription (`None`: all events).
/// Erases `subjects` of `tenant` for the committed command `command_id`: their vault values,
/// and the snapshots of every stream that used them and of `own`, the command's stream, whose
/// worker state may hold them. Returns those streams. A failure is logged and leaves the
/// intent in the vault: the next start carries it out.
/// Finishes what the vault was left with by a crash, before anything is served: drops the
/// secrets of commands that completed, carries out erasures whose events were committed and
/// drops those of commands that did not commit (they run again and note them anew).
pub(crate) fn recover_vault(
    store: &dyn Store,
    vault: &Vault,
    snapshots: &dyn SnapshotStore,
) -> Result<(), BoxError> {
    // Secrets of commands that completed while the kernel was down.
    for command_id in vault.secret_commands()? {
        if store.result(&command_id)?.is_some() {
            vault.drop_secrets(&command_id)?;
        }
    }
    // Erasures whose events were committed before a crash, carried out before
    // anything is served (and before the index reads the values); those of commands
    // that did not commit are dropped: the command runs again and notes them anew.
    for (tenant, subject, command_id) in vault.intents()? {
        match store.result(&command_id)?.and_then(|r| r.outcome) {
            Some(Outcome::Committed(c)) => {
                let own = c.events.first().map(|e| StreamId {
                    tenant_id: e.tenant_id.clone(),
                    domain: e.domain.clone(),
                    aggregate_type: e.aggregate_type.clone(),
                    aggregate_id: e.aggregate_id.clone(),
                });
                let subjects = [subject];
                if let Some(own) = own {
                    erase_subjects(vault, snapshots, &tenant, &command_id, &subjects, &own);
                }
                vault.drop_intents(&command_id)?;
                tracing::info!(command = %command_id, "carried out an erasure committed before a crash");
            }
            _ => vault.drop_intents(&command_id)?,
        }
    }
    Ok(())
}

fn erase_subjects(
    vault: &Vault,
    snapshots: &dyn SnapshotStore,
    tenant: &str,
    command_id: &str,
    subjects: &[String],
    own: &StreamId,
) -> Vec<StreamId> {
    let mut erased = Vec::new();
    for subject in subjects {
        let streams = match vault.erase(tenant, subject, command_id, now_ms()) {
            Ok(streams) => streams,
            Err(e) => {
                tracing::error!(error = %e, command = %command_id, "erasing a subject failed; the next start tries again");
                continue;
            }
        };
        for s in streams.into_iter().chain([own.clone()]) {
            if let Err(e) = snapshots.remove(&s) {
                tracing::error!(error = %e, "dropping a snapshot failed");
            }
            if !erased.contains(&s) {
                erased.push(s);
            }
        }
        tracing::info!(command = %command_id, "erased a subject's vault values");
    }
    erased
}

/// Whether a listener or subscription gets `event`: one of its types, and not a copy its tenant
/// brought along when it moved here (S4): the original is delivered where it was committed.
fn wanted(types: Option<&[String]>, event: &Event) -> bool {
    event.origin_partition.is_empty()
        && types.is_none_or(|patterns| {
            let name = config::qualified(&event.domain, &event.event_type);
            patterns.iter().any(|p| config::covers(p, &name))
        })
}

/// The global position of a result's last event; 0 for none.
fn last_position(result: &CommandResult) -> u64 {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.last().map_or(0, |e| e.global_position),
        _ => 0,
    }
}

fn internal(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_gets_its_rate_with_a_burst_of_one_second_and_no_more() {
        let rates = Rates::default();
        let took = (0..5).filter(|_| rates.take("k", 3)).count();
        assert_eq!(took, 3);
        assert!(rates.take("other", 3), "each key has its own bucket");
        std::thread::sleep(Duration::from_millis(400));
        assert!(
            rates.take("k", 3),
            "a token comes back after a third of a second"
        );
        assert!(!rates.take("k", 3));
    }

    #[test]
    fn the_size_of_a_store_counts_its_journal_and_every_file() {
        let dir = std::env::temp_dir().join(format!("kn-bytes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("data/t1")).unwrap();
        std::fs::write(dir.join("k.db"), [0u8; 100]).unwrap();
        std::fs::write(dir.join("k.db-wal"), [0u8; 20]).unwrap();
        std::fs::write(dir.join("data/t1/a"), [0u8; 7]).unwrap();
        std::fs::write(dir.join("data/b"), [0u8; 3]).unwrap();
        assert_eq!(
            bytes_of(&format!("sqlite:{}", dir.join("k.db").display())),
            120
        );
        assert_eq!(
            bytes_of(&format!("file:{}", dir.join("data").display())),
            10
        );
        assert_eq!(bytes_of("memory:"), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_long_history_is_split_into_parts_that_each_fit() {
        let value = |r: &str| VaultValue {
            r#ref: r.into(),
            value: vec![b'v'; 300],
            ..Default::default()
        };
        // 40 events of about 1 KiB; every fifth refers to a vault value, the third to v0 again.
        let history: Vec<Event> = (0..40u64)
            .map(|n| Event {
                version: n + 1,
                payload: vec![b'x'; 1000],
                vault_refs: match n {
                    _ if n % 5 == 0 => vec![format!("v{n}")],
                    3 => vec!["v0".into()],
                    _ => vec![],
                },
                ..Default::default()
            })
            .collect();
        let mut vault: Vec<VaultValue> = (0..40)
            .step_by(5)
            .map(|n| value(&format!("v{n}")))
            .collect();
        vault.push(value("c"));
        let execute = Execute {
            lease_id: "c1#1".into(),
            command: Some(Command {
                payload: vec![b'c'; 2000],
                vault_refs: vec!["c".into()],
                ..Default::default()
            }),
            history: history.clone(),
            vault,
            ..Default::default()
        };
        let max = 8 << 10;
        let (first, rest) = in_parts(execute, &["c".into()], max).unwrap();

        assert!(first.more && rest.len() >= 4, "{} parts", rest.len() + 1);
        assert!(first.encoded_len() + ENVELOPE_BYTES <= max);
        assert!(
            first.vault.iter().any(|v| v.r#ref == "c"),
            "the command's value comes first"
        );
        for (i, part) in rest.iter().enumerate() {
            assert!(part.encoded_len() + ENVELOPE_BYTES <= max, "part {i}");
            assert_eq!(part.more, i + 1 < rest.len(), "part {i}");
            assert_eq!(part.lease_id, "c1#1");
        }
        // Together: the whole history in order, each event with its values in its own part.
        let parts = std::iter::once((&first.history, &first.vault))
            .chain(rest.iter().map(|p| (&p.history, &p.vault)));
        let mut events = Vec::new();
        for (history, vault) in parts {
            for e in history {
                for r in &e.vault_refs {
                    assert!(
                        vault.iter().any(|v| &v.r#ref == r),
                        "{r} in the part of {}",
                        e.version
                    );
                }
            }
            events.extend(history.iter().cloned());
        }
        assert_eq!(events, history);
    }

    #[test]
    fn a_command_that_does_not_fit_alone_cannot_be_split() {
        let execute = Execute {
            command: Some(Command {
                payload: vec![b'c'; 9000],
                ..Default::default()
            }),
            history: vec![Event::default()],
            ..Default::default()
        };
        assert!(in_parts(execute, &[], 8 << 10).is_none());
        // Nor can an event that does not fit a part of its own.
        let execute = Execute {
            history: vec![Event {
                payload: vec![b'x'; 9000],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(in_parts(execute, &[], 8 << 10).is_none());
    }

    #[test]
    #[cfg(unix)]
    fn a_kernel_socket_lets_its_owner_and_group_connect() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("kn-mode-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        super::socket_mode(path.to_str().unwrap()).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o660);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn plain_tcp_is_for_loopback_addresses_only() {
        for local in [
            "127.0.0.1:7233",
            "localhost:7233",
            "[::1]:7233",
            "127.0.0.2:1",
        ] {
            assert!(super::loopback(local), "{local}");
        }
        for open in [
            "0.0.0.0:7233",
            "[::]:7233",
            "10.0.0.5:7233",
            "kernel.example.internal:7233",
        ] {
            assert!(!super::loopback(open), "{open}");
        }
    }
}
