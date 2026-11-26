//! The standby (design/replication.md): it follows a primary, applies its records, serves
//! nothing else, and becomes the primary when `kernable promote` asks it to.
//!
//! A reader (`--reader`, `reader.rs`) catches up here first: it follows the writer (from a
//! backup, if it is new and one is given) until it holds what the writer may tell, and [`run`]
//! returns; the kernel then opens its stores read-only and follows on.
//!
//! [`run`] returns once the standby was promoted, with its stores closed: the caller then
//! opens them as a kernel does after a restart. Every request but `ReplicationService` is
//! refused with `UNAVAILABLE` and `not_primary`, which the SDKs retry.

use std::{
    collections::HashMap,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        CacheBatch, ChallengeResponse, CheckpointRecord, FollowAck, FollowHello, FollowMode,
        FollowRequest, FollowResponse, Grant, ImageBatch, ImageEnd, Leaving, PromoteRequest,
        PromoteResult, RecordBatch, RecordPoint, SnapshotRecord, StoredKey, TookOver, WitnessOp,
        follow_request, follow_response,
        log_record::Record,
        replication_service_client::ReplicationServiceClient,
        replication_service_server::{ReplicationService, ReplicationServiceServer},
    },
    signing, transport,
};
use kernable_store::{
    Head, Store, StreamId,
    data::{Data, DataKey, DataStore},
    snapshots::SnapshotStore,
    vault::Vault,
};
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    codegen::http,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

use crate::{
    BoxError, GRANT_HEADER, MAX_REQUEST_AGE_MS, Permissions, config, config::Config,
    config::Follow, erase_subjects, node::Link, now_ms, protocol, recover_vault, replication,
};

/// What a standby keeps; also what a backup is restored into.
pub struct Stores {
    pub store: Box<dyn Store>,
    pub vault: Option<Vault>,
    pub data: Box<dyn DataStore>,
    pub snapshots: Box<dyn SnapshotStore>,
}

/// The stores records are applied to: a standby's [`Stores`], or a reader's kernel.
pub(crate) struct Parts<'a> {
    pub(crate) store: &'a dyn Store,
    pub(crate) vault: Option<&'a Vault>,
    pub(crate) data: &'a dyn DataStore,
    pub(crate) snapshots: &'a dyn SnapshotStore,
}

impl Stores {
    pub(crate) fn parts(&self) -> Parts<'_> {
        Parts {
            store: self.store.as_ref(),
            vault: self.vault.as_ref(),
            data: self.data.as_ref(),
            snapshots: self.snapshots.as_ref(),
        }
    }

    fn open(config: &Config) -> Result<Self, BoxError> {
        Self::open_on(
            config,
            kernable_store::open_with(&config.store, config.durability)?,
        )
    }

    fn open_on(config: &Config, store: Box<dyn Store>) -> Result<Self, BoxError> {
        if store.copy_mark()?.is_some() {
            return Err("the store is a masked copy (a preview): it is never a standby".into());
        }
        store.keep_record_log(true)?;
        // Locked as a kernel locks them: the standby or reader writes them alone.
        let vault = match &config.vault {
            Some(v) => Some(Vault::open_locked(&v.store, &*config.read_secret(&v.key)?)?),
            None => None,
        };
        let data = kernable_store::data::open(&config.data)?;
        let snapshots = kernable_store::snapshots::open_locked(&config.snapshots)?;
        if let Some(vault) = &vault {
            recover_vault(store.as_ref(), vault, snapshots.as_ref())?;
        }
        finish_rewind(&config.store, store.as_ref(), vault.as_ref())?;
        Ok(Stores {
            store,
            vault,
            data,
            snapshots,
        })
    }

    /// Whether nothing was ever written to the store.
    fn empty(&self) -> Result<bool, BoxError> {
        let s = &self.store;
        Ok(s.head_position()? == 0
            && s.command_ids()?.is_empty()
            && s.listeners()?.is_empty()
            && s.grants()?.is_empty()
            && s.workers()?.is_empty())
    }
}

/// The standby, shared by its stream to the primary and its own service.
struct Standby {
    config: Config,
    follow: Follow,
    stores: Mutex<Option<Arc<Stores>>>,
    /// Set by a promotion: the stream to the primary ends, the stores close.
    promote: Mutex<Option<oneshot::Sender<()>>>,
    /// The newest record applied, and the primary's head when it was sent.
    applied: AtomicU64,
    primary_head: AtomicU64,
    /// With `--witness`: this kernel towards the witness, through which it claims the
    /// primary's place.
    link: Option<Arc<Link>>,
    /// A reader catching up (`--reader`): it never becomes the primary; once it holds what the
    /// writer may tell, [`run`] returns.
    reader: bool,
    /// The address it follows now; a reader turns to the next of its addresses when one fails.
    current: Mutex<String>,
    /// Snapshots from the primary that wait for their event, and erasures seen per tenant.
    warm: Mutex<Warmed>,
}

/// Snapshots the primary sent before the standby held their event, each with how many erasures
/// of its tenant the standby had applied then; and that count per tenant.
#[derive(Default)]
struct Warmed {
    waiting: Vec<(SnapshotRecord, u64)>,
    erasures: HashMap<String, u64>,
}

/// The tenants whose events in `batch` erase someone.
fn erased_tenants(batch: &RecordBatch) -> Vec<String> {
    let mut tenants: Vec<String> = batch
        .records
        .iter()
        .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref()))
        .filter_map(|r| match r {
            Record::Committed(c) => Some(c),
            _ => None,
        })
        .flat_map(|c| c.events.iter().filter_map(|l| l.event.as_ref()))
        .filter(|e| !e.erases.is_empty())
        .map(|e| e.tenant_id.clone())
        .collect();
    tenants.sort();
    tenants.dedup();
    tenants
}

/// At most this many snapshots wait for their event; older ones go.
const WAITING: usize = 1024;

/// Whether `stores` hold the event `record` was taken at: the same version and hash. `None`
/// while the standby does not have it yet.
fn holds_its_event(stores: &Stores, record: &SnapshotRecord) -> Result<Option<bool>, BoxError> {
    let Some(snapshot) = &record.snapshot else {
        return Ok(Some(false));
    };
    let events = stores.store.read_stream_after(
        &record.tenant_id,
        &record.domain,
        &record.aggregate_type,
        &record.aggregate_id,
        snapshot.version.saturating_sub(1),
    )?;
    Ok(events
        .first()
        .map(|e| e.version == snapshot.version && e.hash == record.event_hash))
}

impl Standby {
    /// Takes what the primary's caches got: cache values at once, snapshots once their event
    /// is here and no erasure of their tenant came in between (best-effort: a snapshot that
    /// cannot be kept is a cache entry rebuilt later).
    fn warmed(&self, stores: &Stores, batch: CacheBatch) {
        if !batch.values.is_empty() {
            crate::cache::carried(
                &self.config.store,
                self.config.limits.cache_bytes,
                |cache| {
                    for v in &batch.values {
                        cache.apply(v);
                    }
                },
            );
        }
        let mut warm = self
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in batch.snapshots {
            let erased = warm.erasures.get(&record.tenant_id).copied().unwrap_or(0);
            warm.waiting.push((record, erased));
        }
        let over = warm.waiting.len().saturating_sub(WAITING);
        warm.waiting.drain(..over);
        Self::keep_waiting(stores, &mut warm);
    }

    /// After records were applied: erasures of their tenants counted, then what waits tried.
    fn applied_records(&self, stores: &Stores, erased_tenants: &[String]) {
        let mut warm = self
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if warm.waiting.is_empty() && erased_tenants.is_empty() {
            return;
        }
        for tenant in erased_tenants {
            *warm.erasures.entry(tenant.clone()).or_default() += 1;
        }
        Self::keep_waiting(stores, &mut warm);
    }

    fn keep_waiting(stores: &Stores, warm: &mut Warmed) {
        let erasures = std::mem::take(&mut warm.erasures);
        warm.waiting.retain(|(record, erased_then)| {
            // An erasure of its tenant came after it: it may hold erased values.
            if erasures.get(&record.tenant_id).copied().unwrap_or(0) != *erased_then {
                return false;
            }
            match holds_its_event(stores, record) {
                Ok(None) => true,
                Ok(Some(true)) => {
                    if let Err(e) = stores.snapshots.save(record) {
                        tracing::debug!(error = %e, "keeping a snapshot from the primary failed");
                    }
                    false
                }
                Ok(Some(false)) | Err(_) => false,
            }
        });
        warm.erasures = erasures;
    }
}

impl Standby {
    fn stores(&self) -> Arc<Stores> {
        Arc::clone(
            self.lock_stores()
                .as_ref()
                .expect("open while the standby runs"),
        )
    }

    fn lock_stores(&self) -> MutexGuard<'_, Option<Arc<Stores>>> {
        self.stores.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the stores and returns once every handle on them is gone (a request still being
    /// answered may hold one), so their locks are released.
    async fn close_stores(&self) {
        let Some(mut old) = self.lock_stores().take() else {
            return;
        };
        let mut warned = Instant::now();
        loop {
            match Arc::try_unwrap(old) {
                Ok(stores) => {
                    drop(stores);
                    return;
                }
                Err(shared) => {
                    old = shared;
                    if warned.elapsed() >= Duration::from_secs(5) {
                        warned = Instant::now();
                        tracing::warn!(
                            handles = Arc::strong_count(&old) - 1,
                            "waiting for requests to let go of the stores"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Closes the stores, moves them aside (`<path>.diverged-<ms>`) and opens new, empty ones:
    /// for a standby whose log diverged, or that needs an image while it holds data
    /// (design/replication.md, "The stream").
    async fn start_over(&self, why: &str) -> Result<(), BoxError> {
        self.forget_in_sync();
        // Every handle closes before the files move: SQLite's lock goes with the last one.
        self.close_stores().await;
        let at = now_ms();
        let mut moved = Vec::new();
        for url in self.urls() {
            // Only the newest copy stays: one store's size at most, however often it starts over.
            for older in set_aside(&url) {
                match std::fs::remove_file(&older) {
                    Ok(()) => {
                        tracing::info!(file = %older.display(), "an older set-aside copy was removed")
                    }
                    Err(e) => {
                        tracing::warn!(file = %older.display(), error = %e, "removing an older set-aside copy failed")
                    }
                }
            }
            moved.extend(move_aside(&url, at)?);
        }
        tracing::warn!(why, moved = ?moved, "the standby starts over, empty; its stores were moved aside");
        *self.lock_stores() = Some(Arc::new(Stores::open(&self.config)?));
        Ok(())
    }

    /// Takes its records after `to` back (design/rewind.md): they are its own, from when it was
    /// the primary, and the new primary never had them. It follows on from `to` when it
    /// connects again. If they cannot be taken back after all, it starts over.
    async fn rewind(&self, to: u64, why: &str) -> Result<(), BoxError> {
        self.forget_in_sync();
        let stores = self.stores();
        let from = stores.store.record_head()?.seq;
        let note = marker(&self.config.store, "rewinding");
        let taken = tokio::task::spawn_blocking({
            let stores = Arc::clone(&stores);
            move || -> Result<Vec<String>, BoxError> {
                let plan = stores.store.rewind_plan(to)?;
                // Commands it puts back to pending run again: the values they brought must
                // still be here (secrets go once a completion is held, or after a crash).
                if !plan.needs.is_empty() {
                    let held = match &stores.vault {
                        Some(vault) => vault.describe(&plan.needs)?.len(),
                        None => 0,
                    };
                    if held < plan.needs.len() {
                        return Err(
                            "the vault values of a command it would put back are gone".into()
                        );
                    }
                }
                // The commands whose values go are noted first: a crash between the store and
                // the vault is finished when the stores open again (finish_rewind).
                if let Some(note) = &note {
                    crate::replication::write_synced(note, plan.deletes.join("\n").as_bytes())?;
                }
                let deleted = stores.store.rewind(to)?;
                if let Some(vault) = &stores.vault {
                    for id in &deleted {
                        vault.drop_command(id)?;
                    }
                }
                if let Some(note) = &note {
                    std::fs::remove_file(note)?;
                }
                Ok(deleted)
            }
        })
        .await?;
        drop(stores);
        match taken {
            Ok(deleted) => {
                confirmed(&self.stores(), to)?;
                tracing::warn!(
                    from,
                    to,
                    commands = deleted.len(),
                    "the standby took back its records the primary does not hold; it follows on"
                );
                Ok(())
            }
            Err(e) => {
                self.start_over(&format!("{why}; taking them back failed: {e}"))
                    .await
            }
        }
    }

    /// The stores a standby fills from its primary, as URLs.
    fn urls(&self) -> Vec<String> {
        let c = &self.config;
        let mut urls = vec![c.store.clone(), c.snapshots.clone(), c.data.clone()];
        urls.extend(c.vault.as_ref().map(|v| v.store.clone()));
        urls
    }

    /// Where the standby notes that its primary (without a witness) told it it is in sync.
    fn in_sync_marker(&self) -> Option<PathBuf> {
        marker(&self.config.store, "in-sync")
    }

    /// Until the primary tells it again, the standby may not be promoted.
    fn forget_in_sync(&self) {
        if let Some(m) = self.in_sync_marker() {
            let _ = std::fs::remove_file(m);
        }
    }

    /// The address it follows now.
    fn current(&self) -> String {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A reader turns to its next address (both kernels of a pair: it follows the primary).
    fn next_address(&self) {
        let addresses = crate::reader::addresses(&self.follow.primary);
        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let at = addresses.iter().position(|a| *a == *current).unwrap_or(0);
        *current = addresses[(at + 1) % addresses.len()].clone();
    }

    /// A reader caught up: the kernel opens its stores next.
    fn caught_up(&self) {
        if let Some(done) = self
            .promote
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            tracing::info!(
                record = self.applied.load(SeqCst),
                "the reader caught up with the writer: it serves from its copy now"
            );
            let _ = done.send(());
        }
    }

    /// A new reader with `--from-backup` fills its empty stores from the backup's newest
    /// generation; whether it did. If the restore fails, the stores start over empty and the
    /// reader takes an image from the writer instead.
    async fn restore_from_backup(&self) -> Result<bool, BoxError> {
        let Some(from) = self.config.replication.from_backup.clone() else {
            return Ok(false);
        };
        self.close_stores().await;
        let (config, keys) = (self.config.clone(), self.follow.primary_kernel_keys.clone());
        let restored =
            tokio::task::spawn_blocking(move || restore_into(&config, &from, &keys)).await?;
        *self.lock_stores() = Some(Arc::new(Stores::open(&self.config)?));
        match restored {
            Ok(r) => {
                tracing::info!(
                    generation = %r.generation,
                    record = r.record,
                    "the reader restored the backup; it catches up the rest from the writer"
                );
                Ok(true)
            }
            Err(e) => {
                self.start_over(&format!(
                    "restoring the backup failed ({e}); the reader takes an image from the writer"
                ))
                .await?;
                Ok(false)
            }
        }
    }

    /// Why this standby may not become the primary, if it may not.
    fn unpromotable(&self) -> Option<String> {
        if self.reader {
            return Some("it is a reader, a read-only copy: readers are never promoted".into());
        }
        if image_marker(&self.config.store).is_some_and(|m| m.exists()) {
            return Some("its image from the primary is not complete".into());
        }
        // Without a witness, a primary whose standby never caught up told writes alone.
        if self.link.is_none() && !self.in_sync_marker().is_some_and(|m| m.exists()) {
            return Some(
                "it never caught up with its primary: the primary may have told writes it lacks"
                    .into(),
            );
        }
        let stores = self.stores();
        match stores.store.record_head() {
            Ok(head) if head.seq == 0 => Some("it has not followed a primary yet".into()),
            Ok(_) => None,
            Err(e) => Some(format!("its record log cannot be read: {e}")),
        }
    }
}

/// Runs the standby until it is promoted; returns with its stores closed.
pub async fn run(config: Config) -> Result<(), BoxError> {
    run_with(config, None).await
}

/// Like [`run`]; with `link`, a kernel of a pair: it claims the primary's place through the
/// witness once the stream to the primary was lost for a lease and the margin.
pub(crate) async fn run_with(config: Config, link: Option<Arc<Link>>) -> Result<(), BoxError> {
    let follow = config
        .replication
        .follow
        .clone()
        .ok_or("run the standby with --follow")?;
    if kernable_store::backend(&config.store).0 == "memory" {
        return Err("a standby keeps what it follows: --store cannot be memory:".into());
    }
    let key = config
        .read_signing_key(&follow.key)
        .map_err(|e| format!("--follow-key {}: {e}", follow.key.display()))?;
    let grant_path = follow.key.with_extension("grant");
    let grant = std::fs::read_to_string(&grant_path)
        .map_err(|e| format!("the standby's grant {}: {e}", grant_path.display()))?
        .trim()
        .to_owned();
    // A standby on a new binary waits for the old one to let go of the stores; only then
    // are its snapshots, a cache the primary does not send, removed: they start empty.
    let stores = crate::taking_over(config.take_over_ms, || {
        let store = kernable_store::open_with(&config.store, config.durability)?;
        for path in move_aside(&config.snapshots, 0)? {
            std::fs::remove_file(path)?;
        }
        Stores::open_on(&config, store)
    })?;
    let (promoted, promoted_rx) = oneshot::channel();
    let reader = config.replication.reader;
    let first = crate::reader::addresses(&follow.primary)
        .first()
        .cloned()
        .ok_or("--reader needs the writer's address")?;
    let standby = Arc::new(Standby {
        follow: follow.clone(),
        stores: Mutex::new(Some(Arc::new(stores))),
        promote: Mutex::new(Some(promoted)),
        applied: AtomicU64::new(0),
        primary_head: AtomicU64::new(0),
        link,
        reader,
        current: Mutex::new(first),
        warm: Mutex::default(),
        config,
    });
    // Following, it is no primary that handed over. Its note that it is in sync stays
    // until it reaches a primary again: a standby restarted after its primary was lost can
    // still be promoted.
    for name in ["handed-over", "standby-caught-up"] {
        if let Some(m) = marker(&standby.config.store, name) {
            let _ = std::fs::remove_file(m);
        }
    }
    // While it follows, a note says so beside the store, for whoever looks after the machine
    // (a pair's kernels are updated standby first); the kernel opened on the stores removes it.
    if let Some(m) = marker(&standby.config.store, "following") {
        let _ = std::fs::write(m, format!("{}\n", follow.primary));
    }
    // A store an image was left half-filled in starts over.
    if image_marker(&standby.config.store).is_some_and(|m| m.exists()) {
        standby
            .start_over("an image from the primary was not complete")
            .await?;
        if let Some(marker) = image_marker(&standby.config.store) {
            let _ = std::fs::remove_file(marker);
        }
    }
    // A new reader with a backup to start from fills its stores from it first.
    let mut tried_backup = false;
    if reader && standby.stores().empty()? {
        tried_backup = true;
        standby.restore_from_backup().await?;
    }
    if reader {
        tracing::info!(writer = %follow.primary, listen = %standby.config.listen, "reader catching up");
    } else {
        tracing::info!(primary = %follow.primary, listen = %standby.config.listen, "standby starting");
    }
    let (stop_serving, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(Arc::clone(&standby), stopped));
    let metrics = match standby.config.metrics_listen.clone() {
        Some(addr) => {
            let standby = Arc::clone(&standby);
            Some(crate::AbortOnDrop(tokio::spawn(async move {
                if let Err(e) = serve_metrics(standby, &addr).await {
                    tracing::error!(error = %e, "the standby's metrics stopped");
                }
            })))
        }
        None => None,
    };
    let admin = match standby.config.admin_socket.clone() {
        Some(path) => {
            let standby = Arc::clone(&standby);
            Some(crate::AbortOnDrop(tokio::spawn(async move {
                if let Err(e) = serve_admin(standby, &path).await {
                    tracing::error!(error = %e, "the standby's local socket stopped");
                }
            })))
        }
        None => None,
    };
    let following = async {
        // The first try again comes soon: a primary whose binary was replaced serves again
        // within a moment, and a pair's writes wait for its standby meanwhile.
        let mut delay = Duration::from_millis(100);
        let mut last_refusal = None;
        loop {
            let followed = follow_once(&standby, &key, &grant, &mut tried_backup).await;
            // A reader that caught up is done here: its kernel opens the stores next.
            if standby.reader
                && standby
                    .promote
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .is_none()
            {
                return std::future::pending().await;
            }
            let mut refused = false;
            match followed {
                Ok(()) => {
                    last_refusal = None;
                    tracing::warn!("the primary ended the stream; connecting again");
                }
                Err(e) => {
                    if let Some(why) = refusal(&*e) {
                        refused = true;
                        refused_once(&mut last_refusal, why, &standby.current());
                    } else {
                        last_refusal = None;
                        let why = with_causes(&*e);
                        tracing::warn!(error = %why, primary = %standby.current(), "following the primary failed; connecting again in {delay:?}");
                    }
                    if standby.reader {
                        standby.next_address();
                    }
                }
            }
            if let Some(link) = &standby.link {
                // The witness decides whether the primary's lease ran out, or the primary handed
                // its place over: asking at once makes a switchover quick.
                match claim(&standby, link).await {
                    Ok(term) => tracing::warn!(term, "claimed the primary's place"),
                    Err(why) => tracing::info!(%why, "cannot take the primary's place"),
                }
                // A pair reconnects and claims promptly.
                delay = delay.min(link.lease() / 3);
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(if refused {
                REFUSED_MAX_DELAY
            } else {
                Duration::from_secs(30)
            });
        }
    };
    tokio::select! {
        () = following => unreachable!("follows until promoted"),
        result = server => {
            result??;
            return Err("the standby's server stopped".into());
        }
        _ = promoted_rx => {}
    }
    let _ = stop_serving.send(());
    drop(metrics);
    drop(admin);
    // Every handle on the stores closes before the kernel opens them, also one held by a
    // request still being answered (the promotion itself).
    standby.close_stores().await;
    // A primary now, no standby in sync: the note would outlast it beside stores it may leave
    // (a move away) and tell of a standby in sync when one starts on them again (a move back).
    standby.forget_in_sync();
    if !standby.reader {
        tracing::warn!(
            record = standby.applied.load(SeqCst),
            "promoted: this kernel is the primary now"
        );
    }
    Ok(())
}

/// The stream to the primary while one is open: a planned stop says on it that it leaves.
static GOODBYE: Mutex<Option<mpsc::Sender<FollowRequest>>> = Mutex::new(None);
/// Wakes [`leave`] once the stream ended.
static ENDED: std::sync::LazyLock<tokio::sync::Notify> =
    std::sync::LazyLock::new(tokio::sync::Notify::new);

/// A standby stopping on purpose (SIGTERM) tells its primary that it leaves, so a pair's primary
/// commits alone at once instead of after `--standby-timeout-ms`; returns once the primary
/// ended the stream, a second at most, or at once without one.
pub async fn leave() {
    let Some(tx) = GOODBYE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    else {
        return;
    };
    let ended = ENDED.notified();
    tokio::pin!(ended);
    ended.as_mut().enable();
    let goodbye = FollowRequest {
        msg: Some(follow_request::Msg::Leaving(Leaving {})),
    };
    if tx.send(goodbye).await.is_ok() {
        let _ = tokio::time::timeout(Duration::from_secs(1), ended).await;
    }
}

/// Clears [`GOODBYE`] when a stream ends, however it does.
struct Following;

impl Drop for Following {
    fn drop(&mut self) {
        GOODBYE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        ENDED.notify_waiters();
    }
}

/// `e` with its causes, `: `-separated: a transport error says what failed underneath (a
/// certificate not trusted, a connection refused) instead of only "transport error".
fn with_causes(e: &(dyn std::error::Error + 'static)) -> String {
    let mut text = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        let next = c.to_string();
        if !text.contains(&next) {
            text.push_str(": ");
            text.push_str(&next);
        }
        cause = c.source();
    }
    text
}

/// One connection to the primary: catch up, then apply its records as they come.
/// A follower's stream to a primary, opened: where to send acknowledgements, what comes, and
/// how the primary started it.
pub(crate) struct Opened {
    pub(crate) tx: mpsc::Sender<FollowRequest>,
    pub(crate) inbound: Inbound,
    pub(crate) start: kernable_core::proto::FollowStart,
}

/// What comes on a follower's stream. From a relay it is sealed: opened here with the copy key,
/// so nothing past this reads a sealed value (src/seal.rs).
pub(crate) struct Inbound {
    stream: Streaming<FollowResponse>,
    key: Option<Arc<crate::seal::CopyKey>>,
    /// The batches of the image under way.
    images: crate::seal::Images,
}

impl Inbound {
    pub(crate) async fn message(&mut self) -> Result<Option<FollowResponse>, Status> {
        let Some(mut message) = self.stream.message().await? else {
            return Ok(None);
        };
        if let (Some(key), Some(msg)) = (&self.key, message.msg.as_mut()) {
            crate::seal::open(key, msg, &mut self.images)
                .map_err(|e| Status::data_loss(format!("what the relay sent: {e}")))?;
        }
        Ok(Some(message))
    }
}

/// Opens a `Follow` stream to `addr` with `hello`: the challenge answered with `key`, `grant`
/// presented. Pinged every `every`, so a primary the network cut off ends it. With `copy_key`,
/// `addr` is a relay and what comes is opened with it.
pub(crate) async fn open_stream(
    addr: &str,
    key: &SigningKey,
    grant: &str,
    hello: FollowHello,
    every: Duration,
    copy_key: Option<Arc<crate::seal::CopyKey>>,
) -> Result<Opened, BoxError> {
    let mut client = ReplicationServiceClient::new(connect_pinging(addr, Some(every)).await?)
        .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let (tx, rx) = mpsc::channel(16);
    let say = |msg| FollowRequest { msg: Some(msg) };
    let copy = hello.copy.clone();
    tx.send(say(follow_request::Msg::Hello(hello))).await?;
    let mut request = Request::new(ReceiverStream::new(rx));
    request.metadata_mut().insert(GRANT_HEADER, grant.parse()?);
    if !copy.is_empty() {
        // A relay: it knows the reader before it reads anything.
        crate::relay::sign_stream(&mut request, key, "follow", &copy)?;
    }
    let mut inbound = Inbound {
        stream: client.follow(request).await?.into_inner(),
        key: copy_key,
        images: Default::default(),
    };
    let Some(follow_response::Msg::Challenge(challenge)) = next(&mut inbound).await? else {
        return Err("the primary sent no challenge".into());
    };
    let proof = signing::sign(key, &signing::challenge_message(&challenge.nonce));
    tx.send(say(follow_request::Msg::ChallengeResponse(
        ChallengeResponse { sig: proof.sig },
    )))
    .await?;
    let Some(follow_response::Msg::Start(start)) = next(&mut inbound).await? else {
        return Err("the primary did not start the stream".into());
    };
    Ok(Opened { tx, inbound, start })
}

async fn follow_once(
    standby: &Arc<Standby>,
    key: &SigningKey,
    grant: &str,
    tried_backup: &mut bool,
) -> Result<(), BoxError> {
    let follow = &standby.follow;
    // A primary the network cut off sends nothing more, and TCP tells nothing for minutes: pings
    // end the stream within two thirds of a lease, so a pair's standby claims in time.
    let every = standby
        .link
        .as_ref()
        .map_or(Duration::from_secs(5), |link| link.lease() / 3);
    let head = standby.stores().store.record_head()?;
    // Where it could go back to, should its newest records be ones the primary lacks.
    let rewind_points = standby
        .stores()
        .store
        .rewind_points()?
        .into_iter()
        .skip(1)
        .map(|p| RecordPoint {
            seq: p.seq,
            hash: p.hash,
        })
        .collect();
    let say = |msg| FollowRequest { msg: Some(msg) };
    let hello = FollowHello {
        key_id: signing::key_id(&key.verifying_key()),
        seq: head.seq,
        hash: head.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        backup: false,
        rewind_points,
        witness_generation: standby.link.as_ref().and_then(|l| l.generations.newest()),
        reader: standby.reader,
        copy: if standby.reader {
            follow.copy.clone()
        } else {
            String::new()
        },
    };
    let Opened {
        tx,
        mut inbound,
        start,
    } = open_stream(
        &standby.current(),
        key,
        grant,
        hello,
        every,
        copy_key(&standby.follow, standby.reader)?,
    )
    .await?;
    *GOODBYE.lock().unwrap_or_else(PoisonError::into_inner) = Some(tx.clone());
    let _following = Following;
    // Whether it is in sync now, the primary tells it again once it caught up.
    standby.forget_in_sync();
    match start.mode() {
        FollowMode::Follow => {
            tracing::info!(from = head.seq, primary = %standby.current(), "following the primary");
            standby.applied.fetch_max(head.seq, SeqCst);
            // Everything it holds the primary holds too: nothing of it is taken back any more.
            confirmed(&standby.stores(), head.seq)?;
        }
        FollowMode::Image => {
            // A reader that has not tried its backup yet starts from it rather than from an
            // image of the writer.
            if standby.reader && !*tried_backup && standby.config.replication.from_backup.is_some()
            {
                *tried_backup = true;
                if !standby.stores().empty()? {
                    standby
                        .start_over("the writer no longer keeps the records after this reader's")
                        .await?;
                }
                standby.restore_from_backup().await?;
                return Ok(());
            }
            if !standby.stores().empty()? {
                standby
                    .start_over("the primary no longer keeps the records after this standby's")
                    .await?;
                return Ok(());
            }
            let seq = take_image(standby, &mut inbound).await?;
            if tx
                .send(say(follow_request::Msg::Ack(FollowAck { seq })))
                .await
                .is_err()
            {
                return Err(ended("the primary", inbound.message()).await);
            }
        }
        FollowMode::Rewind => {
            standby.rewind(start.rewind_to, &start.message).await?;
            return Ok(());
        }
        FollowMode::Diverged | FollowMode::Unspecified => {
            standby
                .start_over(&format!("the record log diverged: {}", start.message))
                .await?;
            return Ok(());
        }
    }
    loop {
        match next(&mut inbound).await? {
            // Where the writer stands, nothing new: a reader that holds it has caught up.
            Some(follow_response::Msg::Records(batch)) if batch.records.is_empty() => {
                standby.primary_head.store(batch.head, SeqCst);
                if standby.reader && standby.applied.load(SeqCst) >= batch.head {
                    standby.caught_up();
                    return Ok(());
                }
            }
            Some(follow_response::Msg::Records(batch)) => {
                crate::copy::refuse_marked(
                    batch
                        .records
                        .iter()
                        .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref())),
                    "the primary",
                )?;
                standby.primary_head.store(batch.head, SeqCst);
                let erased = erased_tenants(&batch);
                let stores = standby.stores();
                let keys = follow.primary_kernel_keys.clone();
                let applied_to = Arc::clone(&stores);
                let seq =
                    tokio::task::spawn_blocking(move || apply(&applied_to, batch, &keys)).await??;
                standby.applied.store(seq, SeqCst);
                standby.applied_records(&stores, &erased);
                if tx
                    .send(say(follow_request::Msg::Ack(FollowAck { seq })))
                    .await
                    .is_err()
                {
                    return Err(ended("the primary", inbound.message()).await);
                }
                if standby.reader && seq >= standby.primary_head.load(SeqCst) {
                    standby.caught_up();
                    return Ok(());
                }
            }
            Some(follow_response::Msg::InSync(_)) => {
                crate::crash::point("standby.in_sync");
                if let Some(m) = standby.in_sync_marker() {
                    std::fs::write(m, b"the primary told this standby it is in sync\n")?;
                }
                tracing::info!(
                    record = standby.applied.load(SeqCst),
                    "in sync: the primary waits for this standby"
                );
            }
            Some(follow_response::Msg::TakeOver(take)) => {
                let applied = standby.applied.load(SeqCst);
                // It holds every record up to there, and may hold more: the primary's own
                // writes after it chose the point (a checkpoint), all of them from that primary.
                // It takes over at what it holds; only less would lose what was told.
                if applied < take.seq {
                    return Err(format!(
                        "asked to take over at record {}, but this standby holds {applied}",
                        take.seq
                    )
                    .into());
                }
                tx.send(say(follow_request::Msg::TookOver(TookOver {
                    seq: applied,
                })))
                .await?;
                crate::crash::point("standby.took_over");
                // The primary ends the stream once it has the answer; then this standby serves.
                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                    while let Ok(Some(_)) = next(&mut inbound).await {}
                })
                .await;
                tracing::warn!(
                    record = take.seq,
                    "the primary handed its role over: taking over"
                );
                if let Some(promote) = standby
                    .promote
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    let _ = promote.send(());
                }
                return Ok(());
            }
            // The primary's newest witness generation: older witnesses no longer count here,
            // so this standby never claims through one.
            Some(follow_response::Msg::Caches(batch)) => {
                let stores = standby.stores();
                standby.warmed(&stores, batch);
            }
            Some(follow_response::Msg::Generation(g)) => {
                if let Some(link) = &standby.link
                    && let Err(e) = link.generations.learn(&g)
                {
                    tracing::warn!(error = %e, "the primary's witness generation is not taken");
                }
            }
            None => return Ok(()),
            Some(_) => return Err("the primary sent something other than records".into()),
        }
    }
}

/// A reader of a relay's copy key (`--reader-copy-key`).
pub(crate) fn copy_key(
    follow: &Follow,
    reader: bool,
) -> Result<Option<Arc<crate::seal::CopyKey>>, BoxError> {
    match (&follow.copy_key, reader && !follow.copy.is_empty()) {
        (Some(path), true) => match crate::seal::CopyKey::read(path) {
            Ok(key) => Ok(Some(key)),
            Err(e) => Err(format!(
                "--reader-copy-key {}: {e} (the copy key reaches a reader by your own means: copy the pusher's --copy-key file here)",
                path.display()
            )
            .into()),
        },
        (None, true) => Err("a reader of a relay needs --reader-copy-key".into()),
        (_, false) => Ok(None),
    }
}

async fn next(inbound: &mut Inbound) -> Result<Option<follow_response::Msg>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

/// Receives a full image into the empty stores; returns the record it stands at.
async fn take_image(standby: &Arc<Standby>, inbound: &mut Inbound) -> Result<u64, BoxError> {
    let marker = image_marker(&standby.config.store).ok_or("no place for the image marker")?;
    std::fs::write(&marker, b"an image from the primary is being applied\n")?;
    let mut records = 0;
    loop {
        match next(inbound).await? {
            Some(follow_response::Msg::Image(batch)) => {
                crate::copy::refuse_marked(
                    batch.records.iter().filter_map(|l| l.record.as_ref()),
                    "the primary",
                )?;
                records += batch.records.len();
                let stores = standby.stores();
                let keys = standby.follow.primary_kernel_keys.clone();
                tokio::task::spawn_blocking(move || apply_image(&stores, batch, &keys)).await??;
                crate::crash::point("standby.imaging");
            }
            Some(follow_response::Msg::ImageEnd(ImageEnd { seq, hash, .. })) => {
                let stores = standby.stores();
                tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
                    stores.store.start_record_log(&Head { seq, hash })?;
                    stores.store.sync()?;
                    Ok(())
                })
                .await??;
                std::fs::remove_file(&marker)?;
                standby.applied.store(seq, SeqCst);
                tracing::info!(records, record = seq, "took a full image from the primary");
                return Ok(seq);
            }
            None => return Err("the primary ended the stream during the image".into()),
            Some(_) => return Err("the primary sent something else during the image".into()),
        }
    }
}

pub(crate) fn apply_image(
    stores: &Stores,
    batch: ImageBatch,
    keys: &[String],
) -> Result<(), BoxError> {
    let records: Vec<Record> = batch.records.into_iter().filter_map(|l| l.record).collect();
    check_checkpoints(records.iter(), keys)?;
    for file in &batch.files {
        stores.data.restore(&file.path, &file.data)?;
    }
    import(stores.vault.as_ref(), &batch.vault)?;
    stores.store.apply_image(&records)?;
    Ok(())
}

fn import(vault: Option<&Vault>, rows: &[kernable_core::proto::VaultRow]) -> Result<(), BoxError> {
    match vault {
        Some(vault) => Ok(vault.import(rows)?),
        None if rows.is_empty() => Ok(()),
        None => Err("the primary sends vault values: start the standby with --vault-key".into()),
    }
}

/// The primary's checkpoints carry its kernel key's signature.
fn check_checkpoints<'a>(
    records: impl Iterator<Item = &'a Record>,
    keys: &[String],
) -> Result<(), BoxError> {
    for record in records {
        if let Record::Checkpoint(CheckpointRecord {
            global_position,
            chain_hash,
            kernel_sig,
            created_at,
        }) = record
        {
            let sig = kernel_sig.as_ref().ok_or("an unsigned checkpoint")?;
            let msg = signing::checkpoint_message(*global_position, chain_hash, *created_at);
            if !keys.contains(&sig.key_id) || signing::verify(sig, &msg).is_err() {
                return Err(format!(
                    "the checkpoint of event {global_position} is not signed by the primary's kernel key (--primary-kernel-key)"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// A commit's command, its stream, and what the vault does around it, as on the primary.
struct Around {
    command_id: String,
    tenant: String,
    stream: StreamId,
    erasing: Vec<String>,
    /// Personal keys the stream held before the commit.
    held: Vec<StoredKey>,
    /// The keys and facts the commit files, if it files them.
    files: Option<Vec<StoredKey>>,
}

/// Applies a batch of the primary's records: files and vault values first (the records refer
/// to them), erasures noted, the records, then what the vault drops after a commit. Returns the
/// last record applied.
pub(crate) fn apply(stores: &Stores, batch: RecordBatch, keys: &[String]) -> Result<u64, BoxError> {
    apply_to(stores.parts(), batch, keys)
}

/// [`apply`] to any [`Parts`]: a standby's stores, or a reader's kernel.
pub(crate) fn apply_to(
    stores: Parts<'_>,
    batch: RecordBatch,
    keys: &[String],
) -> Result<u64, BoxError> {
    let last = batch.records.last().map_or(0, |r| r.seq);
    let records: Vec<&Record> = batch
        .records
        .iter()
        .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref()))
        .collect();
    check_checkpoints(records.iter().copied(), keys)?;
    for f in &batch.files {
        let key = DataKey {
            tenant_id: f.tenant_id.clone(),
            domain: f.domain.clone(),
            key: f.key.clone(),
        };
        if f.absent {
            stores.data.delete(&key)?;
        } else {
            stores.data.put(
                &key,
                &Data {
                    content_type: f.content_type.clone(),
                    data: f.data.clone(),
                },
            )?;
        }
    }
    import(stores.vault, &batch.vault)?;
    let store = stores.store;
    let mut around = Vec::new();
    let mut completed = Vec::new();
    for record in &records {
        match record {
            Record::Committed(c) => {
                let Some(command) = store.command(&c.command_id)?.map(|p| p.command) else {
                    continue; // refused by the store below
                };
                let stream = StreamId {
                    tenant_id: command.tenant_id.clone(),
                    domain: command.domain.clone(),
                    aggregate_type: command.aggregate_type.clone(),
                    aggregate_id: command.aggregate_id.clone(),
                };
                let held = store
                    .stream_index(&stream)?
                    .map(|i| i.keys)
                    .unwrap_or_default();
                let erasing: Vec<String> = c
                    .events
                    .iter()
                    .filter_map(|l| l.event.as_ref())
                    .filter(|e| !e.erases.is_empty())
                    .map(|e| e.erases.clone())
                    .collect();
                if let Some(vault) = &stores.vault {
                    for subject in &erasing {
                        vault.intend_erase(&command.tenant_id, subject, &c.command_id)?;
                    }
                }
                around.push(Around {
                    command_id: c.command_id.clone(),
                    tenant: command.tenant_id,
                    stream,
                    erasing,
                    held,
                    files: c.index.as_ref().map(|i| i.keys.clone()),
                });
                completed.push(c.command_id.clone());
            }
            Record::Rejected(r) => completed.push(r.command_id.clone()),
            _ => {}
        }
    }
    store.apply(&batch.records)?;
    store.sync()?;
    crate::crash::point("standby.applied");
    let Some(vault) = stores.vault else {
        return Ok(last);
    };
    for command_id in &completed {
        vault.drop_secrets(command_id)?;
    }
    for a in around {
        // A personal key the commit released frees its value.
        if let Some(files) = &a.files {
            let released: Vec<String> = a
                .held
                .iter()
                .filter(|k| !k.vault_ref.is_empty())
                .filter(|k| !files.iter().any(|f| f.vault_ref == k.vault_ref))
                .map(|k| k.vault_ref.clone())
                .collect();
            vault.remove(&released)?;
        }
        if !a.erasing.is_empty() {
            erase_subjects(
                vault,
                stores.snapshots,
                &a.tenant,
                &a.command_id,
                &a.erasing,
                &a.stream,
            );
            // Each carried out erasure dropped its intent; one that failed keeps it, and the
            // next start carries it out (recover_vault).
        }
    }
    Ok(last)
}

/// Restores the newest generation of the backup `from` into the reader's empty stores, with
/// the backup's vault key, and encrypts the values with the reader's own vault key after.
fn restore_into(
    config: &Config,
    from: &config::FromBackup,
    kernel_keys: &[String],
) -> Result<crate::backup::Restored, BoxError> {
    // Through the signer, as `kernable restore` does: the reader holds no S3 keys then. It
    // asks with KERNABLE_S3_SIGNER_KEY, else with its own follow key.
    let signer = match &from.signer {
        None => None,
        Some(url) => {
            let key = match (&from.signer_key, &config.replication.follow) {
                (Some(path), _) => config.read_signing_key(path),
                (None, Some(follow)) => config.read_signing_key(&follow.key),
                (None, None) => Err(std::io::Error::other(
                    "a signer needs a key: KERNABLE_S3_SIGNER_KEY",
                )),
            }
            .map_err(|e| format!("the backup signer's key: {e}"))?;
            Some(crate::backup::target::Signer::new(url, key)?)
        }
    };
    let target = crate::backup::target::open(&from.target, signer)?;
    let crypt = crate::backup::crypt::BackupKey::read(&from.key)?;
    let vault = match (&config.vault, &from.vault_key) {
        (Some(v), Some(backup_key)) => Some(Vault::open_locked(
            &v.store,
            &*config.read_secret(backup_key)?,
        )?),
        (Some(v), None) => Some(Vault::open_locked(&v.store, &*config.read_secret(&v.key)?)?),
        (None, _) => None,
    };
    let stores = Stores {
        store: kernable_store::open_with(&config.store, kernable_store::Durability::Normal)?,
        vault,
        data: kernable_store::data::open(&config.data)?,
        snapshots: kernable_store::snapshots::open("memory:")?,
    };
    let restored = crate::backup::restore(
        target.as_ref(),
        &crypt,
        &stores,
        crate::backup::Until::End,
        kernel_keys,
    )?;
    if let (Some(vault), Some(v), Some(_)) = (&stores.vault, &config.vault, &from.vault_key) {
        vault.rekey(&*config.read_secret(&v.key)?)?;
    }
    stores.store.sync()?;
    Ok(restored)
}

/// Where a store marks an image in progress: `<path>.image`.
fn image_marker(url: &str) -> Option<PathBuf> {
    marker(url, "image")
}

/// A note beside the store at `url`: `<path>.<name>`; none for a store in memory.
pub(crate) fn marker(url: &str, name: &str) -> Option<PathBuf> {
    match kernable_store::backend(url) {
        ("memory", _) => None,
        (_, path) => Some(PathBuf::from(format!("{path}.{name}"))),
    }
}

/// The copies of the store at `url` moved aside before (`<file>.diverged-<ms>`, also its
/// `-wal`, `-shm`, `.lock` and `.image`).
fn set_aside(url: &str) -> Vec<PathBuf> {
    let path = match url.split_once(':') {
        Some(("memory", _)) => return Vec::new(),
        Some((_, path)) => PathBuf::from(path),
        None => PathBuf::from(url),
    };
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return Vec::new();
    };
    let dir = if dir.as_os_str().is_empty() {
        std::path::Path::new(".")
    } else {
        dir
    };
    let name = name.to_string_lossy();
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            let file = e.file_name().to_string_lossy().into_owned();
            ["", "-wal", "-shm", ".lock", ".image"]
                .iter()
                .any(|suffix| {
                    file.strip_prefix(&format!("{name}{suffix}.diverged-"))
                        .is_some_and(|at| !at.is_empty() && at.bytes().all(|b| b.is_ascii_digit()))
                })
        })
        .map(|e| e.path())
        .collect()
}

/// The primary holds every record up to `seq`: none of them is taken back any more, and the
/// secrets of commands completed among them go (design/rewind.md).
fn confirmed(stores: &Stores, seq: u64) -> Result<(), BoxError> {
    let completed = stores.store.forget_undo(seq)?;
    if let Some(vault) = &stores.vault {
        for id in completed {
            if stores.store.result(&id)?.is_some() {
                vault.drop_secrets(&id)?;
            }
        }
    }
    Ok(())
}

/// Finishes a rewind a crash cut short (design/rewind.md): the vault values of the commands it
/// took back go, if the store no longer holds them. Run when stores open.
pub fn finish_rewind(url: &str, store: &dyn Store, vault: Option<&Vault>) -> Result<(), BoxError> {
    let Some(note) = marker(url, "rewinding").filter(|m| m.exists()) else {
        return Ok(());
    };
    let ids = std::fs::read_to_string(&note)?;
    if let Some(vault) = vault {
        for id in ids.lines().filter(|l| !l.is_empty()) {
            if store.command(id)?.is_none() {
                vault.drop_command(id)?;
            }
        }
    }
    std::fs::remove_file(&note)?;
    Ok(())
}

/// Moves the files of the store at `url` to `<path>.diverged-<at>`; returns where they went.
fn move_aside(url: &str, at: i64) -> Result<Vec<PathBuf>, BoxError> {
    let path = match url.split_once(':') {
        Some(("memory", _)) => return Ok(Vec::new()),
        Some((_, path)) => path,
        None => url,
    };
    let mut moved = Vec::new();
    for suffix in ["", "-wal", "-shm", ".lock", ".image"] {
        let from = PathBuf::from(format!("{path}{suffix}"));
        if from.exists() {
            let to = PathBuf::from(format!("{path}{suffix}.diverged-{at}"));
            std::fs::rename(&from, &to)?;
            moved.push(to);
        }
    }
    Ok(moved)
}

/// The longest wait between asks while the other side refuses this key (its grant, or a
/// relay's access list): it may be allowed later, by whoever runs it, without a restart here.
pub(crate) const REFUSED_MAX_DELAY: Duration = Duration::from_secs(60);

/// Why the other side refused, if it did (`PERMISSION_DENIED`, `UNAUTHENTICATED`), from the
/// error or one of its causes.
pub(crate) fn refusal(e: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut next = Some(e);
    while let Some(e) = next {
        if let Some(status) = e.downcast_ref::<Status>()
            && matches!(
                status.code(),
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
            )
        {
            return Some(status.message().to_owned());
        }
        next = e.source();
    }
    None
}

/// Logs a refusal as an error once per reason (`last`, the one logged before): a refused key
/// is the operator's to fix, and it says so without repeating itself every minute.
pub(crate) fn refused_once(last: &mut Option<String>, why: String, peer: &str) {
    if last.as_deref() != Some(why.as_str()) {
        tracing::error!(error = %why, %peer, "refused: asking again, at most every 60 s, until it is allowed");
        *last = Some(why);
    }
}

/// Why a stream ended, once sending on it failed ("channel closed"): the other side's status,
/// still unread in what it sent, if it comes within 2 s ("the copy is at its storage limit", "a
/// damaged frame", "refused"); else that `peer` ended it without one.
pub(crate) async fn ended<T>(
    peer: &str,
    next: impl std::future::Future<Output = Result<Option<T>, Status>>,
) -> BoxError {
    match tokio::time::timeout(Duration::from_secs(2), async {
        let mut next = std::pin::pin!(next);
        next.as_mut().await
    })
    .await
    {
        Ok(Err(status)) => status.into(),
        _ => format!("{peer} ended the stream without a reason").into(),
    }
}

/// A channel to the primary, over mutual TLS when `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and
/// `KERNABLE_TLS_KEY` name the files, as for the SDKs; to a Unix socket always without (a kernel
/// there takes none), so a pusher reaches its writer's socket and a relay over TLS.
pub(crate) async fn connect(addr: &str) -> Result<tonic::transport::Channel, BoxError> {
    connect_pinging(addr, None).await
}

/// Like [`connect`], the connection pinged every `every` and ended when a ping goes unanswered
/// as long: a standby's stream to a primary the network cut off ends, so it claims.
pub(crate) async fn connect_pinging(
    addr: &str,
    every: Option<Duration>,
) -> Result<tonic::transport::Channel, BoxError> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    if transport::unix_path(addr).is_some() {
        return Ok(transport::connect_pinging(addr, every).await?);
    }
    // `https://`: TLS against the public CAs, no certificate of its own needed (a relay knows
    // its pushers and readers by their keys); KERNABLE_TLS_* still apply where set.
    if addr.starts_with("https://") {
        let (ca, cert, key) = (
            var("KERNABLE_TLS_CA"),
            var("KERNABLE_TLS_CERT"),
            var("KERNABLE_TLS_KEY"),
        );
        let identity = cert.as_deref().zip(key.as_deref());
        return transport::connect_https_pinging(
            addr,
            ca.as_deref().map(std::path::Path::new),
            identity.map(|(c, k)| (std::path::Path::new(c), std::path::Path::new(k))),
            every,
        )
        .await;
    }
    match (
        var("KERNABLE_TLS_CA"),
        var("KERNABLE_TLS_CERT"),
        var("KERNABLE_TLS_KEY"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let tls = transport::Tls {
                ca: ca.into(),
                cert: cert.into(),
                key: key.into(),
            };
            transport::connect_tls_pinging(addr, &tls, every).await
        }
        _ => Ok(transport::connect_pinging(addr, every).await?),
    }
}

// ---- the standby's own service ----

#[derive(Clone)]
struct Api(Arc<Standby>);

#[tonic::async_trait]
impl ReplicationService for Api {
    type FollowStream = ReceiverStream<Result<FollowResponse, Status>>;

    async fn follow(
        &self,
        _request: Request<Streaming<FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        Err(not_primary(&self.0.follow.primary, self.0.reader))
    }

    async fn switchover(
        &self,
        _request: Request<kernable_core::proto::SwitchoverRequest>,
    ) -> Result<Response<kernable_core::proto::SwitchoverResult>, Status> {
        Err(Status::failed_precondition(format!(
            "this kernel is the standby: ask the primary ({}) to switch over",
            self.0.follow.primary
        )))
    }

    async fn promote(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResult>, Status> {
        let standby = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("the promote request is not signed"))?;
        signing::verify(&sig, &signing::promote_message(req.issued_at))
            .map_err(|_| Status::unauthenticated("invalid promote request signature"))?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "promote request expired; check the clock",
            ));
        }
        may_manage(standby, &metadata, &sig.key_id)?;
        if let Some(why) = standby.unpromotable() {
            return Err(Status::failed_precondition(format!(
                "this standby cannot become the primary: {why}"
            )));
        }
        let seq = standby.applied.load(SeqCst).max(
            standby
                .stores()
                .store
                .record_head()
                .map(|h| h.seq)
                .unwrap_or(0),
        );
        if let Some(link) = &standby.link {
            // A pair: the witness decides, so the two never both serve.
            let term = claim(standby, link)
                .await
                .map_err(Status::failed_precondition)?;
            tracing::warn!(key = %sig.key_id, term, record = seq, "promoted by an operator, through the witness");
            return Ok(Response::new(PromoteResult { seq }));
        }
        let promote = standby
            .promote
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match promote {
            Some(promote) => {
                tracing::warn!(key = %sig.key_id, record = seq, "promoted by an operator");
                let _ = promote.send(());
                Ok(Response::new(PromoteResult { seq }))
            }
            None => Err(Status::failed_precondition("promoted already")),
        }
    }
}

/// Asks the witness for the primary's place; granted, the standby stops following. Returns
/// the new term, or why not.
async fn claim(standby: &Standby, link: &Link) -> Result<u64, String> {
    if let Some(why) = standby.unpromotable() {
        return Err(why);
    }
    let answer = link
        .ask(WitnessOp::Claim, 0, "")
        .await
        .map_err(|e| format!("the witness does not answer: {e}"))?;
    if !answer.granted || answer.primary != link.me {
        return Err(answer.message);
    }
    if let Some(promote) = standby
        .promote
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    {
        let _ = promote.send(());
    }
    Ok(answer.term)
}

/// Whether `key_id` may promote: its grant (presented or stored, signed by a root the kernel
/// trusts, not revoked) allows `manage` for `*`.
fn may_manage(
    standby: &Standby,
    metadata: &tonic::metadata::MetadataMap,
    key_id: &str,
) -> Result<(), Status> {
    let stores = standby.stores();
    let internal = |e: kernable_store::Error| Status::internal(e.to_string());
    let presented = metadata
        .get(GRANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| hex::decode(v.trim()).ok())
        .and_then(|b| Grant::decode(b.as_slice()).ok());
    let stored = stores.store.grants().map_err(internal)?;
    let roots = &standby.config.roots;
    // By a root, or delegated by a key a root let grant it.
    let signed = |g: &Grant| crate::grants::chain(g, roots, None).is_ok();
    let newest = presented
        .into_iter()
        .chain(stored)
        .filter(|g| g.key_id == key_id && signed(g))
        .max_by_key(|g| g.issued_at);
    let revoked: HashMap<String, i64> = stores
        .store
        .revocations()
        .map_err(internal)?
        .into_iter()
        .map(|r| (r.key_id, r.since))
        .collect();
    let grants: HashMap<String, Grant> =
        newest.into_iter().map(|g| (g.key_id.clone(), g)).collect();
    let permissions = Permissions::new(&grants, &revoked);
    if !config::allowed(&permissions.manage, key_id, "*") {
        return Err(Status::permission_denied(
            "the key may not promote: its grant needs manage for *",
        ));
    }
    Ok(())
}

fn not_primary(primary: &str, reader: bool) -> Status {
    if reader {
        return Status::unavailable(format!(
            "reader_catching_up: this kernel is a reader of {primary} and has not caught up yet; ask another kernel or try again"
        ));
    }
    Status::unavailable(format!(
        "not_primary: this kernel is a standby of {primary}; connect to the primary"
    ))
}

/// Serves the standby's own service on `--listen`; every other request is refused as
/// `not_primary`. Stops when `stop` fires.
async fn serve(standby: Arc<Standby>, stop: oneshot::Receiver<()>) -> Result<(), BoxError> {
    let config = &standby.config;
    let listen = config.listen.clone();
    let mut builder = Server::builder();
    if let Some(tls) = &config.tls {
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let client_ca = Certificate::from_pem(std::fs::read(&tls.client_ca)?);
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )?;
    }
    let primary: Arc<str> = standby.follow.primary.as_str().into();
    let reader = standby.reader;
    let router = builder
        .layer(tower::layer::layer_fn(move |inner| NotPrimary {
            inner,
            primary: Arc::clone(&primary),
            reader,
        }))
        .add_service(ReplicationServiceServer::new(Api(Arc::clone(&standby))));
    let stopped = async {
        let _ = stop.await;
    };
    match transport::unix_path(&listen) {
        Some(path) => {
            use std::os::unix::fs::FileTypeExt;
            if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                std::fs::remove_file(path)?;
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            crate::socket_mode(path)?;
            crate::clear_handed_over(path);
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            router
                .serve_with_incoming_shutdown(incoming, stopped)
                .await?;
            let _ = std::fs::remove_file(path);
        }
        None => {
            let addr = tokio::net::lookup_host(&listen)
                .await?
                .next()
                .ok_or_else(|| format!("{listen}: no address"))?;
            router.serve_with_shutdown(addr, stopped).await?;
        }
    }
    Ok(())
}

/// The standby's `GET /metrics`, `/health` and `/ready` (always 503: a standby serves nothing).
async fn serve_metrics(standby: Arc<Standby>, addr: &str) -> Result<(), BoxError> {
    crate::http::serve_with(
        addr,
        |_| {},
        move |request: Vec<u8>| {
            let standby = Arc::clone(&standby);
            async move { respond(&standby, &request) }
        },
    )
    .await
}

/// What a standby, or a reader catching up, answers on its metrics address and its local
/// socket: `/health`, `/ready` (always 503: it serves nothing yet), `/metrics`, `/api/role`.
fn respond(standby: &Standby, request: &[u8]) -> Vec<u8> {
    use crate::http::reply;
    let applied = standby.applied.load(SeqCst);
    let head = standby.primary_head.load(SeqCst);
    let lag = head.saturating_sub(applied);
    let role = if standby.reader { "reader" } else { "standby" };
    if request.starts_with(b"GET /health ") {
        let body = match standby.reader {
            true => format!(
                "ok: a reader of {}, catching up, {lag} records behind\n",
                standby.current()
            ),
            false => format!(
                "ok: a standby of {}, {lag} records behind\n",
                standby.current()
            ),
        };
        reply("200 OK", "text/plain", body.as_bytes())
    } else if request.starts_with(b"GET /ready ") {
        let why = match standby.reader {
            true => format!(
                "a reader of {} catching up: {lag} records behind\n",
                standby.current()
            ),
            false => format!("a standby of {}\n", standby.follow.primary),
        };
        reply("503 Service Unavailable", "text/plain", why.as_bytes())
    } else if request.starts_with(b"GET /metrics ") {
        let body = crate::metrics::render_standby(applied, head, standby.reader);
        reply("200 OK", "text/plain; version=0.0.4", body.as_bytes())
    } else if request.starts_with(b"GET /api/role ") {
        let body = serde_json::json!({
            "role": role,
            "state": if standby.reader { "catching-up" } else { "following" },
            "following": standby.current(),
            "applied": applied,
            "head": head,
            "lag": lag,
        });
        reply("200 OK", "application/json", body.to_string().as_bytes())
    } else {
        reply("404 Not Found", "text/plain", b"")
    }
}

/// Serves [`respond`] on `--admin-socket` while the standby follows.
async fn serve_admin(standby: Arc<Standby>, path: &str) -> Result<(), BoxError> {
    crate::admin::serve_with(path, move |request| respond(&standby, request)).await
}

/// Answers every request but the replication service's with `UNAVAILABLE`, `not_primary`.
#[derive(Clone)]
struct NotPrimary<S> {
    inner: S,
    primary: Arc<str>,
    reader: bool,
}

type Answer<R, E> = Pin<Box<dyn std::future::Future<Output = Result<R, E>> + Send>>;

impl<S, B> tower::Service<http::Request<B>> for NotPrimary<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Answer<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request
            .uri()
            .path()
            .starts_with("/kernable.v1.ReplicationService/")
        {
            return Box::pin(self.inner.call(request));
        }
        let answer = not_primary(&self.primary, self.reader).into_http();
        Box::pin(async move { Ok(answer) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_of(tenant: &str) -> SnapshotRecord {
        SnapshotRecord {
            tenant_id: tenant.into(),
            domain: "orders".into(),
            aggregate_type: "Order".into(),
            aggregate_id: "o1".into(),
            snapshot: Some(kernable_core::proto::Snapshot {
                version: 1,
                ..Default::default()
            }),
            event_hash: vec![1; 32],
            deployment: String::new(),
        }
    }

    #[test]
    fn a_snapshot_waits_for_its_event_and_goes_after_an_erasure_of_its_tenant() {
        let stores = Stores {
            store: kernable_store::open("memory:").unwrap(),
            vault: None,
            data: kernable_store::data::open("memory:").unwrap(),
            snapshots: kernable_store::snapshots::open("memory:").unwrap(),
        };
        let mut warm = Warmed {
            waiting: vec![(snapshot_of("t"), 0), (snapshot_of("u"), 0)],
            erasures: HashMap::from([("t".to_owned(), 1)]),
        };
        Standby::keep_waiting(&stores, &mut warm);
        // Tenant t erased someone after its snapshot was taken: it goes, unkept.
        // Tenant u's snapshot waits: the standby does not hold its event yet.
        let left: Vec<&str> = warm
            .waiting
            .iter()
            .map(|(r, _)| r.tenant_id.as_str())
            .collect();
        assert_eq!(left, ["u"]);
        assert_eq!(warm.erasures.get("t"), Some(&1), "the count stays");
        let stream = kernable_store::StreamId {
            tenant_id: "t".into(),
            domain: "orders".into(),
            aggregate_type: "Order".into(),
            aggregate_id: "o1".into(),
        };
        assert!(stores.snapshots.get(&stream, "").unwrap().is_none());
    }

    #[test]
    fn an_error_is_told_with_its_causes() {
        let inner = std::io::Error::other("invalid peer certificate: UnknownIssuer");
        let outer = std::io::Error::other(OuterError(inner));
        let text = with_causes(&outer);
        assert!(text.starts_with("transport error"), "{text}");
        assert!(text.contains("UnknownIssuer"), "{text}");
    }

    #[derive(Debug)]
    struct OuterError(std::io::Error);
    impl std::fmt::Display for OuterError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("transport error")
        }
    }
    impl std::error::Error for OuterError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    use std::path::Path;

    #[test]
    fn a_store_is_moved_aside_with_its_files() {
        let dir = std::env::temp_dir().join(format!("kn-aside-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("k.db");
        for suffix in ["", "-wal", ".lock"] {
            std::fs::write(format!("{}{suffix}", db.display()), b"x").unwrap();
        }
        let moved = move_aside(&format!("sqlite:{}", db.display()), 7).unwrap();
        assert_eq!(moved.len(), 3);
        assert!(!db.exists());
        assert!(Path::new(&format!("{}.diverged-7", db.display())).exists());
        assert_eq!(move_aside("memory:", 7).unwrap(), Vec::<PathBuf>::new());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod ended_tests {
    use super::*;

    #[tokio::test]
    async fn a_closed_stream_tells_the_other_sides_reason() {
        let full = ended("the relay", async {
            Err::<Option<()>, _>(Status::resource_exhausted("the copy is full"))
        })
        .await;
        assert!(full.to_string().contains("the copy is full"), "{full}");
        let quiet = ended("the relay", async { Ok::<Option<()>, Status>(None) }).await;
        assert_eq!(
            quiet.to_string(),
            "the relay ended the stream without a reason"
        );
        // One that says nothing more within 2 s is not waited for longer.
        let started = std::time::Instant::now();
        let silent = ended(
            "the writer",
            std::future::pending::<Result<Option<()>, Status>>(),
        )
        .await;
        assert!(started.elapsed() < Duration::from_secs(3), "{silent}");
    }
}
