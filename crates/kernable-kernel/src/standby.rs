//! The standby (design/replication.md): it follows a primary, applies its records, serves
//! nothing else, and becomes the primary when `kernable promote` asks it to.
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
        ChallengeResponse, CheckpointRecord, FollowAck, FollowHello, FollowMode, FollowRequest,
        FollowResponse, Grant, ImageBatch, ImageEnd, Leaving, PromoteRequest, PromoteResult,
        RecordBatch, StoredKey, TookOver, WitnessOp, follow_request, follow_response,
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

impl Stores {
    fn open(config: &Config) -> Result<Self, BoxError> {
        Self::open_on(
            config,
            kernable_store::open_with(&config.store, config.durability)?,
        )
    }

    fn open_on(config: &Config, store: Box<dyn Store>) -> Result<Self, BoxError> {
        store.keep_record_log(true)?;
        let vault = match &config.vault {
            Some(v) => Some(Vault::open(&v.store, &*config.read_secret(&v.key)?)?),
            None => None,
        };
        let data = kernable_store::data::open(&config.data)?;
        let snapshots = kernable_store::snapshots::open(&config.snapshots)?;
        if let Some(vault) = &vault {
            recover_vault(store.as_ref(), vault, snapshots.as_ref())?;
        }
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
    /// for a standby whose log diverged, or that needs an image while it holds data (P4).
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

    /// Why this standby may not become the primary, if it may not.
    fn unpromotable(&self) -> Option<String> {
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
    let standby = Arc::new(Standby {
        follow: follow.clone(),
        stores: Mutex::new(Some(Arc::new(stores))),
        promote: Mutex::new(Some(promoted)),
        applied: AtomicU64::new(0),
        primary_head: AtomicU64::new(0),
        link,
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
    tracing::info!(primary = %follow.primary, listen = %standby.config.listen, "standby starting");
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
    let following = async {
        // The first try again comes soon: a primary whose binary was replaced serves again
        // within a moment, and a pair's writes wait for its standby meanwhile.
        let mut delay = Duration::from_millis(100);
        loop {
            match follow_once(&standby, &key, &grant).await {
                Ok(()) => tracing::warn!("the primary ended the stream; connecting again"),
                Err(e) => {
                    let why = with_causes(&*e);
                    tracing::warn!(error = %why, "following the primary failed; connecting again in {delay:?}")
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
            delay = (delay * 2).min(Duration::from_secs(30));
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
    // Every handle on the stores closes before the kernel opens them, also one held by a
    // request still being answered (the promotion itself).
    standby.close_stores().await;
    // A primary now, no standby in sync: the note would outlast it beside stores it may leave
    // (a move away) and tell of a standby in sync when one starts on them again (a move back).
    standby.forget_in_sync();
    tracing::warn!(
        record = standby.applied.load(SeqCst),
        "promoted: this kernel is the primary now"
    );
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
async fn follow_once(
    standby: &Arc<Standby>,
    key: &SigningKey,
    grant: &str,
) -> Result<(), BoxError> {
    let follow = &standby.follow;
    // A primary the network cut off sends nothing more, and TCP tells nothing for minutes: pings
    // end the stream within two thirds of a lease, so a pair's standby claims in time.
    let every = standby
        .link
        .as_ref()
        .map_or(Duration::from_secs(5), |link| link.lease() / 3);
    let mut client =
        ReplicationServiceClient::new(connect_pinging(&follow.primary, Some(every)).await?)
            .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let head = standby.stores().store.record_head()?;
    let (tx, rx) = mpsc::channel(16);
    let say = |msg| FollowRequest { msg: Some(msg) };
    tx.send(say(follow_request::Msg::Hello(FollowHello {
        key_id: signing::key_id(&key.verifying_key()),
        seq: head.seq,
        hash: head.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        backup: false,
    })))
    .await?;
    let mut request = Request::new(ReceiverStream::new(rx));
    request.metadata_mut().insert(GRANT_HEADER, grant.parse()?);
    let mut inbound = client.follow(request).await?.into_inner();
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
    *GOODBYE.lock().unwrap_or_else(PoisonError::into_inner) = Some(tx.clone());
    let _following = Following;
    // Whether it is in sync now, the primary tells it again once it caught up.
    standby.forget_in_sync();
    match start.mode() {
        FollowMode::Follow => {
            tracing::info!(from = head.seq, "following the primary");
        }
        FollowMode::Image => {
            if !standby.stores().empty()? {
                standby
                    .start_over("the primary no longer keeps the records after this standby's")
                    .await?;
                return Ok(());
            }
            standby.forget_in_sync();
            let seq = take_image(standby, &mut inbound).await?;
            tx.send(say(follow_request::Msg::Ack(FollowAck { seq })))
                .await?;
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
            Some(follow_response::Msg::Records(batch)) => {
                standby.primary_head.store(batch.head, SeqCst);
                let stores = standby.stores();
                let keys = follow.primary_kernel_keys.clone();
                let seq =
                    tokio::task::spawn_blocking(move || apply(&stores, batch, &keys)).await??;
                standby.applied.store(seq, SeqCst);
                tx.send(say(follow_request::Msg::Ack(FollowAck { seq })))
                    .await?;
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
            None => return Ok(()),
            Some(_) => return Err("the primary sent something other than records".into()),
        }
    }
}

async fn next(
    inbound: &mut Streaming<FollowResponse>,
) -> Result<Option<follow_response::Msg>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

/// Receives a full image into the empty stores; returns the record it stands at.
async fn take_image(
    standby: &Arc<Standby>,
    inbound: &mut Streaming<FollowResponse>,
) -> Result<u64, BoxError> {
    let marker = image_marker(&standby.config.store).ok_or("no place for the image marker")?;
    std::fs::write(&marker, b"an image from the primary is being applied\n")?;
    let mut records = 0;
    loop {
        match next(inbound).await? {
            Some(follow_response::Msg::Image(batch)) => {
                records += batch.records.len();
                let stores = standby.stores();
                let keys = standby.follow.primary_kernel_keys.clone();
                tokio::task::spawn_blocking(move || apply_image(&stores, batch, &keys)).await??;
                crate::crash::point("standby.imaging");
            }
            Some(follow_response::Msg::ImageEnd(ImageEnd { seq, hash })) => {
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
    import(stores, &batch.vault)?;
    stores.store.apply_image(&records)?;
    Ok(())
}

fn import(stores: &Stores, rows: &[kernable_core::proto::VaultRow]) -> Result<(), BoxError> {
    match &stores.vault {
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
    import(stores, &batch.vault)?;
    let store = &stores.store;
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
    let Some(vault) = &stores.vault else {
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
                stores.snapshots.as_ref(),
                &a.tenant,
                &a.command_id,
                &a.erasing,
                &a.stream,
            );
            vault.drop_intents(&a.command_id)?;
        }
    }
    Ok(last)
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

/// Moves the files of the store at `url` to `<path>.diverged-<at>`; returns where they went.
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

/// A channel to the primary, over mutual TLS when `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and
/// `KERNABLE_TLS_KEY` name the files, as for the SDKs.
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
        Err(not_primary(&self.0.follow.primary))
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
    let signed = |g: &Grant| {
        g.root_sig.as_ref().is_some_and(|s| {
            roots.contains(&s.key_id) && signing::verify(s, &signing::grant_message(g)).is_ok()
        })
    };
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

fn not_primary(primary: &str) -> Status {
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
    let router = builder
        .layer(tower::layer::layer_fn(move |inner| NotPrimary {
            inner,
            primary: Arc::clone(&primary),
        }))
        .add_service(ReplicationServiceServer::new(Api(Arc::clone(&standby))));
    let stopped = async {
        let _ = stop.await;
    };
    match transport::unix_path(&listen) {
        #[cfg(unix)]
        Some(path) => {
            use std::os::unix::fs::FileTypeExt;
            if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                std::fs::remove_file(path)?;
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            crate::socket_mode(path)?;
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            router
                .serve_with_incoming_shutdown(incoming, stopped)
                .await?;
            let _ = std::fs::remove_file(path);
        }
        #[cfg(not(unix))]
        Some(_) => return Err("unix: sockets need Unix; listen on host:port".into()),
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
            async move {
                use crate::http::reply;
                if request.starts_with(b"GET /health ") {
                    reply("200 OK", "text/plain", b"ok\n")
                } else if request.starts_with(b"GET /ready ") {
                    let why = format!("a standby of {}\n", standby.follow.primary);
                    reply("503 Service Unavailable", "text/plain", why.as_bytes())
                } else if request.starts_with(b"GET /metrics ") {
                    let applied = standby.applied.load(SeqCst);
                    let head = standby.primary_head.load(SeqCst);
                    let body = crate::metrics::render_standby(applied, head);
                    reply("200 OK", "text/plain; version=0.0.4", body.as_bytes())
                } else {
                    reply("404 Not Found", "text/plain", b"")
                }
            }
        },
    )
    .await
}

/// Answers every request but the replication service's with `UNAVAILABLE`, `not_primary`.
#[derive(Clone)]
struct NotPrimary<S> {
    inner: S,
    primary: Arc<str>,
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
        let answer = not_primary(&self.primary).into_http();
        Box::pin(async move { Ok(answer) })
    }
}

#[cfg(test)]
mod tests {

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

    use super::*;

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
