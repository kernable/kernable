//! Moving a tenant between partitions.
//!
//! The operator applies a map that lists the tenant under `moving` (`kernable partitions move`).
//! From then on, both partitions answer the tenant's requests with `UNAVAILABLE`, and:
//!
//! 1. the target asks the source for the tenant ([`MoveService::export`]): once the source's
//!    queue holds none of the tenant's commands, it sends every command, commit and rejection
//!    of the tenant with the vault values and files they need, and its head (`source_head`);
//! 2. the target commits the events again, with where they came from (`origin_partition`,
//!    `origin_position`), under the same commands and worker signatures; listeners and
//!    subscriptions do not get these copies, the originals stay delivered where they are;
//! 3. the target tells the control partition ([`MoveService::done`]), which hands it the
//!    tenant's global keys, and notes the move done (`TenantImportedRecord`, with its head
//!    after the copies: `target_after`);
//! 4. the operator applies the final map: the tenant on the target, the move under `moved`
//!    with both heads (an SDK delivers the target's events after `target_after` only once the
//!    source's stream has passed `source_head`). The source then deletes the tenant's vault
//!    values and files; its events stay in its log, history like any other.
//!
//! Every step can be repeated: a target that stops mid-copy copies again, and what it holds
//! already it keeps.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use kernable_core::{
    proto::{
        AggregateIndex, LogRecord, MoveBatch, MoveRequest, MoveResult, NewEvent, PartitionMap,
        Signature, TenantImportedRecord, TenantMove, log_record::Record,
        move_service_client::MoveServiceClient, move_service_server::MoveService,
    },
    signing,
};
use kernable_store::{Error as StoreError, StreamId};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    Api, BoxError, GRANT_HEADER, Kernel, MAX_REQUEST_AGE_MS, config, internal, now_ms,
    partitions::partition_of, protocol, replication,
};

/// How often a partition looks for moves to copy or to clean up after.
const EVERY: Duration = Duration::from_secs(1);

/// How long the source waits for the tenant's queued commands to complete.
const DRAIN: Duration = Duration::from_secs(30);

impl Kernel {
    /// The move of `tenant` the map lists, if it moves.
    pub(crate) fn moving(&self, tenant: &str) -> Option<TenantMove> {
        self.config.partition.as_ref()?;
        self.maps
            .get()?
            .moving
            .into_iter()
            .find(|m| m.tenant == tenant)
    }

    /// Checks a request of another partition's node key.
    fn check_move(&self, request: &Request<MoveRequest>, op: &str) -> Result<(), Status> {
        let r = request.get_ref();
        protocol(r.protocol)?;
        let sig: &Signature = r
            .sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("the request is not signed"))?;
        let msg = signing::move_message(op, &r.tenant, &r.from, &r.to, r.issued_at);
        if r.op != op || signing::verify(sig, &msg).is_err() {
            return Err(Status::unauthenticated("invalid signature"));
        }
        if (now_ms() - r.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }
        self.present(request.metadata(), &sig.key_id)?;
        if !config::allowed(&self.permissions().replicate, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "only a partition's node key (granted replicate) may move tenants",
            ));
        }
        match self.moving(&r.tenant) {
            Some(m) if m.from == r.from && m.to == r.to => Ok(()),
            _ => Err(Status::failed_precondition(format!(
                "the partition map moves no tenant {} from {} to {}",
                r.tenant, r.from, r.to
            ))),
        }
    }

    /// The tenant's commands still in the queue.
    fn queued(&self, tenant: &str) -> Result<usize, Status> {
        Ok(self
            .store
            .pending()
            .map_err(internal)?
            .iter()
            .filter(|p| p.command.tenant_id == tenant)
            .count())
    }

    /// Asks partition `name` with this kernel's node key, at each of its addresses in turn.
    async fn ask_partition<T>(
        &self,
        map: &PartitionMap,
        name: &str,
        op: &str,
        m: &TenantMove,
        global_keys: Vec<Vec<u8>>,
        mut call: impl AsyncFnMut(
            MoveServiceClient<tonic::transport::Channel>,
            Request<MoveRequest>,
        ) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let partition = self
            .config
            .partition
            .as_ref()
            .ok_or_else(|| Status::internal("not a partition"))?;
        let addresses = map
            .partitions
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.addresses.clone())
            .ok_or_else(|| Status::unavailable(format!("the partition map names no {name}")))?;
        let node_key = signing::read_signing_key(&partition.node_key).map_err(|e| {
            Status::failed_precondition(format!(
                "moving tenants needs --node-key ({}): {e}",
                partition.node_key.display()
            ))
        })?;
        let grant = std::fs::read_to_string(partition.node_key.with_extension("grant"))
            .map_err(|e| Status::failed_precondition(format!("the node key's grant: {e}")))?;
        let mut last = Status::unavailable(format!("partition {name} has no address"));
        for address in &addresses {
            let issued_at = now_ms();
            let msg = signing::move_message(op, &m.tenant, &m.from, &m.to, issued_at);
            let mut request = Request::new(MoveRequest {
                op: op.to_owned(),
                tenant: m.tenant.clone(),
                from: m.from.clone(),
                to: m.to.clone(),
                global_keys: global_keys.clone(),
                issued_at,
                sig: Some(signing::sign(&node_key, &msg)),
                protocol: kernable_core::PROTOCOL,
            });
            request.metadata_mut().insert(
                GRANT_HEADER,
                grant
                    .trim()
                    .parse()
                    .map_err(|_| Status::failed_precondition("the node key's grant is not hex"))?,
            );
            let channel = match crate::standby::connect(address).await {
                Ok(channel) => channel,
                Err(e) => {
                    last = Status::unavailable(format!("partition {name} at {address}: {e}"));
                    continue;
                }
            };
            let client = MoveServiceClient::new(channel)
                .max_decoding_message_size(replication::MAX_MESSAGE_BYTES);
            match call(client, request).await {
                Ok(t) => return Ok(t),
                Err(s) if s.code() == tonic::Code::Unavailable => last = s,
                Err(s) => return Err(s),
            }
        }
        Err(last)
    }

    /// On the control partition: `to` holds the tenant's global keys from now on.
    async fn hand_over(&self, keys: &[Vec<u8>], to: &str) -> Result<(), Status> {
        let _deciding = self.global_owner.lock().await;
        for k in keys {
            if k.len() != 32 {
                return Err(Status::invalid_argument("a key's identity has 32 bytes"));
            }
            if self.store.global_key(k).map_err(internal)?.as_deref() != Some(to) {
                self.store.save_global_key(k, to).map_err(internal)?;
            }
        }
        self.sync(0);
        Ok(())
    }
}

#[tonic::async_trait]
impl MoveService for Api {
    type ExportStream = ReceiverStream<Result<MoveBatch, Status>>;

    async fn export(
        &self,
        request: Request<MoveRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let kernel = Arc::clone(&self.0);
        kernel.check_move(&request, "export")?;
        let r = request.into_inner();
        let me = kernel.config.partition.as_ref().map(|p| p.name.as_str());
        if me != Some(r.from.as_str()) {
            return Err(Status::failed_precondition(format!(
                "this is not partition {}",
                r.from
            )));
        }
        // The tenant's commands accepted before the move complete first; new ones are refused.
        // Twice empty, a moment apart: a submit that passed its check just before the map
        // arrived is in the queue by then.
        let started = Instant::now();
        let mut empty = 0;
        while empty < 2 {
            if kernel.queued(&r.tenant)? == 0 {
                empty += 1;
            } else {
                empty = 0;
            }
            if started.elapsed() > DRAIN {
                return Err(Status::unavailable(format!(
                    "tenant {} still has commands in the queue; trying again",
                    r.tenant
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = export(&kernel, &r.tenant, &tx) {
                let _ = tx.blocking_send(Err(e));
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn done(&self, request: Request<MoveRequest>) -> Result<Response<MoveResult>, Status> {
        let kernel = &self.0;
        kernel.check_move(&request, "done")?;
        if kernel
            .config
            .partition
            .as_ref()
            .is_none_or(|p| p.control.is_some())
        {
            return Err(Status::failed_precondition(
                "moves are done on the control partition",
            ));
        }
        let r = request.into_inner();
        kernel.hand_over(&r.global_keys, &r.to).await?;
        tracing::info!(tenant = %r.tenant, from = %r.from, to = %r.to, keys = r.global_keys.len(), "a tenant's global keys moved");
        Ok(Response::new(MoveResult {}))
    }
}

/// Sends the tenant's records in batches: commands as enqueued, then commits in log order,
/// then rejections, each batch with the vault values it refers to; then the tenant's files and
/// the head every event of the tenant is at or before.
fn export(
    kernel: &Kernel,
    tenant: &str,
    tx: &mpsc::Sender<Result<MoveBatch, Status>>,
) -> Result<(), Status> {
    let left = || StoreError::Io(std::io::Error::other("the target left"));
    let source_head = kernel.store.head_position().map_err(internal)?;
    let mut commands: HashSet<String> = HashSet::new();
    let mut batch: Vec<Record> = Vec::new();
    let mut bytes = 0;
    let flush = |batch: &mut Vec<Record>| -> Result<(), StoreError> {
        let (vault, _) = replication::attachments(kernel, batch.iter())
            .map_err(|s| StoreError::Io(std::io::Error::other(s.message().to_owned())))?;
        let records = batch
            .drain(..)
            .map(|r| LogRecord { record: Some(r) })
            .collect();
        tx.blocking_send(Ok(MoveBatch {
            records,
            vault,
            ..Default::default()
        }))
        .map_err(|_| left())
    };
    kernel
        .store
        .tenant_records(tenant, &mut |record| {
            if let Record::Enqueued(e) = &record {
                commands.extend(e.command.iter().map(|c| c.command_id.clone()));
            }
            bytes += prost::Message::encoded_len(&LogRecord {
                record: Some(record.clone()),
            });
            batch.push(record);
            if batch.len() >= replication::BATCH || bytes >= replication::BATCH_BYTES {
                bytes = 0;
                flush(&mut batch)?;
            }
            Ok(())
        })
        .map_err(internal)?;
    flush(&mut batch).map_err(internal)?;
    let prefix = kernable_store::data::tenant_place(tenant);
    let (mut files, mut bytes) = (Vec::new(), 0);
    kernel
        .data
        .dump(&mut |place, stored| {
            if !place.starts_with(&prefix) {
                return Ok(());
            }
            bytes += stored.len();
            files.push(kernable_core::proto::DataFile {
                path: place.to_owned(),
                data: stored.to_vec(),
                ..Default::default()
            });
            if bytes >= replication::BATCH_BYTES {
                bytes = 0;
                tx.blocking_send(Ok(MoveBatch {
                    files: std::mem::take(&mut files),
                    ..Default::default()
                }))
                .map_err(|_| left())?;
            }
            Ok(())
        })
        .map_err(internal)?;
    tx.blocking_send(Ok(MoveBatch {
        files,
        source_head,
        last: true,
        ..Default::default()
    }))
    .map_err(|_| Status::cancelled("the target left"))?;
    tracing::info!(%tenant, commands = commands.len(), source_head, "sent a moving tenant");
    Ok(())
}

/// What an import gathered: the latest index per stream (for the global keys) and the streams.
#[derive(Default)]
struct Imported {
    /// The stream of each command the source sent.
    streams: HashMap<String, StreamId>,
    indexes: HashMap<StreamId, AggregateIndex>,
}

/// Takes one batch of the source's records into this store.
fn apply(
    kernel: &Kernel,
    tenant: &str,
    from: &str,
    batch: MoveBatch,
    imported: &mut Imported,
) -> Result<(), BoxError> {
    if !batch.vault.is_empty() {
        let vault = kernel
            .vault
            .as_ref()
            .ok_or("the tenant has vault values: start this partition with a vault")?;
        vault.import(&batch.vault)?;
    }
    for f in &batch.files {
        kernel.data.restore(&f.path, &f.data)?;
        // Its record carries the file to this partition's standby.
        let k = kernable_store::data::key_of(&f.data)?;
        kernel.store.data_changed(&k.tenant_id, &k.domain, &k.key)?;
    }
    let store = kernel.store.as_ref();
    for record in batch.records.into_iter().filter_map(|r| r.record) {
        match record {
            Record::Enqueued(e) => {
                let cmd = e.command.ok_or("an enqueued record without its command")?;
                if cmd.tenant_id != tenant {
                    return Err(
                        format!("command {} is not of tenant {tenant}", cmd.command_id).into(),
                    );
                }
                if let Some(held) = store.command(&cmd.command_id)?
                    && held.command.tenant_id != tenant
                {
                    return Err(format!(
                        "command {} of tenant {tenant} has the ID of a command of tenant {} here",
                        cmd.command_id, held.command.tenant_id
                    )
                    .into());
                }
                imported.streams.insert(
                    cmd.command_id.clone(),
                    StreamId {
                        tenant_id: cmd.tenant_id.clone(),
                        domain: cmd.domain.clone(),
                        aggregate_type: cmd.aggregate_type.clone(),
                        aggregate_id: cmd.aggregate_id.clone(),
                    },
                );
                store.enqueue(&cmd, e.created_at)?;
                crate::crash::point("move.enqueued");
            }
            Record::Committed(c) => {
                // What each stream holds, also of commits copied before: the global keys move
                // with the tenant however often the copy started.
                if let (Some(index), Some(stream)) = (&c.index, imported.streams.get(&c.command_id))
                {
                    imported.indexes.insert(stream.clone(), index.clone());
                }
                if store.result(&c.command_id)?.is_some() {
                    continue; // copied before, or committed here before the tenant moved away
                }
                let events: Vec<_> = c.events.iter().filter_map(|l| l.event.as_ref()).collect();
                let new: Vec<NewEvent> = events
                    .iter()
                    .map(|e| {
                        // A copy of a copy keeps where the event was first committed.
                        let (origin, position) = if e.origin_partition.is_empty() {
                            (from.to_owned(), e.global_position)
                        } else {
                            (e.origin_partition.clone(), e.origin_position)
                        };
                        NewEvent {
                            event_type: e.event_type.clone(),
                            payload: e.payload.clone(),
                            schema: e.schema,
                            vault_refs: e.vault_refs.clone(),
                            erases: e.erases.clone(),
                            origin_partition: origin,
                            origin_position: position,
                        }
                    })
                    .collect();
                let (expected, sig, at) = match events.first() {
                    Some(first) => (
                        first.version.saturating_sub(1),
                        first.producer_sig.clone().unwrap_or_default(),
                        first.created_at,
                    ),
                    None => (0, Signature::default(), now_ms()),
                };
                match store.commit(&c.command_id, expected, &new, &sig, c.index.as_ref(), at) {
                    // A commit without events stands at whatever version its stream has.
                    Err(StoreError::VersionConflict { actual, .. }) if new.is_empty() => {
                        store.commit(&c.command_id, actual, &new, &sig, c.index.as_ref(), at)?;
                    }
                    other => {
                        other?;
                    }
                }
            }
            Record::Rejected(r) => {
                if store.result(&r.command_id)?.is_none() {
                    store.reject(&r.command_id, r.reason.unwrap_or_default())?;
                }
            }
            _ => return Err("a record a move does not carry".into()),
        }
    }
    Ok(())
}

/// On the target: copies the tenant from the source, tells the control partition, notes it.
async fn import(kernel: &Arc<Kernel>, map: &PartitionMap, m: &TenantMove) -> Result<(), BoxError> {
    let mut imported = Imported::default();
    let mut source_head = None;
    let stream = kernel
        .ask_partition(
            map,
            &m.from,
            "export",
            m,
            Vec::new(),
            async |mut client, request| Ok(client.export(request).await?.into_inner()),
        )
        .await?;
    let mut stream = stream;
    while let Some(batch) = stream.message().await? {
        if batch.last {
            source_head = Some(batch.source_head);
        }
        let k = Arc::clone(kernel);
        let (tenant, from) = (m.tenant.clone(), m.from.clone());
        imported = tokio::task::spawn_blocking(move || {
            apply(&k, &tenant, &from, batch, &mut imported).map(|()| imported)
        })
        .await??;
    }
    let source_head = source_head.ok_or("the source ended the copy early")?;
    let k = Arc::clone(kernel);
    let target_after = tokio::task::spawn_blocking(move || {
        let head = k.store.head_position();
        k.sync(*head.as_ref().unwrap_or(&0));
        head
    })
    .await??;
    // The index caches nothing of the tenant's streams any more.
    crate::crash::point("move.copied");
    let streams: Vec<StreamId> = imported.indexes.keys().cloned().collect();
    kernel.index().erased(&streams);
    let global_keys: Vec<Vec<u8>> = imported
        .indexes
        .values()
        .flat_map(|i| &i.keys)
        .filter(|k| k.global && !k.id.is_empty())
        .map(|k| k.id.clone())
        .collect();
    let keys = global_keys.len();
    if kernel
        .config
        .partition
        .as_ref()
        .is_some_and(|p| p.control.is_none())
    {
        kernel.hand_over(&global_keys, &m.to).await?;
    } else {
        kernel
            .ask_partition(
                map,
                &map.control,
                "done",
                m,
                global_keys,
                async |mut client, request| client.done(request).await.map(|_| ()),
            )
            .await?;
    }
    crate::crash::point("move.handed");
    let record = TenantImportedRecord {
        tenant: m.tenant.clone(),
        from: m.from.clone(),
        source_head,
        target_after,
        epoch: m.epoch,
    };
    let k = Arc::clone(kernel);
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        k.store.save_tenant_imported(&record)?;
        k.sync(0);
        Ok(())
    })
    .await??;
    kernel.appended.notify_waiters();
    tracing::warn!(tenant = %m.tenant, from = %m.from, source_head, target_after, global_keys = keys, "a moving tenant is copied here");
    Ok(())
}

/// On a partition: copies the tenants moving here, and deletes the vault values and files of
/// tenants that moved away, for as long as the kernel runs.
pub(crate) async fn follow_moves(kernel: &Arc<Kernel>) {
    let Some(partition) = kernel.config.partition.clone() else {
        return std::future::pending().await;
    };
    let mut tick = tokio::time::interval(EVERY);
    let mut cleaned = None;
    let mut failing: HashSet<String> = HashSet::new();
    loop {
        tick.tick().await;
        let Some(map) = kernel.maps.get() else {
            continue;
        };
        for m in map.moving.iter().filter(|m| m.to == partition.name) {
            match kernel.store.tenant_imported(&m.tenant) {
                Ok(Some(done)) if done.epoch == m.epoch => continue,
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = %e, "reading the moves failed");
                    continue;
                }
            }
            match import(kernel, &map, m).await {
                Ok(()) => {
                    failing.remove(&m.tenant);
                }
                Err(e) => {
                    if failing.insert(m.tenant.clone()) {
                        tracing::warn!(tenant = %m.tenant, from = %m.from, error = %e, "copying a moving tenant failed; trying again");
                    }
                }
            }
        }
        if cleaned != Some(map.epoch) {
            cleaned = Some(map.epoch);
            let gone: HashSet<&str> = map
                .moved
                .iter()
                .filter(|m| m.from == partition.name)
                .map(|m| m.tenant.as_str())
                .filter(|t| partition_of(&map, t) != partition.name)
                .filter(|t| !map.moving.iter().any(|m| m.tenant == *t))
                .collect();
            for tenant in gone {
                let k = Arc::clone(kernel);
                let t = tenant.to_owned();
                let dropped = tokio::task::spawn_blocking(
                    move || -> Result<(usize, usize, usize), BoxError> {
                        let streams = k.store.drop_tenant_index(&t)?;
                        if streams > 0 {
                            k.sync(0);
                            k.index().forget_tenant(&t);
                            crate::crash::point("move.dropped");
                        }
                        let values = match &k.vault {
                            Some(vault) => vault.drop_tenant(&t)?,
                            None => 0,
                        };
                        Ok((streams, values, k.data.drop_tenant(&t)?))
                    },
                )
                .await;
                match dropped {
                    Ok(Ok((0, 0, 0))) => {}
                    Ok(Ok((streams, values, files))) => {
                        tracing::warn!(%tenant, streams, values, files, "dropped the keys, vault values and files of a tenant that moved away");
                    }
                    Ok(Err(e)) => {
                        tracing::error!(%tenant, error = %e, "deleting a moved tenant's data failed")
                    }
                    Err(e) => {
                        tracing::error!(%tenant, error = %e, "deleting a moved tenant's data failed")
                    }
                }
            }
        }
    }
}
