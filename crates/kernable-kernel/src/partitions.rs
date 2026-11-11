//! Partitions: several kernels share the tenants; the partition map says where each
//! tenant lives.
//!
//! A kernel started with `--partition` serves the tenants the map assigns to it and refuses
//! the others with `FAILED_PRECONDITION` `wrong_partition: …`, which tells an SDK to read the map
//! again. Before it has a map it serves no tenant: two partitions must never both take one.
//!
//! The control partition (started with `--partition` but without `--control`) is where maps are
//! applied, signed by a key granted `manage` for `*`. Every other partition asks it every
//! second for its trust records ([`follow_control`]): the map, every grant, revocation and
//! membership, so a revocation applied once reaches every partition, and a partition that was
//! down catches up when it is back.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use kernable_core::{
    proto::{
        Grant, MapRequest, MapResult, MoveStatus, MoveStatusRequest, PartitionMap, TrustSync,
        TrustSyncRequest, partition_service_server::PartitionService,
        trust_sync_service_client::TrustSyncServiceClient,
        trust_sync_service_server::TrustSyncService,
    },
    signing,
};
use prost::Message;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use crate::{
    Api, BoxError, GRANT_HEADER, Kernel, MAX_REQUEST_AGE_MS, config, internal, memberships, now_ms,
    protocol,
};

/// How often a partition asks the control partition for its trust records.
const EVERY: Duration = Duration::from_secs(1);

/// The partition map a kernel holds; the mutex keeps two maps taken at once in order (an
/// older one never replaces a newer one).
#[derive(Default)]
pub(crate) struct Maps(
    RwLock<Option<PartitionMap>>,
    tokio::sync::Notify,
    std::sync::Mutex<()>,
);

impl Maps {
    pub(crate) fn new(map: Option<PartitionMap>) -> Self {
        Maps(
            RwLock::new(map),
            tokio::sync::Notify::new(),
            std::sync::Mutex::default(),
        )
    }

    /// Completes when a new map is taken (moves start at once, not at the next tick).
    pub(crate) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.1.notified()
    }

    pub(crate) fn get(&self) -> Option<PartitionMap> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set(&self, map: PartitionMap) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Some(map);
        self.1.notify_waiters();
    }
}

/// The partition a map assigns `tenant` to.
pub fn partition_of<'a>(map: &'a PartitionMap, tenant: &str) -> &'a str {
    map.tenants
        .iter()
        .find(|t| t.tenant == tenant)
        .map_or(map.default.as_str(), |t| t.partition.as_str())
}

impl Kernel {
    /// Whether this kernel serves `tenant`: always without `--partition`; with it, only the
    /// tenants its map assigns to it.
    pub(crate) fn owns(&self, tenant: &str) -> Result<(), Status> {
        let Some(partition) = &self.config.partition else {
            return Ok(());
        };
        let Some(map) = self.maps.get() else {
            return Err(Status::failed_precondition(
                "wrong_partition: this partition has no partition map yet; apply one on the control partition (kernable partitions apply)",
            ));
        };
        if let Some(m) = map.moving.iter().find(|m| m.tenant == tenant) {
            return Err(Status::unavailable(format!(
                "moving: tenant {tenant} moves from partition {} to {}; try again in a moment",
                m.from, m.to
            )));
        }
        match partition_of(&map, tenant) {
            p if p == partition.name => Ok(()),
            p => Err(Status::failed_precondition(format!(
                "wrong_partition: tenant {tenant} is on partition {p} (map epoch {})",
                map.epoch
            ))),
        }
    }

    /// Whether this kernel holds `tenant`'s data: the map assigns the tenant to it, or the tenant
    /// is moving here. A stream of any other tenant is history of a tenant that moved away, and
    /// holds no key here any more.
    pub(crate) fn serves(&self, tenant: &str) -> bool {
        let Some(partition) = &self.config.partition else {
            return true;
        };
        let Some(map) = self.maps.get() else {
            return true;
        };
        partition_of(&map, tenant) == partition.name
            || map
                .moving
                .iter()
                .any(|m| m.tenant == tenant && m.to == partition.name)
    }

    /// Takes `map` into effect if it is valid, signed by a key granted `manage` for `*`, and
    /// newer than the one held; stored first.
    fn take_map(&self, map: PartitionMap) -> Result<bool, Status> {
        validate(&map)?;
        let sig = map
            .sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("the partition map is not signed"))?;
        signing::verify(sig, &signing::partitions_message(&map))
            .map_err(|_| Status::unauthenticated("invalid partition map signature"))?;
        if !config::allowed(&self.permissions().manage, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "the key may not apply partition maps: its grant needs manage for *",
            ));
        }
        let _taking = self.maps.2.lock().unwrap_or_else(PoisonError::into_inner);
        if self.maps.get().is_some_and(|held| held.epoch >= map.epoch) {
            return Ok(false);
        }
        let mut map = map;
        map.protocol = 0;
        self.store.save_partition_map(&map).map_err(internal)?;
        self.sync(0);
        tracing::warn!(epoch = map.epoch, partitions = map.partitions.len(), tenants = map.tenants.len(), default = %map.default, "partition map applied");
        self.maps.set(map);
        Ok(true)
    }

    /// The grant a request carries, checked: signed by a trusted root, its key not revoked.
    fn checked_grant(&self, metadata: &MetadataMap) -> Result<Grant, Status> {
        let grant = metadata
            .get(GRANT_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| hex::decode(v.trim()).ok())
            .and_then(|b| Grant::decode(b.as_slice()).ok())
            .ok_or_else(|| Status::unauthenticated("present a grant (kernable-grant)"))?;
        self.check_grant(&grant)?;
        Ok(grant)
    }
}

/// A map's own consistency: named partitions, each listed once with an address; a default and a
/// control partition among them; every tenant once, on a listed partition.
fn validate(map: &PartitionMap) -> Result<(), Status> {
    let bad = |why: String| Err(Status::invalid_argument(format!("partition map: {why}")));
    let mut names = HashSet::new();
    for p in &map.partitions {
        if !config::valid_domain(&p.name) {
            return bad(format!("{} is not a valid partition name", p.name));
        }
        if !names.insert(p.name.as_str()) {
            return bad(format!("partition {} is listed twice", p.name));
        }
        if p.addresses.is_empty() {
            return bad(format!("partition {} has no address", p.name));
        }
    }
    for (what, name) in [("default", &map.default), ("control", &map.control)] {
        if !names.contains(name.as_str()) {
            return bad(format!("the {what} partition {name} is not listed"));
        }
    }
    let mut tenants = HashSet::new();
    for t in &map.tenants {
        if !names.contains(t.partition.as_str()) {
            return bad(format!(
                "tenant {} is on an unlisted partition {}",
                t.tenant, t.partition
            ));
        }
        if !tenants.insert(t.tenant.as_str()) {
            return bad(format!("tenant {} is listed twice", t.tenant));
        }
    }
    let mut moving = HashSet::new();
    for m in map.moving.iter().chain(&map.copying) {
        if !moving.insert(m.tenant.as_str()) {
            return bad(format!(
                "tenant {} moves twice (or is both copied and moved)",
                m.tenant
            ));
        }
        if !names.contains(m.to.as_str()) || m.from == m.to {
            return bad(format!(
                "tenant {} moves to {}, not another listed partition",
                m.tenant, m.to
            ));
        }
        if partition_of(map, &m.tenant) != m.from {
            return bad(format!(
                "tenant {} moves from {}, but the map has it on {}",
                m.tenant,
                m.from,
                partition_of(map, &m.tenant)
            ));
        }
    }
    for m in &map.moved {
        if m.from == m.to {
            return bad(format!(
                "tenant {} moved from {} to itself",
                m.tenant, m.from
            ));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl PartitionService for Api {
    async fn map(&self, request: Request<MapRequest>) -> Result<Response<PartitionMap>, Status> {
        let kernel = &self.0;
        protocol(request.get_ref().protocol)?;
        // Without a map there is nothing to protect: a client learns it serves every tenant.
        let map = kernel
            .maps
            .get()
            .ok_or_else(|| Status::not_found("this kernel holds no partition map"))?;
        kernel.checked_grant(request.metadata())?;
        Ok(Response::new(map))
    }

    async fn apply(&self, request: Request<PartitionMap>) -> Result<Response<MapResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let map = request.into_inner();
        protocol(map.protocol)?;
        let Some(partition) = &kernel.config.partition else {
            return Err(Status::failed_precondition(
                "this kernel runs without --partition",
            ));
        };
        if partition.control.is_some() {
            return Err(Status::failed_precondition(
                "apply maps on the control partition (started without --control)",
            ));
        }
        if map.control != partition.name {
            return Err(Status::invalid_argument(format!(
                "this kernel is partition {}, the control partition; the map names {}",
                partition.name, map.control
            )));
        }
        if let Some(sig) = &map.sig {
            kernel.present(&metadata, &sig.key_id)?;
        }
        if (now_ms() - map.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "partition map expired; check the clock",
            ));
        }
        let epoch = map.epoch;
        if !kernel.take_map(map)? {
            return Err(Status::failed_precondition(format!(
                "a map of epoch {epoch} or later is applied"
            )));
        }
        Ok(Response::new(MapResult {}))
    }

    async fn moving(
        &self,
        request: Request<MoveStatusRequest>,
    ) -> Result<Response<MoveStatus>, Status> {
        let kernel = &self.0;
        protocol(request.get_ref().protocol)?;
        kernel.checked_grant(request.metadata())?;
        let tenant = &request.get_ref().tenant;
        Ok(Response::new(
            match kernel.store.tenant_imported(tenant).map_err(internal)? {
                Some(r) => MoveStatus {
                    done: true,
                    source_head: r.source_head,
                    target_after: r.target_after,
                    epoch: r.epoch,
                    precopy: r.precopy,
                },
                None => MoveStatus::default(),
            },
        ))
    }
}

#[tonic::async_trait]
impl TrustSyncService for Api {
    async fn sync(
        &self,
        request: Request<TrustSyncRequest>,
    ) -> Result<Response<TrustSync>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("the request is not signed"))?;
        signing::verify(&sig, &signing::trust_sync_message(req.issued_at))
            .map_err(|_| Status::unauthenticated("invalid signature"))?;
        if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }
        kernel.present(&metadata, &sig.key_id)?;
        if !config::allowed(&kernel.permissions().replicate, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "the key may not take trust records: its grant needs replicate",
            ));
        }
        let store = &kernel.store;
        Ok(Response::new(TrustSync {
            grants: store.grants().map_err(internal)?,
            revocations: store.revocations().map_err(internal)?,
            memberships: store.memberships().map_err(internal)?,
            map: kernel.maps.get(),
            processes: store.processes().map_err(internal)?,
        }))
    }
}

/// On a partition with `--control`: takes the trust records from the control partition every
/// second, for as long as the kernel runs.
pub(crate) async fn follow_control(kernel: &Arc<Kernel>) {
    let Some(partition) = kernel.config.partition.clone() else {
        return std::future::pending().await;
    };
    let Some(control) = partition.control.clone() else {
        return std::future::pending().await;
    };
    let addresses: Vec<String> = control
        .split(',')
        .map(|a| a.trim().to_owned())
        .filter(|a| !a.is_empty())
        .collect();
    let mut at = 0;
    let mut tick = tokio::time::interval(EVERY);
    let mut failing = false;
    loop {
        tick.tick().await;
        let address = &addresses[at % addresses.len()];
        match sync_once(kernel, &partition.node_key, address).await {
            Ok(()) if failing => {
                failing = false;
                tracing::info!(control = %address, "taking the trust records again");
            }
            Ok(()) => {}
            Err(e) => {
                if !failing {
                    tracing::warn!(error = %e, control = %address, "the control partition does not answer; trying again");
                }
                failing = true;
                at += 1;
            }
        }
    }
}

async fn sync_once(
    kernel: &Arc<Kernel>,
    node_key: &std::path::Path,
    address: &str,
) -> Result<(), BoxError> {
    let key = kernel
        .config()
        .read_signing_key(node_key)
        .map_err(|e| format!("--node-key {}: {e}", node_key.display()))?;
    let grant = std::fs::read_to_string(node_key.with_extension("grant"))?;
    let issued_at = now_ms();
    let mut request = Request::new(TrustSyncRequest {
        issued_at,
        sig: Some(signing::sign(&key, &signing::trust_sync_message(issued_at))),
        protocol: kernable_core::PROTOCOL,
    });
    request
        .metadata_mut()
        .insert(GRANT_HEADER, grant.trim().parse()?);
    let records = TrustSyncServiceClient::new(crate::standby::connect(address).await?)
        .max_decoding_message_size(crate::replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::replication::MAX_MESSAGE_BYTES)
        .sync(request)
        .await?
        .into_inner();
    take_trust(kernel, records);
    Ok(())
}

/// Takes what the control partition holds: grants and maps as signed by their keys, a
/// revocation as this kernel's own, a membership as if applied here.
fn take_trust(kernel: &Arc<Kernel>, records: TrustSync) {
    for grant in records.grants {
        let key_id = grant.key_id.clone();
        if let Err(e) = kernel.grant(grant) {
            tracing::debug!(key = %key_id, error = %e.message(), "a grant of the control partition was not taken");
        }
    }
    for r in records.revocations {
        if kernel.revoked_since(&r.key_id).is_none() {
            match kernel.revoke(&r.key_id, r.since) {
                Ok(()) => tracing::warn!(key = %r.key_id, "revoked, as on the control partition"),
                Err(e) => {
                    tracing::error!(key = %r.key_id, error = %e, "taking a revocation failed")
                }
            }
        }
    }
    let held: HashMap<String, i64> = kernel
        .store
        .memberships()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| r.membership.map(|m| (m.service, m.issued_at)))
        .collect();
    for record in records.memberships {
        let (Some(m), Some(sig)) = (record.membership, record.sig) else {
            continue;
        };
        if held.get(&m.service).is_some_and(|at| *at >= m.issued_at) {
            continue;
        }
        let service = m.service.clone();
        if let Err(e) = memberships::take(kernel, m, sig, true, false) {
            tracing::warn!(%service, error = %e.message(), "a membership of the control partition was not taken");
        }
    }
    for record in records.processes {
        let name = record.name.clone();
        if let Err(e) = kernel.processes.take(kernel, record) {
            tracing::warn!(process = %name, error = %e.message(), "a process of the control partition was not taken");
        }
    }
    if let Some(map) = records.map {
        let epoch = map.epoch;
        if let Err(e) = kernel.take_map(map) {
            tracing::warn!(epoch, error = %e.message(), "a partition map of the control partition was not taken");
        }
    }
}
