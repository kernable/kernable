//! Unique keys with scope global across partitions.
//!
//! Within one kernel, the index keeps a global key unique. Across partitions, the control
//! partition owns them: before a partition commits a global key its aggregate did not hold, it
//! reserves the key there ([`Kernel::reserve_global`]). The owner remembers which partition may
//! hold each key (a `GlobalKey` record, replicated like every record). It grants the key unless
//! another partition still holds it, which it asks that partition ([`Kernel::holds_global`]):
//! the holder's own index is the truth, so nothing has to be confirmed or released, and a lost
//! message never leaves a key taken. A partition also answers "held" for the keys of commits in
//! flight, so the owner never hands a key away just before its commit lands.
//!
//! Personal keys (only their hash lives in the vault) stay unique per partition.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

use kernable_core::{
    proto::{
        GlobalKeyAnswer, GlobalKeyRequest, Signature,
        global_key_service_client::GlobalKeyServiceClient,
        global_key_service_server::GlobalKeyService,
    },
    signing,
};
use tonic::{Request, Response, Status};

use crate::{Api, GRANT_HEADER, Kernel, MAX_REQUEST_AGE_MS, config, internal, now_ms, protocol};

/// The keys of commits in flight on this partition, counted.
#[derive(Default)]
pub(crate) struct InFlight(Mutex<HashMap<[u8; 32], usize>>);

/// Keys of one commit in flight; they count as held until it is dropped.
pub(crate) struct Flying<'a>(&'a InFlight, Vec<[u8; 32]>);

impl InFlight {
    pub(crate) fn hold(&self, keys: &[[u8; 32]]) -> Flying<'_> {
        let mut held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for k in keys {
            *held.entry(*k).or_default() += 1;
        }
        Flying(self, keys.to_vec())
    }

    fn contains(&self, key: &[u8; 32]) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key)
    }
}

impl Drop for Flying<'_> {
    fn drop(&mut self) {
        let mut held = self.0.0.lock().unwrap_or_else(PoisonError::into_inner);
        for k in &self.1 {
            if let Some(n) = held.get_mut(k) {
                *n -= 1;
                if *n == 0 {
                    held.remove(k);
                }
            }
        }
    }
}

fn key(bytes: &[u8]) -> Result<[u8; 32], Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("a key's identity has 32 bytes"))
}

impl Kernel {
    /// Reserves `keys` for this partition with the key owner; per key, whether it was granted.
    pub(crate) async fn reserve_global(
        self: &Arc<Self>,
        keys: &[[u8; 32]],
    ) -> Result<Vec<bool>, Status> {
        let partition = self
            .config
            .partition
            .as_ref()
            .ok_or_else(|| Status::internal("not a partition"))?;
        if partition.control.is_none() {
            return self.reserve_as_owner(keys, &partition.name).await;
        }
        let map = self
            .maps
            .get()
            .ok_or_else(|| Status::unavailable("no partition map yet"))?;
        self.ask(&map.control, "reserve", keys).await
    }

    /// On the key owner: grants each key to `partition` unless another partition still holds it.
    async fn reserve_as_owner(
        self: &Arc<Self>,
        keys: &[[u8; 32]],
        partition: &str,
    ) -> Result<Vec<bool>, Status> {
        // One decision at a time: two partitions never both get a key.
        let _deciding = self.global_owner.lock().await;
        let mut granted = Vec::with_capacity(keys.len());
        let mut changed = false;
        for k in keys {
            let holder = self.store.global_key(k).map_err(internal)?;
            let free = match holder.as_deref() {
                None => true,
                Some(p) if p == partition => {
                    granted.push(true);
                    continue;
                }
                Some(p) => {
                    let holds =
                        if Some(p) == self.config.partition.as_ref().map(|c| c.name.as_str()) {
                            self.holds_global(&[*k])?
                        } else {
                            self.ask(p, "holds", &[*k]).await?
                        };
                    !holds.first().copied().unwrap_or(true)
                }
            };
            if free {
                self.store.save_global_key(k, partition).map_err(internal)?;
                changed = true;
            }
            granted.push(free);
        }
        if changed {
            self.sync(0);
        }
        Ok(granted)
    }

    /// Whether this partition holds each of `keys`: in its index, or in a commit in flight.
    pub(crate) fn holds_global(&self, keys: &[[u8; 32]]) -> Result<Vec<bool>, Status> {
        let store = self.store.as_ref();
        let vault = self.vault.as_ref();
        let mut index = self.index();
        keys.iter()
            .map(|k| {
                Ok(self.in_flight_keys.contains(k)
                    || index
                        .holder(store, vault, k)
                        .map_err(|e| Status::internal(e.to_string()))?
                        .is_some_and(|holder| self.serves(&holder.tenant_id)))
            })
            .collect()
    }

    /// Asks partition `name` (`reserve` on the owner, `holds` on a holder), with this kernel's
    /// node key; tries its addresses in turn.
    async fn ask(&self, name: &str, op: &str, keys: &[[u8; 32]]) -> Result<Vec<bool>, Status> {
        let partition = self
            .config
            .partition
            .as_ref()
            .ok_or_else(|| Status::internal("not a partition"))?;
        let map = self
            .maps
            .get()
            .ok_or_else(|| Status::unavailable("no partition map yet"))?;
        let addresses = map
            .partitions
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.addresses.clone())
            .ok_or_else(|| Status::unavailable(format!("the partition map names no {name}")))?;
        let node_key = self
            .config()
            .read_signing_key(&partition.node_key)
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "global keys need --node-key ({}): {e}",
                    partition.node_key.display()
                ))
            })?;
        let grant = std::fs::read_to_string(partition.node_key.with_extension("grant"))
            .map_err(|e| Status::failed_precondition(format!("the node key's grant: {e}")))?;
        let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        let mut last = Status::unavailable(format!("partition {name} has no address"));
        for address in &addresses {
            let issued_at = now_ms();
            let msg = signing::global_keys_message(op, &keys, &partition.name, issued_at);
            let mut request = Request::new(GlobalKeyRequest {
                op: op.to_owned(),
                keys: keys.clone(),
                partition: partition.name.clone(),
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
            let mut client = GlobalKeyServiceClient::new(channel);
            let answer = match op {
                "reserve" => client.reserve(request).await,
                _ => client.holds(request).await,
            };
            match answer {
                Ok(a) => return Ok(a.into_inner().granted),
                Err(s) if s.code() == tonic::Code::Unavailable => last = s,
                Err(s) => return Err(s),
            }
        }
        Err(last)
    }

    /// Checks a request of another partition's node key.
    fn check_node(&self, request: &Request<GlobalKeyRequest>, op: &str) -> Result<(), Status> {
        let r = request.get_ref();
        protocol(r.protocol)?;
        let sig: &Signature = r
            .sig
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("the request is not signed"))?;
        let msg = signing::global_keys_message(op, &r.keys, &r.partition, r.issued_at);
        if r.op != op || signing::verify(sig, &msg).is_err() {
            return Err(Status::unauthenticated("invalid signature"));
        }
        if (now_ms() - r.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated("request expired; check the clock"));
        }
        self.present(request.metadata(), &sig.key_id)?;
        if !config::allowed(&self.permissions().replicate, &sig.key_id, "*") {
            return Err(Status::permission_denied(
                "only a partition's node key (granted replicate) may ask",
            ));
        }
        let map = self
            .maps
            .get()
            .ok_or_else(|| Status::unavailable("no partition map yet"))?;
        if !map.partitions.iter().any(|p| p.name == r.partition) {
            return Err(Status::invalid_argument(format!(
                "the partition map names no {}",
                r.partition
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl GlobalKeyService for Api {
    async fn reserve(
        &self,
        request: Request<GlobalKeyRequest>,
    ) -> Result<Response<GlobalKeyAnswer>, Status> {
        let kernel = &self.0;
        kernel.check_node(&request, "reserve")?;
        if kernel
            .config
            .partition
            .as_ref()
            .is_none_or(|p| p.control.is_some())
        {
            return Err(Status::failed_precondition(
                "global keys are reserved on the control partition",
            ));
        }
        let r = request.into_inner();
        let keys = r
            .keys
            .iter()
            .map(|k| key(k))
            .collect::<Result<Vec<_>, _>>()?;
        let granted = kernel.reserve_as_owner(&keys, &r.partition).await?;
        Ok(Response::new(GlobalKeyAnswer { granted }))
    }

    async fn holds(
        &self,
        request: Request<GlobalKeyRequest>,
    ) -> Result<Response<GlobalKeyAnswer>, Status> {
        let kernel = &self.0;
        kernel.check_node(&request, "holds")?;
        let keys = request
            .get_ref()
            .keys
            .iter()
            .map(|k| key(k))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(GlobalKeyAnswer {
            granted: kernel.holds_global(&keys)?,
        }))
    }
}
