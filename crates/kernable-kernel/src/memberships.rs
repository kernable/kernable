//! What each service consists of, as its deploy applied it (see `Membership` in kernable.proto).
//!
//! The registrations of workers and listeners stay independent of it: they register whenever
//! they connect. A service's membership only decides what counts. The kernel waits for what it
//! lists, also before it ever connected, and treats the service's other workers and listeners
//! as dynamic. A service without a membership, and a process without a service, count as
//! registered.

use std::{
    collections::HashMap,
    sync::{PoisonError, RwLock},
};

use kernable_core::{
    proto::{
        ApplyRequest, ApplyResult, Membership, MembershipRecord,
        membership_service_server::MembershipService,
    },
    signing,
};
use tonic::{Request, Response, Status};

use crate::{Api, MAX_REQUEST_AGE_MS, config, internal, now_ms, protocol};

#[derive(Default)]
pub(crate) struct Memberships(RwLock<HashMap<String, Membership>>);

impl Memberships {
    pub(crate) fn load(records: Vec<MembershipRecord>) -> Self {
        let all = records
            .into_iter()
            .filter_map(|r| r.membership)
            .map(|m| (m.service.clone(), m))
            .collect();
        Memberships(RwLock::new(all))
    }

    fn set(&self, m: Membership) {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        all.insert(m.service.clone(), m);
    }

    /// Whether the listener `name` of `service` counts: listed, or no membership decides.
    pub(crate) fn counts_listener(&self, service: &str, name: &str) -> bool {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service).is_none_or(|m| listed_listener(m, name))
    }

    /// Whether the listener `name` of `service` may be a middleware: listed as one, or no
    /// membership decides.
    pub(crate) fn may_check(&self, service: &str, name: &str) -> bool {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service)
            .is_none_or(|m| m.middlewares.iter().any(|n| n == name))
    }

    /// Whether the kernel waits for the aggregate of `service`: listed, or no membership decides.
    pub(crate) fn counts_worker(&self, service: &str, aggregate_type: &str) -> bool {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service)
            .is_none_or(|m| m.workers.iter().any(|a| a == aggregate_type))
    }

    /// Whether any service lists the aggregate.
    pub(crate) fn lists_worker(&self, aggregate_type: &str) -> bool {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.values()
            .any(|m| m.workers.iter().any(|a| a == aggregate_type))
    }

    /// Every listed aggregate type, and every listed listener and middleware name.
    pub(crate) fn listed(&self) -> (Vec<String>, Vec<String>) {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        let workers = all.values().flat_map(|m| m.workers.clone()).collect();
        let listeners = all
            .values()
            .flat_map(|m| m.listeners.iter().chain(&m.middlewares).cloned())
            .collect();
        (workers, listeners)
    }
}

pub(crate) fn listed_listener(m: &Membership, name: &str) -> bool {
    m.listeners.iter().chain(&m.middlewares).any(|n| n == name)
}

fn validate(m: &Membership) -> Result<(), Status> {
    if !config::valid_domain(&m.service) {
        return Err(Status::invalid_argument(
            "a membership names its service: letters, digits, - and _",
        ));
    }
    if !m.workers.iter().all(|a| config::valid_qualified(a)) {
        return Err(Status::invalid_argument(
            "list workers as qualified aggregate types: domain.AggregateType",
        ));
    }
    if m.listeners
        .iter()
        .chain(&m.middlewares)
        .any(|name| !config::valid_name(name))
    {
        return Err(Status::invalid_argument(
            "a listener name has 1 to 128 letters, digits, `-`, `_` and `.`",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl MembershipService for Api {
    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<ApplyResult>, Status> {
        let kernel = &self.0;
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        protocol(req.protocol)?;
        let m = req
            .membership
            .ok_or_else(|| Status::invalid_argument("no membership"))?;
        let sig = req
            .sig
            .ok_or_else(|| Status::unauthenticated("membership is not signed"))?;
        signing::verify(&sig, &signing::membership_message(&m))
            .map_err(|_| Status::unauthenticated("invalid membership signature"))?;
        kernel.present(&metadata, &sig.key_id)?;
        if (now_ms() - m.issued_at).abs() > MAX_REQUEST_AGE_MS {
            return Err(Status::unauthenticated(
                "membership expired; check the clock",
            ));
        }
        validate(&m)?;
        if !config::allowed(&kernel.permissions().manage, &sig.key_id, &m.service) {
            return Err(Status::permission_denied(format!(
                "key may not manage {}",
                m.service
            )));
        }

        let record = MembershipRecord {
            membership: Some(m.clone()),
            sig: Some(sig),
            applied_at: now_ms(),
        };
        kernel.store.save_membership(&record).map_err(internal)?;
        crate::crash::point("membership.stored");
        kernel.memberships.set(m.clone());
        let changed = kernel.listeners.apply(&m);
        kernel.store.save_listeners(&changed).map_err(internal)?;
        kernel.workers.apply(kernel, &m)?;
        kernel.listeners.progress.notify_waiters();
        tracing::info!(service = %m.service, workers = m.workers.len(), listeners = m.listeners.len(), middlewares = m.middlewares.len(), "membership applied");
        Ok(Response::new(ApplyResult {}))
    }
}
