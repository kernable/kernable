//! What each service consists of, as its deploy applied it (see `Membership` in kernable.proto).
//!
//! The registrations of workers and listeners stay independent of it: they register whenever
//! they connect. A service's membership only decides what counts. The kernel waits for what it
//! lists, also before it ever connected, and treats the service's other workers and listeners
//! as dynamic. A service without a membership, and a process without a service, count as
//! registered.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, TryLockError},
};

use kernable_core::{
    proto::{
        ApplyRequest, ApplyResult, Membership, MembershipRecord, Signature,
        membership_service_server::MembershipService,
    },
    signing,
};
use tonic::{Request, Response, Status};

use crate::{Api, Kernel, MAX_REQUEST_AGE_MS, config, internal, now_ms, protocol};

#[derive(Default)]
pub(crate) struct Memberships {
    all: RwLock<HashMap<String, Membership>>,
    /// Held while a membership is applied, so that the newest one wins in whatever order they
    /// arrive.
    applying: Mutex<()>,
}

impl Memberships {
    pub(crate) fn load(records: Vec<MembershipRecord>) -> Self {
        let all = records
            .into_iter()
            .filter_map(|r| r.membership)
            .map(|m| (m.service.clone(), m))
            .collect();
        Memberships {
            all: RwLock::new(all),
            applying: Mutex::default(),
        }
    }

    /// Held while the membership is stored, which waits for the standby: a second one waits
    /// for it as a write waits for the standby (`replication::blocking`).
    fn applying(&self) -> MutexGuard<'_, ()> {
        match self.applying.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => crate::replication::blocking(|| {
                self.applying.lock().unwrap_or_else(PoisonError::into_inner)
            }),
        }
    }

    /// Refuses `m` unless it is newer than the applied membership of its service: an older
    /// one, replayed or from a slower deploy, would undo the newer, and could remove a
    /// middleware's check. The applied one again is fine: a retried deploy.
    fn admit(&self, m: &Membership) -> Result<(), Status> {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        match all.get(&m.service) {
            Some(applied) if applied != m && applied.issued_at >= m.issued_at => {
                Err(Status::failed_precondition(format!(
                    "{} has a membership issued at the same time or later; apply again",
                    m.service
                )))
            }
            _ => Ok(()),
        }
    }

    fn set(&self, m: Membership) {
        let mut all = self.all.write().unwrap_or_else(PoisonError::into_inner);
        all.insert(m.service.clone(), m);
    }

    /// Whether the listener `name` of `service` counts: listed, or no membership decides.
    pub(crate) fn counts_listener(&self, service: &str, name: &str) -> bool {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service).is_none_or(|m| listed_listener(m, name))
    }

    /// Whether the listener `name` of `service` may be a middleware: listed as one, or no
    /// membership decides.
    pub(crate) fn may_check(&self, service: &str, name: &str) -> bool {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service)
            .is_none_or(|m| m.middlewares.iter().any(|n| n == name))
    }

    /// Whether the kernel waits for the aggregate of `service`: listed, or no membership decides.
    pub(crate) fn counts_worker(&self, service: &str, aggregate_type: &str) -> bool {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service)
            .is_none_or(|m| m.workers.iter().any(|a| a == aggregate_type))
    }

    /// Whether any service lists the aggregate.
    pub(crate) fn lists_worker(&self, aggregate_type: &str) -> bool {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        all.values()
            .any(|m| m.workers.iter().any(|a| a == aggregate_type))
    }

    /// The deployment that serves `service`, as its membership names it; empty: every one.
    pub(crate) fn serving(&self, service: &str) -> String {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        all.get(service)
            .map(|m| m.deployment.clone())
            .unwrap_or_default()
    }

    /// Whether `deployment` serves `service`: its membership names it or none, or the process
    /// declared no deployment.
    pub(crate) fn serves(&self, service: &str, deployment: &str) -> bool {
        deployment.is_empty() || {
            let serving = self.serving(service);
            serving.is_empty() || serving == deployment
        }
    }

    /// The deployment each service's membership names, by service; services without one are
    /// left out.
    pub(crate) fn deployments(&self) -> Vec<(String, String)> {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
        let mut named: Vec<(String, String)> = all
            .values()
            .filter(|m| !m.deployment.is_empty())
            .map(|m| (m.service.clone(), m.deployment.clone()))
            .collect();
        named.sort();
        named
    }

    /// Every listed aggregate type, and every listed listener and middleware name.
    pub(crate) fn listed(&self) -> (Vec<String>, Vec<String>) {
        let all = self.all.read().unwrap_or_else(PoisonError::into_inner);
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

pub(crate) fn validate(m: &Membership) -> Result<(), Status> {
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
    if let Some(p) = m
        .processes
        .iter()
        .find(|p| !crate::process_model::valid_process_name(p))
    {
        return Err(Status::invalid_argument(format!(
            "list processes by qualified name (domain.process), not {p}"
        )));
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
    crate::deployments::validate(&m.deployment)?;
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
        take(
            kernel,
            m,
            sig,
            Switching {
                force: req.force,
                no_switch: req.no_switch,
                no_rollback: req.no_rollback,
            },
        )?;
        Ok(Response::new(ApplyResult {}))
    }
}

/// How a membership that names another deployment than the serving one may switch to it
/// (`ApplyRequest`'s unsigned flags).
#[derive(Clone, Copy, Default)]
pub(crate) struct Switching {
    /// Switch at once, caught up or not.
    pub(crate) force: bool,
    /// Refuse any switch (`not_serving`).
    pub(crate) no_switch: bool,
    /// Refuse a switch to a deployment that served before (`served_before`): a rollback.
    pub(crate) no_rollback: bool,
}

/// Takes a signed membership into effect: its signer must be granted `manage` for the service,
/// and it must be newer than the applied one (an equal one changes nothing). One that switches
/// the service to another deployment waits until that deployment has caught up
/// (`deployments::warm_up`) unless `force`d; with `no_switch` one that would switch is refused,
/// with `no_rollback` one that would switch back to a deployment that served before.
/// For `Apply` after its age was checked, and for the
/// memberships a partition takes from the control partition, which are older (forced: the
/// control partition decided).
pub(crate) fn take(
    kernel: &Arc<Kernel>,
    m: Membership,
    sig: Signature,
    switching: Switching,
) -> Result<(), Status> {
    let Switching {
        force,
        no_switch,
        no_rollback,
    } = switching;
    validate(&m)?;
    if !config::allowed(&kernel.permissions().manage, &sig.key_id, &m.service) {
        return Err(Status::permission_denied(format!(
            "key may not manage {}",
            m.service
        )));
    }

    let _applying = kernel.memberships.applying();
    kernel.memberships.admit(&m)?;
    let before = kernel
        .memberships
        .all
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&m.service)
        .cloned();
    let serving = before.as_ref().map(|b| b.deployment.as_str());
    if no_switch && serving.is_some_and(|s| s != m.deployment) {
        return Err(Status::failed_precondition(format!(
            "not_serving: {} is served by deployment {}, not {}; switch it with kernable apply --deployment {}",
            m.service,
            serving.filter(|s| !s.is_empty()).unwrap_or("(every one)"),
            if m.deployment.is_empty() {
                "(every one)"
            } else {
                &m.deployment
            },
            m.deployment
        )));
    }
    if let Some(serving) = serving
        && !force
        && !m.deployment.is_empty()
        && serving != m.deployment
    {
        let warm =
            crate::deployments::warm_up(kernel, &m.deployment, Some(&m)).map_err(internal)?;
        if no_rollback && warm.served_before {
            return Err(Status::failed_precondition(format!(
                "served_before: deployment {} served {} before and {} serves it now; switching back is a rollback: kernable apply --deployment {}",
                m.deployment,
                m.service,
                if serving.is_empty() {
                    "every deployment"
                } else {
                    serving
                },
                m.deployment
            )));
        }
        if let Some(why) = warm.why_not() {
            return Err(Status::failed_precondition(format!(
                "not_caught_up: {why}; it takes {} over once it has (apply again), or force the switch",
                m.service
            )));
        }
    }
    let record = MembershipRecord {
        membership: Some(m.clone()),
        sig: Some(sig),
        applied_at: now_ms(),
    };
    kernel.store.save_membership(&record).map_err(internal)?;
    kernel.sync(0);
    crate::crash::point("membership.stored");
    kernel.memberships.set(m.clone());
    let sig = record.sig.clone().unwrap_or_default();
    kernel
        .processes
        .unlisted(kernel, before.as_ref(), &m, &sig)?;
    let changed = kernel.listeners.apply(&m);
    kernel.store.save_listeners(&changed).map_err(internal)?;
    kernel.workers.apply(kernel, &m)?;
    kernel.listeners.progress.notify_waiters();
    // Workers of the deployment that serves now take commands; the others stop taking them.
    kernel.work.notify_waiters();
    let changed_deployment = before.as_ref().map_or("", |b| b.deployment.as_str()) != m.deployment;
    if changed_deployment {
        tracing::info!(service = %m.service, deployment = %m.deployment, before = %before.as_ref().map_or("", |b| b.deployment.as_str()), "the service is served by another deployment now");
    }
    tracing::info!(service = %m.service, deployment = %m.deployment, workers = m.workers.len(), listeners = m.listeners.len(), middlewares = m.middlewares.len(), "membership applied");
    Ok(())
}
