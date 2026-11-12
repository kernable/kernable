//! Deployments: the code version a worker or listener declares when it connects
//! (`Register.deployment`, `Hello.deployment`), and what follows from it.
//!
//! - Every event stores the deployment of the worker that produced it (`Event.deployment`,
//!   hashed when set), and every snapshot the deployment that made it; a worker gets only its own
//!   deployment's snapshots, so a rollback never reads a newer state format.
//! - Read models and middlewares are registered per name and deployment (`listeners.rs`), so a
//!   new deployment builds its own from the first event beside the old one.
//! - A service's membership names the deployment that serves it (`Membership.deployment`): its
//!   workers run the commands, its listeners count. Switching back is applying a membership that
//!   names the earlier one.
//! - A worker of a deployment refuses no command itself on history it cannot read: the kernel
//!   checks the history first ([`unknown`]) and rejects the command `unknown_history`, so nothing
//!   is decided on incomplete state.
//! - [`since`] reports what was written from a position on: per event type, schema and
//!   deployment the events and aggregates, which types or schemas are new there, and the
//!   scheduled commands, process timers and process cases still open. Before a rollback it shows
//!   what the newer deployment left behind (`kernable deployments since`, `GET /api/deployments/
//!   since` of the UI).
//! - [`drop`] forgets a deployment's read model registrations and snapshots: by hand
//!   (`kernable deployments drop`, with the kernel stopped or through its local socket while it
//!   runs) or after `--deployment-retention-days` without a connection. Never the serving
//!   deployment nor one still running, and one that served before only when forced
//!   ([`may_drop`]). Never silently: each logs what it dropped. The events stay.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use kernable_core::proto::{Event, Rejected};
use kernable_store::{Store, StreamId, snapshots::SnapshotStore};
use serde_json::{Value, json};
use tonic::Status;

use crate::config;

/// The rejection code of a command whose aggregate holds events its worker does not know.
pub const UNKNOWN_HISTORY: &str = "unknown_history";

/// Refuses a deployment that is not empty and not a valid name.
pub(crate) fn validate(deployment: &str) -> Result<(), Status> {
    if deployment.is_empty() || config::valid_name(deployment) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "a deployment has 1 to 128 letters, digits, `-`, `_` and `.`",
        ))
    }
}

/// The registration of listener `name` for `deployment`: `name@deployment`. Names and
/// deployments have no `@`, so the key splits back unambiguously.
pub(crate) fn key(name: &str, deployment: &str) -> String {
    format!("{name}@{deployment}")
}

/// A registration's listener name and deployment (empty for one registration for all).
pub(crate) fn split(key: &str) -> (&str, &str) {
    key.split_once('@').unwrap_or((key, ""))
}

/// A registration's listener name.
pub(crate) fn base(key: &str) -> &str {
    split(key).0
}

/// What the kernel knows of the log for the check of [`unknown`]: per deployment, its first
/// event and the types it wrote; per event type, its first event. Read from the store on first
/// need, then kept up to date with every commit.
#[derive(Default)]
pub(crate) struct Written {
    deployments: Mutex<HashMap<String, Arc<Mutex<Wrote>>>>,
    /// Per qualified type: the first event's position and deployment; `None`: no event yet.
    firsts: Mutex<HashMap<String, Option<(u64, String)>>>,
}

#[derive(Default)]
struct Wrote {
    first: Option<u64>,
    types: HashSet<String>,
}

impl Written {
    fn of(&self, store: &dyn Store, deployment: &str) -> kernable_store::Result<Arc<Mutex<Wrote>>> {
        if let Some(w) = self
            .deployments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(deployment)
        {
            return Ok(Arc::clone(w));
        }
        let (first, types) = store.deployment_written(deployment)?;
        let wrote = Arc::new(Mutex::new(Wrote {
            first,
            types: types.into_iter().collect(),
        }));
        Ok(Arc::clone(
            self.deployments
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(deployment.to_owned())
                .or_insert(wrote),
        ))
    }

    fn first(
        &self,
        store: &dyn Store,
        qualified: &str,
    ) -> kernable_store::Result<Option<(u64, String)>> {
        if let Some(first) = self
            .firsts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(qualified)
        {
            return Ok(first.clone());
        }
        let (domain, event_type) = qualified.split_once('.').unwrap_or(("", qualified));
        let first = store.first_of_type(domain, event_type)?;
        self.firsts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(qualified.to_owned(), first.clone());
        Ok(first)
    }

    /// Notes committed events: the first of a deployment and of a type, and the types each
    /// deployment wrote, where already read.
    pub(crate) fn committed(&self, events: &[Event]) {
        let deployments = self
            .deployments
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut firsts = self.firsts.lock().unwrap_or_else(PoisonError::into_inner);
        for e in events {
            let qualified = config::qualified(&e.domain, &e.event_type);
            if let Some(first @ None) = firsts.get_mut(&qualified) {
                *first = Some((e.global_position, e.deployment.clone()));
            }
            if let Some(w) = deployments.get(&e.deployment) {
                let mut w = w.lock().unwrap_or_else(PoisonError::into_inner);
                w.first.get_or_insert(e.global_position);
                w.types.insert(qualified);
            }
        }
    }

    /// Whether `deployment` wrote events: it served.
    pub(crate) fn served(
        &self,
        store: &dyn Store,
        deployment: &str,
    ) -> kernable_store::Result<bool> {
        Ok(self
            .of(store, deployment)?
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first
            .is_some())
    }

    /// Forgets what it read: after a rewind or an import changed the log under it.
    pub(crate) fn forget(&self) {
        self.deployments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.firsts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Whether a worker of `deployment` that knows the event types `known` (qualified, each up to
/// its schema) can decide on `history`: `Some` rejection naming the first event it does not
/// know. An event is unknown when its schema is above the known one, or when its type is not
/// known at all, the deployment never wrote it, and another deployment first wrote it after the
/// deployment's own first event (a type that existed before counts as known: older code read
/// past it then too). A worker without a deployment is not checked.
pub(crate) fn unknown(
    written: &Written,
    store: &dyn Store,
    deployment: &str,
    known: &HashMap<String, u32>,
    history: &[Event],
) -> kernable_store::Result<Option<Rejected>> {
    if deployment.is_empty() {
        return Ok(None);
    }
    let mut checked = HashSet::new();
    for e in history {
        let qualified = config::qualified(&e.domain, &e.event_type);
        let why = match known.get(&qualified) {
            Some(&schema) if e.schema > schema => format!(
                "{qualified} at schema {} (version {}), and deployment {deployment} reads it only up to schema {schema}",
                e.schema, e.version
            ),
            Some(_) => continue,
            None => {
                if !checked.insert(qualified.clone()) {
                    continue;
                }
                let wrote = written.of(store, deployment)?;
                let (mine, since) = {
                    let w = wrote.lock().unwrap_or_else(PoisonError::into_inner);
                    (w.types.contains(&qualified), w.first)
                };
                let Some(since) = since.filter(|_| !mine) else {
                    continue;
                };
                match written.first(store, &qualified)? {
                    Some((position, by)) if position > since && by != deployment => format!(
                        "{qualified} (version {}), a type deployment {deployment} does not know: deployment {} first wrote it, at position {position}",
                        e.version,
                        if by.is_empty() { "(none)" } else { &by }
                    ),
                    _ => continue,
                }
            }
        };
        return Ok(Some(Rejected {
            code: UNKNOWN_HISTORY.into(),
            message: format!(
                "the aggregate holds {why}; its commands run again once a deployment that knows it serves, or after events this deployment knows correct it"
            ),
            ..Default::default()
        }));
    }
    Ok(None)
}

/// The position after which events are newer than `time` (ms since epoch): the last event
/// before it, by commit time; 0 if there is none.
pub fn position_at(store: &dyn Store, time: i64) -> kernable_store::Result<u64> {
    let (mut low, mut high) = (0, store.head_position()?);
    // Commit times grow with positions: the last position whose event is older than `time`.
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let at = store
            .read_all(mid - 1, 1)?
            .first()
            .map_or(i64::MAX, |e| e.created_at);
        if at < time {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    Ok(low)
}

/// One row of the report: the events of one type, schema and deployment.
#[derive(Default)]
struct Row {
    events: u64,
    aggregates: HashSet<StreamId>,
    first: u64,
    last_at: i64,
}

/// What was written after position `from`, for the report before a rollback: per event type,
/// schema and producing deployment the events, aggregates and first position, and whether the
/// type or the schema is new after `from` (no event of the type, or of that schema or a higher
/// one, before it); with `deployment`, only the events it wrote. Then the scheduled commands
/// and process timers stored after `from` and still due, the running process cases moved after
/// it, and the process versions registered since.
pub fn since(
    store: &dyn Store,
    from: u64,
    deployment: Option<&str>,
) -> kernable_store::Result<Value> {
    let head = store.head_position()?;
    let from = from.min(head);
    // What the log held up to `from`: each type's highest schema.
    let mut before: HashMap<String, u32> = HashMap::new();
    let mut since_time = 0;
    let mut after = 0;
    while after < from {
        let events = store.read_all(after, 1000)?;
        let Some(last) = events.last() else { break };
        after = last.global_position;
        for e in events.iter().filter(|e| e.global_position <= from) {
            let max = before
                .entry(config::qualified(&e.domain, &e.event_type))
                .or_default();
            *max = (*max).max(e.schema);
            since_time = e.created_at;
        }
    }
    let mut rows: BTreeMap<(String, u32, String), Row> = BTreeMap::new();
    let mut moved_cases: HashSet<StreamId> = HashSet::new();
    let mut after = from;
    loop {
        let events = store.read_all(after, 1000)?;
        let Some(last) = events.last() else { break };
        after = last.global_position;
        for e in &events {
            let stream = StreamId {
                tenant_id: e.tenant_id.clone(),
                domain: e.domain.clone(),
                aggregate_type: e.aggregate_type.clone(),
                aggregate_id: e.aggregate_id.clone(),
            };
            if e.domain == crate::process_engine::DOMAIN {
                moved_cases.insert(stream.clone());
            }
            if deployment.is_some_and(|d| d != e.deployment) {
                continue;
            }
            let row = rows
                .entry((
                    config::qualified(&e.domain, &e.event_type),
                    e.schema,
                    e.deployment.clone(),
                ))
                .or_default();
            if row.events == 0 {
                row.first = e.global_position;
            }
            row.events += 1;
            row.aggregates.insert(stream);
            row.last_at = e.created_at;
        }
    }
    let types: Vec<Value> = rows
        .into_iter()
        .map(|((event_type, schema, by), row)| {
            let known = before.get(&event_type).copied();
            json!({
                "type": event_type,
                "schema": schema,
                "deployment": by,
                "events": row.events,
                "aggregates": row.aggregates.len(),
                "firstPosition": row.first,
                "lastAt": row.last_at,
                "newType": known.is_none(),
                "newSchema": known.is_some_and(|k| schema > k),
            })
        })
        .collect();

    // Scheduled commands stored since then and still waiting; the process engine's timers apart.
    let mut scheduled = Vec::new();
    let mut timers = Vec::new();
    for p in store.pending()? {
        let c = &p.command;
        if c.deliver_at == 0 || p.received_at < since_time || (from > 0 && p.received_at == 0) {
            continue;
        }
        let entry = json!({
            "commandId": c.command_id,
            "tenant": c.tenant_id,
            "type": config::qualified(&c.domain, &c.command_type),
            "aggregate": format!("{}.{}/{}", c.domain, c.aggregate_type, c.aggregate_id),
            "receivedAt": p.received_at,
            "deliverAt": c.deliver_at,
        });
        if c.domain == crate::process_engine::DOMAIN {
            timers.push(entry);
        } else {
            scheduled.push(entry);
        }
    }
    // Running cases (they hold the fact `running`) that moved since then.
    let mut cases = Vec::new();
    for (stream, index) in store.domain_index(crate::process_engine::DOMAIN)? {
        if !index.facts.iter().any(|f| f == "running") || !moved_cases.contains(&stream) {
            continue;
        }
        let last = store
            .read_stream(
                &stream.tenant_id,
                &stream.domain,
                &stream.aggregate_type,
                &stream.aggregate_id,
            )?
            .pop();
        cases.push(json!({
            "process": stream.aggregate_type,
            "case": stream.aggregate_id,
            "tenant": stream.tenant_id,
            "lastEvent": last.as_ref().map(|e| e.event_type.clone()),
            "lastAt": last.as_ref().map(|e| e.created_at),
        }));
    }
    let processes: Vec<Value> = store
        .processes()?
        .into_iter()
        .filter(|p| p.version > 0 && p.recorded_at >= since_time && since_time > 0)
        .map(|p| json!({ "process": p.name, "version": p.version, "recordedAt": p.recorded_at }))
        .collect();
    Ok(json!({
        "from": from,
        "fromTime": since_time,
        "head": head,
        "deployment": deployment,
        "types": types,
        "scheduled": scheduled,
        "timers": timers,
        "cases": cases,
        "processVersions": processes,
    }))
}

/// Every deployment that wrote events or has read models registered, with its first and last
/// event, how many it wrote, and its registrations; and per service the deployment its
/// membership names. Reads the whole log.
pub fn list(store: &dyn Store) -> kernable_store::Result<Value> {
    #[derive(Default)]
    struct Seen {
        first: u64,
        last: u64,
        last_at: i64,
        events: u64,
        listeners: Vec<String>,
    }
    let mut all: BTreeMap<String, Seen> = BTreeMap::new();
    store.each_event(&mut |e| {
        if e.deployment.is_empty() {
            return;
        }
        let seen = all.entry(e.deployment.clone()).or_default();
        if seen.events == 0 {
            seen.first = e.global_position;
        }
        seen.events += 1;
        seen.last = e.global_position;
        seen.last_at = e.created_at;
    })?;
    for l in store.listeners()? {
        let (_, deployment) = split(&l.name);
        if !deployment.is_empty() {
            all.entry(deployment.to_owned())
                .or_default()
                .listeners
                .push(l.name);
        }
    }
    let serving: Vec<Value> = store
        .memberships()?
        .into_iter()
        .filter_map(|m| m.membership)
        .map(|m| json!({ "service": m.service, "deployment": m.deployment }))
        .collect();
    let deployments: Vec<Value> = all
        .into_iter()
        .map(|(d, s)| {
            json!({
                "deployment": d,
                "events": s.events,
                "firstPosition": s.first,
                "lastPosition": s.last,
                "lastAt": s.last_at,
                "listeners": s.listeners,
            })
        })
        .collect();
    Ok(json!({ "deployments": deployments, "services": serving }))
}

/// How far a deployment is from taking a service over ([`warm_up`]).
pub(crate) struct WarmUp {
    pub(crate) deployment: String,
    /// The newest durable position.
    pub(crate) head: u64,
    /// `--deployment-switch-lag`: how far behind the head a read model may be.
    pub(crate) lag: u64,
    /// The deployment wrote events before, so it served: its kept read models only catch up
    /// on the gap, and it takes over at once.
    pub(crate) served_before: bool,
    /// Per listener name: the registration it takes over with, if any.
    pub(crate) listeners: Vec<(String, Option<crate::listeners::Warming>)>,
    /// Per qualified aggregate type: whether a worker of the deployment is connected.
    pub(crate) workers: Vec<(String, bool)>,
    /// Why it may not take over whatever the wait (only for a deployment that never served):
    /// a read model or middleware failing at one event, events in the history its workers do
    /// not know (their commands would be `unknown_history`). Never values.
    pub(crate) failed: Vec<String>,
}

impl WarmUp {
    fn warm(&self, w: &crate::listeners::Warming) -> bool {
        !w.versioned || self.served_before || w.position.saturating_add(self.lag) >= self.head
    }

    /// Why the deployment may not take over yet; `None` once every listener it takes over
    /// with is registered and within `lag` of the head (or it served before), every worker is
    /// connected, and something of it is.
    pub(crate) fn why_not(&self) -> Option<String> {
        let mut waiting = self.failed.clone();
        for (name, w) in &self.listeners {
            match w {
                None => waiting.push(format!("{name} has not connected")),
                Some(w) if !self.warm(w) => waiting.push(format!(
                    "{} at {} of {}",
                    w.registration, w.position, self.head
                )),
                Some(_) => {}
            }
        }
        for (aggregate, connected) in &self.workers {
            if !connected {
                waiting.push(format!("no worker of {aggregate} is connected"));
            }
        }
        let anything = self.workers.iter().any(|(_, c)| *c)
            || self
                .listeners
                .iter()
                .any(|(_, w)| w.as_ref().is_some_and(|w| w.connected));
        if waiting.is_empty() && !anything {
            waiting.push("nothing of it is connected".into());
        }
        (!waiting.is_empty()).then(|| {
            format!(
                "deployment {} has not caught up: {}",
                self.deployment,
                waiting.join(", ")
            )
        })
    }

    pub(crate) fn json(&self) -> Value {
        let listeners: Vec<Value> = self
            .listeners
            .iter()
            .map(|(name, w)| match w {
                Some(w) => json!({
                    "name": name,
                    "registration": w.registration,
                    "position": w.position,
                    "behind": self.head.saturating_sub(w.position),
                    "connected": w.connected,
                    "caughtUp": self.warm(w),
                }),
                None => json!({ "name": name, "registration": null, "caughtUp": false }),
            })
            .collect();
        let workers: Vec<Value> = self
            .workers
            .iter()
            .map(|(a, c)| json!({ "aggregate": a, "connected": c }))
            .collect();
        let why = self.why_not();
        json!({
            "deployment": self.deployment,
            "head": self.head,
            "maxLag": self.lag,
            "servedBefore": self.served_before,
            "caughtUp": why.is_none(),
            "waitingFor": why,
            "failed": self.failed,
            "listeners": listeners,
            "workers": workers,
        })
    }
}

/// How far `deployment` is from taking a service over. With the membership `m` that would
/// switch to it: its listed listeners and workers. Without one: every read model and middleware
/// registered for the deployment, and the aggregates its connected workers run.
pub(crate) fn warm_up(
    kernel: &crate::Kernel,
    deployment: &str,
    m: Option<&kernable_core::proto::Membership>,
) -> kernable_store::Result<WarmUp> {
    let served_before = kernel
        .written
        .of(kernel.store.as_ref(), deployment)?
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .first
        .is_some();
    let (listeners, workers) = match m {
        Some(m) => {
            let names: Vec<String> = m.listeners.iter().chain(&m.middlewares).cloned().collect();
            let workers = m
                .workers
                .iter()
                .map(|a| (a.clone(), kernel.workers.connected_of(a, deployment)))
                .collect();
            (
                kernel
                    .listeners
                    .warming(deployment, Some(&m.service), Some(&names)),
                workers,
            )
        }
        None => (
            kernel.listeners.warming(deployment, None, None),
            kernel
                .workers
                .aggregates_of(deployment)
                .into_iter()
                .map(|a| (a, true))
                .collect(),
        ),
    };
    let failed = if served_before {
        // A rollback: the since-report says what the newer deployment left behind.
        Vec::new()
    } else {
        failed(kernel, deployment)?
    };
    Ok(WarmUp {
        deployment: deployment.to_owned(),
        head: kernel.durable.load(std::sync::atomic::Ordering::SeqCst),
        lag: kernel.config().deployment_switch_lag,
        served_before,
        listeners,
        workers,
        failed,
    })
}

/// Why `deployment`, which never served, may not take over whatever the wait: its read models
/// or middlewares that keep failing at one event, and event schemas in the history above what
/// its workers read (every command on such an aggregate would be `unknown_history`). Types,
/// schemas and positions only, never values.
fn failed(kernel: &crate::Kernel, deployment: &str) -> kernable_store::Result<Vec<String>> {
    let mut why = Vec::new();
    for f in kernel.listeners.failing(deployment) {
        let at = kernel
            .store
            .read_all(f.position, 1)?
            .into_iter()
            .next()
            .map(|e| {
                format!(
                    " ({} at schema {})",
                    config::qualified(&e.domain, &e.event_type),
                    e.schema
                )
            })
            .unwrap_or_default();
        why.push(format!(
            "{} fails at the event after position {}{at}: its connection ended {} times there",
            f.registration, f.position, f.times
        ));
    }
    let stored = kernel.schemas().clone();
    for (event_type, known) in kernel.workers.schemas_of(deployment) {
        if let Some(&schema) = stored.get(&event_type).filter(|&&s| s > known) {
            why.push(format!(
                "{UNKNOWN_HISTORY}: the history holds {event_type} at schema {schema}, and deployment {deployment}'s workers read it only up to schema {known}"
            ));
        }
    }
    Ok(why)
}

/// Drops what the kernel keeps for `deployment` apart from the log: its read models' and
/// middlewares' registrations (and positions) and its snapshots. The events stay. Returns how
/// many registrations and snapshots went.
pub fn drop(
    store: &dyn Store,
    snapshots: &dyn SnapshotStore,
    deployment: &str,
) -> kernable_store::Result<(Vec<String>, usize)> {
    let names: Vec<String> = store
        .listeners()?
        .into_iter()
        .filter(|l| split(&l.name).1 == deployment && !deployment.is_empty())
        .map(|l| l.name)
        .collect();
    let removed: Vec<_> = names
        .iter()
        .map(|name| kernable_core::proto::ListenerRecord {
            name: name.clone(),
            removed: true,
            ..Default::default()
        })
        .collect();
    store.save_listeners(&removed)?;
    let dropped = snapshots.drop_deployment(deployment)?;
    Ok((names, dropped))
}

/// Why a deployment may not be dropped: the code (`serving`, `running`, `served_before`) and a
/// message for people.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub message: String,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// Whether `deployment` may be dropped. Never the deployment a service's membership names
/// (`serving`: `services` are the services it serves), never one with a process connected
/// (`running`: `running` names it; its read models' state is in use, and an operator's grace
/// period keeps the old deployment's processes running), and one that served before (it wrote
/// events: a rollback could need it, or it may still be in an operator's grace period, which the
/// kernel does not know) only with `force`.
pub fn may_drop(
    deployment: &str,
    services: &[String],
    running: Option<&str>,
    served_before: bool,
    force: bool,
) -> Result<(), Refusal> {
    if !services.is_empty() {
        return Err(Refusal {
            code: "serving",
            message: format!(
                "deployment {deployment} serves {}: switch to another deployment first",
                services.join(", ")
            ),
        });
    }
    if let Some(what) = running {
        return Err(Refusal {
            code: "running",
            message: format!(
                "deployment {deployment} still runs ({what}): stop its processes first"
            ),
        });
    }
    if served_before && !force {
        return Err(Refusal {
            code: "served_before",
            message: format!(
                "deployment {deployment} served before: a rollback would need its read models, and \
                 the kernel cannot tell whether it is still within a grace period; --force drops it"
            ),
        });
    }
    Ok(())
}

/// The services whose membership names `deployment` in `store`, and whether it wrote events:
/// what [`may_drop`] needs, for a store whose kernel is stopped.
pub fn drop_state(
    store: &dyn Store,
    deployment: &str,
) -> kernable_store::Result<(Vec<String>, bool)> {
    let services = store
        .memberships()?
        .into_iter()
        .filter_map(|m| m.membership)
        .filter(|m| m.deployment == deployment)
        .map(|m| m.service)
        .collect();
    let served_before = store.deployment_written(deployment)?.0.is_some();
    Ok((services, served_before))
}

/// What a drop reports, by `kernable deployments drop --json` and the kernel's local socket.
pub fn dropped_json(
    deployment: &str,
    read_models: &[String],
    snapshots: usize,
    forced: bool,
) -> Value {
    json!({
        "deployment": deployment,
        "readModels": read_models,
        "snapshots": snapshots,
        "forced": forced,
    })
}

#[cfg(test)]
mod tests {
    use kernable_core::proto::{Command, NewEvent};

    use super::*;

    /// A memory store with commits of `(aggregate, event type, schema, deployment)`, in order.
    fn store_with(commits: &[(&str, &str, u32, &str)]) -> Box<dyn Store> {
        let store = kernable_store::open("memory:").unwrap();
        for (i, (aggregate, event_type, schema, deployment)) in commits.iter().enumerate() {
            let cmd = Command {
                command_id: format!("c{i}"),
                tenant_id: "t".into(),
                domain: "shop".into(),
                aggregate_type: "cart".into(),
                aggregate_id: (*aggregate).into(),
                command_type: "Do".into(),
                ..Default::default()
            };
            store.enqueue(&cmd, 1_000 + i as i64).unwrap();
            let version = store
                .read_stream("t", "shop", "cart", aggregate)
                .unwrap()
                .len() as u64;
            let event = NewEvent {
                event_type: (*event_type).into(),
                schema: *schema,
                deployment: (*deployment).into(),
                ..Default::default()
            };
            store
                .commit(
                    &cmd.command_id,
                    version,
                    &[event],
                    &Default::default(),
                    None,
                    1_000 + i as i64,
                )
                .unwrap();
        }
        store
    }

    fn known(types: &[(&str, u32)]) -> HashMap<String, u32> {
        types.iter().map(|(t, s)| ((*t).to_owned(), *s)).collect()
    }

    fn check(
        store: &dyn Store,
        deployment: &str,
        known: &HashMap<String, u32>,
        aggregate: &str,
    ) -> Option<String> {
        let history = store.read_stream("t", "shop", "cart", aggregate).unwrap();
        unknown(&Written::default(), store, deployment, known, &history)
            .unwrap()
            .map(|r| r.message)
    }

    #[test]
    fn a_newer_schema_or_a_type_introduced_later_by_another_deployment_is_unknown() {
        let store = store_with(&[
            ("a", "Opened", 1, "v1"),
            ("a", "Added", 2, "v2"),
            ("b", "Opened", 1, "v1"),
            ("b", "Shared", 1, "v2"),
            ("c", "Opened", 1, "v1"),
        ]);
        let v1 = known(&[("shop.Opened", 1), ("shop.Added", 1)]);
        let a = check(store.as_ref(), "v1", &v1, "a").unwrap();
        assert!(
            a.contains(
                "shop.Added at schema 2 (version 2), and deployment v1 reads it only up to schema 1"
            ),
            "{a}"
        );
        let b = check(store.as_ref(), "v1", &v1, "b").unwrap();
        assert!(b.contains("shop.Shared (version 2), a type deployment v1 does not know: deployment v2 first wrote it, at position 4"), "{b}");
        assert_eq!(check(store.as_ref(), "v1", &v1, "c"), None);
        // v2 knows both; a process without a deployment is not checked.
        let v2 = known(&[("shop.Opened", 1), ("shop.Added", 2), ("shop.Shared", 1)]);
        assert_eq!(check(store.as_ref(), "v2", &v2, "a"), None);
        assert_eq!(check(store.as_ref(), "", &v1, "b"), None);
    }

    #[test]
    fn types_older_than_a_deployment_and_its_own_are_known() {
        let store = store_with(&[
            ("a", "Opened", 1, "v1"),
            ("a", "Noted", 1, "v1"),
            ("a", "Shared", 1, "v2"),
            ("a", "Opened", 1, "v3"),
        ]);
        let narrow = known(&[("shop.Opened", 1)]);
        // v1 wrote Noted itself; Shared came after v1's first event, from v2.
        let v1 = check(store.as_ref(), "v1", &narrow, "a").unwrap();
        assert!(v1.contains("shop.Shared"), "{v1}");
        // v3's first event comes after every type was first written: all are known to it.
        assert_eq!(check(store.as_ref(), "v3", &narrow, "a"), None);
        // A deployment that wrote nothing knows what exists.
        assert_eq!(check(store.as_ref(), "v4", &narrow, "a"), None);
    }

    #[test]
    fn what_the_kernel_reads_once_follows_the_commits() {
        let store = store_with(&[("a", "Opened", 1, "v1")]);
        let written = Written::default();
        let narrow = known(&[("shop.Opened", 1)]);
        let history = store.read_stream("t", "shop", "cart", "a").unwrap();
        assert!(
            unknown(&written, store.as_ref(), "v1", &narrow, &history)
                .unwrap()
                .is_none()
        );
        // Read once (no Shared yet), then Shared committed by v2: the cache learns it.
        let shared = Event {
            global_position: 2,
            domain: "shop".into(),
            event_type: "Shared".into(),
            deployment: "v2".into(),
            schema: 1,
            version: 2,
            ..Default::default()
        };
        assert!(
            written
                .first(store.as_ref(), "shop.Shared")
                .unwrap()
                .is_none()
        );
        written.committed(std::slice::from_ref(&shared));
        let refused = unknown(&written, store.as_ref(), "v1", &narrow, &[shared]).unwrap();
        assert_eq!(refused.unwrap().code, UNKNOWN_HISTORY);
    }

    #[test]
    fn the_report_marks_new_types_and_schemas_and_filters_by_deployment() {
        let store = store_with(&[
            ("a", "Opened", 1, "v1"),
            ("a", "Added", 1, "v1"),
            ("b", "Opened", 1, "v2"),
            ("a", "Added", 2, "v2"),
            ("b", "Added", 2, "v2"),
            ("b", "Shared", 1, "v2"),
        ]);
        let report = since(store.as_ref(), 2, Some("v2")).unwrap();
        let rows: Vec<(String, u64, u64, u64, bool, bool)> = report["types"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["type"].as_str().unwrap().to_owned(),
                    t["schema"].as_u64().unwrap(),
                    t["events"].as_u64().unwrap(),
                    t["aggregates"].as_u64().unwrap(),
                    t["newType"].as_bool().unwrap(),
                    t["newSchema"].as_bool().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            [
                ("shop.Added".into(), 2, 2, 2, false, true),
                ("shop.Opened".into(), 1, 1, 1, false, false),
                ("shop.Shared".into(), 1, 1, 1, true, false),
            ]
        );
        assert_eq!(report["from"], 2);
        assert_eq!(report["head"], 6);
        // The position before a time: events were committed 1 ms apart from 1000.
        assert_eq!(position_at(store.as_ref(), 1_002).unwrap(), 2);
        assert_eq!(position_at(store.as_ref(), 0).unwrap(), 0);
        assert_eq!(position_at(store.as_ref(), 9_999).unwrap(), 6);
    }

    #[test]
    fn the_serving_and_a_running_deployment_are_never_dropped_one_that_served_only_by_force() {
        let shop = ["shop".to_owned()];
        let refused = |r: Result<(), Refusal>| r.unwrap_err().code;
        assert_eq!(refused(may_drop("v1", &shop, None, true, true)), "serving");
        let worker = Some("a worker of orders.Order is connected");
        assert_eq!(refused(may_drop("v1", &[], worker, false, true)), "running");
        assert_eq!(
            refused(may_drop("v1", &[], None, true, false)),
            "served_before"
        );
        assert!(may_drop("v1", &[], None, true, true).is_ok());
        assert!(may_drop("pr-1", &[], None, false, false).is_ok());
        let store = store_with(&[("a", "Opened", 1, "v1")]);
        assert_eq!(drop_state(store.as_ref(), "v1").unwrap(), (vec![], true));
        assert_eq!(drop_state(store.as_ref(), "v2").unwrap(), (vec![], false));
    }

    #[test]
    fn keys_split_back_into_name_and_deployment() {
        assert_eq!(key("carts", "v2"), "carts@v2");
        assert_eq!(split("carts@v2"), ("carts", "v2"));
        assert_eq!(split("mailer"), ("mailer", ""));
        assert!(validate("").is_ok() && validate("2026-11-11.1").is_ok());
        assert!(validate("a@b").is_err() && validate(&"x".repeat(129)).is_err());
    }
}
