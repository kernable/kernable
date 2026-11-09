//! What the UI shows of the processes (design/processes.md, "The UI's views"): where each case
//! stands, and statistics per step over the last hour, day and week. Counted from the case
//! events in the log, so a standby and a restarted kernel show the same.
//!
//! The view follows the log only when the UI asks: the first request reads the log from its
//! start, at most [`SCAN`] events per request (the UI says it is still counting meanwhile),
//! later ones read what was appended since. A kernel nobody opens the UI of pays nothing.
//!
//! **Bounded:** running cases are kept, ended ones only the newest [`ENDED`] per process.
//! Statistics are counted per minute, kept for a week; durations go into buckets 10% wide, so a
//! median or 95th percentile is exact to within 10%.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Mutex, PoisonError},
};

use kernable_core::proto::Event;
use serde_json::{Value, json};

use crate::{BoxError, Kernel, now_ms, process_engine::DOMAIN};

/// How many events one request reads at most.
const SCAN: u64 = 200_000;

/// How many events the view reads from the store at once.
const CHUNK: u64 = 1_000;

/// How many ended cases the view keeps per process.
pub(crate) const ENDED: usize = 1_000;

/// The minutes of a week: how long statistics are kept.
const WEEK: i64 = 7 * 24 * 60;

/// The width of a duration bucket: each is 10% wider than the one before.
const RATIO: f64 = 1.1;

/// Tenant, process, case ID.
type CaseRef = (String, String, String);

/// One case as the lists show it.
#[derive(Debug, Clone, Default)]
struct CaseView {
    key: String,
    version: u32,
    /// running, waiting, incident or ended.
    state: &'static str,
    /// The step it is at; once ended, the end it reached (`cancelled` by an operator).
    step: String,
    started_at: i64,
    last_at: i64,
    /// When it reached its step (ms).
    reached_at: i64,
    /// The incident's code and message.
    incident: Option<(String, String)>,
}

/// What happened at one step in one minute.
#[derive(Debug, Clone, Default)]
struct Minute {
    entered: u64,
    left: u64,
    timeouts: u64,
    incidents: u64,
    /// Durations from reaching the step until it completed: bucket → count.
    durations: BTreeMap<u16, u64>,
}

/// What happened to one process in one minute.
#[derive(Debug, Clone, Default)]
struct ProcessMinute {
    started: u64,
    /// Ended per end step (`cancelled`: by an operator).
    ended: BTreeMap<String, u64>,
}

#[derive(Default)]
struct View {
    /// The newest log position read.
    position: u64,
    cases: HashMap<CaseRef, CaseView>,
    /// The ended cases per process, oldest first: the oldest goes beyond [`ENDED`].
    ended: HashMap<String, VecDeque<CaseRef>>,
    /// Per process, version and step: minute → what happened.
    steps: HashMap<(String, u32, String), BTreeMap<i64, Minute>>,
    /// Per process and version: minute → what happened.
    processes: HashMap<(String, u32), BTreeMap<i64, ProcessMinute>>,
}

/// The UI's view of the cases; see the module.
#[derive(Default)]
pub(crate) struct ProcessView {
    view: Mutex<View>,
}

/// A window of the statistics.
#[derive(Clone, Copy)]
pub(crate) enum Window {
    Hour,
    Day,
    Week,
}

impl Window {
    pub(crate) fn parse(s: &str) -> Result<Self, BoxError> {
        match s {
            "" | "day" => Ok(Window::Day),
            "hour" => Ok(Window::Hour),
            "week" => Ok(Window::Week),
            other => Err(format!("window: {other} is not hour, day or week").into()),
        }
    }

    fn minutes(self) -> i64 {
        match self {
            Window::Hour => 60,
            Window::Day => 24 * 60,
            Window::Week => WEEK,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Window::Hour => "hour",
            Window::Day => "day",
            Window::Week => "week",
        }
    }
}

fn bucket(ms: i64) -> u16 {
    ((ms.max(0) as f64 + 1.0).ln() / RATIO.ln()) as u16
}

/// The middle of a bucket in ms.
fn bucket_ms(b: u16) -> i64 {
    let lo = RATIO.powi(b as i32) - 1.0;
    let hi = RATIO.powi(b as i32 + 1) - 1.0;
    ((lo + hi) / 2.0).round() as i64
}

/// The `q`-quantile (0..1) of the counted durations, in ms.
fn quantile(durations: &BTreeMap<u16, u64>, q: f64) -> Option<i64> {
    let total: u64 = durations.values().sum();
    if total == 0 {
        return None;
    }
    let rank = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (b, n) in durations {
        seen += n;
        if seen >= rank {
            return Some(bucket_ms(*b));
        }
    }
    None
}

impl ProcessView {
    fn lock(&self) -> std::sync::MutexGuard<'_, View> {
        self.view.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads what the log holds after the view's position, at most [`SCAN`] events. Returns
    /// the position read up to and the head.
    fn catch_up(&self, kernel: &Kernel) -> Result<(u64, u64), BoxError> {
        let head = kernel.store.head_position()?;
        let mut view = self.lock();
        let end = view.position.saturating_add(SCAN).min(head);
        while view.position < end {
            let batch = kernel
                .store
                .read_all(view.position, (end - view.position).min(CHUNK) as u32)?;
            if batch.is_empty() {
                break;
            }
            for e in &batch {
                view.position = e.global_position;
                if e.domain == DOMAIN && e.aggregate_type != "registry" {
                    view.apply(e);
                }
            }
        }
        view.prune(now_ms() / 60_000);
        Ok((view.position, head))
    }

    /// Every process: its versions with their models, the cases at each step now, and the
    /// statistics over `window`.
    pub(crate) fn processes(&self, kernel: &Kernel, window: Window) -> Result<Value, BoxError> {
        let (read, head) = self.catch_up(kernel)?;
        let now = now_ms();
        let from = now / 60_000 - window.minutes();
        let view = self.lock();

        // Cases now, per process, version, step and state.
        let mut here: HashMap<(&str, u32, &str), BTreeMap<&str, u64>> = HashMap::new();
        for ((_, process, _), c) in &view.cases {
            if c.state != "ended" {
                *here
                    .entry((process, c.version, c.step.as_str()))
                    .or_default()
                    .entry(c.state)
                    .or_default() += 1;
            }
        }

        let mut out = Vec::new();
        for (name, entry) in kernel.processes.all() {
            let mut versions = Vec::new();
            for record in &entry.versions {
                let model: Value = serde_json::from_slice(&record.model).unwrap_or(Value::Null);
                let mut steps = serde_json::Map::new();
                let mut names: Vec<String> = model["steps"]
                    .as_array()
                    .map(|s| {
                        s.iter()
                            .filter_map(|s| s["name"].as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default();
                // Ended by an operator: no step of the model, but an end all the same.
                names.push("cancelled".into());
                for step in names {
                    let mut total = Minute::default();
                    if let Some(minutes) =
                        view.steps
                            .get(&(name.clone(), record.version, step.clone()))
                    {
                        for (_, m) in minutes.range(from + 1..) {
                            total.entered += m.entered;
                            total.left += m.left;
                            total.timeouts += m.timeouts;
                            total.incidents += m.incidents;
                            for (b, n) in &m.durations {
                                *total.durations.entry(*b).or_default() += n;
                            }
                        }
                    }
                    let now_here = here
                        .get(&(name.as_str(), record.version, step.as_str()))
                        .cloned()
                        .unwrap_or_default();
                    steps.insert(
                        step,
                        json!({
                            "now": now_here,
                            "entered": total.entered,
                            "left": total.left,
                            "timeouts": total.timeouts,
                            "incidents": total.incidents,
                            "medianMs": quantile(&total.durations, 0.5),
                            "p95Ms": quantile(&total.durations, 0.95),
                        }),
                    );
                }
                let mut started = 0;
                let mut ended: BTreeMap<String, u64> = BTreeMap::new();
                if let Some(minutes) = view.processes.get(&(name.clone(), record.version)) {
                    for (_, m) in minutes.range(from + 1..) {
                        started += m.started;
                        for (end, n) in &m.ended {
                            *ended.entry(end.clone()).or_default() += n;
                        }
                    }
                }
                versions.push(json!({
                    "version": record.version,
                    "model": model,
                    "registeredAt": record.recorded_at,
                    "key": record.sig.as_ref().map(|s| s.key_id.clone()).unwrap_or_default(),
                    "steps": steps,
                    "started": started,
                    "ended": ended,
                }));
            }
            let acting = entry.acting.as_ref().map(|a| {
                json!({
                    "senderTenant": a.sender_tenant,
                    "senderIdentity": a.sender_identity,
                    "senderSession": a.sender_session,
                })
            });
            out.push(json!({
                "name": name,
                "removed": entry.removed,
                "acting": acting,
                "versions": versions,
            }));
        }
        Ok(json!({
            "processes": out,
            "window": window.name(),
            "read": read,
            "head": head,
            "now": now,
        }))
    }

    /// The cases that match, the most recently moved first, at most `limit`; `total` counts
    /// all that match.
    pub(crate) fn cases(
        &self,
        kernel: &Kernel,
        filter: &CaseFilter,
        limit: usize,
    ) -> Result<Value, BoxError> {
        let (read, head) = self.catch_up(kernel)?;
        let view = self.lock();
        let mut found: Vec<(&CaseRef, &CaseView)> = view
            .cases
            .iter()
            .filter(|((tenant, process, id), c)| {
                (filter.process.is_empty() || *process == filter.process)
                    && filter.version.is_none_or(|v| v == c.version)
                    && (filter.step.is_empty() || c.step == filter.step)
                    && match filter.state.as_str() {
                        "" => true,
                        // Not ended: the cases still on their way.
                        "open" => c.state != "ended",
                        s => c.state == s,
                    }
                    && (filter.text.is_empty()
                        || id.contains(&filter.text)
                        || tenant.contains(&filter.text)
                        || c.key.contains(&filter.text))
            })
            .collect();
        let total = found.len();
        found.sort_by(|a, b| b.1.last_at.cmp(&a.1.last_at).then(a.0.cmp(b.0)));
        let cases: Vec<Value> = found
            .into_iter()
            .take(limit)
            .map(|((tenant, process, id), c)| {
                json!({
                    "tenant": tenant,
                    "process": process,
                    "id": id,
                    "key": c.key,
                    "version": c.version,
                    "state": c.state,
                    "step": c.step,
                    "startedAt": c.started_at,
                    "lastAt": c.last_at,
                    "reachedAt": c.reached_at,
                    "incident": c.incident.as_ref().map(|(code, message)| json!({"code": code, "message": message})),
                })
            })
            .collect();
        Ok(json!({ "cases": cases, "total": total, "limit": limit, "read": read, "head": head }))
    }
}

/// What `/api/cases` matches on; empty fields match everything.
#[derive(Default)]
pub(crate) struct CaseFilter {
    pub process: String,
    pub version: Option<u32>,
    pub step: String,
    /// running, waiting, incident, ended, or `open` (not ended).
    pub state: String,
    /// Part of the case ID, its key or its tenant.
    pub text: String,
}

impl View {
    fn apply(&mut self, e: &Event) {
        let reference = (
            e.tenant_id.clone(),
            e.aggregate_type.clone(),
            e.aggregate_id.clone(),
        );
        let p: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
        let at = e.created_at;
        let minute = at / 60_000;
        let process = e.aggregate_type.clone();
        let text = |name: &str| p[name].as_str().unwrap_or_default().to_owned();

        if e.event_type == "ProcessStarted" {
            let version = p["version"].as_u64().unwrap_or_default() as u32;
            self.cases.insert(
                reference.clone(),
                CaseView {
                    key: text("key"),
                    version,
                    state: "running",
                    started_at: at,
                    last_at: at,
                    reached_at: at,
                    ..Default::default()
                },
            );
            self.processes
                .entry((process, version))
                .or_default()
                .entry(minute)
                .or_default()
                .started += 1;
            return;
        }
        let Some(case) = self.cases.get_mut(&reference) else {
            // Its start was before what the view read: it can not be shown.
            return;
        };
        case.last_at = at;
        let version = case.version;
        match e.event_type.as_str() {
            "StepReached" => {
                case.step = text("step");
                case.reached_at = at;
                case.incident = None;
                case.state = match p["kind"].as_str() {
                    Some("wait" | "timer") => "waiting",
                    _ => "running",
                };
                let step = case.step.clone();
                self.steps
                    .entry((process, version, step))
                    .or_default()
                    .entry(minute)
                    .or_default()
                    .entered += 1;
            }
            "StepCompleted" => {
                let step = text("step");
                let took = at - case.reached_at;
                let m = self
                    .steps
                    .entry((process, version, step))
                    .or_default()
                    .entry(minute)
                    .or_default();
                m.left += 1;
                if p["outcome"] == "timeout" {
                    m.timeouts += 1;
                }
                *m.durations.entry(bucket(took)).or_default() += 1;
            }
            "IncidentRaised" => {
                case.state = "incident";
                case.incident = Some((text("code"), text("message")));
                self.steps
                    .entry((process, version, text("step")))
                    .or_default()
                    .entry(minute)
                    .or_default()
                    .incidents += 1;
            }
            "IncidentResolved" => {
                case.incident = None;
                case.state = "running";
            }
            "ProcessEnded" => {
                let end = text("end");
                case.state = "ended";
                case.incident = None;
                case.step = end.clone();
                if end == "cancelled" {
                    // Ended by an operator: counted as reaching the end `cancelled`.
                    self.steps
                        .entry((process.clone(), version, end.clone()))
                        .or_default()
                        .entry(minute)
                        .or_default()
                        .entered += 1;
                }
                *self
                    .processes
                    .entry((process.clone(), version))
                    .or_default()
                    .entry(minute)
                    .or_default()
                    .ended
                    .entry(end)
                    .or_default() += 1;
                let ended = self.ended.entry(process).or_default();
                ended.push_back(reference);
                if ended.len() > ENDED
                    && let Some(old) = ended.pop_front()
                {
                    self.cases.remove(&old);
                }
            }
            _ => {}
        }
    }

    /// Drops the minutes older than a week.
    fn prune(&mut self, now_minute: i64) {
        let keep = now_minute - WEEK;
        for minutes in self.steps.values_mut() {
            *minutes = minutes.split_off(&keep);
        }
        self.steps.retain(|_, m| !m.is_empty());
        for minutes in self.processes.values_mut() {
            *minutes = minutes.split_off(&keep);
        }
        self.processes.retain(|_, m| !m.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_within_ten_percent() {
        let mut d = BTreeMap::new();
        for ms in [100, 200, 300, 400, 500, 600, 700, 800, 900, 10_000] {
            *d.entry(bucket(ms)).or_default() += 1;
        }
        let median = quantile(&d, 0.5).unwrap();
        assert!((450..=550).contains(&median), "{median}");
        let p95 = quantile(&d, 0.95).unwrap();
        assert!((9_000..=11_000).contains(&p95), "{p95}");
        assert_eq!(quantile(&BTreeMap::new(), 0.5), None);
    }
}
