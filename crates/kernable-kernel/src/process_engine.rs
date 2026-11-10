//! The process engine (design/processes.md): runs the cases of the registered process
//! models on the kernel that serves their tenants.
//!
//! **Cases are aggregates in the log.** A case is the aggregate `<process>` (the qualified
//! process name, `shop.fulfilment`) of the domain `process`, its ID the case's key and a
//! sequence (`o1#1`, then `o1#2` for the next case of that key), under the tenant of the event
//! that started it. Only the engine writes these streams, so it writes them itself instead of
//! through a worker: each write is a command of the kernel (signed with its key, type
//! `Advance`, its ID fixed by the case and the stream's version) committed at once with events
//! the kernel signs as their producer. `kernable verify` checks them like any other, and refuses
//! case events not signed by a kernel key. While a case runs its stream holds the fact
//! `running`, so a restarted or promoted kernel finds its running cases with
//! `Store::domain_index` and replays only those.
//!
//! **Inputs:** the log, read after the engine's durable position
//! (`Store::process_position`), starts cases and moves waiting ones; the outcomes of the
//! commands it sent ([`Kernel::complete`] hands them over), its scheduled commands as they fall
//! due (`timers.rs`), query answers and operators' resolutions come through [`Engine`]'s channel.
//! Handling an event again after a crash is harmless: each case records the positions of the
//! events that moved it, and an event the newest case of its key already saw starts no case
//! again, even when that case has ended since.
//!
//! **Exactly once:** every command, scheduled command and query of a case has an ID fixed by
//! the case and the version of the `StepReached` event of its step, so whatever runs again sends
//! nothing twice. A wait's timeout and a timer are scheduled commands of the case (`Fire`),
//! completed by the engine with the case's events when due, and rejected `cancelled` when the
//! awaited event comes first.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, PoisonError, atomic::Ordering::SeqCst},
    time::{Duration, Instant},
};

use kernable_core::{
    proto::{
        AggregateIndex, Attribute, Command, CommandResult, Event, NewEvent, QueryRequest, Rejected,
        RequestContext, command_result::Outcome, query_result,
    },
    signing,
};
use kernable_store::{Enqueued, Error as StoreError, StreamId};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::{
    Kernel, config, now_ms,
    process_model::{self, Kind, Model, Ref, Source, Step},
    sealed,
};

/// The domain of the cases' streams.
pub(crate) const DOMAIN: &str = "process";

/// The fact a running case's stream holds.
const RUNNING: &str = "running";

/// The ID prefix of the commands the engine sends to workers.
pub(crate) const COMMAND_PREFIX: &str = "proc-c-";

/// The most events a case keeps for waits it has not reached yet.
const MAX_KEPT: usize = 100;

/// The most steps one move of a case takes without stopping: a loop of decisions ends there.
const MAX_STEPS: usize = 200;

/// How long the engine waits before it tries a side effect again that failed for now (a
/// middleware away, a full queue).
const RETRY: Duration = Duration::from_secs(1);

/// How long a query step asks again while no read model can answer (one reconnecting after a
/// restart, say) before the case stops as an incident `query_failed`.
const QUERY_PATIENCE_MS: i64 = 30_000;

/// The buckets of `kernable_process_step_seconds`.
const BUCKETS: [f64; 9] = [
    0.01, 0.1, 1.0, 10.0, 60.0, 600.0, 3600.0, 86_400.0, 604_800.0,
];

/// Tenant, process, case ID.
type CaseRef = (String, String, String);

/// What reaches the engine besides the log.
pub(crate) enum Input {
    /// A command it sent completed.
    Completed(String, CommandResult),
    /// One of its scheduled commands fell due.
    Fired(Box<Command>),
    /// A query step's answer.
    Answered {
        case: CaseRef,
        instance: u64,
        answer: Result<kernable_core::proto::QueryResult, Status>,
    },
    /// A side effect that failed for now, to try again.
    Retry { case: CaseRef, instance: u64 },
    /// An operator resolves a case (`ProcessService.Resolve`); answered with the case's tenant.
    Resolve {
        process: String,
        case_id: String,
        tenant: String,
        action: String,
        by: String,
        reply: oneshot::Sender<Result<String, Status>>,
    },
    /// A tenant arrived by a move: its running cases run here from now on.
    Arrived(String),
    /// A process changed: announce it in its registry stream.
    Announce(String),
}

/// The engine's handle in the kernel.
pub(crate) struct Engine {
    tx: mpsc::UnboundedSender<Input>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Input>>>,
    stats: Mutex<Stats>,
}

impl Default for Engine {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Engine {
            tx,
            rx: Mutex::new(Some(rx)),
            stats: Mutex::default(),
        }
    }
}

impl Engine {
    pub(crate) fn send(&self, input: Input) {
        let _ = self.tx.send(input);
    }

    fn stats(&self) -> std::sync::MutexGuard<'_, Stats> {
        self.stats.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What the metrics show of the cases.
#[derive(Default)]
struct Stats {
    /// Cases held now per process and state (running, waiting, incident).
    cases: BTreeMap<(String, &'static str), u64>,
    /// Cases ended since the kernel started, per process.
    ended: BTreeMap<String, u64>,
    /// Incidents raised since the kernel started, per process.
    incidents: BTreeMap<String, u64>,
    /// Time from reaching a step until it completed, per process and step.
    steps: BTreeMap<(String, String), Histogram>,
}

#[derive(Default, Clone)]
struct Histogram {
    buckets: [u64; BUCKETS.len()],
    count: u64,
    sum: f64,
}

/// The engine's metrics in Prometheus text format.
pub(crate) fn render(kernel: &Kernel, out: &mut String) {
    use std::fmt::Write;
    let stats = kernel.engine.stats();
    let label = |s: &str| {
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    };
    let _ = writeln!(
        out,
        "# HELP kernable_process_cases Cases per process and state: running (a command sent or a query asked), waiting (for an event or a time), incident (stopped until an operator resolves it) held now; ended since the kernel started.\n# TYPE kernable_process_cases gauge"
    );
    for ((process, state), n) in &stats.cases {
        let _ = writeln!(
            out,
            "kernable_process_cases{{process=\"{}\",state=\"{state}\"}} {n}",
            label(process)
        );
    }
    for (process, n) in &stats.ended {
        let _ = writeln!(
            out,
            "kernable_process_cases{{process=\"{}\",state=\"ended\"}} {n}",
            label(process)
        );
    }
    let _ = writeln!(
        out,
        "# HELP kernable_process_incidents Incidents raised since the kernel started, per process.\n# TYPE kernable_process_incidents counter"
    );
    for (process, n) in &stats.incidents {
        let _ = writeln!(
            out,
            "kernable_process_incidents{{process=\"{}\"}} {n}",
            label(process)
        );
    }
    let _ = writeln!(
        out,
        "# HELP kernable_process_step_seconds Seconds from reaching a step until it completed, per process and step.\n# TYPE kernable_process_step_seconds histogram"
    );
    for ((process, step), h) in &stats.steps {
        let labels = format!("process=\"{}\",step=\"{}\"", label(process), label(step));
        let mut cumulative = 0;
        for (le, n) in BUCKETS.iter().zip(h.buckets) {
            cumulative += n;
            let _ = writeln!(
                out,
                "kernable_process_step_seconds_bucket{{{labels},le=\"{le}\"}} {cumulative}"
            );
        }
        let _ = writeln!(
            out,
            "kernable_process_step_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
            h.count
        );
        let _ = writeln!(
            out,
            "kernable_process_step_seconds_sum{{{labels}}} {}",
            h.sum
        );
        let _ = writeln!(
            out,
            "kernable_process_step_seconds_count{{{labels}}} {}",
            h.count
        );
    }
}

/// The request context of everything the engine sends for `process` in `case`: its commands
/// and queries. The one place that says as whom a process acts (design/processes.md, "As whom
/// a process acts"): as `ProcessService.Act` set it (the sender tenant, identity and session:
/// with the kit, the process's identity in the platform tenant and its API key), else as the
/// process's own identity `process:<name>` in the case's tenant; naming the case either way.
pub(crate) fn context(
    kernel: &Kernel,
    process: &str,
    tenant: &str,
    case_id: &str,
    causation: &str,
) -> RequestContext {
    let acting = kernel.processes.get(process).and_then(|e| e.acting);
    let (sender_tenant, sender_identity, sender_session) = match acting {
        Some(a) => (a.sender_tenant, a.sender_identity, a.sender_session),
        None => (
            tenant.to_owned(),
            format!("process:{process}"),
            String::new(),
        ),
    };
    RequestContext {
        sender_tenant,
        sender_identity,
        sender_session,
        correlation_id: format!("{process}/{case_id}"),
        causation_id: causation.to_owned(),
        attributes: vec![
            Attribute {
                key: "process".into(),
                value: process.to_owned(),
            },
            Attribute {
                key: "case".into(),
                value: case_id.to_owned(),
            },
        ],
        ..Default::default()
    }
}

/// An ID fixed by its kind (`c` command, `t` scheduled command, `w` write, `q` query), the
/// case and a number (the version of a step's `StepReached`, or a write's expected version).
fn engine_id(kind: &str, case: &CaseRef, n: u64) -> String {
    let mut h = Sha256::new();
    for part in [kind, &case.0, &case.1, &case.2] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part.as_bytes());
    }
    h.update(n.to_be_bytes());
    let digest = h.finalize();
    let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("proc-{kind}-{hex}")
}

/// Where a case stands.
#[derive(Debug, Clone)]
struct Case {
    tenant: String,
    process: String,
    id: String,
    key: String,
    model: Arc<Model>,
    version_of_model: u32,
    data: Map<String, Value>,
    /// The stream's version.
    version: u64,
    /// The step it is at, and the version of its `StepReached` (the step's instance).
    at: Option<(String, u64)>,
    /// When it reached that step (ms since epoch).
    reached_at: i64,
    /// When the timer of its step falls due (ms since epoch).
    timer_at: Option<i64>,
    incident: Option<(String, String)>,
    ended: bool,
    /// The position of the event that started it. Like `seen` and `min_position`, a position
    /// in this partition's log: a case that came with a moved tenant counts at the positions of
    /// its copied events (see [`State::replay`]).
    started_at: u64,
    /// The newest log position it was moved by or kept: older ones are handled already.
    seen: u64,
    /// The newest position the case knows of: what a query asks the read model to have seen.
    min_position: u64,
    /// Events kept for a wait it has not reached: type and position.
    kept: Vec<(String, u64)>,
}

impl Case {
    fn reference(&self) -> CaseRef {
        (self.tenant.clone(), self.process.clone(), self.id.clone())
    }

    fn step(&self) -> Option<&Step> {
        let (name, _) = self.at.as_ref()?;
        self.model.steps.iter().find(|s| s.name == *name)
    }

    fn instance(&self) -> u64 {
        self.at.as_ref().map_or(0, |(_, i)| *i)
    }

    /// For the metrics: running, waiting or incident.
    fn state(&self) -> &'static str {
        if self.incident.is_some() {
            return "incident";
        }
        match self.step().map(|s| &s.kind) {
            Some(Kind::Wait { .. } | Kind::Timer { .. }) => "waiting",
            _ => "running",
        }
    }
}

/// The events one move of a case writes, and what to do once they are written.
struct Batch {
    base: u64,
    events: Vec<(&'static str, Value)>,
    effects: Vec<Effect>,
    /// The timer to cancel once written.
    cancel: Option<String>,
}

impl Batch {
    fn new(case: &Case) -> Self {
        Batch {
            base: case.version,
            events: Vec::new(),
            effects: Vec::new(),
            cancel: None,
        }
    }

    /// The version the next event gets.
    fn next_version(&self) -> u64 {
        self.base + self.events.len() as u64 + 1
    }

    fn push(&mut self, event_type: &'static str, payload: Value) {
        self.events.push((event_type, payload));
    }
}

enum Effect {
    /// Send the command of the step reached at this instance.
    Submit,
    /// Schedule the case's command falling due at this time.
    Schedule(i64),
    /// Ask the query of the step.
    Ask,
}

/// The value of `path` in an event's payload, as a key.
fn as_key(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn key_of(r: &Ref, e: &Event, payload: &Value) -> Option<String> {
    match r {
        Ref::AggregateId => Some(e.aggregate_id.clone()),
        Ref::Data(path) => as_key(process_model::lookup(payload, path)),
        _ => None,
    }
}

/// The value of `source` for an event being handled.
fn from_event(source: &Source, e: &Event, payload: &Value) -> Option<Value> {
    match source {
        Source::Value(v) => Some(v.clone()),
        Source::Ref(Ref::AggregateId) => Some(Value::String(e.aggregate_id.clone())),
        Source::Ref(Ref::Data(path)) => process_model::lookup(payload, path).cloned(),
        Source::Ref(_) => None,
    }
}

/// The value of `source` in the case's data.
fn from_case(source: &Source, data: &Map<String, Value>) -> Option<Value> {
    match source {
        Source::Value(v) => Some(v.clone()),
        Source::Ref(Ref::Case(path)) => {
            let (first, rest) = path.split_first()?;
            process_model::lookup(data.get(first)?, rest).cloned()
        }
        Source::Ref(_) => None,
    }
}

fn map_event(m: &process_model::Mapping, e: &Event, payload: &Value) -> Map<String, Value> {
    m.iter()
        .filter_map(|(k, s)| Some((k.clone(), from_event(s, e, payload)?)))
        .collect()
}

fn map_case(m: &process_model::Mapping, data: &Map<String, Value>) -> Map<String, Value> {
    m.iter()
        .filter_map(|(k, s)| Some((k.clone(), from_case(s, data)?)))
        .collect()
}

fn payload_of(e: &Event) -> Value {
    serde_json::from_slice(&e.payload).unwrap_or(Value::Null)
}

/// Whether two JSON values are equal, numbers by value.
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// Whether the engine may run now: not handing over, fenced or shutting down.
fn active(kernel: &Kernel) -> bool {
    !sealed(kernel) && !kernel.fenced() && !kernel.draining.load(SeqCst)
}

/// Runs the engine for as long as the kernel runs: while the kernel may write, it loads the
/// running cases and handles the log and its inputs; while it may not, it waits and starts over.
pub(crate) async fn run(kernel: &Arc<Kernel>) {
    let Some(mut rx) = kernel
        .engine
        .rx
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    else {
        return std::future::pending().await;
    };
    loop {
        while !active(kernel) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mut state = match State::load(kernel) {
            Ok(state) => state,
            Err(e) => {
                tracing::error!(error = %e, "the process engine could not load its cases; trying again");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        state.resume_all().await;
        let mut epoch = kernel.maps.get().map(|m| m.epoch);
        loop {
            if !active(kernel) {
                tracing::info!("the process engine pauses: the kernel may not write");
                break;
            }
            let now_epoch = kernel.maps.get().map(|m| m.epoch);
            if now_epoch != epoch {
                epoch = now_epoch;
                state.drop_moved();
            }
            while let Ok(input) = rx.try_recv() {
                state.on_input(input).await;
            }
            let appended = kernel.appended.notified();
            tokio::pin!(appended);
            appended.as_mut().enable();
            let durable = kernel.durable.load(SeqCst);
            if state.position < durable {
                let events = match kernel.store.read_all(state.position, 256) {
                    Ok(events) => events,
                    Err(e) => {
                        tracing::error!(error = %e, "the process engine could not read the log");
                        tokio::time::sleep(RETRY).await;
                        continue;
                    }
                };
                for e in events.into_iter().filter(|e| e.global_position <= durable) {
                    let position = e.global_position;
                    state.on_event(e).await;
                    state.position = position;
                }
                state.save_position(false);
                continue;
            }
            state.save_position(false);
            tokio::select! {
                input = rx.recv() => match input {
                    Some(input) => state.on_input(input).await,
                    None => return std::future::pending().await,
                },
                () = &mut appended => {}
                () = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }
}

struct State<'a> {
    kernel: &'a Arc<Kernel>,
    /// The newest log position handled.
    position: u64,
    /// The newest position saved.
    saved: u64,
    saved_at: Instant,
    cases: HashMap<CaseRef, Case>,
    /// Running cases by tenant, process and key.
    by_key: HashMap<(String, String, String), String>,
    /// The case each command sent waits for.
    awaiting: HashMap<String, CaseRef>,
    /// Query steps asking again while no read model answers: since when (ms).
    asking_since: HashMap<CaseRef, i64>,
    models: HashMap<(String, u32), Arc<Model>>,
    /// The state each case was last counted in, for the metrics.
    counted: HashMap<CaseRef, (String, &'static str)>,
}

impl<'a> State<'a> {
    fn load(kernel: &'a Arc<Kernel>) -> Result<Self, StoreError> {
        let position = kernel.store.process_position()?;
        let mut state = State {
            kernel,
            position,
            saved: position,
            saved_at: Instant::now(),
            cases: HashMap::new(),
            by_key: HashMap::new(),
            awaiting: HashMap::new(),
            asking_since: HashMap::new(),
            models: HashMap::new(),
            counted: HashMap::new(),
        };
        kernel.engine.stats().cases.clear();
        // Writes a crash cut short between their command and their events: they never took
        // effect, and the engine writes anew under other IDs.
        for p in kernel.store.pending()? {
            if p.command.domain == DOMAIN && !crate::queue::scheduled(&p.command, p.received_at) {
                let reason = Rejected {
                    code: "abandoned".into(),
                    message: "a write of the process engine a restart cut short".into(),
                    ..Default::default()
                };
                match kernel.store.reject(&p.command.command_id, reason) {
                    Ok(()) | Err(StoreError::NotPending) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        state.load_cases(None)?;
        let follows = kernel
            .config
            .partition
            .as_ref()
            .is_some_and(|p| p.control.is_some());
        if !follows {
            for name in kernel.processes.all().into_keys() {
                state.announce(&name);
            }
        }
        tracing::info!(
            cases = state.cases.len(),
            position,
            "the process engine runs"
        );
        Ok(state)
    }

    /// Loads the running cases of the tenants this kernel serves (of `tenant` only, if given).
    fn load_cases(&mut self, tenant: Option<&str>) -> Result<(), StoreError> {
        for (stream, index) in self.kernel.store.domain_index(DOMAIN)? {
            if !index.facts.iter().any(|f| f == RUNNING)
                || tenant.is_some_and(|t| t != stream.tenant_id)
                || !self.kernel.serves(&stream.tenant_id)
            {
                continue;
            }
            let events = self.kernel.store.read_stream(
                &stream.tenant_id,
                DOMAIN,
                &stream.aggregate_type,
                &stream.aggregate_id,
            )?;
            match self.replay(&stream, &events) {
                Some(case) if !case.ended => {
                    let reference = case.reference();
                    self.by_key.insert(
                        (case.tenant.clone(), case.process.clone(), case.key.clone()),
                        case.id.clone(),
                    );
                    self.cases.insert(reference.clone(), case);
                    self.count(&reference);
                }
                Some(_) => {}
                None => {
                    tracing::error!(case = %stream.aggregate_id, process = %stream.aggregate_type, "a running case could not be read")
                }
            }
        }
        Ok(())
    }

    fn model(&mut self, process: &str, version: u32) -> Option<Arc<Model>> {
        let key = (process.to_owned(), version);
        if let Some(m) = self.models.get(&key) {
            return Some(Arc::clone(m));
        }
        let entry = self.kernel.processes.get(process)?;
        let record = entry.versions.iter().find(|v| v.version == version)?;
        let (model, _) = process_model::parse(&record.model).ok()?;
        let model = Arc::new(model);
        self.models.insert(key, Arc::clone(&model));
        Some(model)
    }

    /// A case from its events.
    fn replay(&mut self, stream: &StreamId, events: &[Event]) -> Option<Case> {
        let first = events.first()?;
        if first.event_type != "ProcessStarted" {
            return None;
        }
        let started = payload_of(first);
        let version_of_model = started["version"].as_u64()? as u32;
        let model = self.model(&stream.aggregate_type, version_of_model)?;
        let mut case = Case {
            tenant: stream.tenant_id.clone(),
            process: stream.aggregate_type.clone(),
            id: stream.aggregate_id.clone(),
            key: started["key"].as_str()?.to_owned(),
            model,
            version_of_model,
            data: Map::new(),
            version: 0,
            at: None,
            reached_at: first.created_at,
            timer_at: None,
            incident: None,
            ended: false,
            started_at: 0,
            seen: 0,
            min_position: 0,
            kept: Vec::new(),
        };
        // Keyed by the source partition too: a tenant moved more than once brings copies from
        // several partitions, whose positions may coincide.
        let mut kept_there: Vec<(String, u64)> = Vec::new();
        for e in events {
            let p = payload_of(e);
            // The positions an event names are those of the partition that wrote it. A copy a
            // moved tenant brought along names the source's: here it counts at its own position,
            // which comes after everything it names and before the tenant's first event here.
            let copy = !e.origin_partition.is_empty();
            let position = if copy {
                e.global_position
            } else {
                p["position"].as_u64().unwrap_or(0)
            };
            match e.event_type.as_str() {
                "ProcessStarted" => {
                    if let Value::Object(data) = &p["data"] {
                        case.data = data.clone();
                    }
                    case.started_at = position;
                }
                "StepReached" => {
                    case.at = Some((p["step"].as_str().unwrap_or_default().to_owned(), e.version));
                    case.reached_at = e.created_at;
                    case.timer_at = None;
                    case.incident = None;
                }
                "TimerSet" => case.timer_at = p["at"].as_i64(),
                "StepCompleted" => {
                    if let Value::Object(data) = &p["data"] {
                        case.data.extend(data.clone());
                    }
                    if copy {
                        let named = (
                            e.origin_partition.clone(),
                            p["position"].as_u64().unwrap_or(0),
                        );
                        kept_there.retain(|k| *k != named);
                    } else {
                        case.kept.retain(|(_, k)| *k != position);
                        case.min_position = case
                            .min_position
                            .max(p["committedAt"].as_u64().unwrap_or(0));
                    }
                }
                // Kept before the tenant moved: its position is the source's, not found here.
                "EventKept" if copy => kept_there.push((
                    e.origin_partition.clone(),
                    p["position"].as_u64().unwrap_or(0),
                )),
                "EventKept" => case
                    .kept
                    .push((p["event"].as_str().unwrap_or_default().to_owned(), position)),
                "IncidentRaised" => {
                    case.incident = Some((
                        p["code"].as_str().unwrap_or_default().to_owned(),
                        p["message"].as_str().unwrap_or_default().to_owned(),
                    ));
                }
                "IncidentResolved" => case.incident = None,
                "ProcessEnded" => case.ended = true,
                _ => {}
            }
            case.seen = case.seen.max(position);
            case.min_position = case.min_position.max(position);
            case.version = e.version;
        }
        if !kept_there.is_empty() && !case.ended {
            tracing::warn!(case = %case.id, process = %case.process, events = kept_there.len(), "events the case kept before its tenant moved are not kept here");
        }
        Some(case)
    }

    /// Counts `case` in the metrics as it stands now (or no longer, once ended or dropped).
    fn count(&mut self, case: &CaseRef) {
        let now = self
            .cases
            .get(case)
            .filter(|c| !c.ended)
            .map(|c| (c.process.clone(), c.state()));
        let before = self.counted.remove(case);
        if before == now {
            if let Some(now) = now {
                self.counted.insert(case.clone(), now);
            }
            return;
        }
        let mut stats = self.kernel.engine.stats();
        if let Some((process, state)) = before
            && let Some(n) = stats.cases.get_mut(&(process, state))
        {
            *n = n.saturating_sub(1);
        }
        if let Some((process, state)) = now.clone() {
            *stats.cases.entry((process, state)).or_default() += 1;
            self.counted
                .insert(case.clone(), now.expect("just matched"));
        }
    }

    fn record_step(&self, case: &Case, step: &str) {
        let seconds = (now_ms() - case.reached_at).max(0) as f64 / 1000.0;
        let mut stats = self.kernel.engine.stats();
        let h = stats
            .steps
            .entry((case.process.clone(), step.to_owned()))
            .or_default();
        h.count += 1;
        h.sum += seconds;
        if let Some(i) = BUCKETS.iter().position(|le| seconds <= *le) {
            h.buckets[i] += 1;
        }
    }

    /// Saves the position: when `now`, or when it moved and a second passed.
    fn save_position(&mut self, now: bool) {
        if self.position == self.saved || !now && self.saved_at.elapsed() < Duration::from_secs(1) {
            return;
        }
        match self.kernel.store.save_process_position(self.position) {
            Ok(()) => {
                self.saved = self.position;
                self.saved_at = Instant::now();
            }
            Err(e) => tracing::error!(error = %e, "saving the process engine's position failed"),
        }
    }

    /// Drops the cases of tenants this kernel no longer serves: they moved away.
    fn drop_moved(&mut self) {
        let gone: Vec<CaseRef> = self
            .cases
            .keys()
            .filter(|c| !self.kernel.serves(&c.0))
            .cloned()
            .collect();
        for c in gone {
            self.forget(&c);
        }
    }

    fn forget(&mut self, reference: &CaseRef) {
        if let Some(case) = self.cases.remove(reference) {
            self.by_key
                .remove(&(case.tenant.clone(), case.process.clone(), case.key.clone()));
            self.awaiting.retain(|_, c| c != reference);
        }
        self.count(reference);
    }

    // ---- the log ----

    async fn on_event(&mut self, e: Event) {
        // The engine's own events, and copies of a moved tenant's events, move nothing.
        if e.domain == DOMAIN || !e.origin_partition.is_empty() {
            return;
        }
        let event_type = config::qualified(&e.domain, &e.event_type);
        let payload = payload_of(&e);
        self.start_cases(&event_type, &e, &payload).await;
        self.cancel_cases(&event_type, &e, &payload).await;
        self.move_waiting(&event_type, &e, &payload).await;
    }

    /// Ends the running cases an event cancels (`cancelOn`), whatever step they are at: their
    /// timer is withdrawn, an outcome they await is ignored once it comes, an incident goes.
    /// Events the case's own commands caused do not count.
    async fn cancel_cases(&mut self, event_type: &str, e: &Event, payload: &Value) {
        let mut matched: Vec<(CaseRef, String)> = Vec::new();
        let models: Vec<((String, u32), Arc<Model>)> = self
            .models
            .iter()
            .map(|(k, m)| (k.clone(), Arc::clone(m)))
            .collect();
        for ((process, version), model) in models {
            for c in model.cancel_on.iter().filter(|c| c.event == event_type) {
                let Some(k) = key_of(&c.key, e, payload) else {
                    continue;
                };
                let Some(id) = self
                    .by_key
                    .get(&(e.tenant_id.clone(), process.clone(), k))
                    .cloned()
                else {
                    continue;
                };
                let reference = (e.tenant_id.clone(), process.clone(), id);
                if self
                    .cases
                    .get(&reference)
                    .is_some_and(|case| case.version_of_model == version)
                    && !matched.iter().any(|(r, _)| *r == reference)
                {
                    matched.push((reference, c.end.clone()));
                }
            }
        }
        for (reference, end) in matched {
            let Some(mut case) = self.cases.remove(&reference) else {
                continue;
            };
            // Handled already, ended, older than the case, or caused by the case's own commands
            // (its correlation): a case does not cancel itself.
            let own = e
                .context
                .as_ref()
                .is_some_and(|c| c.correlation_id == format!("{}/{}", case.process, case.id));
            if case.seen >= e.global_position
                || case.ended
                || e.global_position <= case.started_at
                || own
            {
                self.cases.insert(reference, case);
                continue;
            }
            let mut batch = Batch::new(&case);
            batch.cancel = case
                .timer_at
                .map(|_| engine_id("t", &reference, case.instance()));
            let step = case.at.as_ref().map(|(s, _)| s.clone()).unwrap_or_default();
            if case.incident.is_some() {
                tracing::info!(case = %case.id, %step, "the incident ends with the case");
            }
            self.record_step(&case, &step);
            batch.push(
                "CaseCancelled",
                json!({"step": step, "event": event_type, "position": e.global_position, "end": end}),
            );
            tracing::info!(process = %case.process, case = %case.id, %step, event = %event_type, "case cancelled");
            case.seen = e.global_position;
            case.min_position = case.min_position.max(e.global_position);
            self.enter(&mut case, &mut batch, &end);
            self.commit(case, batch, None).await;
        }
    }

    async fn start_cases(&mut self, event_type: &str, e: &Event, payload: &Value) {
        let processes: Vec<(String, crate::processes::Entry)> =
            self.kernel.processes.all().into_iter().collect();
        for (name, entry) in processes {
            let Some(newest) = entry.versions.last() else {
                continue;
            };
            if entry.removed {
                continue;
            }
            let Some(model) = self.model(&name, newest.version) else {
                continue;
            };
            if model.start.on != event_type || self.kernel.owns(&e.tenant_id).is_err() {
                continue;
            }
            let Some(key) = key_of(&model.start.key, e, payload) else {
                tracing::debug!(process = %name, position = e.global_position, "a start event without its key");
                continue;
            };
            if let Some(running) =
                self.by_key
                    .get(&(e.tenant_id.clone(), name.clone(), key.clone()))
            {
                let reference = (e.tenant_id.clone(), name.clone(), running.clone());
                if self
                    .cases
                    .get(&reference)
                    .is_some_and(|c| c.started_at != e.global_position)
                {
                    tracing::info!(process = %name, %key, case = %running, position = e.global_position, "a case of this key runs: the start event is ignored");
                }
                continue;
            }
            self.start(&name, newest.version, model, key, e, payload)
                .await;
        }
    }

    /// The next free case ID of `key` for a case the event at `position` starts; `None` if the
    /// newest case of the key ended and already saw that event: the event is handled again after
    /// a crash (the engine's position is saved now and then), and started it, or came while it
    /// ran and so started nothing.
    fn case_id(
        &mut self,
        tenant: &str,
        process: &str,
        key: &str,
        position: u64,
    ) -> Result<Option<String>, StoreError> {
        let mut newest: Option<(String, Vec<Event>)> = None;
        for n in 1.. {
            let id = format!("{key}#{n}");
            let events = self
                .kernel
                .store
                .read_stream_after(tenant, DOMAIN, process, &id, 0)?;
            if !events.is_empty() {
                newest = Some((id, events));
                continue;
            }
            if let Some((before, events)) = newest {
                let stream = StreamId {
                    tenant_id: tenant.to_owned(),
                    domain: DOMAIN.to_owned(),
                    aggregate_type: process.to_owned(),
                    aggregate_id: before,
                };
                if self
                    .replay(&stream, &events)
                    .is_some_and(|c| c.ended && c.seen >= position)
                {
                    return Ok(None);
                }
            }
            return Ok(Some(id));
        }
        unreachable!("some sequence is free")
    }

    async fn start(
        &mut self,
        process: &str,
        version: u32,
        model: Arc<Model>,
        key: String,
        e: &Event,
        payload: &Value,
    ) {
        let id = match self.case_id(&e.tenant_id, process, &key, e.global_position) {
            Ok(Some(id)) => id,
            Ok(None) => {
                tracing::debug!(%process, %key, position = e.global_position, "the event was handled before a restart: no case starts again");
                return;
            }
            Err(err) => {
                tracing::error!(error = %err, "reading the cases of a key failed");
                return;
            }
        };
        let data = map_event(&model.start.data, e, payload);
        let mut case = Case {
            tenant: e.tenant_id.clone(),
            process: process.to_owned(),
            id,
            key: key.clone(),
            model: Arc::clone(&model),
            version_of_model: version,
            data: data.clone(),
            version: 0,
            at: None,
            reached_at: now_ms(),
            timer_at: None,
            incident: None,
            ended: false,
            started_at: e.global_position,
            seen: e.global_position,
            min_position: e.global_position,
            kept: Vec::new(),
        };
        let mut batch = Batch::new(&case);
        batch.push(
            "ProcessStarted",
            json!({"version": version, "key": key, "data": data,
                   "event": config::qualified(&e.domain, &e.event_type),
                   "position": e.global_position}),
        );
        let first = model.steps[0].name.clone();
        self.enter(&mut case, &mut batch, &first);
        tracing::info!(%process, case = %case.id, tenant = %case.tenant, version, "case started");
        self.commit(case, batch, None).await;
    }

    async fn move_waiting(&mut self, event_type: &str, e: &Event, payload: &Value) {
        // The running cases this event concerns: by the key each wait for its type names.
        let mut matched: Vec<CaseRef> = Vec::new();
        let models: Vec<((String, u32), Arc<Model>)> = self
            .models
            .iter()
            .map(|(k, m)| (k.clone(), Arc::clone(m)))
            .collect();
        for ((process, version), model) in models {
            for step in &model.steps {
                let Kind::Wait { event, key, .. } = &step.kind else {
                    continue;
                };
                if event != event_type {
                    continue;
                }
                let Some(k) = key_of(key, e, payload) else {
                    continue;
                };
                let Some(id) = self
                    .by_key
                    .get(&(e.tenant_id.clone(), process.clone(), k))
                    .cloned()
                else {
                    continue;
                };
                let reference = (e.tenant_id.clone(), process.clone(), id);
                if self
                    .cases
                    .get(&reference)
                    .is_some_and(|c| c.version_of_model == version)
                    && !matched.contains(&reference)
                {
                    matched.push(reference);
                }
            }
        }
        for reference in matched {
            let Some(mut case) = self.cases.remove(&reference) else {
                continue;
            };
            if case.seen >= e.global_position || case.ended {
                self.cases.insert(reference, case);
                continue;
            }
            let mut batch = Batch::new(&case);
            let here = match case.step().map(|s| (s.name.clone(), s.kind.clone())) {
                Some((name, Kind::Wait { event, key, .. }))
                    if event == event_type
                        && case.incident.is_none()
                        && key_of(&key, e, payload).as_deref() == Some(case.key.as_str()) =>
                {
                    Some(name)
                }
                _ => None,
            };
            match here {
                Some(step) => {
                    // Its timeout goes once the event is written.
                    batch.cancel = case
                        .timer_at
                        .map(|_| engine_id("t", &reference, case.instance()));
                    self.complete_wait(&mut case, &mut batch, &step, e, payload);
                }
                None if case.kept.len() < MAX_KEPT => {
                    batch.push(
                        "EventKept",
                        json!({"event": event_type, "position": e.global_position}),
                    );
                    case.kept.push((event_type.to_owned(), e.global_position));
                }
                None => {
                    tracing::warn!(case = %case.id, "a case keeps {MAX_KEPT} events already: one more is not kept");
                    self.cases.insert(reference, case);
                    continue;
                }
            }
            case.seen = e.global_position;
            case.min_position = case.min_position.max(e.global_position);
            self.commit(case, batch, None).await;
        }
    }

    /// The case's wait `step` completes with the event `e`: its data merged, on to `next`.
    fn complete_wait(
        &mut self,
        case: &mut Case,
        batch: &mut Batch,
        step: &str,
        e: &Event,
        payload: &Value,
    ) {
        let Some(Kind::Wait { data, next, .. }) = case
            .model
            .steps
            .iter()
            .find(|s| s.name == step)
            .map(|s| s.kind.clone())
        else {
            return;
        };
        let data = map_event(&data, e, payload);
        case.data.extend(data.clone());
        self.record_step(case, step);
        batch.push(
            "StepCompleted",
            json!({"step": step, "outcome": "event", "next": next, "data": data,
                   "position": e.global_position}),
        );
        self.enter(case, batch, &next);
    }

    // ---- moving a case ----

    /// Moves `case` into `step` and on, as far as it goes without waiting, into `batch`.
    fn enter(&mut self, case: &mut Case, batch: &mut Batch, step: &str) {
        let mut step = step.to_owned();
        for _ in 0..MAX_STEPS {
            let Some(kind) = case
                .model
                .steps
                .iter()
                .find(|s| s.name == step)
                .map(|s| s.kind.clone())
            else {
                return;
            };
            let instance = batch.next_version();
            let kind_name = kind_name(&kind);
            batch.push("StepReached", json!({"step": step, "kind": kind_name}));
            case.at = Some((step.clone(), instance));
            case.reached_at = now_ms();
            case.timer_at = None;
            case.incident = None;
            let reference = case.reference();
            match kind {
                Kind::End => {
                    batch.push("ProcessEnded", json!({"end": step}));
                    case.ended = true;
                    return;
                }
                Kind::Decide { when, otherwise } => {
                    let data = Value::Object(case.data.clone());
                    let (outcome, next) = when
                        .iter()
                        .enumerate()
                        .find(|(_, (expr, _))| expr.holds(&data))
                        .map(|(i, (_, next))| (format!("when {}", i + 1), next.clone()))
                        .unwrap_or(("otherwise".into(), otherwise));
                    self.record_step(case, &step);
                    batch.push(
                        "StepCompleted",
                        json!({"step": step, "outcome": outcome, "next": next}),
                    );
                    step = next;
                }
                Kind::Command { command, .. } => {
                    let id = engine_id("c", &reference, instance);
                    batch.push(
                        "CommandSent",
                        json!({"step": step, "commandId": id, "command": command}),
                    );
                    batch.effects.push(Effect::Submit);
                    return;
                }
                Kind::Wait {
                    event,
                    key,
                    timeout_ms,
                    ..
                } => {
                    // An event kept for this wait counts at once.
                    if let Some((e, payload)) = self.kept_for(case, &event, &key) {
                        case.kept.retain(|(_, p)| *p != e.global_position);
                        // Goes on from here.
                        self.complete_wait(case, batch, &step, &e, &payload);
                        return;
                    }
                    if let Some(ms) = timeout_ms {
                        let at = match self.due(Some(ms), None) {
                            Ok(at) => at,
                            Err(why) => {
                                self.raise(case, batch, &step, "no_time", &why);
                                return;
                            }
                        };
                        case.timer_at = Some(at);
                        batch.push(
                            "TimerSet",
                            json!({"step": step, "at": at, "commandId": engine_id("t", &reference, instance)}),
                        );
                        batch.effects.push(Effect::Schedule(at));
                    }
                    return;
                }
                Kind::Timer { after_ms, at, .. } => {
                    let due = match (after_ms, at) {
                        (Some(ms), _) => self.due(Some(ms), None),
                        (None, Some(r)) => {
                            match from_case(&Source::Ref(r), &case.data).and_then(|v| v.as_i64()) {
                                Some(at) => self.due(None, Some(at)),
                                None => Err("the timer's time is not a number of ms in the \
                                             case's data"
                                    .into()),
                            }
                        }
                        (None, None) => Err("the timer has no time".into()),
                    };
                    let due = match due {
                        Ok(due) => due,
                        Err(why) => {
                            self.raise(case, batch, &step, "no_time", &why);
                            return;
                        }
                    };
                    case.timer_at = Some(due);
                    batch.push(
                        "TimerSet",
                        json!({"step": step, "at": due, "commandId": engine_id("t", &reference, instance)}),
                    );
                    batch.effects.push(Effect::Schedule(due));
                    return;
                }
                Kind::Query { .. } => {
                    batch.effects.push(Effect::Ask);
                    return;
                }
            }
        }
        // The step the case stands at, not the next one it was about to reach.
        let at = case.at.as_ref().map_or(step, |(s, _)| s.clone());
        self.raise(
            case,
            batch,
            &at,
            "loop",
            &format!("{MAX_STEPS} steps in one go: the decisions loop"),
        );
    }

    /// When a timer falls due: `after` ms from now, or `at` (ms since epoch). Refused like a
    /// scheduled command of a client: before the epoch, or beyond `--schedule-max-days`.
    fn due(&self, after: Option<u64>, at: Option<i64>) -> Result<i64, String> {
        let now = now_ms();
        let at = match (after, at) {
            (Some(ms), _) => i64::try_from(ms).ok().and_then(|ms| now.checked_add(ms)),
            (None, at) => at,
        }
        .filter(|at| *at >= 0)
        .ok_or("the timer's time is out of range")?;
        let days = self.kernel.config().limits.schedule_max_days;
        let limit = i64::try_from(days)
            .unwrap_or(i64::MAX)
            .saturating_mul(crate::timers::DAY_MS);
        if at > now.saturating_add(limit) {
            return Err(format!(
                "the timer is more than {days} days ahead (--schedule-max-days)"
            ));
        }
        Ok(at)
    }

    /// A kept event of `event_type` whose key, by `key`, is the case's; the oldest.
    fn kept_for(&self, case: &Case, event_type: &str, key: &Ref) -> Option<(Event, Value)> {
        for (t, position) in &case.kept {
            if t != event_type {
                continue;
            }
            // One stale or unreadable entry must not hide the ones after it.
            let Some(e) = self
                .kernel
                .store
                .read_all(position.saturating_sub(1), 1)
                .ok()
                .and_then(|events| events.into_iter().next())
                .filter(|e| e.global_position == *position && e.tenant_id == case.tenant)
            else {
                continue;
            };
            let payload = payload_of(&e);
            if key_of(key, &e, &payload).as_deref() == Some(case.key.as_str()) {
                return Some((e, payload));
            }
        }
        None
    }

    fn raise(&mut self, case: &mut Case, batch: &mut Batch, step: &str, code: &str, message: &str) {
        tracing::warn!(process = %case.process, case = %case.id, %step, %code, %message, "incident");
        *self
            .kernel
            .engine
            .stats()
            .incidents
            .entry(case.process.clone())
            .or_default() += 1;
        batch.push(
            "IncidentRaised",
            json!({"step": step, "code": code, "message": message}),
        );
        case.incident = Some((code.to_owned(), message.to_owned()));
    }

    /// Writes `batch` to the case's stream (under the scheduled command `under`, or a write of
    /// its own), then does what it says.
    async fn commit(&mut self, mut case: Case, batch: Batch, under: Option<Command>) {
        let reference = case.reference();
        if batch.events.is_empty() {
            self.cases.insert(reference.clone(), case);
            return;
        }
        if let Err(e) = self.write(&case, &batch, under) {
            tracing::error!(case = %case.id, process = %case.process, error = %e, "writing a case failed; reading it again");
            self.reload(&reference);
            return;
        }
        case.version += batch.events.len() as u64;
        let key = (case.tenant.clone(), case.process.clone(), case.key.clone());
        if case.ended {
            tracing::info!(process = %case.process, case = %case.id, "case ended");
            self.by_key.remove(&key);
            self.cases.remove(&reference);
            *self
                .kernel
                .engine
                .stats()
                .ended
                .entry(case.process.clone())
                .or_default() += 1;
            self.count(&reference);
            if let Some(id) = batch.cancel {
                crate::timers::withdraw(self.kernel, &id);
            }
            return;
        }
        self.by_key.insert(key, case.id.clone());
        let instance = case.instance();
        self.cases.insert(reference.clone(), case);
        self.count(&reference);
        if let Some(id) = batch.cancel {
            crate::timers::withdraw(self.kernel, &id);
        }
        for effect in batch.effects {
            // Boxed: an effect may complete at once and move the case on (a commit again).
            Box::pin(self.effect(&reference, instance, effect)).await;
        }
    }

    fn write(&self, case: &Case, batch: &Batch, under: Option<Command>) -> Result<(), StoreError> {
        let index = if case.ended {
            AggregateIndex::default()
        } else {
            AggregateIndex {
                facts: vec![RUNNING.into()],
                ..Default::default()
            }
        };
        let stream = StreamId {
            tenant_id: case.tenant.clone(),
            domain: DOMAIN.into(),
            aggregate_type: case.process.clone(),
            aggregate_id: case.id.clone(),
        };
        let context = context(self.kernel, &case.process, &case.tenant, &case.id, "");
        append(
            self.kernel,
            &stream,
            batch.base,
            &batch.events,
            Some(&index),
            under,
            context,
        )
    }

    /// Announces the process `name`'s change in the registry stream, if it lacks it: its first
    /// or a new version, a re-activation (`ProcessRegistered`) or a removal (`ProcessRemoved`).
    /// Compared with what the stream says last, so each change is written once, also when this
    /// runs again after a restart or on a promoted standby.
    fn announce(&mut self, name: &str) {
        let Some(entry) = self.kernel.processes.get(name) else {
            return;
        };
        let Some(newest) = entry.versions.last() else {
            return;
        };
        let stream = registry_stream(name);
        let events = match self.kernel.store.read_stream(
            &stream.tenant_id,
            DOMAIN,
            &stream.aggregate_type,
            &stream.aggregate_id,
        ) {
            Ok(events) => events,
            Err(e) => {
                tracing::error!(process = %name, error = %e, "reading the process registry failed");
                return;
            }
        };
        let said = events.last().map(|e| {
            let p = payload_of(e);
            (e.event_type.clone(), p["version"].as_u64().unwrap_or(0))
        });
        let wanted = if entry.removed {
            ("ProcessRemoved", json!({"name": name}))
        } else {
            (
                "ProcessRegistered",
                json!({"name": name, "version": newest.version}),
            )
        };
        let same = match &said {
            Some((t, _)) if entry.removed => t == "ProcessRemoved",
            Some((t, v)) => t == "ProcessRegistered" && *v == u64::from(newest.version),
            None => false,
        };
        if same {
            return;
        }
        let context = RequestContext {
            sender_identity: "kernel".into(),
            correlation_id: name.to_owned(),
            ..Default::default()
        };
        match append(
            self.kernel,
            &stream,
            events.len() as u64,
            &[wanted],
            None,
            None,
            context,
        ) {
            Ok(()) => tracing::info!(process = %name, "announced in the process registry"),
            Err(e) => {
                tracing::error!(process = %name, error = %e, "writing the process registry failed")
            }
        }
    }

    /// Reads a case again from the store, after a write that failed.
    fn reload(&mut self, reference: &CaseRef) {
        self.forget(reference);
        let stream = StreamId {
            tenant_id: reference.0.clone(),
            domain: DOMAIN.into(),
            aggregate_type: reference.1.clone(),
            aggregate_id: reference.2.clone(),
        };
        let events = self.kernel.store.read_stream(
            &stream.tenant_id,
            DOMAIN,
            &stream.aggregate_type,
            &stream.aggregate_id,
        );
        if let Ok(events) = events
            && let Some(case) = self.replay(&stream, &events)
            && !case.ended
        {
            self.by_key.insert(
                (case.tenant.clone(), case.process.clone(), case.key.clone()),
                case.id.clone(),
            );
            self.cases.insert(reference.clone(), case);
            self.count(reference);
        }
    }

    // ---- side effects ----

    async fn effect(&mut self, reference: &CaseRef, instance: u64, effect: Effect) {
        match effect {
            Effect::Submit => self.submit(reference, instance).await,
            Effect::Schedule(at) => self.schedule(reference, instance, at).await,
            Effect::Ask => self.ask(reference, instance),
        }
    }

    /// The command of the case's command step, as the process sends it.
    fn command_of(&self, case: &Case) -> Result<Command, (&'static str, String)> {
        let Some(Step {
            kind:
                Kind::Command {
                    command,
                    aggregate,
                    data,
                    ..
                },
            ..
        }) = case.step()
        else {
            return Err(("no_step", "the case is not at a command step".into()));
        };
        let parts: Vec<&str> = command.splitn(3, '.').collect();
        let aggregate_id = match from_case(aggregate, &case.data) {
            Some(Value::String(s)) if !s.is_empty() => s,
            Some(Value::Number(n)) => n.to_string(),
            _ => {
                return Err((
                    "no_aggregate",
                    "the command's aggregate is not in the case's data".into(),
                ));
            }
        };
        let payload = Value::Object(map_case(data, &case.data));
        let reference = case.reference();
        let mut cmd = Command {
            command_id: engine_id("c", &reference, case.instance()),
            tenant_id: case.tenant.clone(),
            domain: parts[0].into(),
            aggregate_type: parts[1].into(),
            aggregate_id,
            command_type: parts[2].into(),
            payload: serde_json::to_vec(&payload).expect("JSON serializes"),
            context: Some(context(
                self.kernel,
                &case.process,
                &case.tenant,
                &case.id,
                &format!("{}#{}", case.id, case.instance()),
            )),
            ..Default::default()
        };
        cmd.client_sig = Some(signing::sign(
            &self.kernel.key(),
            &signing::command_message(&cmd),
        ));
        Ok(cmd)
    }

    async fn submit(&mut self, reference: &CaseRef, instance: u64) {
        let Some(case) = self
            .cases
            .get(reference)
            .filter(|c| c.instance() == instance)
        else {
            return;
        };
        let cmd = match self.command_of(case) {
            Ok(cmd) => cmd,
            Err((code, message)) => {
                let mut case = self.cases.remove(reference).expect("just found");
                let mut batch = Batch::new(&case);
                let step = case.at.clone().map(|(s, _)| s).unwrap_or_default();
                self.raise(&mut case, &mut batch, &step, code, &message);
                self.commit(case, batch, None).await;
                return;
            }
        };
        let id = cmd.command_id.clone();
        match submit(self.kernel, cmd, now_ms()).await {
            Ok(Some(result)) => self.completed(&id, result).await,
            Ok(None) => {
                self.awaiting.insert(id, reference.clone());
            }
            Err(status) => {
                tracing::warn!(case = %reference.2, error = %status.message(), "sending a case's command failed for now; trying again");
                self.retry_later(reference, instance);
            }
        }
    }

    fn retry_later(&self, reference: &CaseRef, instance: u64) {
        let engine = self.kernel.engine.tx.clone();
        let case = reference.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RETRY).await;
            let _ = engine.send(Input::Retry { case, instance });
        });
    }

    async fn schedule(&mut self, reference: &CaseRef, instance: u64, at: i64) {
        let Some(case) = self.cases.get(reference) else {
            return;
        };
        let mut cmd = Command {
            command_id: engine_id("t", reference, instance),
            tenant_id: case.tenant.clone(),
            domain: DOMAIN.into(),
            aggregate_type: case.process.clone(),
            aggregate_id: case.id.clone(),
            command_type: "Fire".into(),
            payload: serde_json::to_vec(&json!({"instance": instance})).expect("JSON"),
            context: Some(context(
                self.kernel,
                &case.process,
                &case.tenant,
                &case.id,
                "",
            )),
            deliver_at: at,
            ..Default::default()
        };
        cmd.client_sig = Some(signing::sign(
            &self.kernel.key(),
            &signing::command_message(&cmd),
        ));
        // Received just before it is due, so the queue keeps it for its time even if that is
        // now (`queue::scheduled`).
        let received_at = now_ms().min(at - 1);
        if let Err(status) = submit(self.kernel, cmd, received_at).await {
            tracing::warn!(case = %reference.2, error = %status.message(), "scheduling a case's timer failed for now; trying again");
            self.retry_later(reference, instance);
        }
    }

    fn ask(&self, reference: &CaseRef, instance: u64) {
        let Some(case) = self.cases.get(reference) else {
            return;
        };
        let Some(Step {
            kind: Kind::Query { query, data, .. },
            ..
        }) = case.step()
        else {
            return;
        };
        let (domain, query_type) = query.split_once('.').expect("checked when registered");
        let mut request = QueryRequest {
            query_id: engine_id("q", reference, instance),
            tenant_id: case.tenant.clone(),
            domain: domain.into(),
            query_type: query_type.into(),
            payload: serde_json::to_vec(&Value::Object(map_case(data, &case.data)))
                .expect("JSON serializes"),
            context: Some(context(
                self.kernel,
                &case.process,
                &case.tenant,
                &case.id,
                &format!("{}#{instance}", case.id),
            )),
            min_position: case.min_position,
            issued_at: now_ms(),
            protocol: kernable_core::PROTOCOL,
            ..Default::default()
        };
        request.client_sig = Some(signing::sign(
            &self.kernel.key(),
            &signing::query_message(&request),
        ));
        let name = query.clone();
        let kernel = Arc::clone(self.kernel);
        let case = reference.clone();
        tokio::spawn(async move {
            let answer = crate::answer_query(&kernel, &name, request, None).await;
            kernel.engine.send(Input::Answered {
                case,
                instance,
                answer,
            });
        });
    }

    /// After loading: does again what each case's step waits on, in case it was not done.
    async fn resume_all(&mut self) {
        let cases: Vec<(CaseRef, u64)> = self
            .cases
            .iter()
            .map(|(r, c)| (r.clone(), c.instance()))
            .collect();
        for (reference, instance) in cases {
            self.resume(&reference, instance).await;
        }
    }

    async fn resume(&mut self, reference: &CaseRef, instance: u64) {
        let Some(case) = self
            .cases
            .get(reference)
            .filter(|c| c.instance() == instance)
        else {
            return;
        };
        if case.incident.is_some() {
            return;
        }
        let kind = case.step().map(|s| s.kind.clone());
        let timer_at = case.timer_at;
        match kind {
            Some(Kind::Command { .. }) => {
                let id = engine_id("c", reference, instance);
                match self.kernel.store.result(&id) {
                    Ok(Some(result)) => self.completed(&id, result).await,
                    _ => self.submit(reference, instance).await,
                }
            }
            Some(Kind::Wait { .. } | Kind::Timer { .. }) => {
                if let Some(at) = timer_at {
                    let id = engine_id("t", reference, instance);
                    if matches!(self.kernel.store.result(&id), Ok(None)) {
                        self.schedule(reference, instance, at).await;
                    }
                }
            }
            Some(Kind::Query { .. }) => self.ask(reference, instance),
            _ => {}
        }
    }

    // ---- inputs ----

    async fn on_input(&mut self, input: Input) {
        match input {
            Input::Completed(id, result) => self.completed(&id, result).await,
            Input::Fired(cmd) => self.fired(*cmd).await,
            Input::Answered {
                case,
                instance,
                answer,
            } => self.answered(&case, instance, answer).await,
            Input::Retry { case, instance } => self.resume(&case, instance).await,
            Input::Resolve {
                process,
                case_id,
                tenant,
                action,
                by,
                reply,
            } => {
                let result = self
                    .resolve(&process, &case_id, &tenant, &action, &by)
                    .await;
                let _ = reply.send(result);
            }
            Input::Announce(name) => self.announce(&name),
            Input::Arrived(tenant) => {
                if let Err(e) = self.load_cases(Some(&tenant)) {
                    tracing::error!(%tenant, error = %e, "loading an arrived tenant's cases failed");
                }
                let arrived: Vec<(CaseRef, u64)> = self
                    .cases
                    .iter()
                    .filter(|(r, _)| r.0 == tenant)
                    .map(|(r, c)| (r.clone(), c.instance()))
                    .collect();
                for (reference, instance) in arrived {
                    self.resume(&reference, instance).await;
                }
            }
        }
    }

    /// A command the engine sent completed.
    async fn completed(&mut self, id: &str, result: CommandResult) {
        let reference = match self.awaiting.remove(id) {
            Some(r) => r,
            None => match self
                .cases
                .iter()
                .find(|(r, c)| engine_id("c", r, c.instance()) == id)
            {
                Some((r, _)) => r.clone(),
                None => return,
            },
        };
        let Some(mut case) = self.cases.remove(&reference) else {
            return;
        };
        let Some(Step {
            name,
            kind: Kind::Command { rejected, next, .. },
        }) = case.step().cloned()
        else {
            self.cases.insert(reference, case);
            return;
        };
        if engine_id("c", &reference, case.instance()) != id || case.incident.is_some() {
            self.cases.insert(reference, case);
            return;
        }
        let mut batch = Batch::new(&case);
        match result.outcome {
            Some(Outcome::Committed(c)) => {
                let at = c.events.last().map_or(0, |e| e.global_position);
                case.min_position = case.min_position.max(at);
                self.record_step(&case, &name);
                batch.push(
                    "StepCompleted",
                    json!({"step": name, "outcome": "committed", "next": next, "committedAt": at}),
                );
                self.enter(&mut case, &mut batch, &next);
            }
            Some(Outcome::Rejected(r)) => match rejected.iter().find(|(code, _)| *code == r.code) {
                Some((code, target)) => {
                    self.record_step(&case, &name);
                    batch.push(
                        "StepCompleted",
                        json!({"step": name, "outcome": "rejected", "code": code, "next": target}),
                    );
                    let target = target.clone();
                    self.enter(&mut case, &mut batch, &target);
                }
                None => self.raise(&mut case, &mut batch, &name, &r.code, &r.message),
            },
            _ => {
                self.cases.insert(reference, case);
                return;
            }
        }
        self.commit(case, batch, None).await;
    }

    /// A scheduled command of a case fell due.
    async fn fired(&mut self, cmd: Command) {
        let reference = (
            cmd.tenant_id.clone(),
            cmd.aggregate_type.clone(),
            cmd.aggregate_id.clone(),
        );
        let instance = serde_json::from_slice::<Value>(&cmd.payload)
            .ok()
            .and_then(|p| p["instance"].as_u64())
            .unwrap_or(0);
        let current = self.cases.get(&reference).and_then(|c| {
            (c.instance() == instance && c.timer_at.is_some() && c.incident.is_none())
                .then(|| c.step().cloned())
                .flatten()
        });
        let Some(step) = current else {
            // Its step is over: the event came first, or an operator moved the case on.
            stale(self.kernel, &cmd);
            return;
        };
        let mut case = self.cases.remove(&reference).expect("just found");
        let mut batch = Batch::new(&case);
        batch.push("TimerFired", json!({"step": step.name}));
        let (outcome, next) = match &step.kind {
            Kind::Wait { on_timeout, .. } => ("timeout", on_timeout.clone().unwrap_or_default()),
            Kind::Timer { next, .. } => ("fired", next.clone()),
            _ => ("fired", String::new()),
        };
        self.record_step(&case, &step.name);
        batch.push(
            "StepCompleted",
            json!({"step": step.name, "outcome": outcome, "next": next}),
        );
        case.timer_at = None;
        self.enter(&mut case, &mut batch, &next);
        self.commit(case, batch, Some(cmd)).await;
    }

    async fn answered(
        &mut self,
        reference: &CaseRef,
        instance: u64,
        answer: Result<kernable_core::proto::QueryResult, Status>,
    ) {
        let Some(mut case) = self.cases.remove(reference) else {
            return;
        };
        let step = case.step().cloned();
        let Some(Step {
            name,
            kind:
                Kind::Query {
                    answer: path,
                    paths,
                    into,
                    otherwise,
                    ..
                },
        }) = step.filter(|_| case.instance() == instance && case.incident.is_none())
        else {
            self.cases.insert(reference.clone(), case);
            return;
        };
        let mut batch = Batch::new(&case);
        let response = match answer.map(|a| a.outcome) {
            Ok(Some(query_result::Outcome::Response(bytes))) => {
                serde_json::from_slice::<Value>(&bytes)
                    .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
            }
            Ok(Some(query_result::Outcome::Rejected(r))) => {
                self.raise(&mut case, &mut batch, &name, &r.code, &r.message);
                self.commit(case, batch, None).await;
                return;
            }
            Ok(None) => Value::Null,
            Err(status) => {
                // Nobody to answer right now (UNAVAILABLE: no read model connected, or one
                // reconnecting): asked again every second for a while, like a command whose
                // middleware is away; then, or for any other failure, an incident.
                let since = *self
                    .asking_since
                    .entry(reference.clone())
                    .or_insert_with(now_ms);
                if status.code() == tonic::Code::Unavailable && now_ms() - since < QUERY_PATIENCE_MS
                {
                    tracing::warn!(case = %reference.2, error = %status.message(), "a case's query found no answer for now; asking again");
                    self.cases.insert(reference.clone(), case);
                    self.retry_later(reference, instance);
                    return;
                }
                self.asking_since.remove(reference);
                let message = if status.code() == tonic::Code::Unavailable {
                    format!("{} (for {} s)", status.message(), QUERY_PATIENCE_MS / 1000)
                } else {
                    status.message().to_owned()
                };
                self.raise(&mut case, &mut batch, &name, "query_failed", &message);
                self.commit(case, batch, None).await;
                return;
            }
        };
        self.asking_since.remove(reference);
        let matched = process_model::lookup(&response, &path)
            .cloned()
            .unwrap_or(Value::Null);
        let next = paths
            .iter()
            .find(|(v, _)| same_value(v, &matched))
            .map(|(_, n)| n.clone())
            .or(otherwise);
        let Some(next) = next else {
            self.raise(
                &mut case,
                &mut batch,
                &name,
                "no_path",
                &format!("no path for the answer {matched}"),
            );
            self.commit(case, batch, None).await;
            return;
        };
        let mut data = Map::new();
        if let Some(field) = into {
            data.insert(field, response);
        }
        case.data.extend(data.clone());
        self.record_step(&case, &name);
        batch.push(
            "StepCompleted",
            json!({"step": name, "outcome": "answer", "value": matched, "next": next, "data": data}),
        );
        self.enter(&mut case, &mut batch, &next);
        self.commit(case, batch, None).await;
    }

    async fn resolve(
        &mut self,
        process: &str,
        case_id: &str,
        tenant: &str,
        action: &str,
        by: &str,
    ) -> Result<String, Status> {
        let found: Vec<CaseRef> = self
            .cases
            .keys()
            .filter(|(t, p, c)| p == process && c == case_id && (tenant.is_empty() || t == tenant))
            .cloned()
            .collect();
        let reference = match found.as_slice() {
            [one] => one.clone(),
            [] => {
                return Err(Status::not_found(format!(
                    "no running case {case_id} of {process}"
                )));
            }
            many => {
                let tenants: Vec<&str> = many.iter().map(|r| r.0.as_str()).collect();
                return Err(Status::failed_precondition(format!(
                    "case {case_id} of {process} runs in the tenants {}: name the tenant",
                    tenants.join(", ")
                )));
            }
        };
        let mut case = self.cases.remove(&reference).expect("just found");
        let step = case.step().cloned();
        let step_name = step.as_ref().map(|s| s.name.clone()).unwrap_or_default();
        let incident = case.incident.is_some();
        let refuse = |case: Case, this: &mut Self, status: Status| {
            this.cases.insert(reference.clone(), case);
            Err(status)
        };
        let mut batch = Batch::new(&case);
        match action {
            "retry" | "skip" if !incident => {
                return refuse(
                    case,
                    self,
                    Status::failed_precondition(format!(
                        "case {case_id} has no incident: nothing to {action}"
                    )),
                );
            }
            "retry" => {
                batch.push(
                    "IncidentResolved",
                    json!({"step": step_name, "action": "retry", "by": by}),
                );
                case.incident = None;
                self.enter(&mut case, &mut batch, &step_name);
            }
            "skip" => {
                let next = match step.as_ref().map(|s| &s.kind) {
                    Some(
                        Kind::Command { next, .. }
                        | Kind::Wait { next, .. }
                        | Kind::Timer { next, .. },
                    ) => next.clone(),
                    _ => {
                        return refuse(
                            case,
                            self,
                            Status::failed_precondition(format!(
                                "step {step_name} has no single next step to skip to; retry \
                                 or cancel the case"
                            )),
                        );
                    }
                };
                batch.push(
                    "IncidentResolved",
                    json!({"step": step_name, "action": "skip", "by": by}),
                );
                batch.push(
                    "StepCompleted",
                    json!({"step": step_name, "outcome": "skipped", "next": next}),
                );
                case.incident = None;
                self.enter(&mut case, &mut batch, &next);
            }
            "cancel" => {
                if incident {
                    batch.push(
                        "IncidentResolved",
                        json!({"step": step_name, "action": "cancel", "by": by}),
                    );
                }
                batch.push("ProcessEnded", json!({"end": "cancelled", "by": by}));
                if case.timer_at.is_some() {
                    batch.cancel = Some(engine_id("t", &reference, case.instance()));
                }
                case.ended = true;
                case.incident = None;
            }
            other => {
                return refuse(
                    case,
                    self,
                    Status::invalid_argument(format!("{other} is none of retry, skip, cancel")),
                );
            }
        }
        tracing::info!(%process, case = %case_id, %action, %by, "case resolved by an operator");
        let tenant = case.tenant.clone();
        self.commit(case, batch, None).await;
        Ok(tenant)
    }
}

fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::Command { .. } => "command",
        Kind::Wait { .. } => "wait",
        Kind::Timer { .. } => "timer",
        Kind::Decide { .. } => "decide",
        Kind::Query { .. } => "query",
        Kind::End => "end",
    }
}

/// The registry stream of process `name` (design/processes.md, "With the kit"): the aggregate
/// `registry` of the domain `process`, its ID the process's name, under the empty tenant: a
/// process belongs to no tenant (its cases do), as a global unique key has none. A listener
/// granted `listen` for `process.ProcessRegistered` gets it like any event; it is written on
/// the control partition only.
fn registry_stream(name: &str) -> StreamId {
    StreamId {
        tenant_id: String::new(),
        domain: DOMAIN.into(),
        aggregate_type: "registry".into(),
        aggregate_id: name.to_owned(),
    }
}

/// Appends `events` to `stream` at `expected` (its version now), written and signed by the
/// kernel: under the pending command `under` (a case's timer), or a write command of its own.
/// `index`, if set, replaces what the stream holds.
fn append(
    kernel: &Kernel,
    stream: &StreamId,
    expected: u64,
    events: &[(&'static str, Value)],
    index: Option<&AggregateIndex>,
    under: Option<Command>,
    context: RequestContext,
) -> Result<(), StoreError> {
    let key = kernel.key();
    let cmd = match under {
        Some(cmd) => cmd,
        None => {
            // Not fixed: a write that failed half way is abandoned (see `State::load`), and the
            // expected version keeps a stream from being written twice.
            let mut nonce = [0u8; 8];
            getrandom::fill(&mut nonce).expect("the OS gives random bytes");
            let nonce: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
            let reference = (
                stream.tenant_id.clone(),
                stream.aggregate_type.clone(),
                stream.aggregate_id.clone(),
            );
            let mut cmd = Command {
                command_id: format!("{}-{nonce}", engine_id("w", &reference, expected)),
                tenant_id: stream.tenant_id.clone(),
                domain: DOMAIN.into(),
                aggregate_type: stream.aggregate_type.clone(),
                aggregate_id: stream.aggregate_id.clone(),
                command_type: "Advance".into(),
                payload: b"{}".to_vec(),
                context: Some(context),
                ..Default::default()
            };
            cmd.client_sig = Some(signing::sign(&key, &signing::command_message(&cmd)));
            kernel.store.enqueue(&cmd, now_ms())?;
            cmd
        }
    };
    let new_events: Vec<NewEvent> = events
        .iter()
        .map(|(t, payload)| NewEvent {
            event_type: (*t).into(),
            payload: serde_json::to_vec(payload).expect("JSON serializes"),
            schema: 1,
            ..Default::default()
        })
        .collect();
    let sig = signing::sign(&key, &signing::result_message(&cmd, expected, &new_events));
    let stored = {
        let mut schemas = kernel.schemas();
        let stored = kernel.store.commit(
            &cmd.command_id,
            expected,
            &new_events,
            &sig,
            index,
            now_ms(),
        )?;
        for (t, _) in events {
            schemas.entry(config::qualified(DOMAIN, t)).or_insert(1);
        }
        stored
    };
    kernel.sync(stored.last().map_or(0, |e| e.global_position));
    kernel.queue.forget(&cmd.command_id);
    kernel.appended.notify_waiters();
    Ok(())
}

/// Completes a scheduled command of a case whose step is over as rejected `stale`.
fn stale(kernel: &Arc<Kernel>, cmd: &Command) {
    let reason = Rejected {
        code: "stale".into(),
        message: "the case's step was over when it fell due".into(),
        ..Default::default()
    };
    match kernel.store.reject(&cmd.command_id, reason) {
        Ok(()) | Err(StoreError::NotPending) => {
            kernel.sync(0);
            kernel.queue.forget(&cmd.command_id);
        }
        Err(e) => {
            tracing::error!(error = %e, "completing a stale timer failed");
            kernel.queue.defer(&cmd.command_id, now_ms() + 1_000);
        }
    }
}

/// Whether the stored `held` is the command `cmd` (but for its signature): not a client's
/// command that took the engine's ID first.
fn same_command(held: &Command, cmd: &Command) -> bool {
    held.tenant_id == cmd.tenant_id
        && held.domain == cmd.domain
        && held.aggregate_type == cmd.aggregate_type
        && held.aggregate_id == cmd.aggregate_id
        && held.command_type == cmd.command_type
        && held.payload == cmd.payload
}

/// Submits a command of the engine, authorized when the process was registered: its worker
/// must be known and the middleware passes it as any other (not the engine's own scheduled
/// commands). `Ok(Some(result))` if it is completed already (rejected by a middleware, or sent
/// before); `Ok(None)` once it is queued; an error for a refusal that may pass later.
async fn submit(
    kernel: &Arc<Kernel>,
    cmd: Command,
    received_at: i64,
) -> Result<Option<CommandResult>, Status> {
    let rejected = |code: &str, message: String| CommandResult {
        command_id: cmd.command_id.clone(),
        outcome: Some(Outcome::Rejected(Rejected {
            code: code.into(),
            message,
            ..Default::default()
        })),
    };
    if cmd.domain != DOMAIN {
        let name = config::qualified(&cmd.domain, &cmd.command_type);
        let aggregate = config::qualified(&cmd.domain, &cmd.aggregate_type);
        if !kernel.workers.runs(kernel, &aggregate, &name) {
            return Ok(Some(rejected(
                "no_worker",
                format!("no worker registered {name} for {aggregate}"),
            )));
        }
        let stored = kernel
            .store
            .command(&cmd.command_id)
            .map_err(crate::internal)?;
        if stored.is_none() {
            kernel.queue_room(&cmd)?;
            if let Some(result) = kernel.check_before(&name, &cmd, &[]).await? {
                return Ok(Some(result));
            }
        }
    }
    let taken = || {
        rejected(
            "command_id_taken",
            "another command holds the ID the process sends this one under".into(),
        )
    };
    match kernel
        .store
        .enqueue(&cmd, received_at)
        .map_err(crate::internal)?
    {
        Enqueued::Duplicate { result } => {
            let Some(held) = kernel
                .store
                .command(&cmd.command_id)
                .map_err(crate::internal)?
            else {
                return Ok(result);
            };
            if !same_command(&held.command, &cmd) {
                return Ok(Some(taken()));
            }
            if result.is_some() {
                return Ok(result);
            }
            kernel.queue.push(held.command, held.received_at);
        }
        Enqueued::New => {
            kernel.queue.push(cmd, received_at);
        }
    }
    kernel.work.notify_waiters();
    kernel.timers.notify_one();
    Ok(None)
}

/// The case IDs and their tenants of `process` a store holds, for the command line: every
/// case, with its events.
pub fn cases_in(
    store: &dyn kernable_store::Store,
    process: &str,
) -> Result<BTreeMap<(String, String), Vec<Event>>, StoreError> {
    let mut cases: BTreeMap<(String, String), Vec<Event>> = BTreeMap::new();
    store.each_event(&mut |e| {
        if e.domain == DOMAIN && e.aggregate_type == process {
            cases
                .entry((e.tenant_id.clone(), e.aggregate_id.clone()))
                .or_default()
                .push(e.clone());
        }
    })?;
    Ok(cases)
}

/// A case's state from its events: running, waiting, incident or ended; with its step.
pub fn case_state(events: &[Event]) -> (&'static str, String) {
    let mut state = "running";
    let mut step = String::new();
    for e in events {
        let p = payload_of(e);
        match e.event_type.as_str() {
            "StepReached" => {
                step = p["step"].as_str().unwrap_or_default().to_owned();
                state = match p["kind"].as_str() {
                    Some("wait" | "timer") => "waiting",
                    _ => "running",
                };
            }
            "IncidentRaised" => state = "incident",
            "IncidentResolved" => state = "running",
            "ProcessEnded" => {
                state = "ended";
                step = p["end"].as_str().unwrap_or_default().to_owned();
            }
            _ => {}
        }
    }
    (state, step)
}
