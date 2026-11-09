//! Business processes (design/processes.md): a flow across commands, events and queries, defined
//! in code next to the domain, built into a model the kernel runs.
//!
//! [`crate::Domain::process`] returns a [`Process`]; its methods add the steps in the order
//! written, each returning the process, so they chain:
//!
//! ```
//! use kernable::{command, domain, event, query};
//! use kernable::process::{Ask, Decide, Send, Start, Wait, value};
//!
//! #[event("shop")]
//! pub struct OrderSubmitted { pub total: i64 }
//! #[event("shop")]
//! pub struct OrderApproved {}
//! #[event("payments")]
//! pub struct PaymentReceived { pub order_id: String, pub at: i64 }
//! #[command("shop", aggregate = "order")]
//! pub struct Confirm {}
//! #[command("shop", aggregate = "order")]
//! pub struct Ship {}
//! #[command("shop", aggregate = "order")]
//! pub struct NotifyDelay {}
//! #[command("shop", aggregate = "order")]
//! pub struct Cancel { pub reason: String }
//! #[query("shop", answer = bool)]
//! pub struct NeedsApproval { pub order_id: String }
//!
//! let shop = domain("shop");
//! let fulfilment = shop
//!     .process("fulfilment")
//!     .start::<OrderSubmitted>(Start::key("aggregate_id").data([("order_id", "aggregate_id"), ("total", "data.total")]))
//!     .command::<Confirm>("confirm", Send::aggregate("case.order_id"))
//!     .decide("large", Decide::when("case.total > 1000", "approval").otherwise("paid"))
//!     .query::<NeedsApproval>("approval", Ask::data([("order_id", "case.order_id")]).path(true, "approved").path(false, "paid"))
//!     .wait::<OrderApproved>("approved", Wait::key("aggregate_id"))
//!     .wait::<PaymentReceived>("paid", Wait::key("data.order_id").data([("paid_at", "data.at")]).timeout("3d", "cancel"))
//!     .command::<Ship>("ship", Send::aggregate("case.order_id").rejected("out_of_stock", "notify").next("done"))
//!     .command::<NotifyDelay>("notify", Send::aggregate("case.order_id").next("done"))
//!     .command::<Cancel>("cancel", Send::aggregate("case.order_id").data([("reason", value("unpaid"))]))
//!     .end("done");
//! assert_eq!(fulfilment.model().unwrap()["name"], "shop.fulfilment");
//! ```
//!
//! - **Steps run in the order written**, unless a step names `next`; a `decide` without
//!   `otherwise` goes on with the next step written. Step names are required (letters, digits,
//!   `_`, `-`): they are the stable IDs of versions, the UI and the statistics.
//! - **References** are strings: `aggregate_id` and `data.<field>` (the event being handled, in
//!   `start` and `wait`), `case.<field>` (the case's data) and, in a query step, `answer` or
//!   `answer.<field>`. Field names are written in Rust's style (`case.order_id`); the model has
//!   the wire names (`case.orderId`), as the log does. A literal is [`value`].
//! - **Conditions** (`decide`) are small expressions: references to `case.<field>`, literals,
//!   `== != < <= > >=`, `and`, `or`, `not`. The kernel parses them when the process registers.
//! - **Durations** are `30s`, `15m`, `12h`, `3d`.
//! - **Messages** are types (`command::<Confirm>`), or names (`command_named("confirm",
//!   "order.Confirm", ...)`, unqualified: of the process's domain).
//!
//! An app registers its domains' processes ([`crate::Kernable::app`]);
//! [`crate::Kernable::register_process`] registers one alone. Import the option types from this
//! module: `Send` and `Wait` share their names with `std::marker::Send` and [`crate::Wait`].

use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{CommandType, Error, EventType, QueryType};

/// A literal in a mapping, where a string would be a reference:
/// `Send::aggregate("case.order_id").data([("reason", value("unpaid"))])`.
///
/// # Panics
///
/// If `literal` cannot be represented as JSON.
pub fn value(literal: impl Serialize) -> Source {
    Source(Repr::Value(
        serde_json::to_value(literal).expect("a literal is JSON"),
    ))
}

/// What a mapping's field gets: a reference (a string, e.g. `"case.order_id"`) or a literal
/// ([`value`]).
#[derive(Clone, Debug, PartialEq)]
pub struct Source(Repr);

#[derive(Clone, Debug, PartialEq)]
enum Repr {
    Ref(String),
    Value(Value),
}

impl From<&str> for Source {
    fn from(reference: &str) -> Source {
        Source(Repr::Ref(reference.to_owned()))
    }
}

impl From<String> for Source {
    fn from(reference: String) -> Source {
        Source(Repr::Ref(reference))
    }
}

type Mapping = Vec<(String, Source)>;

fn mapping<K: AsRef<str>, V: Into<Source>>(fields: impl IntoIterator<Item = (K, V)>) -> Mapping {
    fields
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_owned(), v.into()))
        .collect()
}

/// How a case starts: where its key is in the event, and the case's data from the event.
/// `Start::key("aggregate_id").data([("order_id", "aggregate_id"), ("total", "data.total")])`.
#[derive(Clone, Debug)]
pub struct Start {
    key: String,
    data: Mapping,
}

impl Start {
    /// The case's key: `aggregate_id`, or a field of the event's data (`data.order_id`). One
    /// case runs per key.
    pub fn key(reference: &str) -> Start {
        Start {
            key: reference.to_owned(),
            data: Vec::new(),
        }
    }

    /// The case's data from the event: field → reference (`aggregate_id`, `data.<field>`) or
    /// [`value`].
    pub fn data<K: AsRef<str>, V: Into<Source>>(
        mut self,
        fields: impl IntoIterator<Item = (K, V)>,
    ) -> Start {
        self.data.extend(mapping(fields));
        self
    }
}

/// A command step: the aggregate it goes to, its data, the paths for rejections, and the next
/// step. `Send::aggregate("case.order_id").rejected("out_of_stock", "notify").next("done")`.
#[derive(Clone, Debug)]
pub struct Send {
    aggregate: String,
    data: Mapping,
    rejected: Vec<(String, String)>,
    next: Option<String>,
}

impl Send {
    /// The command's aggregate ID: a reference to the case's data (`case.order_id`).
    pub fn aggregate(reference: &str) -> Send {
        Send {
            aggregate: reference.to_owned(),
            data: Vec::new(),
            rejected: Vec::new(),
            next: None,
        }
    }

    /// The command's data: field → reference (`case.<field>`) or [`value`].
    pub fn data<K: AsRef<str>, V: Into<Source>>(
        mut self,
        fields: impl IntoIterator<Item = (K, V)>,
    ) -> Send {
        self.data.extend(mapping(fields));
        self
    }

    /// A rejection with `code` goes on with `step`. A rejection without a path stops the case
    /// with an incident.
    pub fn rejected(mut self, code: &str, step: &str) -> Send {
        self.rejected.push((code.to_owned(), step.to_owned()));
        self
    }

    /// The step after it; default: the next step written.
    pub fn next(mut self, step: &str) -> Send {
        self.next = Some(step.to_owned());
        self
    }
}

/// A wait step: where this case's key is in the awaited event, what the event adds to the case,
/// how long to wait. `Wait::key("data.order_id").timeout("3d", "cancel")`.
#[derive(Clone, Debug)]
pub struct Wait {
    key: String,
    data: Mapping,
    timeout: Option<(String, String)>,
    next: Option<String>,
}

impl Wait {
    /// Where the event names the case's key: `aggregate_id`, or `data.<field>`.
    pub fn key(reference: &str) -> Wait {
        Wait {
            key: reference.to_owned(),
            data: Vec::new(),
            timeout: None,
            next: None,
        }
    }

    /// Merged into the case's data from the event: field → reference (`aggregate_id`,
    /// `data.<field>`, `case.<field>`) or [`value`].
    pub fn data<K: AsRef<str>, V: Into<Source>>(
        mut self,
        fields: impl IntoIterator<Item = (K, V)>,
    ) -> Wait {
        self.data.extend(mapping(fields));
        self
    }

    /// Without the event within `duration` (`30s`, `15m`, `12h`, `3d`), the case goes on with
    /// `step`.
    pub fn timeout(mut self, duration: &str, step: &str) -> Wait {
        self.timeout = Some((duration.to_owned(), step.to_owned()));
        self
    }

    /// The step after the event; default: the next step written.
    pub fn next(mut self, step: &str) -> Wait {
        self.next = Some(step.to_owned());
        self
    }
}

/// A timer step: waits a while (`Timer::after("12h")`) or until a time in the case's data, in
/// ms since epoch (`Timer::at("case.due_at")`).
#[derive(Clone, Debug)]
pub struct Timer {
    after: Option<String>,
    at: Option<String>,
    next: Option<String>,
}

impl Timer {
    /// Waits `duration`: `30s`, `15m`, `12h`, `3d`.
    pub fn after(duration: &str) -> Timer {
        Timer {
            after: Some(duration.to_owned()),
            at: None,
            next: None,
        }
    }

    /// Waits until the time (ms since epoch) a reference to the case's data names:
    /// `case.due_at`.
    pub fn at(reference: &str) -> Timer {
        Timer {
            after: None,
            at: Some(reference.to_owned()),
            next: None,
        }
    }

    /// The step after it; default: the next step written.
    pub fn next(mut self, step: &str) -> Timer {
        self.next = Some(step.to_owned());
        self
    }
}

/// A decision on the case's data: the first condition that holds picks the path.
/// `Decide::when("case.total > 1000", "approval").or_when("case.total > 100", "check").otherwise("paid")`.
#[derive(Clone, Debug)]
pub struct Decide {
    when: Vec<(String, String)>,
    otherwise: Option<String>,
}

impl Decide {
    /// If `condition` holds, the case goes on with `step`.
    pub fn when(condition: &str, step: &str) -> Decide {
        Decide {
            when: vec![(condition.to_owned(), step.to_owned())],
            otherwise: None,
        }
    }

    /// Another condition, tried after the ones before it.
    pub fn or_when(mut self, condition: &str, step: &str) -> Decide {
        self.when.push((condition.to_owned(), step.to_owned()));
        self
    }

    /// Where the case goes if no condition holds; default: the next step written.
    pub fn otherwise(mut self, step: &str) -> Decide {
        self.otherwise = Some(step.to_owned());
        self
    }
}

/// A query step: asks a read model; the answer picks the path.
/// `Ask::data([("order_id", "case.order_id")]).path(true, "approved").path(false, "paid")`.
#[derive(Clone, Debug, Default)]
pub struct Ask {
    data: Mapping,
    answer: Option<String>,
    paths: Vec<(Value, String)>,
    into: Option<String>,
    otherwise: Option<String>,
}

impl Ask {
    /// A query without data; add paths with [`Ask::path`].
    pub fn new() -> Ask {
        Ask::default()
    }

    /// A query with data: field → reference (`case.<field>`) or [`value`].
    pub fn data<K: AsRef<str>, V: Into<Source>>(fields: impl IntoIterator<Item = (K, V)>) -> Ask {
        Ask {
            data: mapping(fields),
            ..Ask::default()
        }
    }

    /// The field of an object answer the paths match (`status`); default: the answer itself (a
    /// boolean, string or number).
    pub fn answer(mut self, field: &str) -> Ask {
        self.answer = Some(field.to_owned());
        self
    }

    /// An answer equal to `value` goes on with `step`.
    ///
    /// # Panics
    ///
    /// If `value` cannot be represented as JSON.
    pub fn path(mut self, value: impl Serialize, step: &str) -> Ask {
        let value = serde_json::to_value(value).expect("a path's value is JSON");
        self.paths.push((value, step.to_owned()));
        self
    }

    /// Keeps the answer in this field of the case's data.
    pub fn into_field(mut self, field: &str) -> Ask {
        self.into = Some(field.to_owned());
        self
    }

    /// Where the case goes if no path matches; without it, an incident.
    pub fn otherwise(mut self, step: &str) -> Ask {
        self.otherwise = Some(step.to_owned());
        self
    }
}

#[derive(Clone, Debug)]
enum Step {
    Command { command: String, send: Send },
    Wait { event: String, wait: Wait },
    Timer(Timer),
    Decide(Decide),
    Query { query: String, ask: Ask },
    End,
}

struct Def {
    /// Qualified: `shop.fulfilment`.
    name: String,
    domain: String,
    start: Option<(String, Start)>,
    steps: Vec<(String, Step)>,
}

/// A business process of a domain, made with [`crate::Domain::process`]: its start and steps,
/// added in the order written (see the [module](self) for an example). Cloning gives another
/// handle to the same process. [`Process::model`] builds the model the kernel registers.
#[derive(Clone)]
pub struct Process {
    inner: Arc<Mutex<Def>>,
}

impl std::fmt::Debug for Process {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Process")
            .field("name", &self.name())
            .finish()
    }
}

/// Registered by [`crate::Kernable::register_process`]: the process's version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    /// Qualified: `shop.fulfilment`.
    pub name: String,
    /// From 1: the kernel numbers the versions.
    pub version: u32,
    /// `false` if the model equals the newest version: nothing new was registered.
    pub new: bool,
}

impl Process {
    pub(crate) fn new(domain: &str, name: &str) -> Process {
        Process {
            inner: Arc::new(Mutex::new(Def {
                name: format!("{domain}.{name}"),
                domain: domain.to_owned(),
                start: None,
                steps: Vec::new(),
            })),
        }
    }

    /// Its qualified name: `shop.fulfilment`.
    pub fn name(&self) -> String {
        self.inner.lock().unwrap().name.clone()
    }

    fn with(&self, f: impl FnOnce(&mut Def)) -> Process {
        f(&mut self.inner.lock().unwrap());
        self.clone()
    }

    fn step(&self, name: &str, step: Step) -> Process {
        self.with(|d| d.steps.push((name.to_owned(), step)))
    }

    /// Starts a case when an `E` event commits; see [`Start`].
    pub fn start<E: EventType>(&self, start: Start) -> Process {
        self.start_named(&format!("{}.{}", E::DOMAIN, E::TYPE), start)
    }

    /// [`Process::start`] with the event type by name: `shop.OrderSubmitted`, or `OrderSubmitted`
    /// of the process's domain.
    pub fn start_named(&self, event_type: &str, start: Start) -> Process {
        self.with(|d| d.start = Some((qualify(&d.domain, event_type, 1), start)))
    }

    /// A step `name` that sends a `C` command as the process; see [`Send`].
    pub fn command<C: CommandType>(&self, name: &str, send: Send) -> Process {
        let command = format!("{}.{}.{}", C::DOMAIN, C::AGGREGATE, C::TYPE);
        self.step(name, Step::Command { command, send })
    }

    /// [`Process::command`] with the command type by name: `shop.order.Confirm`, or
    /// `order.Confirm` of the process's domain.
    pub fn command_named(&self, name: &str, command_type: &str, send: Send) -> Process {
        let domain = self.inner.lock().unwrap().domain.clone();
        let command = qualify(&domain, command_type, 2);
        self.step(name, Step::Command { command, send })
    }

    /// A step `name` that waits for an `E` event of this case; see [`Wait`].
    pub fn wait<E: EventType>(&self, name: &str, wait: Wait) -> Process {
        let event = format!("{}.{}", E::DOMAIN, E::TYPE);
        self.step(name, Step::Wait { event, wait })
    }

    /// [`Process::wait`] with the event type by name: `payments.PaymentReceived`, or a type of
    /// the process's domain.
    pub fn wait_named(&self, name: &str, event_type: &str, wait: Wait) -> Process {
        let domain = self.inner.lock().unwrap().domain.clone();
        let event = qualify(&domain, event_type, 1);
        self.step(name, Step::Wait { event, wait })
    }

    /// A step `name` that waits for a time; see [`Timer`].
    pub fn timer(&self, name: &str, timer: Timer) -> Process {
        self.step(name, Step::Timer(timer))
    }

    /// A step `name` that picks a path by the case's data; see [`Decide`].
    pub fn decide(&self, name: &str, decide: Decide) -> Process {
        self.step(name, Step::Decide(decide))
    }

    /// A step `name` that asks a `Q` query; its answer picks the path; see [`Ask`].
    pub fn query<Q: QueryType>(&self, name: &str, ask: Ask) -> Process {
        let query = format!("{}.{}", Q::DOMAIN, Q::TYPE);
        self.step(name, Step::Query { query, ask })
    }

    /// [`Process::query`] with the query type by name: `shop.NeedsApproval`, or a type of the
    /// process's domain.
    pub fn query_named(&self, name: &str, query_type: &str, ask: Ask) -> Process {
        let domain = self.inner.lock().unwrap().domain.clone();
        let query = qualify(&domain, query_type, 1);
        self.step(name, Step::Query { query, ask })
    }

    /// A step `name` that ends the case.
    pub fn end(&self, name: &str) -> Process {
        self.step(name, Step::End)
    }

    /// The model the kernel registers (design/processes.md "The model format"): steps in the
    /// order written, each but `end` with its explicit successors, field names as on the wire.
    /// [`Error::Config`] naming the process and the step if it has no start, a step name is
    /// invalid or taken, a target names no step, a step other than `end` has no successor, a
    /// reference or duration is malformed, or a decision or query has no paths. The kernel
    /// checks the rest when it registers the model (conditions, types, grants).
    pub fn model(&self) -> Result<Value, Error> {
        let d = self.inner.lock().unwrap();
        let fail = |step: Option<&str>, reason: String| {
            Error::Config(match step {
                Some(step) => format!("process {}, step {step}: {reason}", d.name),
                None => format!("process {}: {reason}", d.name),
            })
        };
        let Some((on, start)) = &d.start else {
            return Err(fail(None, "no start: add one with start".into()));
        };
        let mut names: Vec<&str> = Vec::new();
        for (name, _) in &d.steps {
            let valid = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
            if !valid {
                return Err(fail(
                    Some(name),
                    "a step name is letters, digits, `_` and `-`".into(),
                ));
            }
            if names.contains(&name.as_str()) {
                return Err(fail(Some(name), "the name is taken by another step".into()));
            }
            names.push(name);
        }
        if d.steps.is_empty() {
            return Err(fail(None, "no steps".into()));
        }
        const EVENT: &[&str] = &["aggregateId", "data"];
        const EVENT_OR_CASE: &[&str] = &["aggregateId", "data", "case"];
        const CASE: &[&str] = &["case"];
        let start_model = {
            let mut m = Map::new();
            m.insert("on".into(), json!(on));
            m.insert(
                "key".into(),
                json!(reference(&start.key, EVENT).map_err(|e| fail(None, format!("start: {e}")))?),
            );
            if !start.data.is_empty() {
                m.insert(
                    "data".into(),
                    mapped(&start.data, EVENT).map_err(|e| fail(None, format!("start: {e}")))?,
                );
            }
            Value::Object(m)
        };
        let mut steps = Vec::new();
        for (i, (name, step)) in d.steps.iter().enumerate() {
            let at = Some(name.as_str());
            let following = d.steps.get(i + 1).map(|(n, _)| n.clone());
            let target = |what: &str, step: &str| -> Result<Value, Error> {
                if names.contains(&step) {
                    Ok(json!(step))
                } else {
                    Err(fail(at, format!("{what} names no step: {step}")))
                }
            };
            let successor = |explicit: &Option<String>| -> Result<Value, Error> {
                match explicit.as_ref().or(following.as_ref()) {
                    Some(next) => target("next", next),
                    None => Err(fail(
                        at,
                        "no step after it: name one with next, or end the process".into(),
                    )),
                }
            };
            let refs = |r: &str, allowed: &[&str]| reference(r, allowed).map_err(|e| fail(at, e));
            let map = |m: &Mapping, allowed: &[&str]| mapped(m, allowed).map_err(|e| fail(at, e));
            let duration = |text: &str| millis(text).map_err(|e| fail(at, e));
            let mut m = Map::new();
            m.insert("name".into(), json!(name));
            match step {
                Step::Command { command, send } => {
                    m.insert("kind".into(), json!("command"));
                    m.insert("command".into(), json!(command));
                    m.insert("aggregate".into(), json!(refs(&send.aggregate, CASE)?));
                    if !send.data.is_empty() {
                        m.insert("data".into(), map(&send.data, CASE)?);
                    }
                    if !send.rejected.is_empty() {
                        let mut rejected = Map::new();
                        for (code, step) in &send.rejected {
                            rejected.insert(code.clone(), target("rejected", step)?);
                        }
                        m.insert("rejected".into(), Value::Object(rejected));
                    }
                    m.insert("next".into(), successor(&send.next)?);
                }
                Step::Wait { event, wait } => {
                    m.insert("kind".into(), json!("wait"));
                    m.insert("event".into(), json!(event));
                    m.insert("key".into(), json!(refs(&wait.key, EVENT)?));
                    if !wait.data.is_empty() {
                        m.insert("data".into(), map(&wait.data, EVENT_OR_CASE)?);
                    }
                    if let Some((after, step)) = &wait.timeout {
                        m.insert("timeoutMs".into(), json!(duration(after)?));
                        m.insert("onTimeout".into(), target("timeout", step)?);
                    }
                    m.insert("next".into(), successor(&wait.next)?);
                }
                Step::Timer(timer) => {
                    m.insert("kind".into(), json!("timer"));
                    match (&timer.after, &timer.at) {
                        (Some(after), _) => {
                            m.insert("afterMs".into(), json!(duration(after)?));
                        }
                        (None, Some(time)) => {
                            m.insert("at".into(), json!(refs(time, CASE)?));
                        }
                        (None, None) => unreachable!("made with Timer::after or Timer::at"),
                    }
                    m.insert("next".into(), successor(&timer.next)?);
                }
                Step::Decide(decide) => {
                    m.insert("kind".into(), json!("decide"));
                    let mut when = Vec::new();
                    for (condition, step) in &decide.when {
                        if condition.trim().is_empty() {
                            return Err(fail(at, "an empty condition".into()));
                        }
                        when.push(json!({ "if": condition_names(condition), "next": target("when", step)? }));
                    }
                    m.insert("when".into(), Value::Array(when));
                    let otherwise = match &decide.otherwise {
                        Some(step) => target("otherwise", step)?,
                        None => successor(&None)?,
                    };
                    m.insert("otherwise".into(), otherwise);
                }
                Step::Query { query, ask } => {
                    m.insert("kind".into(), json!("query"));
                    m.insert("query".into(), json!(query));
                    if !ask.data.is_empty() {
                        m.insert("data".into(), map(&ask.data, CASE)?);
                    }
                    if let Some(field) = &ask.answer {
                        m.insert("answer".into(), json!(path(field)));
                    }
                    if ask.paths.is_empty() {
                        return Err(fail(
                            at,
                            "a query step needs paths: add them with Ask::path".into(),
                        ));
                    }
                    let mut paths = Vec::new();
                    for (value, step) in &ask.paths {
                        paths.push(json!({ "value": value, "next": target("path", step)? }));
                    }
                    m.insert("paths".into(), Value::Array(paths));
                    if let Some(field) = &ask.into {
                        m.insert("into".into(), json!(path(field)));
                    }
                    if let Some(step) = &ask.otherwise {
                        m.insert("otherwise".into(), target("otherwise", step)?);
                    }
                }
                Step::End => {
                    m.insert("kind".into(), json!("end"));
                }
            }
            steps.push(Value::Object(m));
        }
        Ok(json!({ "name": d.name, "start": start_model, "steps": steps }))
    }
}

/// `type_name` qualified with `domain` unless it has `dots` dots already (an event or query: 1,
/// a command: 2).
fn qualify(domain: &str, type_name: &str, dots: usize) -> String {
    if type_name.matches('.').count() >= dots {
        type_name.to_owned()
    } else {
        format!("{domain}.{type_name}")
    }
}

/// `order_id` → `orderId`, as the message attributes write field names.
fn camel(name: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for c in name.chars() {
        if c == '_' {
            upper = !out.is_empty();
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// A dotted field path with each part's wire name.
fn path(dotted: &str) -> String {
    dotted.split('.').map(camel).collect::<Vec<_>>().join(".")
}

/// A reference with wire names, if well-formed and of a kind in `allowed` (`aggregateId`,
/// `data`, `case`, `answer`).
fn reference(r: &str, allowed: &[&str]) -> Result<String, String> {
    let wire = path(r);
    let (head, rest) = wire.split_once('.').unwrap_or((&wire, ""));
    let parts_ok = rest.is_empty() || rest.split('.').all(|p| !p.is_empty());
    let shape_ok = match head {
        "aggregateId" => rest.is_empty() && !r.contains('.'),
        "data" | "case" => !rest.is_empty() && parts_ok,
        "answer" => parts_ok,
        _ => false,
    };
    if shape_ok && allowed.contains(&head) {
        Ok(wire)
    } else {
        let kinds: Vec<String> = allowed
            .iter()
            .map(|k| match *k {
                "aggregateId" => "aggregate_id".to_owned(),
                "answer" => "answer".to_owned(),
                k => format!("{k}.<field>"),
            })
            .collect();
        Err(format!(
            "{r:?} is not a reference here ({})",
            kinds.join(", ")
        ))
    }
}

/// A mapping with wire names: field → reference or `{"value": literal}`.
fn mapped(m: &Mapping, allowed: &[&str]) -> Result<Value, String> {
    let mut out = Map::new();
    for (field, source) in m {
        if field.is_empty() {
            return Err("a mapping's field has no name".into());
        }
        let value = match &source.0 {
            Repr::Ref(r) => json!(reference(r, allowed)?),
            Repr::Value(v) => json!({ "value": v }),
        };
        out.insert(path(field), value);
    }
    Ok(Value::Object(out))
}

/// `30s`, `15m`, `12h`, `3d` in ms.
fn millis(text: &str) -> Result<i64, String> {
    let bad = || format!("{text:?} is not a duration (30s, 15m, 12h, 3d)");
    let unit = match text.chars().last() {
        Some('s') => 1_000,
        Some('m') => 60_000,
        Some('h') => 3_600_000,
        Some('d') => 86_400_000,
        _ => return Err(bad()),
    };
    let number = &text[..text.len() - 1];
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let n: i64 = number.parse().map_err(|_| bad())?;
    match n.checked_mul(unit) {
        Some(ms) if ms > 0 => Ok(ms),
        _ => Err(format!("{text:?} is not a positive duration")),
    }
}

/// A condition with the wire names of its references (`case.`, `data.`, `answer.`); string
/// literals stay as written.
fn condition_names(condition: &str) -> String {
    let mut out = String::new();
    let mut chars = condition.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' || c == '"' {
            out.push(c);
            for s in chars.by_ref() {
                out.push(s);
                if s == c {
                    break;
                }
            }
        } else if c.is_ascii_alphabetic() || c == '_' {
            let mut word = c.to_string();
            while let Some(&n) = chars.peek() {
                if n.is_ascii_alphanumeric() || n == '_' || n == '.' {
                    word.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            let reference = ["case.", "data.", "answer."]
                .iter()
                .any(|p| word.starts_with(p));
            out.push_str(&if reference { path(&word) } else { word });
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::{command, domain, event, query};

    #[event("shop")]
    pub struct OrderSubmitted {
        pub total: i64,
    }
    #[event("shop")]
    pub struct OrderApproved {}
    #[event("payments")]
    pub struct PaymentReceived {
        pub order_id: String,
        pub at: i64,
    }
    #[command("shop", aggregate = "order")]
    pub struct Confirm {}
    #[command("shop", aggregate = "order")]
    pub struct Ship {}
    #[command("shop", aggregate = "order")]
    pub struct NotifyDelay {}
    #[command("shop", aggregate = "order")]
    pub struct Cancel {
        pub reason: String,
    }
    #[query("shop", answer = bool)]
    pub struct NeedsApproval {
        pub order_id: String,
    }

    fn fixture() -> Value {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../proto/test-vectors/process-model.json"
        );
        let v: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        v["model"].clone()
    }

    #[test]
    fn the_design_s_example_builds_the_shared_model() {
        let shop = domain("shop");
        let fulfilment = shop
            .process("fulfilment")
            .start::<OrderSubmitted>(
                Start::key("aggregate_id")
                    .data([("order_id", "aggregate_id"), ("total", "data.total")]),
            )
            .command::<Confirm>("confirm", Send::aggregate("case.order_id"))
            .decide(
                "large",
                Decide::when("case.total > 1000", "approval").otherwise("paid"),
            )
            .query::<NeedsApproval>(
                "approval",
                Ask::data([("order_id", "case.order_id")])
                    .path(true, "approved")
                    .path(false, "paid"),
            )
            .wait::<OrderApproved>("approved", Wait::key("aggregate_id"))
            .wait::<PaymentReceived>(
                "paid",
                Wait::key("data.order_id")
                    .data([("paid_at", "data.at")])
                    .timeout("3d", "cancel"),
            )
            .command::<Ship>(
                "ship",
                Send::aggregate("case.order_id")
                    .rejected("out_of_stock", "notify")
                    .next("done"),
            )
            .command::<NotifyDelay>("notify", Send::aggregate("case.order_id").next("done"))
            .command::<Cancel>(
                "cancel",
                Send::aggregate("case.order_id").data([("reason", value("unpaid"))]),
            )
            .end("done");
        assert_eq!(fulfilment.model().unwrap(), fixture());
        assert_eq!(fulfilment.name(), "shop.fulfilment");
        // The domain remembers it, for apps.
        assert_eq!(shop.processes().len(), 1);
    }

    #[test]
    fn messages_by_name_build_the_same_model() {
        let shop = domain("shop");
        let by_name = shop
            .process("fulfilment")
            .start_named(
                "OrderSubmitted",
                Start::key("aggregate_id")
                    .data([("order_id", "aggregate_id"), ("total", "data.total")]),
            )
            .command_named("confirm", "order.Confirm", Send::aggregate("case.order_id"))
            .decide(
                "large",
                Decide::when("case.total > 1000", "approval").otherwise("paid"),
            )
            .query_named(
                "approval",
                "shop.NeedsApproval",
                Ask::data([("order_id", "case.order_id")])
                    .path(true, "approved")
                    .path(false, "paid"),
            )
            .wait_named("approved", "OrderApproved", Wait::key("aggregate_id"))
            .wait_named(
                "paid",
                "payments.PaymentReceived",
                Wait::key("data.orderId")
                    .data([("paidAt", "data.at")])
                    .timeout("72h", "cancel"),
            )
            .command_named(
                "ship",
                "shop.order.Ship",
                Send::aggregate("case.order_id")
                    .rejected("out_of_stock", "notify")
                    .next("done"),
            )
            .command_named(
                "notify",
                "order.NotifyDelay",
                Send::aggregate("case.order_id").next("done"),
            )
            .command_named(
                "cancel",
                "order.Cancel",
                Send::aggregate("case.order_id").data([("reason", value("unpaid"))]),
            )
            .end("done");
        assert_eq!(by_name.model().unwrap(), fixture());
    }

    #[test]
    fn steps_follow_in_the_order_written_and_conditions_get_wire_names() {
        let p = domain("ops")
            .process("p")
            .start_named("Started", Start::key("data.case_id"))
            .timer("pause", Timer::after("15m"))
            .decide(
                "big",
                Decide::when("case.line_total > 10 and case.kind == 'big_one'", "later")
                    .or_when("not case.is_vip", "done"),
            )
            .timer("later", Timer::at("case.due_at"))
            .query_named(
                "check",
                "Check",
                Ask::new()
                    .answer("check_status")
                    .path("ok", "done")
                    .into_field("check_result")
                    .otherwise("done"),
            )
            .end("done");
        let model = p.model().unwrap();
        let steps = &model["steps"];
        assert_eq!(
            model["start"],
            json!({ "on": "ops.Started", "key": "data.caseId" })
        );
        assert_eq!(steps[0]["afterMs"], 900_000);
        assert_eq!(steps[0]["next"], "big");
        assert_eq!(
            steps[1]["when"],
            json!([
                { "if": "case.lineTotal > 10 and case.kind == 'big_one'", "next": "later" },
                { "if": "not case.isVip", "next": "done" },
            ])
        );
        // Without otherwise, a decision goes on with the next step written.
        assert_eq!(steps[1]["otherwise"], "later");
        assert_eq!(steps[2]["at"], "case.dueAt");
        assert_eq!(steps[2]["next"], "check");
        assert_eq!(
            steps[3],
            json!({
                "name": "check", "kind": "query", "query": "ops.Check", "answer": "checkStatus",
                "paths": [{ "value": "ok", "next": "done" }], "into": "checkResult", "otherwise": "done",
            })
        );
        assert_eq!(steps[4], json!({ "name": "done", "kind": "end" }));
    }

    fn refused(p: &Process, needle: &str) {
        match p.model() {
            Err(Error::Config(m)) => assert!(m.contains(needle), "{m}"),
            other => panic!("expected an error with {needle:?}, got {other:?}"),
        }
    }

    fn started(name: &str) -> Process {
        domain("ops")
            .process(name)
            .start_named("Started", Start::key("aggregate_id"))
    }

    #[test]
    fn a_process_without_a_start_or_steps_is_refused() {
        refused(
            &domain("ops").process("p").end("done"),
            "process ops.p: no start",
        );
        refused(&started("p"), "process ops.p: no steps");
    }

    #[test]
    fn a_step_name_is_valid_and_unique() {
        refused(
            &started("p").end("done").end("done"),
            "process ops.p, step done: the name is taken",
        );
        refused(&started("p").end("not ok"), "step not ok: a step name is");
    }

    #[test]
    fn every_target_names_a_step() {
        refused(
            &started("p")
                .command_named("a", "order.Do", Send::aggregate("case.id").next("nowhere"))
                .end("done"),
            "process ops.p, step a: next names no step: nowhere",
        );
        refused(
            &started("p")
                .command_named(
                    "a",
                    "order.Do",
                    Send::aggregate("case.id").rejected("x", "gone"),
                )
                .end("done"),
            "step a: rejected names no step: gone",
        );
        refused(
            &started("p")
                .wait_named("w", "Done", Wait::key("aggregate_id").timeout("1d", "gone"))
                .end("done"),
            "step w: timeout names no step: gone",
        );
        refused(
            &started("p")
                .decide("d", Decide::when("case.x", "gone"))
                .end("done"),
            "step d: when names no step: gone",
        );
        refused(
            &started("p")
                .query_named("q", "Q", Ask::new().path(1, "gone"))
                .end("done"),
            "step q: path names no step: gone",
        );
    }

    #[test]
    fn a_step_other_than_end_needs_a_successor() {
        refused(
            &started("p").timer("t", Timer::after("1s")),
            "process ops.p, step t: no step after it",
        );
        refused(
            &started("p").decide("d", Decide::when("case.x", "d")),
            "step d: no step after it",
        );
        refused(
            &started("p").query_named("q", "Q", Ask::new()).end("done"),
            "step q: a query step needs paths",
        );
    }

    #[test]
    fn durations_and_references_are_checked() {
        for bad in ["3", "d", "3w", "-1d", "0s", "1.5h", ""] {
            refused(
                &started("p").timer("t", Timer::after(bad)).end("done"),
                "step t:",
            );
        }
        assert_eq!(millis("30s"), Ok(30_000));
        assert_eq!(millis("3d"), Ok(259_200_000));
        refused(
            &started("p")
                .command_named("a", "order.Do", Send::aggregate("data.id"))
                .end("done"),
            "step a: \"data.id\" is not a reference here (case.<field>)",
        );
        refused(
            &domain("ops")
                .process("p")
                .start_named("Started", Start::key("case.id"))
                .end("done"),
            "process ops.p: start: \"case.id\" is not a reference",
        );
        refused(
            &started("p")
                .command_named("a", "order.Do", Send::aggregate("case."))
                .end("done"),
            "step a:",
        );
    }
}
