//! Process models (design/processes.md, "The model format"): parsing and checking the JSON an
//! SDK's builder produces, its canonical form, and the conditions of `decide` steps, parsed when
//! a model is registered and evaluated by the engine on a case's data.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{Map, Value};

use crate::config;

/// A process model, checked: every target names a step and every step is reachable.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Model {
    /// Qualified: `shop.fulfilment`.
    pub name: String,
    pub start: Start,
    /// In the order written; a case starts at the first.
    pub steps: Vec<Step>,
    /// Events that end a running case at once, whatever step it is at.
    pub cancel_on: Vec<CancelOn>,
}

/// An event that ends a running case of its key at the end step `end`: the case's timer is
/// withdrawn, an outcome it still awaits is ignored, an incident is resolved by it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CancelOn {
    /// The qualified event type.
    pub event: String,
    /// Where the case's key is in that event.
    pub key: Ref,
    /// The end step the case ends at.
    pub end: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Start {
    /// The qualified event type that starts a case.
    pub on: String,
    /// Where the case's key is in that event.
    pub key: Ref,
    /// The case's first data, from the event.
    pub data: Mapping,
}

/// A reference to a value: in the event being handled, the case's data or a query's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ref {
    AggregateId,
    /// `data.<path>`: the event's payload.
    Data(Vec<String>),
    /// `case.<path>`: the case's data.
    Case(Vec<String>),
    /// `answer` / `answer.<path>`: a query's answer.
    Answer(Vec<String>),
}

/// A field's value in a mapping: a reference, or a literal (`{"value": …}`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Source {
    Ref(Ref),
    Value(Value),
}

/// Field -> where its value comes from, in the order written.
pub(crate) type Mapping = Vec<(String, Source)>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Step {
    pub name: String,
    pub kind: Kind,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Kind {
    Command {
        /// Qualified with its aggregate type: `shop.order.Confirm`.
        command: String,
        aggregate: Source,
        data: Mapping,
        /// Rejection code -> step.
        rejected: Vec<(String, String)>,
        next: String,
    },
    Wait {
        event: String,
        key: Ref,
        data: Mapping,
        timeout_ms: Option<u64>,
        on_timeout: Option<String>,
        next: String,
    },
    Timer {
        after_ms: Option<u64>,
        /// A reference to a time (ms since epoch) in the case's data.
        at: Option<Ref>,
        next: String,
    },
    Decide {
        when: Vec<(Expr, String)>,
        otherwise: String,
    },
    Query {
        query: String,
        data: Mapping,
        /// The path in the answer its value is matched at; empty: the answer itself.
        answer: Vec<String>,
        paths: Vec<(Value, String)>,
        into: Option<String>,
        otherwise: Option<String>,
    },
    End,
}

impl Kind {
    /// The steps it may go on to.
    pub(crate) fn targets(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        match self {
            Kind::Command { rejected, next, .. } => {
                out.push(next);
                out.extend(rejected.iter().map(|(_, s)| s.as_str()));
            }
            Kind::Wait {
                on_timeout, next, ..
            } => {
                out.push(next);
                out.extend(on_timeout.as_deref());
            }
            Kind::Timer { next, .. } => out.push(next),
            Kind::Decide { when, otherwise } => {
                out.extend(when.iter().map(|(_, s)| s.as_str()));
                out.push(otherwise);
            }
            Kind::Query {
                paths, otherwise, ..
            } => {
                out.extend(paths.iter().map(|(_, s)| s.as_str()));
                out.extend(otherwise.as_deref());
            }
            Kind::End => {}
        }
        out
    }
}

impl Model {
    /// The command types (`domain.aggregateType.Type`), query types and event types it names.
    pub(crate) fn commands(&self) -> impl Iterator<Item = &str> {
        self.steps.iter().filter_map(|s| match &s.kind {
            Kind::Command { command, .. } => Some(command.as_str()),
            _ => None,
        })
    }

    pub(crate) fn queries(&self) -> impl Iterator<Item = &str> {
        self.steps.iter().filter_map(|s| match &s.kind {
            Kind::Query { query, .. } => Some(query.as_str()),
            _ => None,
        })
    }

    /// The start event, then every waited event, then every event that cancels a case.
    pub(crate) fn events(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.start.on.as_str())
            .chain(self.steps.iter().filter_map(|s| match &s.kind {
                Kind::Wait { event, .. } => Some(event.as_str()),
                _ => None,
            }))
            .chain(self.cancel_on.iter().map(|c| c.event.as_str()))
    }
}

/// A command type of a model, `domain.aggregateType.Type`, as the kernel names it elsewhere: the
/// qualified aggregate type (`domain.aggregateType`) and command type (`domain.Type`).
pub(crate) fn split_command(command: &str) -> (String, String) {
    let mut parts = command.splitn(3, '.');
    let (d, a, t) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    (config::qualified(d, a), config::qualified(d, t))
}

/// Why a model is invalid, with where: the start, a step, or the model as a whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Invalid {
    pub process: String,
    /// `start`, `step <name>`, or empty for the model.
    pub at: String,
    pub reason: String,
}

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let process = if self.process.is_empty() {
            "the process"
        } else {
            &self.process
        };
        match self.at.as_str() {
            "" => write!(f, "process {process}: {}", self.reason),
            at => write!(f, "process {process}, {at}: {}", self.reason),
        }
    }
}

type Checked<T> = Result<T, String>;

/// Parses and checks a model. Returns it with warnings: `case.` fields a step reads that are not
/// written on every path before it.
pub(crate) fn parse(json: &[u8]) -> Result<(Model, Vec<String>), Invalid> {
    let invalid = |process: &str, at: &str, reason: String| Invalid {
        process: process.to_owned(),
        at: at.to_owned(),
        reason,
    };
    let value: Value = serde_json::from_slice(json)
        .map_err(|e| invalid("", "", format!("the model is not JSON: {e}")))?;
    let Value::Object(top) = value else {
        return Err(invalid("", "", "the model is not a JSON object".into()));
    };
    let name = match top.get("name") {
        Some(Value::String(n)) => n.clone(),
        _ => return Err(invalid("", "", "no name (a string)".into())),
    };
    if !valid_process_name(&name) {
        return Err(invalid(
            &name,
            "",
            "the name is not a qualified process name (domain.process)".into(),
        ));
    }
    let at_model = |reason: String| invalid(&name, "", reason);
    only(&top, &["name", "start", "steps", "cancelOn"]).map_err(at_model)?;
    let start = match top.get("start") {
        Some(Value::Object(o)) => parse_start(o).map_err(|r| invalid(&name, "start", r))?,
        _ => return Err(at_model("no start (an object)".into())),
    };
    let Some(Value::Array(raw_steps)) = top.get("steps") else {
        return Err(at_model("no steps (an array)".into()));
    };
    if raw_steps.is_empty() {
        return Err(at_model("no steps".into()));
    }
    let mut steps = Vec::with_capacity(raw_steps.len());
    let mut names = HashSet::new();
    for (i, raw) in raw_steps.iter().enumerate() {
        let Value::Object(o) = raw else {
            return Err(at_model(format!("step {} is not an object", i + 1)));
        };
        let step_name = match o.get("name") {
            Some(Value::String(n)) if valid_step_name(n) => n.clone(),
            Some(Value::String(n)) => {
                return Err(at_model(format!(
                    "step {} has the name {n:?}: letters, digits, _ and - only",
                    i + 1
                )));
            }
            _ => return Err(at_model(format!("step {} has no name", i + 1))),
        };
        let at = format!("step {step_name}");
        if !names.insert(step_name.clone()) {
            return Err(invalid(&name, &at, "a second step of this name".into()));
        }
        let kind = parse_step(o).map_err(|r| invalid(&name, &at, r))?;
        steps.push(Step {
            name: step_name,
            kind,
        });
    }
    let cancel_on = match top.get("cancelOn") {
        None => Vec::new(),
        Some(Value::Array(list)) => {
            let mut out = Vec::with_capacity(list.len());
            for (i, raw) in list.iter().enumerate() {
                let at = format!("cancelOn {}", i + 1);
                let Value::Object(o) = raw else {
                    return Err(invalid(&name, &at, "not an object".into()));
                };
                let c = parse_cancel(o).map_err(|r| invalid(&name, &at, r))?;
                match steps.iter().find(|s| s.name == c.end) {
                    Some(Step {
                        kind: Kind::End, ..
                    }) => {}
                    Some(_) => {
                        return Err(invalid(&name, &at, format!("{} is no end step", c.end)));
                    }
                    None => return Err(invalid(&name, &at, format!("{} is no step", c.end))),
                }
                if out.iter().any(|o: &CancelOn| o.event == c.event) {
                    return Err(invalid(
                        &name,
                        &at,
                        format!("a second cancelOn for {}", c.event),
                    ));
                }
                out.push(c);
            }
            out
        }
        Some(_) => return Err(at_model("cancelOn is not an array".into())),
    };
    let model = Model {
        name,
        start,
        steps,
        cancel_on,
    };
    for step in &model.steps {
        if let Some(t) = step
            .kind
            .targets()
            .into_iter()
            .find(|t| !names.contains(*t))
        {
            return Err(invalid(
                &model.name,
                &format!("step {}", step.name),
                format!("goes on to {t}, which is no step"),
            ));
        }
    }
    let reachable = reachable(&model);
    if let Some(s) = model.steps.iter().find(|s| !reachable.contains(&s.name)) {
        return Err(invalid(
            &model.name,
            &format!("step {}", s.name),
            "no path from the start reaches it".into(),
        ));
    }
    let warnings = unwritten(&model);
    Ok((model, warnings))
}

/// A process's qualified name: `domain.process`, the process an identifier too.
pub(crate) fn valid_process_name(name: &str) -> bool {
    name.split_once('.')
        .is_some_and(|(d, p)| config::valid_domain(d) && config::valid_domain(p))
}

fn valid_step_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A field name of a mapping or a path segment.
fn valid_field(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Refuses fields other than `allowed`.
fn only(o: &Map<String, Value>, allowed: &[&str]) -> Checked<()> {
    match o.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!("unknown field {k}")),
        None => Ok(()),
    }
}

fn string<'a>(o: &'a Map<String, Value>, field: &str) -> Checked<&'a str> {
    match o.get(field) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s),
        Some(_) => Err(format!("{field} is not a string")),
        None => Err(format!("no {field}")),
    }
}

fn optional_string(o: &Map<String, Value>, field: &str) -> Checked<Option<String>> {
    match o.get(field) {
        None => Ok(None),
        Some(_) => string(o, field).map(|s| Some(s.to_owned())),
    }
}

/// A duration in ms: a positive integer.
fn duration(o: &Map<String, Value>, field: &str) -> Checked<Option<u64>> {
    match o.get(field) {
        None => Ok(None),
        Some(v) => match v.as_u64() {
            Some(ms) if ms > 0 => Ok(Some(ms)),
            _ => Err(format!("{field} is not a positive number of milliseconds")),
        },
    }
}

/// `domain.Type`.
fn qualified_type(o: &Map<String, Value>, field: &str) -> Checked<String> {
    let t = string(o, field)?;
    if config::valid_qualified(t) {
        Ok(t.to_owned())
    } else {
        Err(format!("{field} {t} is not a qualified type (domain.Type)"))
    }
}

/// Parses a reference; `allowed` lists the kinds this place takes (`aggregateId`, `data`,
/// `case`, `answer`).
pub(crate) fn parse_ref(s: &str, allowed: &[&str]) -> Checked<Ref> {
    let (head, path) = match s.split_once('.') {
        Some((head, rest)) => (head, rest.split('.').map(str::to_owned).collect::<Vec<_>>()),
        None => (s, Vec::new()),
    };
    if let Some(bad) = path.iter().find(|p| !valid_field(p)) {
        return Err(format!("the reference {s} has an invalid field {bad:?}"));
    }
    let r = match (head, path.is_empty()) {
        ("aggregateId", true) => Ref::AggregateId,
        ("data", false) => Ref::Data(path),
        ("case", false) => Ref::Case(path),
        ("answer", _) => Ref::Answer(path),
        _ => {
            return Err(format!(
                "{s} is no reference (aggregateId, data.<field>, case.<field>, answer.<field>)"
            ));
        }
    };
    let kind = match r {
        Ref::AggregateId => "aggregateId",
        Ref::Data(_) => "data",
        Ref::Case(_) => "case",
        Ref::Answer(_) => "answer",
    };
    if !allowed.contains(&kind) {
        return Err(format!(
            "{s}: here a reference is one of {}",
            allowed
                .iter()
                .map(|a| match *a {
                    "aggregateId" => "aggregateId",
                    "data" => "data.<field>",
                    "case" => "case.<field>",
                    _ => "answer.<field>",
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(r)
}

fn source(v: &Value, allowed: &[&str], what: &str) -> Checked<Source> {
    match v {
        Value::String(s) => parse_ref(s, allowed).map(Source::Ref),
        Value::Object(o) if o.len() == 1 && o.contains_key("value") => {
            Ok(Source::Value(o["value"].clone()))
        }
        _ => Err(format!(
            "{what} is neither a reference nor a literal ({{\"value\": …}})"
        )),
    }
}

fn mapping(o: &Map<String, Value>, field: &str, allowed: &[&str]) -> Checked<Mapping> {
    match o.get(field) {
        None => Ok(Vec::new()),
        Some(Value::Object(m)) => m
            .iter()
            .map(|(k, v)| {
                if !valid_field(k) {
                    return Err(format!(
                        "{field} has the field {k:?}: letters, digits, _ and - only"
                    ));
                }
                Ok((k.clone(), source(v, allowed, &format!("{field}.{k}"))?))
            })
            .collect(),
        Some(_) => Err(format!("{field} is not an object")),
    }
}

/// What an event's mappings may read: the event.
const FROM_EVENT: &[&str] = &["aggregateId", "data"];
/// What a command's or query's data may read: the case.
const FROM_CASE: &[&str] = &["case"];

fn parse_start(o: &Map<String, Value>) -> Checked<Start> {
    only(o, &["on", "key", "data"])?;
    Ok(Start {
        on: qualified_type(o, "on")?,
        key: parse_ref(string(o, "key")?, FROM_EVENT)?,
        data: mapping(o, "data", FROM_EVENT)?,
    })
}

fn parse_cancel(o: &Map<String, Value>) -> Checked<CancelOn> {
    only(o, &["event", "key", "end"])?;
    Ok(CancelOn {
        event: qualified_type(o, "event")?,
        key: parse_ref(string(o, "key")?, FROM_EVENT)?,
        end: string(o, "end")?.to_owned(),
    })
}

fn parse_step(o: &Map<String, Value>) -> Checked<Kind> {
    let kind = string(o, "kind")?;
    let next = || string(o, "next").map(str::to_owned);
    Ok(match kind {
        "command" => {
            only(
                o,
                &[
                    "name",
                    "kind",
                    "command",
                    "aggregate",
                    "data",
                    "rejected",
                    "next",
                ],
            )?;
            let command = string(o, "command")?;
            let parts: Vec<&str> = command.split('.').collect();
            if parts.len() != 3
                || !config::valid_domain(parts[0])
                || !parts[1..].iter().all(|p| config::valid_type(p))
            {
                return Err(format!(
                    "command {command} is not domain.aggregateType.Command"
                ));
            }
            let aggregate = source(
                o.get("aggregate").ok_or("no aggregate")?,
                FROM_CASE,
                "aggregate",
            )?;
            let rejected = match o.get("rejected") {
                None => Vec::new(),
                Some(Value::Object(m)) => m
                    .iter()
                    .map(|(code, step)| match step {
                        Value::String(s) if !code.is_empty() => Ok((code.clone(), s.clone())),
                        _ => Err(format!("rejected.{code} is not a step name")),
                    })
                    .collect::<Checked<_>>()?,
                Some(_) => return Err("rejected is not an object (code: step)".into()),
            };
            Kind::Command {
                command: command.to_owned(),
                aggregate,
                data: mapping(o, "data", FROM_CASE)?,
                rejected,
                next: next()?,
            }
        }
        "wait" => {
            only(
                o,
                &[
                    "name",
                    "kind",
                    "event",
                    "key",
                    "data",
                    "timeoutMs",
                    "onTimeout",
                    "next",
                ],
            )?;
            let timeout_ms = duration(o, "timeoutMs")?;
            let on_timeout = optional_string(o, "onTimeout")?;
            match (timeout_ms, &on_timeout) {
                (Some(_), None) => return Err("timeoutMs without onTimeout".into()),
                (None, Some(_)) => return Err("onTimeout without timeoutMs".into()),
                _ => {}
            }
            Kind::Wait {
                event: qualified_type(o, "event")?,
                key: parse_ref(string(o, "key")?, FROM_EVENT)?,
                data: mapping(o, "data", FROM_EVENT)?,
                timeout_ms,
                on_timeout,
                next: next()?,
            }
        }
        "timer" => {
            only(o, &["name", "kind", "afterMs", "at", "next"])?;
            let after_ms = duration(o, "afterMs")?;
            let at = match o.get("at") {
                None => None,
                Some(Value::String(s)) => Some(parse_ref(s, FROM_CASE)?),
                Some(_) => return Err("at is not a reference".into()),
            };
            if after_ms.is_some() == at.is_some() {
                return Err("a timer has either afterMs or at".into());
            }
            Kind::Timer {
                after_ms,
                at,
                next: next()?,
            }
        }
        "decide" => {
            only(o, &["name", "kind", "when", "otherwise"])?;
            let Some(Value::Array(when)) = o.get("when") else {
                return Err("no when (an array of {if, next})".into());
            };
            if when.is_empty() {
                return Err("when is empty".into());
            }
            let when = when
                .iter()
                .map(|w| {
                    let Value::Object(w) = w else {
                        return Err("when holds {if, next} objects".to_owned());
                    };
                    only(w, &["if", "next"])?;
                    let condition = string(w, "if")?;
                    let expr = parse_condition(condition)
                        .map_err(|e| format!("the condition {condition:?}: {e}"))?;
                    Ok((expr, string(w, "next")?.to_owned()))
                })
                .collect::<Checked<_>>()?;
            Kind::Decide {
                when,
                otherwise: string(o, "otherwise")?.to_owned(),
            }
        }
        "query" => {
            only(
                o,
                &[
                    "name",
                    "kind",
                    "query",
                    "data",
                    "answer",
                    "paths",
                    "into",
                    "otherwise",
                ],
            )?;
            let answer = match o.get("answer") {
                None => Vec::new(),
                Some(Value::String(s)) => match parse_ref(s, &["answer"]) {
                    Ok(Ref::Answer(path)) => path,
                    // A plain path in the answer.
                    _ => match parse_ref(&format!("answer.{s}"), &["answer"]) {
                        Ok(Ref::Answer(path)) => path,
                        _ => return Err(format!("answer {s} is not a path in the answer")),
                    },
                },
                Some(_) => return Err("answer is not a string".into()),
            };
            let Some(Value::Array(paths)) = o.get("paths") else {
                return Err("no paths (an array of {value, next})".into());
            };
            if paths.is_empty() {
                return Err("paths is empty".into());
            }
            let paths = paths
                .iter()
                .map(|p| {
                    let Value::Object(p) = p else {
                        return Err("paths holds {value, next} objects".to_owned());
                    };
                    only(p, &["value", "next"])?;
                    let value = match p.get("value") {
                        Some(v @ (Value::Bool(_) | Value::String(_) | Value::Number(_))) => {
                            v.clone()
                        }
                        _ => {
                            return Err(
                                "a path's value is a boolean, a string or a number".to_owned()
                            );
                        }
                    };
                    Ok((value, string(p, "next")?.to_owned()))
                })
                .collect::<Checked<_>>()?;
            let into = optional_string(o, "into")?;
            if let Some(into) = &into
                && !valid_field(into)
            {
                return Err(format!("into {into:?} is not a field name"));
            }
            Kind::Query {
                query: qualified_type(o, "query")?,
                data: mapping(o, "data", FROM_CASE)?,
                answer,
                paths,
                into,
                otherwise: optional_string(o, "otherwise")?,
            }
        }
        "end" => {
            only(o, &["name", "kind"])?;
            Kind::End
        }
        other => {
            return Err(format!(
                "kind {other} is none of command, wait, timer, decide, query, end"
            ));
        }
    })
}

/// The steps reachable from the start, which goes to the first step.
fn reachable(model: &Model) -> HashSet<String> {
    let by_name: HashMap<&str, &Step> = model.steps.iter().map(|s| (s.name.as_str(), s)).collect();
    let mut seen = HashSet::new();
    // The first step, and the ends a cancelling event leads to.
    let mut todo = vec![model.steps[0].name.as_str()];
    todo.extend(model.cancel_on.iter().map(|c| c.end.as_str()));
    while let Some(name) = todo.pop() {
        if !seen.insert(name.to_owned()) {
            continue;
        }
        if let Some(step) = by_name.get(name) {
            todo.extend(step.kind.targets());
        }
    }
    seen
}

/// The case fields `step` reads (the first segment of each `case.` reference).
fn reads(kind: &Kind) -> Vec<String> {
    let mut out = Vec::new();
    let mut from_source = |s: &Source| {
        if let Source::Ref(Ref::Case(path)) = s {
            out.push(path[0].clone());
        }
    };
    match kind {
        Kind::Command {
            aggregate, data, ..
        } => {
            from_source(aggregate);
            data.iter().for_each(|(_, s)| from_source(s));
        }
        Kind::Query { data, .. } => data.iter().for_each(|(_, s)| from_source(s)),
        Kind::Timer {
            at: Some(Ref::Case(path)),
            ..
        } => out.push(path[0].clone()),
        Kind::Decide { when, .. } => {
            for (expr, _) in when {
                expr.refs(&mut |path| out.push(path[0].clone()));
            }
        }
        _ => {}
    }
    out
}

/// Where a step goes on to, each with the case fields it has written on that way: a wait's
/// data only when its event came, not on a timeout.
fn edges(kind: &Kind) -> Vec<(&str, Vec<String>)> {
    match kind {
        Kind::Wait {
            data,
            on_timeout,
            next,
            ..
        } => {
            let mut out = vec![(next.as_str(), data.iter().map(|(k, _)| k.clone()).collect())];
            out.extend(on_timeout.as_deref().map(|t| (t, Vec::new())));
            out
        }
        Kind::Query { into, .. } => kind
            .targets()
            .into_iter()
            .map(|t| (t, into.iter().cloned().collect()))
            .collect(),
        _ => kind
            .targets()
            .into_iter()
            .map(|t| (t, Vec::new()))
            .collect(),
    }
}

/// Warnings for `case.` fields a step reads that are not written on every path before it (by
/// the start or an earlier step): definitely-written fields, computed to a fixed point.
fn unwritten(model: &Model) -> Vec<String> {
    let index: HashMap<&str, usize> = model
        .steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    // Per step: its predecessors, each with what it wrote on the way.
    let mut preds: Vec<Vec<(usize, Vec<String>)>> = vec![Vec::new(); model.steps.len()];
    for (i, s) in model.steps.iter().enumerate() {
        for (t, written) in edges(&s.kind) {
            preds[index[t]].push((i, written));
        }
    }
    let started: BTreeSet<String> = model.start.data.iter().map(|(k, _)| k.clone()).collect();
    // None: not computed yet (every field, for the intersection).
    let mut into: Vec<Option<BTreeSet<String>>> = vec![None; model.steps.len()];
    loop {
        let mut changed = false;
        for i in 0..model.steps.len() {
            let mut known: Option<BTreeSet<String>> = (i == 0).then(|| started.clone());
            for (p, written) in &preds[i] {
                let Some(before) = &into[*p] else { continue };
                let mut out = before.clone();
                out.extend(written.iter().cloned());
                known = Some(match known {
                    None => out,
                    Some(k) => k.intersection(&out).cloned().collect(),
                });
            }
            if known.is_some() && known != into[i] {
                into[i] = known;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let mut warnings = Vec::new();
    for (i, s) in model.steps.iter().enumerate() {
        let known = into[i].clone().unwrap_or_default();
        let mut missing: Vec<String> = reads(&s.kind)
            .into_iter()
            .filter(|f| !known.contains(f))
            .collect();
        missing.dedup();
        for field in missing {
            warnings.push(format!(
                "process {}, step {}: case.{field} is not written on every path before it",
                model.name, s.name
            ));
        }
    }
    warnings
}

/// The canonical form of a JSON document: object keys sorted, no whitespace.
pub(crate) fn canonical(value: &Value) -> Vec<u8> {
    fn write(v: &Value, out: &mut String) {
        match v {
            Value::Object(o) => {
                let sorted: BTreeMap<&String, &Value> = o.iter().collect();
                out.push('{');
                for (i, (k, v)) in sorted.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(k).expect("a string serializes"));
                    out.push(':');
                    write(v, out);
                }
                out.push('}');
            }
            Value::Array(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(v, out);
                }
                out.push(']');
            }
            scalar => out.push_str(&serde_json::to_string(scalar).expect("a scalar serializes")),
        }
    }
    let mut out = String::new();
    write(value, &mut out);
    out.into_bytes()
}

// ---- conditions ----

/// A condition of a `decide` step, parsed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Cmp(Op, Box<Expr>, Box<Expr>),
    /// `case.<path>`.
    Ref(Vec<String>),
    Lit(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Expr {
    /// Calls `f` with the path of every `case.` reference.
    fn refs(&self, f: &mut impl FnMut(&[String])) {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) | Expr::Cmp(_, a, b) => {
                a.refs(f);
                b.refs(f);
            }
            Expr::Not(a) => a.refs(f),
            Expr::Ref(path) => f(path),
            Expr::Lit(_) => {}
        }
    }

    /// Whether the condition holds on the case's data `case`.
    #[allow(dead_code)] // the engine (stage 2) evaluates conditions; the tests do now
    pub(crate) fn holds(&self, case: &Value) -> bool {
        truthy(&self.eval(case))
    }

    fn eval(&self, case: &Value) -> Value {
        match self {
            Expr::Or(a, b) => Value::Bool(a.holds(case) || b.holds(case)),
            Expr::And(a, b) => Value::Bool(a.holds(case) && b.holds(case)),
            Expr::Not(a) => Value::Bool(!a.holds(case)),
            Expr::Cmp(op, a, b) => Value::Bool(compare(*op, &a.eval(case), &b.eval(case))),
            Expr::Ref(path) => lookup(case, path).cloned().unwrap_or(Value::Null),
            Expr::Lit(v) => v.clone(),
        }
    }
}

/// The value at `path` in `value`, if there is one.
pub(crate) fn lookup<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |v, field| v.get(field))
}

/// false for null, false, 0, "" and empty arrays and objects; true otherwise.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `==` and `!=` compare any values (numbers by value: 1 == 1.0); the orderings compare two
/// numbers or two strings and are false for anything else (a missing field among them).
fn compare(op: Op, a: &Value, b: &Value) -> bool {
    let ordering = match (a, b) {
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .zip(y.as_f64())
            .and_then(|(x, y)| x.partial_cmp(&y)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    };
    let equal = match ordering {
        Some(o) => o.is_eq(),
        None => a == b,
    };
    match op {
        Op::Eq => equal,
        Op::Ne => !equal,
        Op::Lt => ordering.is_some_and(|o| o.is_lt()),
        Op::Le => ordering.is_some_and(|o| o.is_le()),
        Op::Gt => ordering.is_some_and(|o| o.is_gt()),
        Op::Ge => ordering.is_some_and(|o| o.is_ge()),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ref(Vec<String>),
    Lit(Value),
    Op(Op),
    And,
    Or,
    Not,
    Open,
    Close,
}

fn tokens(s: &str) -> Checked<Vec<Token>> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(Token::Open);
                i += 1;
            }
            ')' => {
                out.push(Token::Close);
                i += 1;
            }
            '=' | '!' | '<' | '>' => {
                let two = chars.get(i + 1) == Some(&'=');
                let op = match (c, two) {
                    ('=', true) => Op::Eq,
                    ('!', true) => Op::Ne,
                    ('<', true) => Op::Le,
                    ('>', true) => Op::Ge,
                    ('<', false) => Op::Lt,
                    ('>', false) => Op::Gt,
                    _ => return Err(format!("{c} at {} is no operator (== != < <= > >=)", i + 1)),
                };
                out.push(Token::Op(op));
                i += if two { 2 } else { 1 };
            }
            '\'' | '"' => {
                let quote = c;
                let mut text = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("a string is not closed".into()),
                        Some(&'\\') => {
                            match chars.get(i + 1) {
                                Some(&e) => text.push(e),
                                None => return Err("a string is not closed".into()),
                            }
                            i += 2;
                        }
                        Some(&q) if q == quote => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            text.push(ch);
                            i += 1;
                        }
                    }
                }
                out.push(Token::Lit(Value::String(text)));
            }
            c if c.is_ascii_digit() || c == '-' => {
                let begin = i;
                i += 1;
                while i < chars.len()
                    && (chars[i].is_ascii_digit()
                        || matches!(chars[i], '.' | 'e' | 'E' | '+' | '-'))
                {
                    // A sign only right after an exponent.
                    if matches!(chars[i], '+' | '-') && !matches!(chars[i - 1], 'e' | 'E') {
                        break;
                    }
                    i += 1;
                }
                let text: String = chars[begin..i].iter().collect();
                let number: serde_json::Number =
                    serde_json::from_str(&text).map_err(|_| format!("{text} is not a number"))?;
                out.push(Token::Lit(Value::Number(number)));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let begin = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '_' | '-' | '.'))
                {
                    i += 1;
                }
                let word: String = chars[begin..i].iter().collect();
                out.push(match word.as_str() {
                    "and" => Token::And,
                    "or" => Token::Or,
                    "not" => Token::Not,
                    "true" => Token::Lit(Value::Bool(true)),
                    "false" => Token::Lit(Value::Bool(false)),
                    "null" => Token::Lit(Value::Null),
                    w => match parse_ref(w, FROM_CASE) {
                        Ok(Ref::Case(path)) => Token::Ref(path),
                        _ => {
                            return Err(format!(
                                "{w} is no case.<field> reference, literal or operator"
                            ));
                        }
                    },
                });
            }
            c => return Err(format!("{c:?} at {} is not allowed", i + 1)),
        }
    }
    Ok(out)
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn take(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.at).cloned();
        self.at += 1;
        t
    }

    fn or(&mut self) -> Checked<Expr> {
        let mut left = self.and()?;
        while self.peek() == Some(&Token::Or) {
            self.at += 1;
            left = Expr::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Checked<Expr> {
        let mut left = self.not()?;
        while self.peek() == Some(&Token::And) {
            self.at += 1;
            left = Expr::And(Box::new(left), Box::new(self.not()?));
        }
        Ok(left)
    }

    fn not(&mut self) -> Checked<Expr> {
        if self.peek() == Some(&Token::Not) {
            self.at += 1;
            return Ok(Expr::Not(Box::new(self.not()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Checked<Expr> {
        let left = self.operand()?;
        if let Some(Token::Op(op)) = self.peek().cloned() {
            self.at += 1;
            let right = self.operand()?;
            return Ok(Expr::Cmp(op, Box::new(left), Box::new(right)));
        }
        Ok(left)
    }

    fn operand(&mut self) -> Checked<Expr> {
        match self.take() {
            Some(Token::Ref(path)) => Ok(Expr::Ref(path)),
            Some(Token::Lit(v)) => Ok(Expr::Lit(v)),
            Some(Token::Open) => {
                let inner = self.or()?;
                match self.take() {
                    Some(Token::Close) => Ok(inner),
                    _ => Err("a ( is not closed".into()),
                }
            }
            Some(t) => Err(format!(
                "expected a reference, a literal or (, found {}",
                describe(&t)
            )),
            None => Err("the condition ends too early".into()),
        }
    }
}

fn describe(t: &Token) -> String {
    match t {
        Token::Op(_) => "an operator".into(),
        Token::And => "and".into(),
        Token::Or => "or".into(),
        Token::Not => "not".into(),
        Token::Open => "(".into(),
        Token::Close => ")".into(),
        Token::Ref(p) => format!("case.{}", p.join(".")),
        Token::Lit(v) => v.to_string(),
    }
}

/// Parses a condition: `expr := or; or := and ("or" and)*; and := not ("and" not)*;
/// not := "not" not | cmp; cmp := operand (op operand)?; operand := case.<path> | number |
/// 'string' | "string" | true | false | null | "(" expr ")"`.
pub(crate) fn parse_condition(s: &str) -> Checked<Expr> {
    let mut parser = Parser {
        tokens: tokens(s)?,
        at: 0,
    };
    if parser.tokens.is_empty() {
        return Err("the condition is empty".into());
    }
    let expr = parser.or()?;
    match parser.peek() {
        None => Ok(expr),
        Some(t) => Err(format!(
            "unexpected {} after a complete condition",
            describe(t)
        )),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn example() -> Value {
        let file: Value = serde_json::from_str(include_str!(
            "../../../proto/test-vectors/process-model.json"
        ))
        .unwrap();
        file["model"].clone()
    }

    fn check(model: &Value) -> Result<(Model, Vec<String>), Invalid> {
        parse(&serde_json::to_vec(model).unwrap())
    }

    fn reason(model: &Value) -> String {
        check(model).unwrap_err().to_string()
    }

    #[test]
    fn the_example_parses_without_warnings() {
        let (model, warnings) = check(&example()).unwrap();
        assert_eq!(model.name, "shop.fulfilment");
        assert_eq!(model.steps.len(), 9);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            model.commands().collect::<Vec<_>>(),
            [
                "shop.order.Confirm",
                "shop.order.Ship",
                "shop.order.NotifyDelay",
                "shop.order.Cancel"
            ]
        );
        assert_eq!(model.queries().collect::<Vec<_>>(), ["shop.NeedsApproval"]);
        assert_eq!(
            model.events().collect::<Vec<_>>(),
            [
                "shop.OrderSubmitted",
                "shop.OrderApproved",
                "payments.PaymentReceived"
            ]
        );
        assert_eq!(
            split_command("shop.order.Confirm"),
            ("shop.order".into(), "shop.Confirm".into())
        );
        let Kind::Decide { when, otherwise } = &model.steps[1].kind else {
            panic!("not a decision");
        };
        assert_eq!(otherwise, "paid");
        assert!(when[0].0.holds(&json!({"total": 1500})));
        assert!(!when[0].0.holds(&json!({"total": 500})));
    }

    #[test]
    fn every_check_names_the_process_the_step_and_why() {
        let with = |f: &dyn Fn(&mut Value)| {
            let mut m = example();
            f(&mut m);
            reason(&m)
        };
        let cases: Vec<(String, &str)> = vec![
            (
                with(&|m| m["name"] = json!("fulfilment")),
                "process fulfilment: the name is not a qualified",
            ),
            (
                with(&|m| m["extra"] = json!(1)),
                "process shop.fulfilment: unknown field extra",
            ),
            (
                with(&|m| m["start"]["on"] = json!("OrderSubmitted")),
                "process shop.fulfilment, start: on OrderSubmitted is not a qualified type",
            ),
            (
                with(&|m| m["start"]["key"] = json!("case.orderId")),
                "process shop.fulfilment, start: case.orderId: here a reference is one of aggregateId, data.<field>",
            ),
            (
                with(&|m| m["steps"][0]["kind"] = json!("send")),
                "process shop.fulfilment, step confirm: kind send is none of",
            ),
            (
                with(&|m| m["steps"][0]["next"] = json!("nowhere")),
                "process shop.fulfilment, step confirm: goes on to nowhere, which is no step",
            ),
            (
                with(&|m| m["steps"][0]["command"] = json!("shop.Confirm")),
                "process shop.fulfilment, step confirm: command shop.Confirm is not domain.aggregateType.Command",
            ),
            (
                with(&|m| m["steps"][0]["aggregate"] = json!("data.orderId")),
                "process shop.fulfilment, step confirm: data.orderId: here a reference is one of case.<field>",
            ),
            (
                with(&|m| m["steps"][0]["aggregate"] = json!("case.")),
                "step confirm: the reference case. has an invalid field",
            ),
            (
                with(&|m| m["steps"][0]["colour"] = json!("red")),
                "step confirm: unknown field colour",
            ),
            (
                with(&|m| m["steps"][1]["when"][0]["if"] = json!("case.total >")),
                "step large: the condition \"case.total >\": the condition ends too early",
            ),
            (
                with(&|m| m["steps"][1]["when"][0]["if"] = json!("total > 1000")),
                "step large: the condition \"total > 1000\": total is no case.<field> reference",
            ),
            (
                with(&|m| m["steps"][4]["timeoutMs"] = json!(0)),
                "step paid: timeoutMs is not a positive number of milliseconds",
            ),
            (
                with(&|m| {
                    m["steps"][4].as_object_mut().unwrap().remove("onTimeout");
                }),
                "step paid: timeoutMs without onTimeout",
            ),
            (
                with(&|m| m["steps"][2]["paths"][0]["value"] = json!(null)),
                "step approval: a path's value is a boolean, a string or a number",
            ),
            (
                with(&|m| m["steps"][1]["name"] = json!("confirm")),
                "step confirm: a second step of this name",
            ),
            (
                with(&|m| m["steps"][1]["name"] = json!("big one")),
                "step 2 has the name \"big one\"",
            ),
            (
                with(&|m| m["steps"][8]["next"] = json!("done")),
                "step done: unknown field next",
            ),
            (
                with(&|m| {
                    m["steps"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"name": "lost", "kind": "end"}));
                }),
                "step lost: no path from the start reaches it",
            ),
            (
                with(&|m| m["steps"] = json!([])),
                "process shop.fulfilment: no steps",
            ),
            (
                with(&|m| m["steps"][0]["data"] = json!({"x": 5})),
                "step confirm: data.x is neither a reference nor a literal",
            ),
        ];
        for (got, want) in cases {
            assert!(got.contains(want), "{got:?} does not contain {want:?}");
        }
        assert!(
            parse(b"[1]")
                .unwrap_err()
                .reason
                .contains("not a JSON object")
        );
        assert!(parse(b"{").unwrap_err().reason.contains("not JSON"));
    }

    #[test]
    fn timers_take_one_of_after_and_at() {
        let model = |timer: Value| {
            json!({"name": "shop.t", "start": {"on": "shop.A", "key": "aggregateId", "data": {"due": "data.due"}},
                   "steps": [timer, {"name": "done", "kind": "end"}]})
        };
        let ok = check(&model(
            json!({"name": "t", "kind": "timer", "afterMs": 1000, "next": "done"}),
        ));
        assert!(ok.is_ok());
        let at = check(&model(
            json!({"name": "t", "kind": "timer", "at": "case.due", "next": "done"}),
        ));
        assert!(at.unwrap().1.is_empty());
        let both = reason(&model(
            json!({"name": "t", "kind": "timer", "afterMs": 1, "at": "case.due", "next": "done"}),
        ));
        assert!(both.contains("either afterMs or at"), "{both}");
        let negative = reason(&model(
            json!({"name": "t", "kind": "timer", "afterMs": -5, "next": "done"}),
        ));
        assert!(negative.contains("positive"), "{negative}");
    }

    #[test]
    fn a_case_field_not_written_on_every_path_is_a_warning() {
        let mut m = example();
        // `paidAt` is written by the wait `paid` only: `cancel` comes after a timeout of it.
        m["steps"][7]["data"] = json!({"reason": "case.paidAt"});
        m["steps"][5]["data"] = json!({"paidAt": "case.paidAt"});
        let (_, warnings) = check(&m).unwrap();
        assert_eq!(
            warnings,
            [
                "process shop.fulfilment, step cancel: case.paidAt is not written on every path before it"
            ]
        );
        m["steps"][0]["aggregate"] = json!("case.nothing");
        let (_, warnings) = check(&m).unwrap();
        assert!(
            warnings[0].contains("step confirm: case.nothing"),
            "{warnings:?}"
        );
    }

    #[test]
    fn the_canonical_form_sorts_keys_and_drops_whitespace() {
        let a: Value =
            serde_json::from_str(r#"{ "b": [1, {"y": 2, "x": "é"}], "a": null }"#).unwrap();
        assert_eq!(
            String::from_utf8(canonical(&a)).unwrap(),
            r#"{"a":null,"b":[1,{"x":"é","y":2}]}"#
        );
        let b: Value = serde_json::from_str(r#"{"a":null,"b":[1,{"x":"é","y":2}]}"#).unwrap();
        assert_eq!(canonical(&a), canonical(&b));
    }

    #[test]
    fn conditions_parse_with_precedence() {
        let c = |s: &str| parse_condition(s).unwrap();
        let r = |p: &str| Box::new(Expr::Ref(vec![p.to_owned()]));
        let n = |v: i64| Box::new(Expr::Lit(json!(v)));
        assert_eq!(
            c("case.total > 1000"),
            Expr::Cmp(Op::Gt, r("total"), n(1000))
        );
        // not binds tighter than and, and than or.
        assert_eq!(
            c("not case.a or case.b and case.c"),
            Expr::Or(
                Box::new(Expr::Not(r("a"))),
                Box::new(Expr::And(r("b"), r("c")))
            )
        );
        assert_eq!(
            c("(case.a or case.b) and case.c"),
            Expr::And(Box::new(Expr::Or(r("a"), r("b"))), r("c"))
        );
        assert_eq!(
            c(r#"case.addr.country == 'AT' and case.n != -2.5e1"#),
            Expr::And(
                Box::new(Expr::Cmp(
                    Op::Eq,
                    Box::new(Expr::Ref(vec!["addr".into(), "country".into()])),
                    Box::new(Expr::Lit(json!("AT")))
                )),
                Box::new(Expr::Cmp(Op::Ne, r("n"), Box::new(Expr::Lit(json!(-25.0)))))
            )
        );
        assert_eq!(c(r#""it's""#), Expr::Lit(json!("it's")));
        assert_eq!(c(r"'a\'b'"), Expr::Lit(json!("a'b")));
        assert_eq!(
            c("case.x <= null"),
            Expr::Cmp(Op::Le, r("x"), Box::new(Expr::Lit(Value::Null)))
        );
        for (bad, why) in [
            ("", "empty"),
            ("case.a >", "ends too early"),
            ("case.a = 1", "no operator"),
            ("case.a > 1 > 2", "unexpected an operator"),
            ("(case.a", "not closed"),
            ("case.a)", "unexpected )"),
            ("data.a == 1", "no case.<field>"),
            ("case.a == 'x", "not closed"),
            ("case.a + 1", "not allowed"),
            ("and case.a", "found and"),
            ("1.2.3 == 1", "not a number"),
        ] {
            let e = parse_condition(bad).unwrap_err();
            assert!(e.contains(why), "{bad:?}: {e}");
        }
    }

    #[test]
    fn conditions_evaluate_on_the_case_data() {
        let case = json!({"total": 1500, "rate": 0.5, "country": "AT", "vip": true, "tags": [],
                          "addr": {"zip": "1010"}, "none": null});
        let holds = |s: &str| parse_condition(s).unwrap().holds(&case);
        assert!(holds("case.total > 1000"));
        assert!(holds("case.total >= 1500 and case.total <= 1500.0"));
        assert!(holds("case.total == 1500.0"), "numbers compare by value");
        assert!(holds("case.rate < 1"));
        assert!(holds("case.country == 'AT' and case.addr.zip == \"1010\""));
        assert!(holds("case.country < 'BE'"), "strings order");
        assert!(holds("case.vip"));
        assert!(!holds("case.tags"), "an empty array is false");
        assert!(holds("not case.missing"));
        assert!(holds("case.missing == null and case.none == null"));
        assert!(
            !holds("case.missing > 0"),
            "an ordering with a missing field is false"
        );
        assert!(!holds("case.missing < 0"));
        assert!(
            !holds("case.country > 1"),
            "a string and a number do not order"
        );
        assert!(holds("case.country != 1"));
        assert!(holds("case.vip == true or case.total < 0"));
        assert!(!holds("not (case.vip or false)"));
        assert!(holds("0 == 0 and not 0 and not ''"));
    }

    #[test]
    fn cancel_on_names_an_end_step_and_makes_it_reachable() {
        let model = |cancel: Value| {
            json!({"name": "shop.f", "start": {"on": "shop.OrderPlaced", "key": "aggregateId"},
                   "steps": [
                       {"name": "w", "kind": "wait", "event": "shop.OrderPaid", "key": "aggregateId", "next": "done"},
                       {"name": "done", "kind": "end"},
                       {"name": "withdrawn", "kind": "end"}],
                   "cancelOn": cancel})
        };
        let ok = model(
            json!([{"event": "shop.OrderCancelled", "key": "aggregateId", "end": "withdrawn"}]),
        );
        let (m, _) = parse(&serde_json::to_vec(&ok).unwrap()).unwrap();
        assert_eq!(m.cancel_on[0].end, "withdrawn");
        assert!(m.events().any(|e| e == "shop.OrderCancelled"));
        let not_end =
            model(json!([{"event": "shop.OrderCancelled", "key": "aggregateId", "end": "w"}]));
        assert!(
            parse(&serde_json::to_vec(&not_end).unwrap())
                .unwrap_err()
                .reason
                .contains("no end step")
        );
        let twice = model(json!([
            {"event": "shop.OrderCancelled", "key": "aggregateId", "end": "withdrawn"},
            {"event": "shop.OrderCancelled", "key": "aggregateId", "end": "done"}]));
        assert!(parse(&serde_json::to_vec(&twice).unwrap()).is_err());
        // Without cancelOn, `withdrawn` is reached by nothing.
        let mut none = ok.clone();
        none.as_object_mut().unwrap().remove("cancelOn");
        assert!(
            parse(&serde_json::to_vec(&none).unwrap())
                .unwrap_err()
                .reason
                .contains("no path")
        );
    }
}
