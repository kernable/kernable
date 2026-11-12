//! The check before switching (docs/how-to/deployments.md, "Check a deployment before
//! switching"): `kernable replay` sends the commands a source stored after a point, in their
//! original order and with their original times, to a kernel on a masked copy of that point
//! (`--replay on`) where the new deployment's workers run, and compares each outcome with the
//! original one: the events (types, schemas, number, payload fields) or the rejection code.
//!
//! **Same code, same events.** The kernel of the check runs each command as if received at its
//! original time (`SubmitRequest.received_at`), and the command keeps its ID, so `ctx.now()` and
//! `ctx.id()` give the original values: equal code writes equal events.
//!
//! **Not sent, reported:** scheduled commands and process steps (the kernel of the check fires
//! neither; they ran at their own time in the source), commands that carry vault values (the
//! copy holds none of the source's, and a placeholder would change both the command and what the
//! code decides on it), commands the source has not completed. Once one of these touched an
//! aggregate, the aggregate's later commands are not sent either: they would run on another
//! state than they did. Personal values the copy reads as `null` in an aggregate's history
//! (masked) are compared as equal to the original's vault references, and a difference on such
//! an aggregate is marked `masked`.
//!
//! **No personal data in the report:** command IDs, types and aggregate types, event types and
//! schemas, rejection codes and the names of payload fields, never values, aggregate IDs or
//! rejection messages.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, Instant},
};

use kernable_core::proto::{
    Command, Event, Grant, SubmitRequest, Wait, command_result::Outcome,
    command_service_client::CommandServiceClient,
};
use kernable_store::{Store, StreamId};
use prost::Message;
use serde_json::{Value, json};
use tonic::{Code, Status, transport::Channel};

use crate::{BoxError, GRANT_HEADER, MAX_MESSAGE_BYTES, config, deployments, process_engine};

/// The most entries a list of the report holds; its count says how many there were.
const LISTED: usize = 1000;
/// How long one command may take, unavailable or not completed yet, before the check stops.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);
/// The most payload fields a difference names per kind.
const FIELDS: usize = 10;

/// What a submit may carry, by whether the kernel runs a replay check (`--replay on`).
pub(crate) fn submitted(replay: bool, received_at: i64, cmd: &Command) -> Result<(), Status> {
    match (replay, received_at > 0) {
        (false, true) => Err(Status::failed_precondition(
            "received_at is only for a kernel started with --replay on (a check on a masked copy)",
        )),
        (true, false) => Err(Status::failed_precondition(
            "replay_only: this kernel runs a replay check (--replay on): it takes only the \
             commands kernable replay sends",
        )),
        (true, true) if cmd.deliver_at > 0 => Err(Status::failed_precondition(
            "a replay check (--replay on) fires no scheduled command: kernable replay reports them",
        )),
        _ => Ok(()),
    }
}

/// The point the copy was made at.
#[derive(Debug, Clone, Copy)]
pub enum Since {
    /// An event position: the copy's mark names it.
    Position(u64),
    /// A time (ms since epoch): the last event before it.
    Time(i64),
}

/// How many differences (and errors) pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Threshold {
    Count(u64),
    /// Percent of the commands replayed.
    Percent(u64),
}

impl Threshold {
    /// `5` or `5%`.
    pub fn parse(s: &str) -> Option<Threshold> {
        match s.strip_suffix('%') {
            Some(p) => p.parse().ok().filter(|p| *p <= 100).map(Threshold::Percent),
            None => s.parse().ok().map(Threshold::Count),
        }
    }

    fn passes(self, failed: u64, replayed: u64) -> bool {
        match self {
            Threshold::Count(n) => failed <= n,
            Threshold::Percent(p) => failed * 100 <= p * replayed,
        }
    }

    fn describe(self) -> String {
        match self {
            Threshold::Count(n) => n.to_string(),
            Threshold::Percent(p) => format!("{p}%"),
        }
    }
}

pub struct Options {
    /// The source, as the report names it.
    pub source: String,
    /// The kernel of the check, as the report names it.
    pub target: String,
    pub since: Since,
    /// At most this many commands after the point (the oldest ones).
    pub limit: Option<usize>,
    /// The deployment whose workers must write the replayed events.
    pub deployment: String,
    pub max_differences: Threshold,
}

/// An outcome, original or replayed.
#[derive(Debug, Clone)]
enum Result_ {
    Committed(Vec<Event>),
    Rejected(String),
    /// The kernel of the check refused the submit (a gRPC status), e.g. `NOT_FOUND`: no worker
    /// of the new deployment runs the command any more.
    Refused(String, String),
}

impl Result_ {
    fn summary(&self) -> Value {
        match self {
            Result_::Committed(events) => json!({
                "committed": events
                    .iter()
                    .map(|e| json!({ "type": qualified(e), "schema": e.schema }))
                    .collect::<Vec<_>>()
            }),
            Result_::Rejected(code) => json!({ "rejected": code }),
            Result_::Refused(code, message) => json!({ "refused": code, "message": message }),
        }
    }

    fn describe(&self) -> String {
        match self {
            Result_::Committed(events) if events.is_empty() => "committed no events".into(),
            Result_::Committed(events) => format!("committed {}", types(events)),
            Result_::Rejected(code) => format!("rejected {code}"),
            Result_::Refused(code, message) => format!("refused {code} ({message})"),
        }
    }
}

fn qualified(e: &Event) -> String {
    format!("{}.{}", e.domain, e.event_type)
}

fn types(events: &[Event]) -> String {
    events
        .iter()
        .map(|e| format!("{}/{}", qualified(e), e.schema))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Runs the check: reads the commands of `source` after the point, sends those it can to the
/// kernel of the check on `channel`, and returns the report (see [`text`]). Fails, without a
/// report, when the kernel is not one of a replay check, when it stays unavailable, or when its
/// events come from another deployment than `options.deployment`.
pub async fn run(
    source: &dyn Store,
    channel: Channel,
    options: &Options,
) -> Result<Value, BoxError> {
    let head = source.head_position()?;
    let from = match options.since {
        Since::Position(p) => p.min(head),
        Since::Time(t) => deployments::position_at(source, t)?,
    };
    // Commands without events are placed by when they arrived: after the event at the point.
    let from_time = match from {
        0 => i64::MIN,
        p => source
            .read_all(p - 1, 1)?
            .first()
            .map_or(i64::MIN, |e| e.created_at),
    };
    let schemas: HashMap<String, u32> = source.event_schemas()?.into_iter().collect();
    let mut grants: HashMap<String, Grant> = HashMap::new();
    for g in source.grants()? {
        if grants
            .get(&g.key_id)
            .is_none_or(|held| held.issued_at < g.issued_at)
        {
            grants.insert(g.key_id.clone(), g);
        }
    }
    let mut client = CommandServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);

    let mut r = Tally::default();
    // Aggregates a command touched that was not sent, and those with a difference.
    let mut tainted: HashSet<StreamId> = HashSet::new();
    let mut differed: HashSet<StreamId> = HashSet::new();
    for id in source.command_ids()? {
        let Some(pending) = source.command(&id)? else {
            continue;
        };
        let original = match source.result(&id)?.and_then(|r| r.outcome) {
            Some(Outcome::Committed(c)) => Some(Result_::Committed(c.events)),
            Some(Outcome::Rejected(r)) => Some(Result_::Rejected(r.code)),
            _ => None,
        };
        let after = match &original {
            Some(Result_::Committed(events)) if !events.is_empty() => {
                events[0].global_position > from
            }
            _ => pending.received_at > from_time,
        };
        if !after {
            continue;
        }
        if options
            .limit
            .is_some_and(|limit| r.commands >= limit as u64)
        {
            break;
        }
        r.commands += 1;
        let cmd = pending.command;
        let stream = StreamId {
            tenant_id: cmd.tenant_id.clone(),
            domain: cmd.domain.clone(),
            aggregate_type: cmd.aggregate_type.clone(),
            aggregate_id: cmd.aggregate_id.clone(),
        };
        let entry = |extra: Value| {
            let mut e = json!({
                "commandId": cmd.command_id,
                "command": format!("{}.{}", cmd.domain, cmd.command_type),
                "aggregate": format!("{}.{}", cmd.domain, cmd.aggregate_type),
            });
            if let (Some(e), Value::Object(extra)) = (e.as_object_mut(), extra) {
                e.extend(extra);
            }
            e
        };
        let Some(original) = original else {
            r.skip(entry(json!({ "reason": "notCompleted" })));
            continue;
        };
        if cmd.command_id.starts_with("proc-") || cmd.domain == process_engine::DOMAIN {
            r.process_steps += 1;
            push(
                &mut r.process_list,
                entry(json!({ "original": original.summary() })),
            );
            tainted.insert(stream);
            continue;
        }
        if cmd.deliver_at > 0 {
            r.scheduled += 1;
            push(
                &mut r.scheduled_list,
                entry(json!({
                    "deliverAt": config::utc(cmd.deliver_at),
                    "original": original.summary(),
                })),
            );
            tainted.insert(stream);
            continue;
        }
        if !cmd.vault_refs.is_empty() {
            r.skip(entry(json!({ "reason": "personal" })));
            tainted.insert(stream);
            continue;
        }
        if tainted.contains(&stream) {
            r.skip(entry(json!({ "reason": "afterSkipped" })));
            continue;
        }

        let key = cmd.client_sig.as_ref().map(|s| s.key_id.as_str());
        let grant = key.and_then(|k| grants.get(k));
        let replayed = send(&mut client, &cmd, pending.received_at, grant, options).await?;
        r.replayed += 1;
        if let Result_::Committed(events) = &replayed {
            for e in events {
                if e.deployment != options.deployment {
                    return Err(format!(
                        "{}: command {} wrote events of deployment {:?}, not {}: the copy's \
                         membership must name the deployment to check (kernable apply … \
                         --deployment {} --force)",
                        options.target,
                        cmd.command_id,
                        e.deployment,
                        options.deployment,
                        options.deployment
                    )
                    .into());
                }
                let t = qualified(e);
                let known = schemas.get(&t).copied();
                if known.is_none_or(|max| e.schema > max) {
                    *r.new_types.entry((t, e.schema)).or_default() += 1;
                }
            }
        }
        if let Result_::Rejected(code) = &replayed
            && !matches!(&original, Result_::Rejected(c) if c == code)
        {
            *r.new_rejections.entry(code.clone()).or_default() += 1;
        }
        let changes = compare(&original, &replayed);
        if changes.is_empty() {
            r.identical += 1;
            continue;
        }
        match replayed {
            Result_::Refused(..) => r.errors += 1,
            _ => r.different += 1,
        }
        let masked = masked(source, &stream, &original)?;
        push(
            &mut r.differences,
            entry(json!({
                "original": original.summary(),
                "replayed": replayed.summary(),
                "changes": changes,
                "afterDifference": differed.contains(&stream),
                "masked": masked,
            })),
        );
        differed.insert(stream);
    }

    let failed = r.different + r.errors;
    let passed = options.max_differences.passes(failed, r.replayed);
    Ok(json!({
        "source": options.source,
        "target": options.target,
        "deployment": options.deployment,
        "from": from,
        "head": head,
        "commands": r.commands,
        "replayed": r.replayed,
        "identical": r.identical,
        "different": r.different,
        "errors": r.errors,
        "skipped": r.skipped,
        "scheduled": r.scheduled,
        "processSteps": r.process_steps,
        "newTypes": r.new_types.iter().map(|((t, schema), n)| json!({
            "type": t,
            "schema": schema,
            "newType": !schemas.contains_key(t),
            "newSchema": schemas.contains_key(t),
            "commands": n,
        })).collect::<Vec<_>>(),
        "newRejections": r.new_rejections.iter().map(|(code, n)| json!({
            "code": code,
            "commands": n,
        })).collect::<Vec<_>>(),
        "differences": r.differences,
        "skippedCommands": r.skipped_list,
        "scheduledCommands": r.scheduled_list,
        "processStepCommands": r.process_list,
        "maxDifferences": options.max_differences.describe(),
        "passed": passed,
    }))
}

#[derive(Default)]
struct Tally {
    commands: u64,
    replayed: u64,
    identical: u64,
    different: u64,
    errors: u64,
    skipped: u64,
    scheduled: u64,
    process_steps: u64,
    new_types: BTreeMap<(String, u32), u64>,
    new_rejections: BTreeMap<String, u64>,
    differences: Vec<Value>,
    skipped_list: Vec<Value>,
    scheduled_list: Vec<Value>,
    process_list: Vec<Value>,
}

impl Tally {
    fn skip(&mut self, entry: Value) {
        self.skipped += 1;
        push(&mut self.skipped_list, entry);
    }
}

fn push(list: &mut Vec<Value>, entry: Value) {
    if list.len() < LISTED {
        list.push(entry);
    }
}

/// Sends `cmd` to the kernel of the check as received at `received_at`, with the grant its key
/// had in the source, and waits until it completed.
async fn send(
    client: &mut CommandServiceClient<Channel>,
    cmd: &Command,
    received_at: i64,
    grant: Option<&Grant>,
    options: &Options,
) -> Result<Result_, BoxError> {
    let started = Instant::now();
    loop {
        let mut request = tonic::Request::new(SubmitRequest {
            command: Some(cmd.clone()),
            wait: Wait::Committed as i32,
            protocol: kernable_core::PROTOCOL,
            received_at,
            ..Default::default()
        });
        if let Some(grant) = grant {
            request
                .metadata_mut()
                .insert(GRANT_HEADER, hex::encode(grant.encode_to_vec()).parse()?);
        }
        let status = match client.submit(request).await {
            Ok(response) => {
                return match response.into_inner().outcome {
                    Some(Outcome::Committed(c)) => Ok(Result_::Committed(c.events)),
                    Some(Outcome::Rejected(r)) => Ok(Result_::Rejected(r.code)),
                    _ => Err(format!(
                        "{}: command {} was accepted, not run",
                        options.target, cmd.command_id
                    )
                    .into()),
                };
            }
            Err(status) => status,
        };
        let again = matches!(
            status.code(),
            Code::DeadlineExceeded | Code::Unavailable | Code::ResourceExhausted
        );
        if status.code() == Code::FailedPrecondition && status.message().contains("--replay on") {
            return Err(format!(
                "{}: {} (start the kernel of the check on a masked copy with --replay on)",
                options.target,
                status.message()
            )
            .into());
        }
        if !again {
            return Ok(Result_::Refused(
                code_name(status.code()).into(),
                status.message().chars().take(200).collect(),
            ));
        }
        if started.elapsed() > COMMAND_TIMEOUT {
            return Err(format!(
                "{}: command {} did not complete within {} s: {}",
                options.target,
                cmd.command_id,
                COMMAND_TIMEOUT.as_secs(),
                status.message()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::Unauthenticated => "UNAUTHENTICATED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::DataLoss => "DATA_LOSS",
        Code::Cancelled => "CANCELLED",
        _ => "UNKNOWN",
    }
}

/// What differs between the original and the replayed outcome, for people; empty: nothing.
fn compare(original: &Result_, replayed: &Result_) -> Vec<String> {
    match (original, replayed) {
        (Result_::Committed(a), Result_::Committed(b)) => {
            let shape = |events: &[Event]| -> Vec<(String, u32)> {
                events.iter().map(|e| (qualified(e), e.schema)).collect()
            };
            if shape(a) != shape(b) {
                let list = |events: &[Event]| match events {
                    [] => "none".to_owned(),
                    events => types(events),
                };
                return vec![format!("events: {} -> {}", list(a), list(b))];
            }
            let mut changes = Vec::new();
            for (a, b) in a.iter().zip(b) {
                if let Some(change) = payload_change(a, b) {
                    changes.push(change);
                }
                match (a.erases.is_empty(), b.erases.is_empty()) {
                    (false, true) => {
                        changes.push(format!("{}: erases a subject -> none", qualified(a)))
                    }
                    (true, false) => {
                        changes.push(format!("{}: erases none -> a subject", qualified(a)))
                    }
                    (false, false) if a.erases != b.erases => {
                        changes.push(format!("{}: erases another subject", qualified(a)))
                    }
                    _ => {}
                }
            }
            changes
        }
        (Result_::Rejected(a), Result_::Rejected(b)) if a == b => Vec::new(),
        (Result_::Rejected(a), Result_::Rejected(b)) => vec![format!("rejection: {a} -> {b}")],
        (a, b) => vec![format!("outcome: {} -> {}", a.describe(), b.describe())],
    }
}

/// The payload fields that differ between two events of the same type, by name only.
fn payload_change(a: &Event, b: &Event) -> Option<String> {
    if a.payload == b.payload {
        return None;
    }
    let parse = |bytes: &[u8]| serde_json::from_slice::<Value>(bytes).ok();
    let (Some(x), Some(y)) = (parse(&a.payload), parse(&b.payload)) else {
        return Some(format!("payload of {}: differs (not JSON)", qualified(a)));
    };
    let mut d = Fields::default();
    diff(&x, &y, "", &mut d);
    if d.changed.is_empty() && d.added.is_empty() && d.removed.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for (what, fields) in [
        ("changed", &d.changed),
        ("added", &d.added),
        ("removed", &d.removed),
    ] {
        if fields.is_empty() {
            continue;
        }
        let mut names: Vec<&str> = fields.iter().take(FIELDS).map(String::as_str).collect();
        let more = fields.len().saturating_sub(FIELDS);
        let more = (more > 0).then(|| format!("{more} more"));
        names.extend(more.as_deref());
        parts.push(format!("{what} {}", names.join(", ")));
    }
    Some(format!("payload of {}: {}", qualified(a), parts.join("; ")))
}

#[derive(Default)]
struct Fields {
    changed: Vec<String>,
    added: Vec<String>,
    removed: Vec<String>,
}

/// A personal value: `{"$vault": ref}`.
fn vault(v: &Value) -> bool {
    v.as_object()
        .is_some_and(|o| o.len() == 1 && o.contains_key("$vault"))
}

fn diff(a: &Value, b: &Value, path: &str, out: &mut Fields) {
    let at = |key: &str| match path {
        "" => key.to_owned(),
        p => format!("{p}.{key}"),
    };
    let here = || match path {
        "" => "(the whole payload)".to_owned(),
        p => p.to_owned(),
    };
    // Masked: the copy reads the source's personal values as null, and new ones get new refs.
    if (vault(a) || vault(b)) && (vault(a) || a.is_null()) && (vault(b) || b.is_null()) {
        return;
    }
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            for (k, v) in x {
                match y.get(k) {
                    Some(w) => diff(v, w, &at(k), out),
                    None if vault(v) => {}
                    None => out.removed.push(at(k)),
                }
            }
            for k in y.keys().filter(|k| !x.contains_key(*k)) {
                out.added.push(at(k));
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            for (i, (v, w)) in x.iter().zip(y).enumerate() {
                diff(v, w, &format!("{path}[{i}]"), out);
            }
            for i in y.len()..x.len() {
                out.removed.push(format!("{path}[{i}]"));
            }
            for i in x.len()..y.len() {
                out.added.push(format!("{path}[{i}]"));
            }
        }
        (x, y) if x != y => out.changed.push(here()),
        _ => {}
    }
}

/// Whether the aggregate held personal values when the command originally ran, which the copy
/// reads as null: a difference there may come from the masking, not from the code.
fn masked(source: &dyn Store, stream: &StreamId, original: &Result_) -> Result<bool, BoxError> {
    let before = match original {
        Result_::Committed(events) => {
            if events.iter().any(|e| !e.vault_refs.is_empty()) {
                return Ok(true);
            }
            events.first().map_or(u64::MAX, |e| e.global_position)
        }
        _ => u64::MAX,
    };
    Ok(source
        .read_stream(
            &stream.tenant_id,
            &stream.domain,
            &stream.aggregate_type,
            &stream.aggregate_id,
        )?
        .iter()
        .any(|e| e.global_position < before && !e.vault_refs.is_empty()))
}

/// The report for people.
pub fn text(report: &Value) -> String {
    use std::fmt::Write;
    let n = |field: &str| report[field].as_u64().unwrap_or_default();
    let s = |v: &Value| v.as_str().unwrap_or_default().to_owned();
    let empty = Vec::new();
    let list = |field: &str| report[field].as_array().unwrap_or(&empty);
    let plural = |n: u64, one: &str, many: &str| match n {
        1 => format!("1 {one}"),
        n => format!("{n} {many}"),
    };
    let mut out = String::new();
    let _ = writeln!(
        out,
        "replayed the commands of {} after position {} (up to {}) on {}, deployment {}",
        s(&report["source"]),
        n("from"),
        n("head"),
        s(&report["target"]),
        s(&report["deployment"]),
    );
    let _ = writeln!(
        out,
        "  {}: {} replayed ({} identical, {} different, {}), {} skipped, not sent: {} and {}",
        plural(n("commands"), "command", "commands"),
        n("replayed"),
        n("identical"),
        n("different"),
        plural(n("errors"), "error", "errors"),
        n("skipped"),
        plural(n("scheduled"), "scheduled command", "scheduled commands"),
        plural(n("processSteps"), "process step", "process steps"),
    );
    if !list("newTypes").is_empty() {
        let _ = writeln!(out, "new event types and schemas:");
        for t in list("newTypes") {
            let mark = if t["newType"] == true {
                "NEW TYPE"
            } else {
                "NEW SCHEMA"
            };
            let _ = writeln!(
                out,
                "  {} schema {}  {mark}  ({})",
                s(&t["type"]),
                t["schema"],
                plural(
                    t["commands"].as_u64().unwrap_or_default(),
                    "command",
                    "commands"
                )
            );
        }
    }
    if !list("newRejections").is_empty() {
        let _ = writeln!(out, "new rejections:");
        for r in list("newRejections") {
            let _ = writeln!(
                out,
                "  {}  ({})",
                s(&r["code"]),
                plural(
                    r["commands"].as_u64().unwrap_or_default(),
                    "command",
                    "commands"
                )
            );
        }
    }
    let line = |e: &Value| {
        format!(
            "  {} {} on {}",
            s(&e["commandId"]),
            s(&e["command"]),
            s(&e["aggregate"])
        )
    };
    const SHOWN: usize = 20;
    let more = |out: &mut String, total: usize| {
        if total > SHOWN {
            let _ = writeln!(out, "  … {} more (--json lists them)", total - SHOWN);
        }
    };
    if !list("differences").is_empty() {
        let _ = writeln!(out, "differences:");
        for d in list("differences").iter().take(SHOWN) {
            let mut notes = Vec::new();
            if d["afterDifference"] == true {
                notes.push("after a difference on its aggregate");
            }
            if d["masked"] == true {
                notes.push("its aggregate holds masked personal values");
            }
            let notes = match notes.is_empty() {
                true => String::new(),
                false => format!(" [{}]", notes.join("; ")),
            };
            let changes: Vec<String> = d["changes"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .map(s)
                .collect();
            let _ = writeln!(out, "{}: {}{notes}", line(d), changes.join("; "));
        }
        more(&mut out, list("differences").len());
    }
    if !list("skippedCommands").is_empty() {
        let _ = writeln!(out, "skipped:");
        for c in list("skippedCommands").iter().take(SHOWN) {
            let why = match c["reason"].as_str() {
                Some("personal") => "carries personal values, which the copy does not hold",
                Some("afterSkipped") => "follows a command on its aggregate that was not sent",
                _ => "not completed in the source",
            };
            let _ = writeln!(out, "{}: {why}", line(c));
        }
        more(&mut out, list("skippedCommands").len());
    }
    for (field, what) in [
        ("scheduledCommands", "scheduled commands, not sent"),
        ("processStepCommands", "process steps, not sent"),
    ] {
        if list(field).is_empty() {
            continue;
        }
        let _ = writeln!(out, "{what}:");
        for c in list(field).iter().take(SHOWN) {
            let at = c["deliverAt"]
                .as_str()
                .map_or(String::new(), |at| format!(" due {at}"));
            let original = summary_text(&c["original"]);
            let _ = writeln!(out, "{}{at}: {original}", line(c));
        }
        more(&mut out, list(field).len());
    }
    let failed = n("different") + n("errors");
    let _ = match report["passed"] == true {
        true => writeln!(
            out,
            "OK: {} (at most {})",
            plural(failed, "difference", "differences"),
            s(&report["maxDifferences"])
        ),
        false => writeln!(
            out,
            "FAILED: {}, more than --max-differences {}",
            plural(failed, "difference", "differences"),
            s(&report["maxDifferences"])
        ),
    };
    out
}

fn summary_text(summary: &Value) -> String {
    if let Some(events) = summary["committed"].as_array() {
        if events.is_empty() {
            return "committed no events".into();
        }
        let list: Vec<String> = events
            .iter()
            .map(|e| format!("{}/{}", e["type"].as_str().unwrap_or_default(), e["schema"]))
            .collect();
        return format!("committed {}", list.join(", "));
    }
    match summary["rejected"].as_str() {
        Some(code) => format!("rejected {code}"),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(t: &str, payload: &str) -> Event {
        Event {
            domain: "shop".into(),
            event_type: t.into(),
            schema: 1,
            payload: payload.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn payloads_differ_by_field_name_never_by_value() {
        let a = event(
            "OrderPlaced",
            r#"{"total":10,"email":{"$vault":"ab"},"items":[{"qty":1}],"note":"x"}"#,
        );
        let b = event(
            "OrderPlaced",
            r#"{"total":12,"email":null,"items":[{"qty":2},{"qty":1}],"discount":1}"#,
        );
        let change = payload_change(&a, &b).unwrap();
        assert_eq!(
            change,
            "payload of shop.OrderPlaced: changed total, items[0].qty; added items[1], discount; removed note"
        );
        assert!(!change.contains("10") && !change.contains("ab"));
        // Masked values and new refs compare equal.
        let a = event("OrderPlaced", r#"{"email":{"$vault":"ab"}}"#);
        let b = event("OrderPlaced", r#"{"email":{"$vault":"cd"}}"#);
        assert_eq!(payload_change(&a, &b), None);
    }

    #[test]
    fn outcomes_compare_by_types_schemas_and_codes() {
        let placed = || Result_::Committed(vec![event("OrderPlaced", "{}")]);
        assert!(compare(&placed(), &placed()).is_empty());
        let mut v2 = event("OrderPlaced", "{}");
        v2.schema = 2;
        assert_eq!(
            compare(&placed(), &Result_::Committed(vec![v2])),
            ["events: shop.OrderPlaced/1 -> shop.OrderPlaced/2"]
        );
        assert_eq!(
            compare(
                &Result_::Rejected("a".into()),
                &Result_::Rejected("b".into())
            ),
            ["rejection: a -> b"]
        );
        assert_eq!(
            compare(&placed(), &Result_::Rejected("b".into())),
            ["outcome: committed shop.OrderPlaced/1 -> rejected b"]
        );
    }

    #[test]
    fn thresholds() {
        assert_eq!(Threshold::parse("3"), Some(Threshold::Count(3)));
        assert_eq!(Threshold::parse("5%"), Some(Threshold::Percent(5)));
        assert_eq!(Threshold::parse("101%"), None);
        assert!(Threshold::Percent(5).passes(5, 100));
        assert!(!Threshold::Percent(5).passes(6, 100));
        assert!(Threshold::Count(0).passes(0, 10));
        assert!(!Threshold::Count(0).passes(1, 10));
    }

    #[test]
    fn submits_by_mode() {
        let cmd = Command::default();
        assert!(submitted(false, 0, &cmd).is_ok());
        assert!(submitted(true, 5, &cmd).is_ok());
        assert!(
            submitted(false, 5, &cmd)
                .unwrap_err()
                .message()
                .contains("--replay on")
        );
        assert!(
            submitted(true, 0, &cmd)
                .unwrap_err()
                .message()
                .starts_with("replay_only")
        );
        let scheduled = Command {
            deliver_at: 9,
            ..Default::default()
        };
        assert!(submitted(true, 5, &scheduled).is_err());
    }
}
