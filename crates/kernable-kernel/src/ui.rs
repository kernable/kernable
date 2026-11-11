//! The kernel's UI in a browser, on `ui_listen` (loopback only): one page (`ui.html`), served at
//! `/` and at each of its views' addresses, its logo (`/logo.png`, `/logo-dark.png`), its icons
//! (`/favicon.png`, `/apple-touch-icon.png`) and the JSON it reads.
//!
//! - `GET /api/state`: the kernel, the services' memberships, the registered aggregates and the
//!   connected workers, the listeners, and which aggregate writes which event type;
//! - `GET /api/health`: the verdict (ok, warn, bad) and every problem behind it, with what
//!   fixes it, as the UI's top bar shows it; and the counts it shows beside (incidents, parked);
//! - `GET /api/watch?topics=&<filters>`: server-sent events, at most one message per topic a
//!   second: the log moved on with the aggregates it changed (`head`), new events, case
//!   changes, commands stored and completed, the verdict, the counts and listener positions
//!   (`pulse`), each filtered; resumes from `Last-Event-ID`; see [`Watch`];
//! - `GET /api/streams?q=&type=&engine=&limit=`: the aggregates with events, the most recently
//!   changed first, those whose tenant, domain, type or ID contains `q` (and of the type
//!   `domain.aggregate` with `type=`); with `summary=1`, per aggregate type how many there are
//!   and when the last one changed;
//! - `GET /api/history?tenant=&domain=&type=&id=`: one aggregate's events with their commands,
//!   and the unique keys (by name and identity, never the value) and facts it holds;
//! - `GET /api/events?…`: events of the whole log that match a filter, newest first, or the
//!   ones after a position (the live tail); see [`events`];
//! - `GET /api/rejections?limit=&brief=&since=`: the most recently received rejected commands,
//!   with why (`brief=1`: without payload and context; `since=`: only how many of the business
//!   since then);
//! - `GET /api/commands?…`: stored commands that match a filter, newest first, each with its
//!   request context and outcome (committed with its events, rejected with why, waiting,
//!   scheduled); see [`commands`];
//! - `GET /api/parked?limit=`: every listener's parked events, not only the first few that
//!   `/api/state` shows;
//! - `GET /api/trend`: the head and every listener's position over the last hour, and the
//!   kernel's counts (events, commands, rejected, queries) once a minute over the last day;
//! - `GET /api/system`: what an operator checks besides the services: replication and the
//!   standby, the backup follower, the stores' size against the quota, the partition, what
//!   limits refused, queued and scheduled commands, and when the kernel started;
//! - `GET /api/processes?window=hour|day|week`: the business processes, each version with its
//!   model, the cases at each step now and the statistics over the window (`process_view.rs`);
//! - `GET /api/cases?process=&version=&step=&state=&text=&limit=`: the cases that match;
//! - `GET /api/case?tenant=&process=&id=`: one case's events, each with what it links to: the
//!   command it sent (its aggregate and outcome) or the event that moved it;
//! - `GET /api/counts?domain=`: per event type of the domain (without: of every domain), how
//!   many the log holds, the newest's time and the last hour per minute; per command type, how
//!   many came in the last hour, and how many of them were rejected; per query type, how many
//!   were asked in the last hour, answered, rejected or failed, and how long they took;
//! - `GET /api/vault` (or `?tenant=&subject=`: one subject): what the vault holds, counted,
//!   never a value: personal and secret values, subjects, per aggregate type, erasures and
//!   those not carried out yet, and how many values the key encrypted against its limit;
//! - `GET /api/snapshots`: per aggregate type, its snapshots (never their state): how many,
//!   their size and schemas, and how many events the aggregates have gained since;
//! - `GET /api/files` (or `?tenant=&domain=&prefix=&after=`: the files there): per tenant and
//!   domain, how many files and their size; never their contents;
//! - `GET /api/cache` (or `?keys=1&tenant=&domain=&text=`: the keys): what the cache holds,
//!   its hits, misses, expired and evicted values since the kernel started; never a value.
//!
//! It never decrypts the vault: a vault field shows its subject, whether it is secret, or that
//! it was erased. Snapshot states are not shown either, as they may hold personal values.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use kernable_core::proto::{Event, RequestContext};
use kernable_store::StreamId;
use serde_json::{Value, json};

use crate::{
    BoxError, Kernel,
    http::reply,
    now_ms,
    process_engine::DOMAIN,
    process_view::{CaseFilter, ENDED, Window},
};

const PAGE: &str = include_str!("ui.html");
/// The logo, for light and for dark backgrounds.
const LOGO: &[u8] = include_bytes!("logo.png");
const LOGO_DARK: &[u8] = include_bytes!("logo-dark.png");

/// The icon for the browser tab, and the larger one for home screens.
const FAVICON: &[u8] = include_bytes!("favicon.png");
const TOUCH_ICON: &[u8] = include_bytes!("apple-touch-icon.png");

/// How many aggregates `/api/streams` lists.
const STREAMS: usize = 500;

/// How many of the most recently changed aggregates a search in `/api/streams` looks through.
const STREAMS_SEARCH: usize = 20_000;

/// How many events `/api/events` returns at most, and by default.
const EVENTS_MAX: usize = 500;
const EVENTS_DEFAULT: usize = 100;

/// How many events one `/api/events` request reads at most, matching or not: a search over a
/// large log goes on page by page, so no request holds the store for long.
const SCAN: u64 = 20_000;

/// How many events `/api/events` reads from the store at once.
const CHUNK: u64 = 1_000;

/// How many rejected commands `/api/rejections` returns at most, and by default.
const REJECTIONS_MAX: usize = 1_000;
const REJECTIONS_DEFAULT: usize = 200;

/// How many parked events `/api/parked` returns at most, over all listeners, and by default.
const PARKED_MAX: usize = 10_000;
const PARKED_DEFAULT: usize = 1_000;

/// Serves the UI until the listener fails.
pub(crate) async fn serve(kernel: Arc<Kernel>, addr: &str) -> Result<(), BoxError> {
    tracing::info!(%addr, "serving the UI");
    crate::http::serve(kernel, addr, respond, |kernel, request| {
        (request.starts_with(b"GET /api/watch ") || request.starts_with(b"GET /api/watch?")).then(
            || {
                let kernel = Arc::clone(kernel);
                let watch = Watch::new(request);
                crate::http::Answer::Stream(Box::new(move |conn| {
                    Box::pin(watch.serve(kernel, conn))
                }))
            },
        )
    })
    .await
}

pub(crate) fn respond(kernel: &Kernel, request: &[u8]) -> Vec<u8> {
    let line = String::from_utf8_lossy(request);
    let target = line
        .strip_prefix("GET ")
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params = parse_query(query);
    if is_page(path) {
        return reply("200 OK", "text/html; charset=utf-8", PAGE.as_bytes());
    }
    let result = match path {
        "/logo.png" => return reply("200 OK", "image/png", LOGO),
        "/logo-dark.png" => return reply("200 OK", "image/png", LOGO_DARK),
        "/favicon.png" | "/favicon.ico" => return reply("200 OK", "image/png", FAVICON),
        "/apple-touch-icon.png" => return reply("200 OK", "image/png", TOUCH_ICON),
        "/api/state" => state(kernel),
        "/api/streams" => streams(kernel, &params),
        "/api/health" => health(kernel),
        "/api/history" => history(kernel, &params),
        "/api/events" => events(kernel, &params),
        "/api/rejections" => rejections(kernel, &params),
        "/api/commands" => commands(kernel, &params),
        "/api/parked" => parked(kernel, &params),
        "/api/trend" => Ok(kernel.trend.view()),
        "/api/system" => Ok(system(kernel)),
        "/api/processes" => processes(kernel, &params),
        "/api/cases" => cases(kernel, &params),
        "/api/case" => case(kernel, &params),
        "/api/counts" => counts(kernel, &params),
        "/api/vault" => vault(kernel, &params),
        "/api/snapshots" => snapshots(kernel),
        "/api/files" => files(kernel, &params),
        "/api/cache" => cache(kernel, &params),
        "/api/deployments" => deployments(kernel),
        "/api/deployments/since" => deployments_since(kernel, &params),
        "/api/deployments/warm-up" => deployments_warm_up(kernel, &params),
        _ => return reply("404 Not Found", "text/plain", b"not found\n"),
    };
    match result {
        Ok(value) => reply("200 OK", "application/json", value.to_string().as_bytes()),
        Err(e) => reply(
            "500 Internal Server Error",
            "application/json",
            json!({ "error": e.to_string() }).to_string().as_bytes(),
        ),
    }
}

/// The page's own addresses: the page decides from the path what to show (`/processes/<process>`,
/// `/processes/<process>/cases/<key>`, `/operations`, `/explore/…`, `/search?q=`).
fn is_page(path: &str) -> bool {
    path == "/"
        || ["/processes", "/operations", "/explore", "/search"]
            .iter()
            .any(|p| {
                path == *p
                    || path
                        .strip_prefix(p)
                        .is_some_and(|rest| rest.starts_with('/'))
            })
}

/// `a=1&b=x%20y` as a map, percent-decoded.
fn parse_query(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (decode(k), decode(v)))
        .collect()
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'+', _) => out.push(b' '),
            (b'%', Some(b)) => {
                out.push(b);
                i += 2;
            }
            (b, _) => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `/api/state`: the kernel, its services and their connections, as the UI's views show them.
fn state(kernel: &Kernel) -> Result<Value, BoxError> {
    let services: Vec<Value> = kernel
        .store
        .memberships()?
        .into_iter()
        .filter_map(|r| {
            let m = r.membership?;
            let metadata: serde_json::Map<String, Value> = m
                .metadata
                .iter()
                .map(|a| (a.key.clone(), Value::String(a.value.clone())))
                .collect();
            Some(json!({
                "service": m.service,
                "workers": m.workers,
                "listeners": m.listeners,
                "middlewares": m.middlewares,
                "metadata": metadata,
                "appliedAt": r.applied_at,
                "key": r.sig.map(|s| s.key_id).unwrap_or_default(),
            }))
        })
        .collect();
    let (aggregates, instances) = kernel.workers.view(&kernel.memberships);
    let mut events: Vec<Value> = Vec::new();
    let sources = kernel.store.event_sources()?;
    for (event_type, schema) in kernel.store.event_schemas()? {
        let writers: Vec<String> = sources
            .iter()
            .filter(|(domain, _, name)| format!("{domain}.{name}") == event_type)
            .map(|(domain, aggregate, _)| format!("{domain}.{aggregate}"))
            .collect();
        events.push(json!({ "type": event_type, "schema": schema, "writers": writers }));
    }
    Ok(json!({
        "kernel": {
            "version": env!("CARGO_PKG_VERSION"),
            "head": kernel.store.head_position()?,
            "queued": kernel.queue.len(),
            "ready": match kernel.readiness() {
                Ok(()) => "ready".to_owned(),
                Err(why) => why,
            },
            "now": now_ms(),
        },
        "services": services,
        "aggregates": aggregates,
        "instances": instances,
        "listeners": kernel.listeners.view(),
        "events": events,
    }))
}

fn system(kernel: &Kernel) -> Value {
    let c = &kernel.config;
    let usage = crate::usage::now(kernel);
    let replication = kernel.replication.as_ref().map(|p| {
        json!({
            "state": p.state_name(),
            "standby": p.standby(),
            "standbyLeft": p.standby_left(),
        })
    });
    json!({
        "now": now_ms(),
        "addr": c.listen,
        "startedAt": kernel.started_at,
        "role": if c.replication.follow.is_some() { "standby" } else { "primary" },
        "replication": {
            "sync": c.replication.sync,
            "witness": c.replication.pair.as_ref().map(|p| p.witness.clone()),
            "primary": replication,
        },
        "backup": {
            "enabled": c.replication.backup,
            "connected": kernel.backup.connected(),
            "acked": kernel.backup.acked(),
        },
        "store": {
            "head": usage.events,
            "bytes": usage.store_bytes,
            "maxBytes": c.limits.store_bytes,
        },
        "counts": {
            "commands": usage.commands,
            "rejected": usage.rejected,
            "queries": usage.queries,
        },
        "queue": {
            "queued": kernel.queue.len(),
            "scheduled": kernel.queue.scheduled_len(),
        },
        "partition": c.partition.as_ref().map(|p| json!({ "name": p.name, "control": p.control.is_none() })),
        "limits": {
            "refused": kernel.metrics.refused(),
            "requestsPerSecondPerKey": c.limits.requests_per_second_per_key,
        },
    })
}

fn streams(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let q = params
        .get("q")
        .map(|v| v.trim().to_lowercase())
        .unwrap_or_default();
    // The process engine's cases (domain `process`) only when asked for.
    let engine = params.get("engine").is_some_and(|v| v == "1");
    // `type=domain.aggregate`: that aggregate type only.
    let only = params.get("type").map_or("", |v| v.trim());
    if params.get("summary").is_some_and(|v| v == "1") {
        return stream_summary(kernel, engine);
    }
    let limit = match params.get("limit").filter(|v| !v.is_empty()) {
        Some(v) => v.parse::<usize>().map_err(|_| "limit: not a number")?,
        None => 50,
    }
    .clamp(1, STREAMS);
    let streams: Vec<Value> = kernel
        .store
        .streams(if q.is_empty() && only.is_empty() && engine {
            limit
        } else {
            STREAMS_SEARCH
        })?
        .into_iter()
        .filter(|s| engine || s.stream.domain != "process")
        .filter(|s| {
            only.is_empty() || format!("{}.{}", s.stream.domain, s.stream.aggregate_type) == only
        })
        .filter(|s| {
            q.is_empty()
                || format!(
                    "{}/{}/{}/{}",
                    s.stream.tenant_id,
                    s.stream.domain,
                    s.stream.aggregate_type,
                    s.stream.aggregate_id
                )
                .to_lowercase()
                .contains(&q)
        })
        .take(limit)
        .map(|s| {
            json!({
                "tenant": s.stream.tenant_id,
                "domain": s.stream.domain,
                "type": s.stream.aggregate_type,
                "id": s.stream.aggregate_id,
                "version": s.version,
                "lastAt": s.last_at,
            })
        })
        .collect();
    Ok(json!({ "streams": streams, "limit": limit }))
}

/// Per aggregate type, how many aggregates the newest [`STREAMS_SEARCH`] changed hold and when
/// the last one changed; `complete` false when there are more than that.
fn stream_summary(kernel: &Kernel, engine: bool) -> Result<Value, BoxError> {
    let all = kernel.store.streams(STREAMS_SEARCH)?;
    let complete = all.len() < STREAMS_SEARCH;
    let mut types: BTreeMap<String, (u64, i64)> = BTreeMap::new();
    for s in all
        .iter()
        .filter(|s| engine || s.stream.domain != "process")
    {
        let t = types
            .entry(format!("{}.{}", s.stream.domain, s.stream.aggregate_type))
            .or_default();
        t.0 += 1;
        t.1 = t.1.max(s.last_at);
    }
    let types: Vec<Value> = types
        .into_iter()
        .map(|(t, (count, last_at))| json!({ "type": t, "count": count, "lastAt": last_at }))
        .collect();
    Ok(json!({ "types": types, "complete": complete, "looked": all.len() }))
}

/// The verdict the UI's top bar shows, computed here so every page asks one small question:
/// a listener missing or behind, parked events, incidents, a standby not in sync, a backup
/// follower away, the store near its quota, a burst of rejections.
fn health(kernel: &Kernel) -> Result<Value, BoxError> {
    let head = kernel.store.head_position()?;
    let c = &kernel.config;
    let mut issues: Vec<Value> = Vec::new();
    let mut add = |level: &str, text: String, sub: String, extra: Value| {
        let mut issue = json!({ "level": level, "text": text, "sub": sub });
        if let (Some(i), Value::Object(more)) = (issue.as_object_mut(), extra) {
            i.extend(more);
        }
        issues.push(issue);
    };
    let listeners = kernel.listeners.view();
    let mut missing = false;
    for l in &listeners {
        if l["live"].as_bool() == Some(true) {
            continue;
        }
        let name = l["name"].as_str().unwrap_or_default();
        let middleware = l["middlewares"].as_array().is_some_and(|m| !m.is_empty());
        let behind = head.saturating_sub(l["position"].as_u64().unwrap_or(head));
        if l["counts"].as_bool() == Some(true) && l["connected"].as_bool() != Some(true) {
            missing = true;
            let (what, why) = if middleware {
                (
                    "Middleware",
                    "The commands it checks are refused until it is back; the kernel is not ready.",
                )
            } else {
                (
                    "Listener",
                    "The kernel waits for it: it is not ready, and commands that settle wait too.",
                )
            };
            add(
                "bad",
                format!("{what} {name} is missing"),
                why.into(),
                json!({ "href": "/operations" }),
            );
        } else if l["connected"].as_bool() == Some(true) && behind >= 100 {
            add(
                "warn",
                format!("Listener {name} is {behind} events behind"),
                "Queries to its read model answer from older data.".into(),
                json!({ "href": "/operations" }),
            );
        }
    }
    if let (Err(why), false) = (kernel.readiness(), missing) {
        add("warn", "Not ready".into(), why, Value::Null);
    }
    let parked = parked(
        kernel,
        &HashMap::from([("limit".to_owned(), PARKED_MAX.to_string())]),
    )?;
    let mut by_listener: std::collections::BTreeMap<String, (u64, String)> = Default::default();
    for p in parked["parked"].as_array().into_iter().flatten() {
        let entry = by_listener
            .entry(p["listener"].as_str().unwrap_or_default().to_owned())
            .or_default();
        entry.0 += 1;
        if entry.1.is_empty() {
            entry.1 = p["message"].as_str().unwrap_or_default().to_owned();
        }
    }
    for (listener, (n, message)) in by_listener {
        add(
            "bad",
            format!(
                "{n} event{} parked in {listener}",
                if n == 1 { "" } else { "s" }
            ),
            if message.is_empty() {
                "a handler gave up on them".into()
            } else {
                message
            },
            json!({ "cmd": format!("kernable listeners retry {} <key> {listener} --all", c.listen) }),
        );
    }
    let filter = crate::process_view::CaseFilter {
        state: "incident".into(),
        ..Default::default()
    };
    let incidents = kernel.process_view.cases(kernel, &filter, 1000)?;
    // Incidents of one process with one code are one problem: listed together, with the
    // process to open; a single one with its case.
    let mut grouped: std::collections::BTreeMap<(String, String), Vec<&Value>> = Default::default();
    for i in incidents["cases"].as_array().into_iter().flatten() {
        let key = (
            i["process"].as_str().unwrap_or_default().to_owned(),
            i["incident"]["code"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        );
        grouped.entry(key).or_default().push(i);
    }
    for ((process, code), cases) in grouped {
        let first = cases[0];
        let message = first["incident"]["message"].as_str().unwrap_or_default();
        let step = first["step"].as_str().unwrap_or_default();
        if let [one] = cases.as_slice() {
            add(
                "bad",
                format!(
                    "Incident in {process}: {}",
                    one["key"].as_str().unwrap_or_default()
                ),
                format!("{code}: {message} · at step {step}"),
                json!({ "case": { "tenant": one["tenant"], "process": one["process"], "id": one["id"] } }),
            );
        } else {
            add(
                "bad",
                format!("{} incidents in {process}: {code}", cases.len()),
                format!("{message} · at step {step}"),
                json!({ "href": format!("/processes/{process}") }),
            );
        }
    }
    if let (true, Some(primary)) = (c.replication.sync, kernel.replication.as_ref()) {
        let state = primary.state_name();
        if state != "in-sync" {
            let (level, sub) = if state == "waiting" {
                ("bad", "Writes wait for a standby that is away.")
            } else {
                ("warn", "Writes are told without a standby holding them.")
            };
            add(level, format!("Standby {state}"), sub.into(), Value::Null);
        }
    }
    if c.replication.backup && kernel.backup.connected().is_none() {
        add(
            "warn",
            "No backup follower connected".into(),
            "--backup on, but nothing follows the kernel now.".into(),
            Value::Null,
        );
    }
    let max = c.limits.store_bytes;
    let held = kernel
        .stored_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && held * 10 >= max * 8 {
        add(
            if held >= max { "bad" } else { "warn" },
            format!("Store at {} % of its quota", held * 100 / max),
            format!("{held} of {max} bytes: at the limit new commands are refused."),
            Value::Null,
        );
    }
    // The vault key past half of what one key may encrypt: time for `kernable vault rekey`.
    if let Some(vault) = &kernel.vault {
        let sealed = vault.sealed()?;
        let limit = kernable_store::vault::MAX_SEALED;
        if sealed > limit / 2 {
            add(
                if sealed * 10 >= limit * 9 { "bad" } else { "warn" },
                format!("Vault key at {} % of its limit", sealed * 100 / limit),
                "It has encrypted more than half the values one key may: plan a `kernable vault rekey`.".into(),
                json!({ "href": "/explore/data/vault" }),
            );
        }
    }
    // A burst of rejections: of the business (not the process engine's own), 5 or more and
    // more than 5 % of the commands in the last 10 minutes.
    let since = now_ms() - 600_000;
    let rejected = kernel
        .store
        .rejections(REJECTIONS_DEFAULT)?
        .iter()
        .filter(|(p, _)| p.received_at >= since && p.command.domain != "process")
        .count() as u64;
    let commands = kernel.trend.commands_since(since);
    if rejected >= 5 && rejected * 20 > commands {
        add(
            "warn",
            format!("{rejected} commands rejected in the last 10 min"),
            format!("of {commands} commands"),
            json!({ "href": "/operations" }),
        );
    }
    let level = if issues.iter().any(|i| i["level"] == "bad") {
        "bad"
    } else if issues.is_empty() {
        "ok"
    } else {
        "warn"
    };
    Ok(json!({
        "level": level,
        "issues": issues,
        "incidents": incidents["total"],
        "parked": parked["total"],
        "head": head,
        "version": env!("CARGO_PKG_VERSION"),
        "addr": c.listen,
        "now": now_ms(),
    }))
}

/// The most events one message of `/api/watch` covers; more since the last, and it says `gap`.
const WATCH_BATCH: u64 = 500;

/// The most commands `/api/watch` follows until they complete.
const WATCH_PENDING: usize = 1_000;

/// `/api/watch?topics=…&<filters>`: server-sent events, checked every second, at most one
/// message per topic a second. Topics (default `head,health`):
///
/// - `head`: the log moved on: `{head, streams}` with the aggregates the new events changed;
/// - `events`: the new events that match, as `/api/events` gives them;
/// - `cases`: the process engine's case events that match (`process=`, `case=`);
/// - `commands`: commands that match, when stored and again when they complete;
/// - `health`: the verdict, when it changed;
/// - `pulse`: the head, the counts and each listener's position, when any changed.
///
/// Filters: `tenant`, `domain`, `type` (aggregate type), `id` (aggregate ID), `event` (part of
/// `domain.EventType`), `command` (command or correlation ID), `sender` (part of the identity),
/// `process`, `case`, `status` (for commands); `engine=1` includes the process engine's own
/// events and commands. `events` and `cases` carry the log position as their id: a client
/// that connects again with `Last-Event-ID` (or `from=`) goes on from there; more than
/// [`WATCH_BATCH`] events behind, it gets `gap` and should reload.
struct Watch {
    topics: Vec<String>,
    params: HashMap<String, String>,
    /// The last log position looked at; `None` until the first look.
    pos: Option<u64>,
    verdict: Option<String>,
    pulse: Option<String>,
    /// The newest stored command already told.
    command_seq: Option<u64>,
    /// Commands told while waiting: told again when they complete.
    pending: Vec<String>,
}

impl Watch {
    fn new(request: &[u8]) -> Self {
        let text = String::from_utf8_lossy(request);
        let target = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("");
        let params = parse_query(target.split_once('?').map_or("", |(_, q)| q));
        let resume = text
            .lines()
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.trim().eq_ignore_ascii_case("last-event-id"))
            })
            .and_then(|(_, v)| v.trim().parse::<u64>().ok())
            .or_else(|| params.get("from").and_then(|v| v.parse().ok()));
        let topics: Vec<String> = params
            .get("topics")
            .filter(|v| !v.is_empty())
            .map_or("head,health", String::as_str)
            .split(',')
            .map(|t| t.trim().to_owned())
            .collect();
        Watch {
            topics,
            params,
            pos: resume,
            verdict: None,
            pulse: None,
            command_seq: None,
            pending: Vec::new(),
        }
    }

    fn wants(&self, topic: &str) -> bool {
        self.topics.iter().any(|t| t == topic)
    }

    fn param(&self, name: &str) -> &str {
        self.params.get(name).map_or("", |v| v.trim())
    }

    /// Whether an event passes the filters (`cases` without the aggregate filters, which name
    /// the case's process and ID instead).
    fn matches(&self, e: &Event, case: bool) -> bool {
        let engine = self.param("engine") == "1";
        let has =
            |name: &str, value: &str| self.param(name).is_empty() || self.param(name) == value;
        let part = |name: &str, value: &str| {
            self.param(name).is_empty()
                || value
                    .to_lowercase()
                    .contains(&self.param(name).to_lowercase())
        };
        let ctx = e.context.as_ref();
        if case {
            return e.domain == "process"
                && has("tenant", &e.tenant_id)
                && has("process", &e.aggregate_type)
                && has("case", &e.aggregate_id);
        }
        (engine || e.domain != "process")
            && has("tenant", &e.tenant_id)
            && has("domain", &e.domain)
            && has("type", &e.aggregate_type)
            && has("id", &e.aggregate_id)
            && part("event", &format!("{}.{}", e.domain, e.event_type))
            && part("sender", ctx.map_or("", |c| c.sender_identity.as_str()))
            && (self.param("command").is_empty()
                || e.command_id == self.param("command")
                || ctx.is_some_and(|c| c.correlation_id == self.param("command")))
    }

    /// One look at the kernel: what to tell, as server-sent events.
    fn look(&mut self, kernel: &Kernel) -> String {
        let mut out = String::new();
        let head = kernel.store.head_position().unwrap_or(0);
        let from = self.pos.unwrap_or(head);
        if head > from {
            if head - from > WATCH_BATCH {
                out += &format!(
                    "id: {head}\nevent: gap\ndata: {}\n\n",
                    json!({ "from": from, "to": head })
                );
                if self.wants("head") {
                    out += &format!("event: head\ndata: {}\n\n", json!({ "head": head }));
                }
            } else {
                let events = kernel
                    .store
                    .read_all(from, (head - from) as u32)
                    .unwrap_or_default();
                if self.wants("head") {
                    // The aggregates the new events changed, newest first.
                    let mut streams: Vec<Value> = Vec::new();
                    for e in events.iter().rev() {
                        let same = |s: &Value| {
                            s["tenant"] == e.tenant_id.as_str()
                                && s["domain"] == e.domain.as_str()
                                && s["type"] == e.aggregate_type.as_str()
                                && s["id"] == e.aggregate_id.as_str()
                        };
                        if !streams.iter().any(same) {
                            streams.push(json!({
                                "tenant": e.tenant_id, "domain": e.domain, "type": e.aggregate_type,
                                "id": e.aggregate_id, "version": e.version, "lastAt": e.created_at,
                            }));
                        }
                    }
                    out += &format!(
                        "event: head\ndata: {}\n\n",
                        json!({ "head": head, "streams": streams })
                    );
                }
                for (topic, case) in [("events", false), ("cases", true)] {
                    if self.wants(topic) {
                        let found: Vec<Value> = events
                            .iter()
                            .filter(|e| self.matches(e, case))
                            .map(event)
                            .collect();
                        if !found.is_empty() {
                            out += &format!(
                                "id: {head}\nevent: {topic}\ndata: {}\n\n",
                                Value::Array(found)
                            );
                        }
                    }
                }
            }
        } else if self.pos.is_none() && self.wants("head") {
            out += &format!("event: head\ndata: {}\n\n", json!({ "head": head }));
        }
        self.pos = Some(head);

        if self.wants("commands") {
            out += &self.commands(kernel);
        }
        if self.wants("health")
            && let Ok(mut h) = health(kernel)
        {
            // Without the time and head: told only when the verdict itself changed.
            if let Some(o) = h.as_object_mut() {
                o.remove("now");
                o.remove("head");
            }
            let verdict = h.to_string();
            if self.verdict.as_ref() != Some(&verdict) {
                out += &format!("event: health\ndata: {verdict}\n\n");
                self.verdict = Some(verdict);
            }
        }
        if self.wants("pulse") {
            let usage = crate::usage::now(kernel);
            let listeners: Vec<Value> = kernel
                .listeners
                .view()
                .iter()
                .map(|l| json!({ "name": l["name"], "position": l["position"], "connected": l["connected"], "parked": l["parked"] }))
                .collect();
            let pulse = json!({
                "head": head,
                "commands": usage.commands,
                "rejected": usage.rejected,
                "queries": usage.queries,
                "queued": kernel.queue.len(),
                "listeners": listeners,
            })
            .to_string();
            if self.pulse.as_ref() != Some(&pulse) {
                out += &format!("event: pulse\ndata: {pulse}\n\n");
                self.pulse = Some(pulse);
            }
        }
        out
    }

    /// The commands stored since the last look, and those told while waiting that completed.
    fn commands(&mut self, kernel: &Kernel) -> String {
        let f = CommandFilter::new(&self.params);
        let now = now_ms();
        let mut told: Vec<Value> = Vec::new();
        let newest = kernel
            .store
            .commands(None, WATCH_BATCH as usize)
            .unwrap_or_default();
        let since = self
            .command_seq
            .unwrap_or_else(|| newest.first().map_or(0, |s| s.seq));
        for s in newest.iter().rev().filter(|s| s.seq > since) {
            let status = command_status(s, now);
            if f.matches(s, status) {
                if matches!(status, "waiting" | "scheduled") && self.pending.len() < WATCH_PENDING {
                    self.pending.push(s.pending.command.command_id.clone());
                }
                told.push(command_json(s, status));
            }
        }
        self.command_seq = Some(newest.first().map_or(since, |s| s.seq.max(since)));
        // Those told while waiting: again once they completed.
        let mut still = Vec::new();
        for id in std::mem::take(&mut self.pending) {
            match (kernel.store.command(&id), kernel.store.result(&id)) {
                (Ok(Some(pending)), Ok(Some(result))) => {
                    let s = kernable_store::StoredCommand {
                        seq: 0,
                        pending,
                        result: Some(result),
                    };
                    let status = command_status(&s, now);
                    told.push(command_json(&s, status));
                }
                (Ok(Some(_)), _) => still.push(id),
                _ => {}
            }
        }
        self.pending = still;
        if told.is_empty() {
            return String::new();
        }
        format!("event: commands\ndata: {}\n\n", Value::Array(told))
    }

    /// Writes what the looks find, once a second; a comment after 15 quiet seconds keeps the
    /// connection and finds one that is gone.
    async fn serve(mut self, kernel: Arc<Kernel>, conn: tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // The page closes the stream when it moves to another view: noticed at once by
        // reading (it sends nothing more, so a read ends only then), not at the next write.
        let (mut incoming, mut conn) = conn.into_split();
        let closed = async move {
            let mut buf = [0u8; 256];
            while matches!(incoming.read(&mut buf).await, Ok(n) if n > 0) {}
        };
        tokio::pin!(closed);
        let start =
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\n\
                      connection: close\r\n\r\nretry: 2000\n\n";
        if conn.write_all(start).await.is_err() {
            return;
        }
        let mut quiet = 0u32;
        loop {
            let k = Arc::clone(&kernel);
            let Ok((watch, mut said)) = tokio::task::spawn_blocking(move || {
                let said = self.look(&k);
                (self, said)
            })
            .await
            else {
                return;
            };
            self = watch;
            quiet += 1;
            if said.is_empty() && quiet >= 15 {
                said = ": still here\n\n".into();
            }
            if !said.is_empty() {
                quiet = 0;
                if conn.write_all(said.as_bytes()).await.is_err() {
                    return;
                }
            }
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                () = &mut closed => return,
            }
        }
    }
}

/// One aggregate's events, each with the command that caused it, and what the vault still
/// holds of the values they refer to.
fn history(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| params.get(name).cloned().unwrap_or_default();
    let stream = StreamId {
        tenant_id: param("tenant"),
        domain: param("domain"),
        aggregate_type: param("type"),
        aggregate_id: param("id"),
    };
    let events = kernel.store.read_stream(
        &stream.tenant_id,
        &stream.domain,
        &stream.aggregate_type,
        &stream.aggregate_id,
    )?;
    let mut commands = serde_json::Map::new();
    let mut refs: Vec<String> = Vec::new();
    for e in &events {
        refs.extend(e.vault_refs.iter().cloned());
        if commands.contains_key(&e.command_id) {
            continue;
        }
        if let Some(p) = kernel.store.command(&e.command_id)? {
            let c = p.command;
            refs.extend(c.vault_refs.iter().cloned());
            commands.insert(
                c.command_id.clone(),
                json!({
                    "type": c.command_type,
                    "payload": String::from_utf8_lossy(&c.payload),
                    "context": context(c.context.as_ref()),
                    "key": c.client_sig.map(|s| s.key_id).unwrap_or_default(),
                    "receivedAt": p.received_at,
                    "vaultRefs": c.vault_refs,
                }),
            );
        }
    }
    let vault: serde_json::Map<String, Value> = match &kernel.vault {
        Some(v) => v
            .describe(&refs)?
            .into_iter()
            .map(|(r, subject, secret)| (r, json!({ "subject": subject, "secret": secret })))
            .collect(),
        None => serde_json::Map::new(),
    };
    // A snapshot per deployment: of workers without one, and of those memberships name.
    let mut deployments: Vec<String> = kernel
        .memberships
        .deployments()
        .into_iter()
        .map(|(_, d)| d)
        .chain(kernel.workers.deployments())
        .collect();
    deployments.push(String::new());
    deployments.sort();
    deployments.dedup();
    let mut snapshots = Vec::new();
    for d in &deployments {
        if let Some(s) = kernel.snapshots.get(&stream, d)?.and_then(|r| r.snapshot) {
            snapshots.push(json!({ "deployment": d, "version": s.version, "schema": s.schema, "bytes": s.state.len() }));
        }
    }
    let snapshot = snapshots.first().cloned();
    let index = kernel.store.stream_index(&stream)?.map(|i| {
        let keys: Vec<Value> = i
            .keys
            .iter()
            .map(|k| {
                json!({
                    "name": k.name,
                    "global": k.global,
                    // A personal key's identity is in the vault, erased with its subject.
                    "personal": !k.vault_ref.is_empty(),
                    "id": hex::encode(&k.id),
                })
            })
            .collect();
        json!({ "keys": keys, "facts": i.facts })
    });
    Ok(json!({
        "index": index,
        "events": events.iter().map(event).collect::<Vec<_>>(),
        "commands": commands,
        "vault": vault,
        "snapshot": snapshot,
        "snapshots": snapshots,
    }))
}

/// What `/api/events` matches on; empty fields match everything, set ones must all match.
struct Filter {
    tenant: String,
    /// The domain, exactly.
    domain: String,
    /// Part of `domain.EventType`, any case.
    event_type: String,
    /// The aggregate id, exactly.
    id: String,
    /// The command id or the correlation id, exactly.
    command: String,
    /// Part of the payload as stored (vault fields are references, never values).
    text: String,
    /// `business=1`: leaves out the process engine's own events (domain `process`).
    business: bool,
}

impl Filter {
    fn new(params: &HashMap<String, String>) -> Self {
        let param = |name: &str| {
            params
                .get(name)
                .map(|v| v.trim().to_owned())
                .unwrap_or_default()
        };
        Self {
            tenant: param("tenant"),
            domain: param("domain"),
            event_type: param("type").to_lowercase(),
            id: param("id"),
            command: param("command"),
            text: param("text"),
            business: param("business") == "1",
        }
    }

    fn matches(&self, e: &Event) -> bool {
        let correlation = e.context.as_ref().map(|c| c.correlation_id.as_str());
        !(self.business && e.domain == "process")
            && (self.tenant.is_empty() || e.tenant_id == self.tenant)
            && (self.domain.is_empty() || e.domain == self.domain)
            && (self.event_type.is_empty()
                || format!("{}.{}", e.domain, e.event_type)
                    .to_lowercase()
                    .contains(&self.event_type))
            && (self.id.is_empty() || e.aggregate_id == self.id)
            && (self.command.is_empty()
                || e.command_id == self.command
                || correlation == Some(self.command.as_str()))
            && (self.text.is_empty() || String::from_utf8_lossy(&e.payload).contains(&self.text))
    }
}

/// Events of the whole log that match a [`Filter`], reading at most [`SCAN`] events.
///
/// - Search (no `after`): newest first, from before `before` (default: the head). `next` is
///   the `before` that continues with older events, null once the log's start was read.
/// - Tail (`after`): the events after that position, oldest first. `last` is the `after` of
///   the next call: the last position read, so a tail never reads an event twice.
fn events(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let number = |name: &str| -> Result<Option<u64>, BoxError> {
        match params.get(name).filter(|v| !v.is_empty()) {
            Some(v) => Ok(Some(
                v.parse().map_err(|_| format!("{name}: not a number"))?,
            )),
            None => Ok(None),
        }
    };
    let filter = Filter::new(params);
    let limit = number("limit")?
        .map_or(EVENTS_DEFAULT, |n| n as usize)
        .clamp(1, EVENTS_MAX);
    let head = kernel.store.head_position()?;
    let mut found: Vec<Value> = Vec::new();

    if let Some(after) = number("after")? {
        let mut last = after;
        let end = after.saturating_add(SCAN).min(head);
        'scan: while last < end {
            let batch = kernel
                .store
                .read_all(last, (end - last).min(CHUNK) as u32)?;
            if batch.is_empty() {
                break;
            }
            for e in &batch {
                last = e.global_position;
                if filter.matches(e) {
                    found.push(event(e));
                    if found.len() == limit {
                        break 'scan;
                    }
                }
            }
        }
        return Ok(json!({ "head": head, "events": found, "last": last }));
    }

    // (lo, hi] holds at most hi - lo events, so `read_all(lo, hi - lo)` returns all of them.
    let mut hi = number("before")?
        .map_or(head, |b| b.saturating_sub(1))
        .min(head);
    let floor = hi.saturating_sub(SCAN);
    let mut next = None;
    'scan: while hi > floor {
        let lo = hi.saturating_sub(CHUNK).max(floor);
        let batch = kernel.store.read_all(lo, (hi - lo) as u32)?;
        for e in batch.iter().rev().filter(|e| e.global_position <= hi) {
            if filter.matches(e) {
                found.push(event(e));
                if found.len() == limit {
                    next = Some(e.global_position);
                    break 'scan;
                }
            }
        }
        hi = lo;
    }
    if next.is_none() && hi > 0 {
        next = Some(hi + 1);
    }
    Ok(json!({ "head": head, "events": found, "next": next }))
}

/// How many erasures `/api/vault` lists.
const VAULT_ERASURES: usize = 50;

/// How many aggregates furthest from their snapshot `/api/snapshots` lists.
const SNAPSHOTS_BEHIND: usize = 20;

/// How many files `/api/files` lists at most at once.
const FILES_PAGE: usize = 500;

/// How many cache keys `/api/cache` lists at most.
const CACHE_KEYS: usize = 500;

fn vault(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let Some(vault) = &kernel.vault else {
        return Ok(json!({ "enabled": false }));
    };
    let subject = params.get("subject").map_or("", |v| v.trim());
    if !subject.is_empty() {
        let tenant = params.get("tenant").map_or("default", |v| v.trim());
        let v = vault.subject(tenant, subject)?;
        return Ok(json!({
            "tenant": tenant,
            "subject": subject,
            "held": v.held.iter().map(|(d, t, n)| json!({ "domain": d, "type": t, "count": n })).collect::<Vec<_>>(),
            "erased": v.erased.iter().map(|(n, at)| json!({ "erased": n, "at": at })).collect::<Vec<_>>(),
            "pending": v.pending,
        }));
    }
    let o = vault.overview(VAULT_ERASURES)?;
    Ok(json!({
        "enabled": true,
        "personal": o.personal,
        "secret": o.secret,
        "subjects": o.subjects,
        "types": o.types.iter().map(|(d, t, n)| json!({ "domain": d, "type": t, "count": n })).collect::<Vec<_>>(),
        "erasures": o.erasures,
        "recent": o.recent.iter().map(|(t, n, at)| json!({ "tenant": t, "erased": n, "at": at })).collect::<Vec<_>>(),
        "pending": o.intents,
        "sealed": o.sealed,
        "limit": kernable_store::vault::MAX_SEALED,
    }))
}

/// The deployments of the connected workers and listeners and of the memberships, with their
/// read models; what the log says of each is `kernable deployments`, which reads all of it.
fn deployments(kernel: &Kernel) -> Result<Value, BoxError> {
    let serving: Vec<Value> = kernel
        .memberships
        .deployments()
        .into_iter()
        .map(|(service, deployment)| json!({ "service": service, "deployment": deployment }))
        .collect();
    let listeners: Vec<Value> = kernel
        .listeners
        .deployments()
        .into_iter()
        .map(|(deployment, last_seen, used)| json!({ "deployment": deployment, "lastSeen": last_seen, "inUse": used }))
        .collect();
    Ok(json!({
        "serving": serving,
        "workers": kernel.workers.deployments(),
        "readModels": listeners,
    }))
}

/// `GET /api/deployments/warm-up?deployment=<d>`: how far the deployment is from taking over
/// (`deployments::warm_up`): per read model and middleware registered for it its position and
/// how far behind the head, the aggregates its connected workers run, and whether it has caught
/// up.
fn deployments_warm_up(
    kernel: &Kernel,
    params: &HashMap<String, String>,
) -> Result<Value, BoxError> {
    let deployment = params
        .get("deployment")
        .filter(|d| !d.is_empty() && crate::config::valid_name(d))
        .ok_or("give deployment=<name>")?;
    Ok(crate::deployments::warm_up(kernel, deployment, None)?.json())
}

/// `GET /api/deployments/since?position=<n>` (or `time=<ms>`), optionally `&version=<v>`: what
/// was written after that point (`deployments::since`), to check before a rollback.
fn deployments_since(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let from = match (params.get("position"), params.get("time")) {
        (Some(p), _) => p.parse().map_err(|_| "position: not a number")?,
        (None, Some(t)) => crate::deployments::position_at(
            kernel.store.as_ref(),
            t.parse().map_err(|_| "time: not ms since epoch")?,
        )?,
        (None, None) => return Err("give position=<n> or time=<ms since epoch>".into()),
    };
    let version = params
        .get("version")
        .map(String::as_str)
        .filter(|v| !v.is_empty());
    Ok(crate::deployments::since(
        kernel.store.as_ref(),
        from,
        version,
    )?)
}

fn snapshots(kernel: &Kernel) -> Result<Value, BoxError> {
    let infos = kernel.snapshots.infos()?;
    // The aggregates' versions now, of the most recently changed ones.
    let versions: HashMap<StreamId, u64> = kernel
        .store
        .streams(STREAMS_SEARCH)?
        .into_iter()
        .map(|s| (s.stream, s.version))
        .collect();
    struct Type {
        count: u64,
        bytes: u64,
        schemas: std::collections::BTreeSet<String>,
        oldest: i64,
        newest: i64,
        behind: u64,
        max_behind: u64,
    }
    let mut types: BTreeMap<String, Type> = BTreeMap::new();
    let mut furthest: Vec<(u64, &kernable_store::snapshots::SnapshotInfo)> = Vec::new();
    for i in &infos {
        let t = types
            .entry(format!("{}.{}", i.stream.domain, i.stream.aggregate_type))
            .or_insert(Type {
                count: 0,
                bytes: 0,
                schemas: Default::default(),
                oldest: i64::MAX,
                newest: 0,
                behind: 0,
                max_behind: 0,
            });
        let behind = versions
            .get(&i.stream)
            .map_or(0, |v| v.saturating_sub(i.version));
        t.count += 1;
        t.bytes += i.bytes;
        t.schemas.insert(i.schema.clone());
        if i.created_at > 0 {
            t.oldest = t.oldest.min(i.created_at);
            t.newest = t.newest.max(i.created_at);
        }
        t.behind += behind;
        t.max_behind = t.max_behind.max(behind);
        furthest.push((behind, i));
    }
    furthest.sort_by_key(|a| std::cmp::Reverse(a.0));
    let types: Vec<Value> = types
        .into_iter()
        .map(|(name, t)| {
            json!({
                "type": name,
                "count": t.count,
                "bytes": t.bytes,
                "schemas": t.schemas,
                "oldest": if t.oldest == i64::MAX { 0 } else { t.oldest },
                "newest": t.newest,
                "behind": t.behind,
                "maxBehind": t.max_behind,
            })
        })
        .collect();
    let furthest: Vec<Value> = furthest
        .iter()
        .filter(|(b, _)| *b > 0)
        .take(SNAPSHOTS_BEHIND)
        .map(|(behind, i)| {
            json!({
                "tenant": i.stream.tenant_id,
                "domain": i.stream.domain,
                "type": i.stream.aggregate_type,
                "id": i.stream.aggregate_id,
                "version": i.version,
                "behind": behind,
                "createdAt": i.created_at,
            })
        })
        .collect();
    Ok(json!({ "count": infos.len(), "types": types, "furthest": furthest }))
}

fn files(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| params.get(name).map_or("", |v| v.trim());
    if !param("domain").is_empty() {
        let tenant = if param("tenant").is_empty() {
            "default"
        } else {
            param("tenant")
        };
        let mut files = kernel.data.list(
            tenant,
            param("domain"),
            param("prefix"),
            param("after"),
            FILES_PAGE + 1,
        )?;
        let more = files.len() > FILES_PAGE;
        files.truncate(FILES_PAGE);
        let next = if more {
            files.last().map(|f| f.key.clone())
        } else {
            None
        };
        return Ok(json!({
            "tenant": tenant,
            "domain": param("domain"),
            "files": files.iter().map(|f| json!({ "key": f.key, "contentType": f.content_type, "size": f.size })).collect::<Vec<_>>(),
            "next": next,
        }));
    }
    let summary = kernel.data.summary()?;
    Ok(json!({
        "domains": summary.iter().map(|d| json!({ "tenant": d.tenant, "domain": d.domain, "files": d.files, "bytes": d.bytes })).collect::<Vec<_>>(),
    }))
}

fn cache(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| params.get(name).map_or("", |v| v.trim());
    if param("keys") == "1" {
        let keys = kernel
            .cache
            .keys(param("tenant"), param("domain"), param("text"), CACHE_KEYS);
        return Ok(json!({
            "keys": keys.iter().map(|k| json!({ "tenant": k.tenant, "domain": k.domain, "key": k.key, "bytes": k.bytes, "expiresInMs": k.expires_in_ms })).collect::<Vec<_>>(),
            "limit": CACHE_KEYS,
        }));
    }
    let s = kernel.cache.stats();
    Ok(json!({
        "entries": s.entries,
        "bytes": s.bytes,
        "maxBytes": s.max_bytes,
        "sets": s.sets,
        "hits": s.hits,
        "misses": s.misses,
        "expired": s.expired,
        "evicted": s.evicted,
        "domains": s.domains.iter().map(|(t, d, n, b)| json!({ "tenant": t, "domain": d, "entries": n, "bytes": b })).collect::<Vec<_>>(),
        "startedAt": kernel.started_at,
    }))
}

/// How many events one `/api/counts` request counts at most: a long log is counted over
/// several, and the answer says how far it got.
const COUNT_BUDGET: u64 = 200_000;

/// How many of the newest commands `/api/counts` looks through at most.
const COUNT_COMMANDS: usize = 5_000;

fn counts(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let domain = params.get("domain").map_or("", |v| v.trim());
    let now = now_ms();
    let head = kernel.store.head_position()?;
    let counted = kernel
        .tally
        .catch_up(kernel.store.as_ref(), head, COUNT_BUDGET, now)?;
    // The commands of the last hour, newest first, as far as the newest COUNT_COMMANDS reach.
    let since = now - crate::trend::TALLY_MINUTES * 60_000;
    let minute = now / 60_000;
    let mut commands: BTreeMap<String, (u64, u64, Vec<u64>)> = BTreeMap::new();
    let mut looked = 0;
    let mut before = None;
    let mut complete = false;
    'pages: while looked < COUNT_COMMANDS {
        let page = kernel.store.commands(before, 500)?;
        if page.is_empty() {
            complete = true;
            break;
        }
        for s in &page {
            looked += 1;
            if s.pending.received_at < since {
                complete = true;
                break 'pages;
            }
            let c = &s.pending.command;
            if !domain.is_empty() && c.domain != domain {
                continue;
            }
            let entry = commands
                .entry(format!(
                    "{}.{}.{}",
                    c.domain, c.aggregate_type, c.command_type
                ))
                .or_insert_with(|| (0, 0, vec![0; crate::trend::TALLY_MINUTES as usize]));
            entry.0 += 1;
            if command_status(s, now) == "rejected" {
                entry.1 += 1;
            }
            let back = minute - s.pending.received_at / 60_000;
            if let Ok(back) = usize::try_from(back)
                && back < entry.2.len()
            {
                let i = entry.2.len() - 1 - back;
                entry.2[i] += 1;
            }
        }
        before = page.last().map(|s| s.seq);
    }
    let commands: serde_json::Map<String, Value> = commands
        .into_iter()
        .map(|(t, (count, rejected, per_minute))| {
            (
                t,
                json!({ "count": count, "rejected": rejected, "perMinute": per_minute }),
            )
        })
        .collect();
    Ok(json!({
        "now": now,
        "head": head,
        "counted": counted,
        "events": kernel.tally.view(domain, now),
        "queries": kernel.query_counts.view(domain, now),
        "commands": commands,
        "commandsComplete": complete,
    }))
}

/// The newest rejected commands: what was sent, by whom, and why it was refused. The rejection
/// itself carries no time; `receivedAt` is when the kernel accepted the command.
fn rejections(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let limit = match params.get("limit").filter(|v| !v.is_empty()) {
        Some(v) => v.parse().map_err(|_| "limit: not a number")?,
        None => REJECTIONS_DEFAULT,
    }
    .clamp(1, REJECTIONS_MAX);
    // `since=` (ms): only how many of the business (not the process engine's own) were
    // received since then, of the newest `limit`.
    if let Some(since) = params.get("since").filter(|v| !v.is_empty()) {
        let since: i64 = since.parse().map_err(|_| "since: not a number")?;
        let count = kernel
            .store
            .rejections(limit)?
            .iter()
            .filter(|(p, _)| p.received_at >= since && p.command.domain != "process")
            .count();
        return Ok(json!({ "count": count, "limit": limit }));
    }
    // `brief=1`: without payload and context.
    let brief = params.get("brief").is_some_and(|v| v == "1");
    let rejected: Vec<Value> = kernel
        .store
        .rejections(limit)?
        .into_iter()
        .map(|(p, r)| {
            let c = p.command;
            // The requirement as the worker named it; its value hash is never shown.
            let requirement = r.requirement.map(|q| {
                json!({
                    "domain": q.domain,
                    "aggregateType": q.aggregate_type,
                    "aggregateId": q.aggregate_id,
                    "fact": q.fact,
                    "key": q.key,
                    "absent": q.absent,
                })
            });
            json!({
                "commandId": c.command_id,
                "tenant": c.tenant_id,
                "domain": c.domain,
                "aggregateType": c.aggregate_type,
                "aggregateId": c.aggregate_id,
                "type": c.command_type,
                "payload": String::from_utf8_lossy(&c.payload),
                "context": context(c.context.as_ref()),
                "key": c.client_sig.map(|s| s.key_id).unwrap_or_default(),
                "receivedAt": p.received_at,
                "code": r.code,
                "message": r.message,
                "uniqueKey": r.key,
                "requirement": requirement,
            })
        })
        .map(|mut r| {
            if let (true, Some(o)) = (brief, r.as_object_mut()) {
                o.remove("payload");
                o.remove("context");
            }
            r
        })
        .collect();
    Ok(json!({ "rejections": rejected, "limit": limit }))
}

/// What `/api/commands` matches on; empty fields match everything, set ones must all match.
struct CommandFilter {
    tenant: String,
    /// The domain, exactly.
    domain: String,
    /// Part of `domain.Command`, any case.
    command_type: String,
    /// The aggregate id, exactly.
    id: String,
    /// The command id or the correlation id, exactly.
    command: String,
    /// Part of the sender's identity, any case.
    sender: String,
    /// `committed`, `rejected`, `waiting` or `scheduled`.
    status: String,
    /// `business=1`: leaves out the process engine's own commands (domain `process`).
    business: bool,
}

impl CommandFilter {
    fn new(params: &HashMap<String, String>) -> Self {
        let param = |name: &str| {
            params
                .get(name)
                .map(|v| v.trim().to_owned())
                .unwrap_or_default()
        };
        CommandFilter {
            tenant: param("tenant"),
            domain: param("domain"),
            command_type: param("type").to_lowercase(),
            id: param("id"),
            command: param("command"),
            sender: param("sender").to_lowercase(),
            status: param("status"),
            business: param("business") == "1",
        }
    }

    fn matches(&self, s: &kernable_store::StoredCommand, status: &str) -> bool {
        let c = &s.pending.command;
        let ctx = c.context.as_ref();
        let correlation = ctx.map(|x| x.correlation_id.as_str());
        let sender = ctx
            .map(|x| x.sender_identity.to_lowercase())
            .unwrap_or_default();
        !(self.business && c.domain == "process")
            && (self.tenant.is_empty() || c.tenant_id == self.tenant)
            && (self.domain.is_empty() || c.domain == self.domain)
            && (self.command_type.is_empty()
                || format!("{}.{}", c.domain, c.command_type)
                    .to_lowercase()
                    .contains(&self.command_type))
            && (self.id.is_empty() || c.aggregate_id == self.id)
            && (self.command.is_empty()
                || c.command_id == self.command
                || correlation == Some(self.command.as_str()))
            && (self.sender.is_empty() || sender.contains(&self.sender))
            && (self.status.is_empty() || self.status == status)
    }
}

/// `committed`, `rejected`, `scheduled` (due later) or `waiting`.
fn command_status(s: &kernable_store::StoredCommand, now: i64) -> &'static str {
    use kernable_core::proto::command_result::Outcome;
    match s.result.as_ref().and_then(|r| r.outcome.as_ref()) {
        Some(Outcome::Committed(_)) => "committed",
        Some(Outcome::Rejected(_)) => "rejected",
        _ if s.pending.command.deliver_at > now => "scheduled",
        _ => "waiting",
    }
}

/// A stored command as the UI shows it: who sent it, and what came of it.
fn command_json(s: &kernable_store::StoredCommand, status: &str) -> Value {
    use kernable_core::proto::command_result::Outcome;
    let c = &s.pending.command;
    let outcome = s.result.as_ref().and_then(|r| r.outcome.as_ref());
    // An event's time is when the kernel committed it: the command took from when it was
    // received (or due, if scheduled) until then. Rejections keep no time.
    let committed_at = match outcome {
        Some(Outcome::Committed(done)) => done.events.first().map(|e| e.created_at),
        _ => None,
    };
    let (events, rejection) = match outcome {
        Some(Outcome::Committed(done)) => (
            done.events
                .iter()
                .map(|e| json!({ "position": e.global_position, "type": e.event_type }))
                .collect::<Vec<_>>(),
            Value::Null,
        ),
        Some(Outcome::Rejected(r)) => (
            Vec::new(),
            json!({ "code": r.code, "message": r.message, "uniqueKey": r.key }),
        ),
        _ => (Vec::new(), Value::Null),
    };
    json!({
        "seq": s.seq,
        "commandId": c.command_id,
        "tenant": c.tenant_id,
        "domain": c.domain,
        "aggregateType": c.aggregate_type,
        "aggregateId": c.aggregate_id,
        "type": c.command_type,
        "payload": String::from_utf8_lossy(&c.payload),
        "context": context(c.context.as_ref()),
        "key": c.client_sig.as_ref().map(|s| s.key_id.clone()).unwrap_or_default(),
        "receivedAt": s.pending.received_at,
        "deliverAt": c.deliver_at,
        "status": status,
        "committedAt": committed_at,
        "tookMs": committed_at.map(|at| at - s.pending.received_at.max(c.deliver_at)),
        "events": events,
        "rejection": rejection,
    })
}

/// Stored commands that match a [`CommandFilter`], newest first, reading at most [`SCAN`]: `next`
/// is the `before` that continues with older ones, null once the first stored was read.
fn commands(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let f = CommandFilter::new(params);
    let limit = match params.get("limit").filter(|v| !v.is_empty()) {
        Some(v) => v.parse::<usize>().map_err(|_| "limit: not a number")?,
        None => EVENTS_DEFAULT,
    }
    .clamp(1, EVENTS_MAX);
    let mut before = match params.get("before").filter(|v| !v.is_empty()) {
        Some(v) => Some(v.parse::<u64>().map_err(|_| "before: not a number")?),
        None => None,
    };
    let now = now_ms();
    let mut found = Vec::new();
    let mut read = 0u64;
    let mut newest = None;
    let mut cut = None;
    let next = loop {
        let page = kernel.store.commands(before, 500)?;
        let Some(last) = page.last() else { break None };
        newest = newest.or(page.first().map(|c| c.seq));
        before = Some(last.seq);
        for s in &page {
            read += 1;
            let status = command_status(s, now);
            if !f.matches(s, status) {
                continue;
            }
            found.push(command_json(s, status));
            if found.len() == limit {
                // The next page starts right after the last one returned.
                cut = Some(s.seq);
                break;
            }
        }
        if cut.is_some() {
            break cut;
        }
        if read >= SCAN {
            break before;
        }
    };
    Ok(json!({ "commands": found, "next": next, "newest": newest }))
}

/// Parked events of all listeners, by listener and position, up to `limit`; `total` counts
/// them all.
fn parked(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let limit = match params.get("limit").filter(|v| !v.is_empty()) {
        Some(v) => v.parse().map_err(|_| "limit: not a number")?,
        None => PARKED_DEFAULT,
    }
    .clamp(1, PARKED_MAX);
    let mut total = 0;
    let mut shown: Vec<Value> = Vec::new();
    for (listener, mut events) in kernel.listeners.parked() {
        total += events.len();
        events.sort_by_key(|p| p.position);
        for p in events.into_iter().take(limit - shown.len()) {
            shown.push(json!({
                "listener": listener,
                "position": p.position,
                "type": p.event_type,
                "code": p.code,
                "message": p.message,
                "attempts": p.attempts,
                "parkedAt": p.parked_at,
                "retry": p.retry,
            }));
        }
    }
    Ok(json!({ "parked": shown, "total": total, "limit": limit }))
}

/// The processes with their statistics; `addr` is where the CLI reaches this kernel, for the
/// commands that resolve incidents.
fn processes(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let window = Window::parse(params.get("window").map_or("", String::as_str))?;
    let mut out = kernel.process_view.processes(kernel, window)?;
    out["addr"] = json!(kernel.config.listen);
    out["endedKept"] = json!(ENDED);
    Ok(out)
}

/// How many cases `/api/cases` returns at most, and by default.
const CASES_MAX: usize = 1_000;
const CASES_DEFAULT: usize = 200;

fn cases(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| {
        params
            .get(name)
            .map(|v| v.trim().to_owned())
            .unwrap_or_default()
    };
    let version = match param("version") {
        v if v.is_empty() => None,
        v => Some(v.parse().map_err(|_| "version: not a number")?),
    };
    let limit = match param("limit") {
        v if v.is_empty() => CASES_DEFAULT,
        v => v.parse().map_err(|_| "limit: not a number")?,
    }
    .clamp(1, CASES_MAX);
    let filter = CaseFilter {
        process: param("process"),
        version,
        step: param("step"),
        state: param("state"),
        text: param("text"),
    };
    kernel.process_view.cases(kernel, &filter, limit)
}

/// One case's events, each with `link`: for `CommandSent` the command's aggregate and its
/// outcome; for an event that names a log position (what started or moved the case, or an
/// event kept for it) that event's type and aggregate.
fn case(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| params.get(name).cloned().unwrap_or_default();
    let (tenant, process, id) = (param("tenant"), param("process"), param("id"));
    let events = kernel.store.read_stream(&tenant, DOMAIN, &process, &id)?;
    let mut out = Vec::with_capacity(events.len());
    for e in &events {
        let payload: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
        let mut link = Value::Null;
        if let Some(command_id) = payload["commandId"].as_str()
            && e.event_type == "CommandSent"
            && let Some(p) = kernel.store.command(command_id)?
        {
            let c = p.command;
            let outcome = kernel.store.result(command_id)?.and_then(|r| r.outcome);
            let (outcome, code) = match outcome {
                Some(kernable_core::proto::command_result::Outcome::Committed(_)) => {
                    ("committed", String::new())
                }
                Some(kernable_core::proto::command_result::Outcome::Rejected(r)) => {
                    ("rejected", r.code)
                }
                _ => ("pending", String::new()),
            };
            link = json!({
                "kind": "command",
                "commandId": command_id,
                "type": c.command_type,
                "tenant": c.tenant_id,
                "domain": c.domain,
                "aggregateType": c.aggregate_type,
                "aggregateId": c.aggregate_id,
                "outcome": outcome,
                "code": code,
            });
        } else if let Some(position) = payload["position"]
            .as_u64()
            .or_else(|| payload["committedAt"].as_u64())
            .filter(|p| *p > 0)
            && let Some(at) = kernel
                .store
                .read_all(position - 1, 1)?
                .into_iter()
                .find(|x| x.global_position == position)
        {
            link = json!({
                "kind": "event",
                "position": position,
                "type": format!("{}.{}", at.domain, at.event_type),
                "tenant": at.tenant_id,
                "domain": at.domain,
                "aggregateType": at.aggregate_type,
                "aggregateId": at.aggregate_id,
            });
        }
        let mut shown = event(e);
        shown["link"] = link;
        out.push(shown);
    }
    Ok(json!({ "tenant": tenant, "process": process, "id": id, "events": out }))
}

fn event(e: &Event) -> Value {
    json!({
        "position": e.global_position,
        "tenant": e.tenant_id,
        "domain": e.domain,
        "aggregateType": e.aggregate_type,
        "aggregateId": e.aggregate_id,
        "version": e.version,
        "type": e.event_type,
        "schema": e.schema,
        "payload": String::from_utf8_lossy(&e.payload),
        "commandId": e.command_id,
        "createdAt": e.created_at,
        "key": e.producer_sig.as_ref().map(|s| s.key_id.clone()).unwrap_or_default(),
        "context": context(e.context.as_ref()),
        "vaultRefs": e.vault_refs,
        "erases": e.erases,
        "hash": hex::encode(&e.hash),
    })
}

/// A request context as the SDKs show it: camelCase, empty fields left out.
fn context(c: Option<&RequestContext>) -> Value {
    let Some(c) = c else {
        return json!({});
    };
    let mut out = serde_json::Map::new();
    let mut text = |name: &str, value: &str| {
        if !value.is_empty() {
            out.insert(name.to_owned(), Value::String(value.to_owned()));
        }
    };
    text("senderTenant", &c.sender_tenant);
    text("senderIdentity", &c.sender_identity);
    text("senderAccount", &c.sender_account);
    text("senderSession", &c.sender_session);
    text("onBehalfOf", &c.on_behalf_of);
    text("correlationId", &c.correlation_id);
    text("causationId", &c.causation_id);
    text("traceId", &c.trace_id);
    if let Some(v) = c.expected_version {
        out.insert("expectedVersion".into(), json!(v));
    }
    if c.bypass_authorization {
        out.insert("bypassAuthorization".into(), json!(true));
    }
    if !c.attributes.is_empty() {
        let attributes: serde_json::Map<String, Value> = c
            .attributes
            .iter()
            .map(|a| (a.key.clone(), Value::String(a.value.clone())))
            .collect();
        out.insert("attributes".into(), Value::Object(attributes));
    }
    Value::Object(out)
}
