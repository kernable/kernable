//! The kernel's UI in a browser, on `ui_listen` (loopback only): one page (`ui.html`), served at
//! `/` and at each of its views' addresses, its logo (`/logo.png`, `/logo-dark.png`) and the
//! JSON it reads.
//!
//! - `GET /api/state`: the kernel, the services' memberships, the registered aggregates and the
//!   connected workers, the listeners, and which aggregate writes which event type;
//! - `GET /api/health`: the verdict (ok, warn, bad) and every problem behind it, with what
//!   fixes it, as the UI's top bar shows it; and the counts it shows beside (incidents, parked);
//! - `GET /api/watch`: server-sent events while the page is open: `head` when the log moved on
//!   (`{"head": position, "streams": [the aggregates the new events changed, newest first]}`,
//!   without `streams` after many events), `health` when the verdict changed, at most once a
//!   second, and a comment every 15 s; the page reloads what it shows only then, or not at all
//!   when what changed is not on it;
//! - `GET /api/streams?q=&engine=&limit=`: the aggregates with events, the most recently
//!   changed first, those whose tenant, domain, type or ID contains `q`;
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
//!   command it sent (its aggregate and outcome) or the event that moved it.
//!
//! It never decrypts the vault: a vault field shows its subject, whether it is secret, or that
//! it was erased. Snapshot states are not shown either, as they may hold personal values.

use std::{collections::HashMap, sync::Arc};

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

/// How many aggregates `/api/streams` lists.
const STREAMS: usize = 500;

/// The most events `/api/watch` looks at to tell which aggregates changed; more since the last
/// message, and it tells only that the log moved on.
const WATCH_STREAMS: usize = 200;

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
        request.starts_with(b"GET /api/watch ").then(|| {
            let kernel = Arc::clone(kernel);
            crate::http::Answer::Stream(Box::new(move |conn| Box::pin(watch(kernel, conn))))
        })
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

/// Everything the map and the connections view show.
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
    let (aggregates, instances) = kernel.workers.view();
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
    let limit = match params.get("limit").filter(|v| !v.is_empty()) {
        Some(v) => v.parse::<usize>().map_err(|_| "limit: not a number")?,
        None => 50,
    }
    .clamp(1, STREAMS);
    let streams: Vec<Value> = kernel
        .store
        .streams(if q.is_empty() && engine {
            limit
        } else {
            STREAMS_SEARCH
        })?
        .into_iter()
        .filter(|s| engine || s.stream.domain != "process")
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

/// `/api/watch`: tells the page when the log moved on (`head`) and when the verdict changed
/// (`health`), checked every second and said at most once a second; a comment every 15 s keeps
/// the connection and finds one that is gone.
async fn watch(kernel: Arc<Kernel>, mut conn: tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;
    let start =
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\n\
                  connection: close\r\n\r\nretry: 2000\n\n";
    if conn.write_all(start).await.is_err() {
        return;
    }
    let (mut head, mut verdict, mut quiet) = (None::<u64>, None, 0u32);
    loop {
        let k = Arc::clone(&kernel);
        let before = head;
        let now = tokio::task::spawn_blocking(move || {
            let head = k.store.head_position().ok();
            // The aggregates the new events changed, so the page updates its list itself and
            // reloads an aggregate only if it changed; beyond WATCH_STREAMS, only "many".
            let changed = match (before, head) {
                (Some(from), Some(to)) if to > from => {
                    if to - from > WATCH_STREAMS as u64 {
                        None
                    } else {
                        let events = k
                            .store
                            .read_all(from, WATCH_STREAMS as u32)
                            .unwrap_or_default();
                        let mut latest: Vec<Value> = Vec::new();
                        for e in events.iter().rev() {
                            let same = |s: &Value| {
                                s["tenant"] == e.tenant_id.as_str()
                                    && s["domain"] == e.domain.as_str()
                                    && s["type"] == e.aggregate_type.as_str()
                                    && s["id"] == e.aggregate_id.as_str()
                            };
                            if !latest.iter().any(same) {
                                latest.push(json!({
                                    "tenant": e.tenant_id,
                                    "domain": e.domain,
                                    "type": e.aggregate_type,
                                    "id": e.aggregate_id,
                                    "version": e.version,
                                    "lastAt": e.created_at,
                                }));
                            }
                        }
                        Some(latest)
                    }
                }
                _ => None,
            };
            // What the top bar shows, without the time: changes only when the verdict does.
            let verdict = health(&k).ok().map(|mut h| {
                if let Some(o) = h.as_object_mut() {
                    o.remove("now");
                    o.remove("head");
                }
                h.to_string()
            });
            (head, verdict, changed)
        })
        .await;
        let Ok((new_head, new_verdict, changed)) = now else {
            return;
        };
        let mut said = String::new();
        if new_head.is_some() && new_head != head {
            // Newest first; without `streams` the page reloads what it shows.
            let data = match changed {
                Some(streams) => json!({ "head": new_head, "streams": streams }),
                None => json!({ "head": new_head }),
            };
            said += &format!("event: head\ndata: {data}\n\n");
        }
        if new_verdict.is_some() && new_verdict != verdict {
            said += "event: health\ndata: changed\n\n";
        }
        (head, verdict) = (new_head, new_verdict);
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
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
    let snapshot = kernel
        .snapshots
        .get(&stream)?
        .and_then(|r| r.snapshot)
        .map(|s| json!({ "version": s.version, "schema": s.schema, "bytes": s.state.len() }));
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
    }))
}

/// What `/api/events` matches on; empty fields match everything, set ones must all match.
struct Filter {
    tenant: String,
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

/// Stored commands that match a [`CommandFilter`], newest first, reading at most [`SCAN`]: `next`
/// is the `before` that continues with older ones, null once the first stored was read.
fn commands(kernel: &Kernel, params: &HashMap<String, String>) -> Result<Value, BoxError> {
    let param = |name: &str| {
        params
            .get(name)
            .map(|v| v.trim().to_owned())
            .unwrap_or_default()
    };
    let f = CommandFilter {
        tenant: param("tenant"),
        command_type: param("type").to_lowercase(),
        id: param("id"),
        command: param("command"),
        sender: param("sender").to_lowercase(),
        status: param("status"),
        business: param("business") == "1",
    };
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
            let c = &s.pending.command;
            let ctx = c.context.as_ref();
            let correlation = ctx.map(|x| x.correlation_id.as_str());
            let sender = ctx
                .map(|x| x.sender_identity.to_lowercase())
                .unwrap_or_default();
            use kernable_core::proto::command_result::Outcome;
            let outcome = s.result.as_ref().and_then(|r| r.outcome.as_ref());
            let status = match outcome {
                Some(Outcome::Committed(_)) => "committed",
                Some(Outcome::Rejected(_)) => "rejected",
                _ if c.deliver_at > now => "scheduled",
                _ => "waiting",
            };
            let matches = !(f.business && c.domain == "process")
                && (f.tenant.is_empty() || c.tenant_id == f.tenant)
                && (f.command_type.is_empty()
                    || format!("{}.{}", c.domain, c.command_type)
                        .to_lowercase()
                        .contains(&f.command_type))
                && (f.id.is_empty() || c.aggregate_id == f.id)
                && (f.command.is_empty()
                    || c.command_id == f.command
                    || correlation == Some(f.command.as_str()))
                && (f.sender.is_empty() || sender.contains(&f.sender))
                && (f.status.is_empty() || f.status == status);
            if !matches {
                continue;
            }
            // An event's time is when the kernel committed it: the command took from when it
            // was received (or due, if scheduled) until then. Rejections keep no time.
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
            found.push(json!({
                "seq": s.seq,
                "commandId": c.command_id,
                "tenant": c.tenant_id,
                "domain": c.domain,
                "aggregateType": c.aggregate_type,
                "aggregateId": c.aggregate_id,
                "type": c.command_type,
                "payload": String::from_utf8_lossy(&c.payload),
                "context": context(ctx),
                "key": c.client_sig.as_ref().map(|s| s.key_id.clone()).unwrap_or_default(),
                "receivedAt": s.pending.received_at,
                "deliverAt": c.deliver_at,
                "status": status,
                "committedAt": committed_at,
                "tookMs": committed_at.map(|at| at - s.pending.received_at.max(c.deliver_at)),
                "events": events,
                "rejection": rejection,
            }));
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
