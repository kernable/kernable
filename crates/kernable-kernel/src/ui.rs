//! The kernel's UI in a browser, on `ui_listen` (loopback only): one page (`ui.html`) and the
//! JSON it reads.
//!
//! - `GET /api/state`: the kernel, the services' memberships, the registered aggregates and the
//!   connected workers, the listeners, and which aggregate writes which event type;
//! - `GET /api/streams`: the aggregates with events, the most recently changed first;
//! - `GET /api/history?tenant=&domain=&type=&id=`: one aggregate's events with their commands.
//!
//! It never decrypts the vault: a vault field shows its subject, whether it is secret, or that
//! it was erased. Snapshot states are not shown either, as they may hold personal values.

use std::{collections::HashMap, sync::Arc};

use kernable_core::proto::{Event, RequestContext};
use kernable_store::StreamId;
use serde_json::{Value, json};

use crate::{BoxError, Kernel, http::reply, now_ms};

const PAGE: &str = include_str!("ui.html");

/// How many aggregates `/api/streams` lists.
const STREAMS: usize = 500;

/// Serves the UI until the listener fails.
pub(crate) async fn serve(kernel: Arc<Kernel>, addr: &str) -> Result<(), BoxError> {
    tracing::info!(%addr, "serving the UI");
    crate::http::serve(kernel, addr, respond).await
}

pub(crate) fn respond(kernel: &Kernel, request: &[u8]) -> Vec<u8> {
    let line = String::from_utf8_lossy(request);
    let target = line
        .strip_prefix("GET ")
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params = parse_query(query);
    let result = match path {
        "/" => return reply("200 OK", "text/html; charset=utf-8", PAGE.as_bytes()),
        "/api/state" => state(kernel),
        "/api/streams" => streams(kernel),
        "/api/history" => history(kernel, &params),
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

fn streams(kernel: &Kernel) -> Result<Value, BoxError> {
    let streams: Vec<Value> = kernel
        .store
        .streams(STREAMS)?
        .into_iter()
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
    Ok(json!({ "streams": streams, "limit": STREAMS }))
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
    Ok(json!({
        "events": events.iter().map(event).collect::<Vec<_>>(),
        "commands": commands,
        "vault": vault,
        "snapshot": snapshot,
    }))
}

fn event(e: &Event) -> Value {
    json!({
        "position": e.global_position,
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
