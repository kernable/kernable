//! Prometheus metrics, served as text at `GET /metrics` on `metrics_listen`.

use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use crate::{BoxError, Kernel, http::reply};

/// A running sum and count, rendered as a Prometheus summary without quantiles.
#[derive(Default)]
pub(crate) struct Timer {
    micros: AtomicU64,
    count: AtomicU64,
}

impl Timer {
    pub(crate) fn record(&self, elapsed: Duration) {
        self.micros.fetch_add(elapsed.as_micros() as u64, Relaxed);
        self.count.fetch_add(1, Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) committed: AtomicU64,
    pub(crate) rejected: AtomicU64,
    pub(crate) queries_answered: AtomicU64,
    pub(crate) queries_rejected: AtomicU64,
    pub(crate) queries_failed: AtomicU64,
    /// From a query's arrival until its answer.
    pub(crate) query: Timer,
    pub(crate) middleware_passed: AtomicU64,
    pub(crate) middleware_rejected: AtomicU64,
    pub(crate) middleware_failed: AtomicU64,
    /// From a request's arrival until the whole middleware chain passed or rejected it.
    pub(crate) middleware: Timer,
    pub(crate) worker_sessions: AtomicU64,
    /// From lease to commit: worker round trip plus storage.
    pub(crate) execution: Timer,
    /// From commit until every listener acknowledged the events (settling submits only).
    pub(crate) settle: Timer,
    /// What a limit refused, by reason (see `Limits`).
    refused: Mutex<BTreeMap<&'static str, u64>>,
}

impl Metrics {
    /// Counts a refusal by a limit.
    pub(crate) fn limited(&self, reason: &'static str) {
        let mut refused = self.refused.lock().unwrap_or_else(PoisonError::into_inner);
        *refused.entry(reason).or_default() += 1;
    }
}

/// A Prometheus label value: backslash, quote and newline escaped.
fn label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The metrics in Prometheus text format.
pub(crate) fn render(kernel: &Kernel) -> String {
    let m = &kernel.metrics;
    let head = kernel.store.head_position().unwrap_or(0);
    let mut out = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, samples: &[(String, u64)]| {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };
    let one = |v: u64| vec![(String::new(), v)];
    metric(
        "kernable_commands_total",
        "counter",
        "Commands completed since the kernel started, by outcome: committed, or rejected by a worker or the kernel. Commands a middleware refused before the queue are not counted.",
        &[
            ("{outcome=\"committed\"}".into(), m.committed.load(Relaxed)),
            ("{outcome=\"rejected\"}".into(), m.rejected.load(Relaxed)),
        ],
    );
    metric(
        "kernable_queries_total",
        "counter",
        "Queries since the kernel started, by outcome: answered; rejected by a read model or a middleware; failed when no read model answered, because none was connected or none answered in time.",
        &[
            (
                "{outcome=\"answered\"}".into(),
                m.queries_answered.load(Relaxed),
            ),
            (
                "{outcome=\"rejected\"}".into(),
                m.queries_rejected.load(Relaxed),
            ),
            (
                "{outcome=\"failed\"}".into(),
                m.queries_failed.load(Relaxed),
            ),
        ],
    );
    metric(
        "kernable_middleware_total",
        "counter",
        "Runs of a middleware chain, before or after a command or query, by outcome: passed, rejected, or failed when a middleware in the chain was not connected or did not answer within --middleware-timeout-ms.",
        &[
            (
                "{outcome=\"passed\"}".into(),
                m.middleware_passed.load(Relaxed),
            ),
            (
                "{outcome=\"rejected\"}".into(),
                m.middleware_rejected.load(Relaxed),
            ),
            (
                "{outcome=\"failed\"}".into(),
                m.middleware_failed.load(Relaxed),
            ),
        ],
    );
    let refused: Vec<(String, u64)> = m
        .refused
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .map(|(reason, n)| (format!("{{reason=\"{reason}\"}}"), *n))
        .collect();
    metric(
        "kernable_refused_total",
        "counter",
        "Requests and connections a limit refused since the kernel started, by reason: connections, requests_per_key, subscriptions_per_key, listeners_per_key, queued_commands, queued_per_aggregate, queued_bytes, cache_bytes, handshake_timeout, too_large, http_host, http_connections.",
        &refused,
    );
    metric(
        "kernable_queued_commands",
        "gauge",
        "Commands stored in the queue and not completed yet: waiting for a worker or running.",
        &one(kernel.queue.len() as u64),
    );
    metric(
        "kernable_worker_sessions",
        "gauge",
        "Worker sessions connected now.",
        &one(m.worker_sessions.load(Relaxed)),
    );
    metric(
        "kernable_head_position",
        "gauge",
        "Global position of the newest stored event; 0 while the store is empty.",
        &one(head),
    );
    for (name, help, timer) in [
        (
            "kernable_execution_seconds",
            "Seconds from handing a command to a worker until its events are committed: the worker round trip plus storage. Committed commands only.",
            &m.execution,
        ),
        (
            "kernable_settle_seconds",
            "Seconds from the commit until every listener that counts acknowledged the events. Settling submits that settled in time only.",
            &m.settle,
        ),
        (
            "kernable_query_seconds",
            "Seconds from handing a query to the read models until one answered or rejected it. Queries that failed are not included.",
            &m.query,
        ),
        (
            "kernable_middleware_seconds",
            "Seconds one middleware chain took to pass, reject or fail a request.",
            &m.middleware,
        ),
    ] {
        let micros = timer.micros.load(Relaxed);
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} summary");
        let _ = writeln!(out, "{name}_sum {}", micros as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {}", timer.count.load(Relaxed));
    }
    let listeners = kernel.listeners.snapshot();
    let label = |name: &str| format!("{{listener=\"{}\"}}", label_value(name));
    let per = |f: &dyn Fn(&crate::listeners::ListenerStatus) -> u64| -> Vec<(String, u64)> {
        listeners.iter().map(|l| (label(&l.name), f(l))).collect()
    };
    let mut metric = |name: &str, help: &str, samples: Vec<(String, u64)>| {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };
    metric(
        "kernable_listener_position",
        "Global position of the last event the listener acknowledged.",
        per(&|l| l.position),
    );
    metric(
        "kernable_listener_lag",
        "Global positions between the newest stored event and the listener's position: how far it is behind.",
        per(&|l| head.saturating_sub(l.position)),
    );
    metric(
        "kernable_listener_connected",
        "1 while the listener is connected, 0 while it is registered but away.",
        per(&|l| l.connected as u64),
    );
    metric(
        "kernable_ready",
        "1 while the kernel is ready: every aggregate that counts has a worker, every listener that counts is connected and caught up, and it is not shutting down.",
        vec![(String::new(), kernel.readiness().is_ok() as u64)],
    );
    out
}

/// Serves `GET /metrics`, `GET /health` (the process runs) and `GET /ready` (see
/// `Kernel::readiness`) until the listener fails. Deliberately minimal HTTP/1, each connection
/// on its own, so a slow one cannot hold up the health checks.
pub(crate) async fn serve(kernel: Arc<Kernel>, addr: &str) -> Result<(), BoxError> {
    tracing::info!(%addr, "serving metrics");
    crate::http::serve(kernel, addr, respond).await
}

fn respond(kernel: &Kernel, request: &[u8]) -> Vec<u8> {
    if request.starts_with(b"GET /metrics ") {
        reply(
            "200 OK",
            "text/plain; version=0.0.4",
            render(kernel).as_bytes(),
        )
    } else if request.starts_with(b"GET /health ") {
        reply("200 OK", "text/plain", b"ok\n")
    } else if request.starts_with(b"GET /ready ") {
        match kernel.readiness() {
            Ok(()) => reply("200 OK", "text/plain", b"ready\n"),
            Err(why) => reply(
                "503 Service Unavailable",
                "text/plain",
                format!("{why}\n").as_bytes(),
            ),
        }
    } else {
        reply("404 Not Found", "text/plain", b"")
    }
}
