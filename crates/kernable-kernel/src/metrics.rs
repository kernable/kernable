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

    /// What limits refused since the start, by reason.
    pub(crate) fn refused(&self) -> BTreeMap<&'static str, u64> {
        self.refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
        "Commands completed since the kernel started, by outcome: committed, or rejected by a worker or the kernel (scheduled commands cancelled, expired, or refused by a middleware when due included). Commands a middleware refused before the queue are not counted.",
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
        "Requests and connections a limit refused since the kernel started, by reason: connections, requests_per_key, rate, quota_storage, subscriptions_per_key, listeners_per_key, queued_commands, queued_per_aggregate, queued_bytes, cache_bytes, handshake_timeout, too_large, http_host, http_connections.",
        &refused,
    );
    metric(
        "kernable_queued_commands",
        "gauge",
        "Commands stored in the queue and not completed yet: waiting for a worker or running.",
        &one(kernel.queue.len() as u64),
    );
    metric(
        "kernable_scheduled_commands",
        "gauge",
        "Scheduled commands (deliver_at) not due yet, or due and being checked before they join their aggregate's queue; counted in kernable_queued_commands too.",
        &one(kernel.queue.scheduled_len() as u64),
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
        "kernable_listener_parked",
        "Events the listener gave up on and parked, waiting for an operator to retry or dismiss them.",
        per(&|l| l.parked as u64),
    );
    metric(
        "kernable_listener_skipped",
        "Events the listener skipped since the kernel started because its code does not know them (a newer schema): its deployment is older than the events.",
        per(&|l| l.skipped),
    );
    if let Some(primary) = &kernel.replication {
        let head = kernel.store.record_head().map_or(0, |h| h.seq);
        metric(
            "kernable_record_head",
            "The newest record of the record log a standby follows (--replication sync).",
            vec![(String::new(), head)],
        );
        metric(
            "kernable_standby_acknowledged",
            "The newest record the standby holds on its disk; writes after it wait.",
            vec![(String::new(), primary.acked_seq())],
        );
        metric(
            "kernable_standby_in_sync",
            "1 while a standby is connected and has caught up; without it, writes wait.",
            vec![(String::new(), primary.in_sync() as u64)],
        );
        metric(
            "kernable_standby_catching_up",
            "1 while no standby has caught up yet (without a witness): writes are told without one.",
            vec![(String::new(), primary.catching_up() as u64)],
        );
    }
    if kernel.config().replication.readers {
        let head = kernel.store.record_head().map_or(0, |h| h.seq);
        let readers = kernel.readers.connected();
        metric(
            "kernable_readers",
            "Readers (read-only copies) following this kernel now (--readers on).",
            vec![(String::new(), readers.len() as u64)],
        );
        metric(
            "kernable_readers_lag_max",
            "Records the furthest behind of the connected readers lacks of this kernel's record log (0 without readers).",
            vec![(
                String::new(),
                readers
                    .iter()
                    .map(|r| head.saturating_sub(r.acked))
                    .max()
                    .unwrap_or(0),
            )],
        );
    }
    if let Some(reader) = &kernel.reader {
        metric(
            "kernable_reader_following",
            "On a reader: 1 while it follows its writer (connected, and heard from it within 5 s).",
            vec![(String::new(), reader.following() as u64)],
        );
        metric(
            "kernable_reader_applied",
            "On a reader: the newest record of its writer's log it holds.",
            vec![(String::new(), reader.applied())],
        );
        metric(
            "kernable_reader_lag",
            "On a reader: records it is behind what its writer may tell, as last heard.",
            vec![(String::new(), reader.lag())],
        );
        metric(
            "kernable_reader_silent_ms",
            "On a reader: milliseconds since its writer last sent anything (it sends at least every second).",
            vec![(
                String::new(),
                reader.silent_ms().map_or(u64::MAX, |ms| ms as u64),
            )],
        );
    }
    let limits = &kernel.config().limits;
    metric(
        "kernable_store_bytes",
        "Bytes the event store, the vault and the files take on the disk, measured every 10 s.",
        vec![(String::new(), kernel.stored_bytes.load(Relaxed))],
    );
    if limits.store_bytes > 0 {
        metric(
            "kernable_store_bytes_max",
            "--max-store-bytes: at it, new commands and file puts are refused (quota_storage).",
            vec![(String::new(), limits.store_bytes)],
        );
    }
    metric(
        "kernable_ready",
        "1 while the kernel is ready: every aggregate that counts has a worker, every listener that counts is connected and caught up, and it is not shutting down.",
        vec![(String::new(), kernel.readiness().is_ok() as u64)],
    );
    crate::process_engine::render(kernel, &mut out);
    out
}

/// What a standby, or a reader catching up, serves at `GET /metrics`: it has no kernel to
/// render yet.
pub(crate) fn render_standby(applied: u64, primary_head: u64, reader: bool) -> String {
    let mut out = String::new();
    if reader {
        let _ = write!(
            out,
            "# HELP kernable_reader_following On a reader: 0 while it catches up.\n# TYPE kernable_reader_following gauge\nkernable_reader_following 0\n\
             # HELP kernable_reader_applied On a reader: the newest record of its writer's log it holds.\n# TYPE kernable_reader_applied gauge\nkernable_reader_applied {applied}\n\
             # HELP kernable_reader_lag On a reader: records it is behind what its writer may tell, as last heard.\n# TYPE kernable_reader_lag gauge\nkernable_reader_lag {}\n\
             # HELP kernable_ready 0 on a reader catching up.\n# TYPE kernable_ready gauge\nkernable_ready 0\n",
            primary_head.saturating_sub(applied)
        );
        return out;
    }
    let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
        );
    };
    metric(
        "kernable_standby_applied",
        "gauge",
        "On a standby: the newest record of the primary's log it holds.",
        applied,
    );
    metric(
        "kernable_standby_lag",
        "gauge",
        "On a standby: records it is behind the primary's head as last seen.",
        primary_head.saturating_sub(applied),
    );
    // As on a primary (documented there), always 0: a standby serves nothing.
    out.push_str(
        "# HELP kernable_ready 0 on a standby.\n# TYPE kernable_ready gauge\nkernable_ready 0\n",
    );
    out
}

/// Serves `GET /metrics`, `GET /health` (the process runs), `GET /ready` (see
/// `Kernel::readiness`) and `GET /ready?deployment=<d>` (see `Kernel::deployment_readiness`)
/// until the listener fails. Deliberately minimal HTTP/1, each connection
/// on its own, so a slow one cannot hold up the health checks.
pub(crate) async fn serve(kernel: Arc<Kernel>, addr: &str) -> Result<(), BoxError> {
    tracing::info!(%addr, "serving metrics");
    crate::http::serve(kernel, addr, respond, |_, _| None).await
}

pub(crate) fn respond(kernel: &Kernel, request: &[u8]) -> Vec<u8> {
    if request.starts_with(b"GET /metrics ") {
        reply(
            "200 OK",
            "text/plain; version=0.0.4",
            render(kernel).as_bytes(),
        )
    } else if request.starts_with(b"GET /health ") {
        match &kernel.reader {
            // A reader's process runs; how it stands, for whoever looks.
            Some(r) => reply(
                "200 OK",
                "text/plain",
                format!(
                    "ok: a reader of {}, {}, {} records behind\n",
                    r.writer(),
                    if r.following() {
                        "following"
                    } else {
                        "away from its writer"
                    },
                    r.lag()
                )
                .as_bytes(),
            ),
            None => reply("200 OK", "text/plain", b"ok\n"),
        }
    } else if let Some(rest) = request.strip_prefix(b"GET /ready?deployment=".as_slice()) {
        // How far a deployment is from taking over: 200 once it could.
        let end = rest.iter().position(|b| *b == b' ').unwrap_or(rest.len());
        let deployment = String::from_utf8_lossy(&rest[..end]).into_owned();
        match kernel.deployment_readiness(&deployment) {
            Ok(()) => reply("200 OK", "text/plain", b"caught up\n"),
            Err(why) => reply(
                "503 Service Unavailable",
                "text/plain",
                format!("{why}\n").as_bytes(),
            ),
        }
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
