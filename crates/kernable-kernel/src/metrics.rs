//! Prometheus metrics, served as text at `GET /metrics` on `metrics_listen`.

use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{BoxError, Kernel};

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
        "Completed commands by outcome.",
        &[
            ("{outcome=\"committed\"}".into(), m.committed.load(Relaxed)),
            ("{outcome=\"rejected\"}".into(), m.rejected.load(Relaxed)),
        ],
    );
    metric(
        "kernable_queries_total",
        "counter",
        "Queries by outcome; failed means no read model answered.",
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
        "Commands and queries checked by middleware, by outcome; failed means a middleware was missing or too slow.",
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
    metric(
        "kernable_queued_commands",
        "gauge",
        "Commands waiting or running.",
        &one(kernel.queue.len() as u64),
    );
    metric(
        "kernable_worker_sessions",
        "gauge",
        "Connected worker sessions.",
        &one(m.worker_sessions.load(Relaxed)),
    );
    metric(
        "kernable_head_position",
        "gauge",
        "Global position of the newest event.",
        &one(head),
    );
    for (name, help, timer) in [
        (
            "kernable_execution_seconds",
            "From lease to commit.",
            &m.execution,
        ),
        (
            "kernable_settle_seconds",
            "From commit until every listener acknowledged the events.",
            &m.settle,
        ),
        (
            "kernable_query_seconds",
            "From a query's arrival until its answer.",
            &m.query,
        ),
        (
            "kernable_middleware_seconds",
            "From a request's arrival until the middleware chain passed or rejected it.",
            &m.middleware,
        ),
    ] {
        let micros = timer.micros.load(Relaxed);
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} summary");
        let _ = writeln!(out, "{name}_sum {}", micros as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {}", timer.count.load(Relaxed));
    }
    let listeners = kernel.listeners.snapshot();
    let label = |name: &str| format!("{{listener=\"{name}\"}}");
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
        "Last acknowledged global position per listener.",
        per(&|l| l.position),
    );
    metric(
        "kernable_listener_lag",
        "Events a listener has not acknowledged yet.",
        per(&|l| head.saturating_sub(l.position)),
    );
    metric(
        "kernable_listener_connected",
        "1 while the listener is connected.",
        per(&|l| l.connected as u64),
    );
    out
}

/// Serves `GET /metrics` until the listener fails. Deliberately minimal HTTP/1.
pub(crate) async fn serve(kernel: &Kernel, addr: &str) -> Result<(), BoxError> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "serving metrics");
    loop {
        let (mut conn, _) = listener.accept().await?;
        let mut request = [0u8; 1024];
        let n = match tokio::time::timeout(Duration::from_secs(5), conn.read(&mut request)).await {
            Ok(Ok(n)) => n,
            _ => continue,
        };
        let response = if request[..n].starts_with(b"GET /metrics ") {
            let body = render(kernel);
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
        } else {
            "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
        };
        let _ = conn.write_all(response.as_bytes()).await;
    }
}
