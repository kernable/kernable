//! The kernel's local socket (`--admin-socket`): a Unix socket for the operator's tools on the
//! same machine, readable and writable by the kernel's own user only (mode 0600; root passes
//! file permissions anyway). It needs no login for the same reason the metrics address on a
//! loopback address needs none, and reaches further: a tool reaches it through the file system,
//! so it need not enter the network namespace the kernel runs in (a pod's).
//!
//! What it serves, plain HTTP/1 with one request per connection:
//!
//! - `GET /metrics`, `/health`, `/ready`, `/ready?deployment=<d>`: as on `--metrics-listen`.
//! - `GET /api/role`: what this kernel is (`writer`, `reader`, or while it follows `standby`)
//!   and how it stands, as JSON ([`crate::reader::role`]). A standby and a reader catching up
//!   serve it too, with `/health`, `/ready` and `/metrics`.
//! - `GET /api/deployments`, `/api/deployments/warm-up?deployment=<d>`,
//!   `/api/deployments/since?position=<n>|time=<ms>[&version=<v>]`: as in the UI.
//! - `POST /api/deployments/drop?deployment=<d>[&force=true]`: drops a deployment's read models
//!   and snapshots while the kernel runs ([`Kernel::drop_deployment`]); 200 with what went, 409
//!   with `{"error": <code>, "message": …}` when refused.
//! - `POST /api/switchover`: the primary hands its role to its standby in sync, with the checks
//!   of `kernable switchover` over the network (standby in sync, a handover without loss) but no
//!   key: reaching the socket is the permission. 200 with `{"standby", "seq"}` once handed over,
//!   409 with `{"error": "refused", "message": …}` when refused.

use std::{collections::HashMap, sync::Arc};

use serde_json::json;
use tokio::{io::AsyncWriteExt, net::UnixListener, sync::Semaphore};

use crate::{
    BoxError, Kernel,
    http::{CONNECTIONS, READ_TIMEOUT, read_head, reply},
};

/// Binds `path` (replacing a stale socket, never another file; making its directory if
/// missing), owner only, and serves it until the listener fails.
pub(crate) async fn serve(kernel: Arc<Kernel>, path: &str) -> Result<(), BoxError> {
    let limited = Arc::clone(&kernel);
    serve_on(
        path,
        move |reason| limited.metrics.limited(reason),
        move |request| respond(&kernel, request),
    )
    .await
}

/// Like [`serve`], answering with `respond`: for a standby or a reader catching up, which have
/// no kernel yet.
pub(crate) async fn serve_with(
    path: &str,
    respond: impl Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> Result<(), BoxError> {
    serve_on(path, |_| {}, respond).await
}

async fn serve_on(
    path: &str,
    limited: impl Fn(&'static str) + Send + Sync + 'static,
    respond: impl Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> Result<(), BoxError> {
    let respond = Arc::new(respond);
    let listener = bind(path).map_err(|e| format!("--admin-socket {path}: {e}"))?;
    tracing::info!(%path, "serving the local socket");
    let permits = Arc::new(Semaphore::new(CONNECTIONS));
    loop {
        let mut conn = match listener.accept().await {
            Ok((conn, _)) => conn,
            Err(e) => {
                tracing::warn!(error = %e, "accepting a connection on the local socket failed");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            limited("http_connections");
            continue;
        };
        let respond = Arc::clone(&respond);
        tokio::spawn(async move {
            let _permit = permit;
            let request = match tokio::time::timeout(READ_TIMEOUT, read_head(&mut conn)).await {
                Ok(Ok(Some(request))) => request,
                Ok(Ok(None)) => {
                    let large = reply(
                        "431 Request Header Fields Too Large",
                        "text/plain",
                        b"the request is too large\n",
                    );
                    let _ = conn.write_all(&large).await;
                    return;
                }
                _ => return,
            };
            let answer = tokio::task::spawn_blocking(move || respond(&request))
                .await
                .unwrap_or_else(|_| {
                    reply(
                        "500 Internal Server Error",
                        "text/plain",
                        b"the request failed\n",
                    )
                });
            let _ = conn.write_all(&answer).await;
        });
    }
}

fn bind(path: &str) -> std::io::Result<UnixListener> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    let file = std::path::Path::new(path);
    if let Some(dir) = file.parent().filter(|d| !d.as_os_str().is_empty())
        && !dir.exists()
    {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    if std::fs::symlink_metadata(file).is_ok_and(|m| m.file_type().is_socket()) {
        std::fs::remove_file(file)?;
    }
    let listener = UnixListener::bind(file)?;
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// The answer to one request.
pub(crate) fn respond(kernel: &Kernel, request: &[u8]) -> Vec<u8> {
    let line = String::from_utf8_lossy(request);
    let line = line.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    let (method, target) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match (method, path) {
        ("GET", "/metrics" | "/health" | "/ready") => crate::metrics::respond(kernel, request),
        ("GET", "/api/role") => {
            // With the kernel's own figures: a managed node reports them to its control point.
            let mut role = crate::reader::role(kernel);
            role["process"] = process(kernel);
            reply("200 OK", "application/json", role.to_string().as_bytes())
        }
        ("GET", "/api/deployments" | "/api/deployments/warm-up" | "/api/deployments/since") => {
            crate::ui::respond(kernel, request)
        }
        ("POST", "/api/deployments/drop") => {
            let params: HashMap<&str, &str> = query
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .collect();
            let deployment = params.get("deployment").copied().unwrap_or_default();
            let force = matches!(params.get("force").copied(), Some("true" | "1"));
            match kernel.drop_deployment(deployment, force) {
                Ok(dropped) => reply("200 OK", "application/json", dropped.to_string().as_bytes()),
                Err(refused) => reply(
                    match refused.code {
                        "invalid" => "400 Bad Request",
                        "unavailable" => "503 Service Unavailable",
                        "failed" => "500 Internal Server Error",
                        _ => "409 Conflict",
                    },
                    "application/json",
                    json!({ "error": refused.code, "message": refused.message })
                        .to_string()
                        .as_bytes(),
                ),
            }
        }
        (_, "/api/deployments/drop") => {
            reply("405 Method Not Allowed", "text/plain", b"POST only\n")
        }
        ("POST", "/api/switchover") => {
            // On a blocking thread of the runtime (see serve_on): it may wait for the handover.
            let asked = tokio::runtime::Handle::current()
                .block_on(crate::replication::switch_over(kernel, "the local socket"));
            match asked {
                Ok(result) => reply(
                    "200 OK",
                    "application/json",
                    json!({ "standby": result.standby, "seq": result.seq })
                        .to_string()
                        .as_bytes(),
                ),
                Err(status) => reply(
                    "409 Conflict",
                    "application/json",
                    json!({ "error": "refused", "message": status.message() })
                        .to_string()
                        .as_bytes(),
                ),
            }
        }
        (_, "/api/switchover") => reply("405 Method Not Allowed", "text/plain", b"POST only\n"),
        _ => reply("404 Not Found", "text/plain", b"not found\n"),
    }
}

/// The kernel process's own figures: CPU time used (ms), memory it holds now (bytes; the most it
/// held where the system tells no current figure), what its stores take on disk. Nothing of the
/// machine around it.
pub(crate) fn process(kernel: &crate::Kernel) -> serde_json::Value {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: RUSAGE_SELF and a rusage the call fills.
    let ok = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0;
    // `tv_usec` is an i32 on macOS and an i64 on Linux: the conversion is needed on one only.
    #[allow(clippy::useless_conversion)]
    let ms = |t: libc::timeval| t.tv_sec * 1000 + i64::from(t.tv_usec) / 1000;
    let cpu_ms = ok.then(|| ms(usage.ru_utime) + ms(usage.ru_stime));
    // Linux: the resident set now, from /proc/self/statm (pages); elsewhere the peak.
    let rss = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|t| t.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * page_size())
        .or_else(|| {
            ok.then(|| {
                let max = usage.ru_maxrss as u64;
                // macOS counts bytes, the others kilobytes.
                if cfg!(target_os = "macos") {
                    max
                } else {
                    max * 1024
                }
            })
        });
    serde_json::json!({
        "cpu_ms": cpu_ms,
        "rss_bytes": rss,
        "store_bytes": crate::usage::now(kernel).store_bytes,
    })
}

fn page_size() -> u64 {
    // SAFETY: sysconf with a valid name.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if size > 0 { size as u64 } else { 4096 }
}
