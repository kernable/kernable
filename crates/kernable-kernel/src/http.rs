//! The kernel's two plain HTTP endpoints, metrics and the UI: one `GET` per connection, at
//! most [`CONNECTIONS`] at once, each read within [`READ_TIMEOUT`]. On a loopback address a
//! request must name a loopback `Host`, so a web page cannot reach them through DNS rebinding.

use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
};

use crate::{BoxError, Kernel};

/// Connections served at once; one beyond is closed.
const CONNECTIONS: usize = 32;
/// How long a connection may take to send its request.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Serves `respond(request)` on `addr` until the listener fails.
pub(crate) async fn serve(
    kernel: Arc<Kernel>,
    addr: &str,
    respond: impl Fn(&Kernel, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> Result<(), BoxError> {
    let listener = TcpListener::bind(addr).await?;
    let check_host = crate::loopback(addr);
    let permits = Arc::new(Semaphore::new(CONNECTIONS));
    let respond = Arc::new(respond);
    loop {
        let (mut conn, _) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            kernel.metrics.limited("http_connections");
            continue;
        };
        let (kernel, respond) = (Arc::clone(&kernel), Arc::clone(&respond));
        tokio::spawn(async move {
            let _permit = permit;
            let mut request = [0u8; 4096];
            let Ok(Ok(n)) = tokio::time::timeout(READ_TIMEOUT, conn.read(&mut request)).await
            else {
                return;
            };
            let request = &request[..n];
            let response = if check_host && !host(request).is_some_and(|h| loopback_host(&h)) {
                kernel.metrics.limited("http_host");
                reply(
                    "403 Forbidden",
                    "text/plain",
                    b"a loopback Host is required\n",
                )
            } else {
                respond(&kernel, request)
            };
            let _ = conn.write_all(&response).await;
        });
    }
}

/// An HTTP/1.1 response that closes the connection.
pub(crate) fn reply(status: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\
         cache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

/// The `Host` header of a request, if it has one.
pub(crate) fn host(request: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(request);
    text.lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("host")
                .then(|| value.trim().to_owned())
        })
}

/// Whether a `Host` value (with or without a port) names this machine.
pub(crate) fn loopback_host(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => host.rsplit_once(':').map_or(host, |(name, _)| name),
    };
    name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_hosts_pass() {
        let get = |h: &str| format!("GET /api/state HTTP/1.1\r\n{h}\r\nAccept: */*\r\n\r\n");
        for ok in [
            "Host: localhost:7234",
            "host: 127.0.0.1:7234",
            "HOST: [::1]:7234",
            "Host: LOCALHOST",
            "Host: 127.0.0.2",
        ] {
            let h = host(get(ok).as_bytes()).unwrap();
            assert!(loopback_host(&h), "{ok}");
        }
        for bad in [
            "Host: evil.example:7234",
            "Host: 127.0.0.1.evil.example",
            "Host: localhost.evil.example",
            "Host: 10.0.0.1:7234",
        ] {
            let h = host(get(bad).as_bytes()).unwrap();
            assert!(!loopback_host(&h), "{bad}");
        }
        assert_eq!(host(b"GET / HTTP/1.1\r\nAccept: */*\r\n\r\n"), None);
        assert_eq!(
            host(b"GET / HTTP/1.1\r\n\r\nHost: localhost\r\n"),
            None,
            "the body is not a header"
        );
    }
}
