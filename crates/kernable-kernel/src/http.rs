//! The kernel's two plain HTTP endpoints, metrics and the UI: one `GET` per connection, at
//! most [`CONNECTIONS`] at once, each read within [`READ_TIMEOUT`]. On a loopback address a
//! request must name a loopback `Host`, so a web page cannot reach them through DNS rebinding.
//! An answer is either a whole response or a stream that keeps the connection (the UI's
//! `/api/watch`); one beyond [`CONNECTIONS`] gets `503`.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

use crate::{BoxError, Kernel};

/// Connections served at once; one beyond is closed.
const CONNECTIONS: usize = 32;
/// How long a connection may take to send its request.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A stream: given the connection, writes to it for as long as it wants.
pub(crate) type Streamer =
    Box<dyn FnOnce(TcpStream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// What answers a request: a whole response, or a stream.
pub(crate) enum Answer {
    Bytes(Vec<u8>),
    Stream(Streamer),
}

impl From<Vec<u8>> for Answer {
    fn from(bytes: Vec<u8>) -> Self {
        Answer::Bytes(bytes)
    }
}

/// Serves `respond(request)` on `addr` until the listener fails, each on a thread meant for
/// blocking work (a request may read much of the store); `stream(request)` may take a
/// request over instead.
pub(crate) async fn serve(
    kernel: Arc<Kernel>,
    addr: &str,
    respond: impl Fn(&Kernel, &[u8]) -> Vec<u8> + Send + Sync + 'static,
    stream: impl Fn(&Arc<Kernel>, &[u8]) -> Option<Answer> + Send + Sync + 'static,
) -> Result<(), BoxError> {
    let (respond, stream) = (Arc::new(respond), Arc::new(stream));
    let metrics = Arc::clone(&kernel);
    serve_with(
        addr,
        move |reason| metrics.metrics.limited(reason),
        move |request| {
            let (kernel, respond, stream) = (
                Arc::clone(&kernel),
                Arc::clone(&respond),
                Arc::clone(&stream),
            );
            async move {
                if let Some(answer) = stream(&kernel, &request) {
                    return answer;
                }
                let blocking =
                    tokio::task::spawn_blocking(move || respond(&kernel, &request)).await;
                Answer::Bytes(blocking.unwrap_or_else(|_| {
                    reply(
                        "500 Internal Server Error",
                        "text/plain",
                        b"the request failed\n",
                    )
                }))
            }
        },
    )
    .await
}

/// Like [`serve`], for a server without a kernel of its own (`kernable demo`): `limited` gets
/// the reason whenever a connection is turned away.
pub(crate) async fn serve_with<F, R>(
    addr: &str,
    limited: impl Fn(&'static str) + Send + Sync + 'static,
    respond: F,
) -> Result<(), BoxError>
where
    F: Fn(Vec<u8>) -> R + Send + Sync + 'static,
    R: Future + Send + 'static,
    R::Output: Into<Answer>,
{
    let listener = TcpListener::bind(addr).await?;
    let check_host = crate::loopback(addr);
    let permits = Arc::new(Semaphore::new(CONNECTIONS));
    let (limited, respond) = (Arc::new(limited), Arc::new(respond));
    loop {
        let (mut conn, _) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            limited("http_connections");
            // Said, not just closed: the browser shows why, and the UI tries again.
            tokio::spawn(async move {
                let busy = reply(
                    "503 Service Unavailable",
                    "text/plain",
                    b"too many connections; try again\n",
                );
                let _ = conn.write_all(&busy).await;
            });
            continue;
        };
        let (limited, respond) = (Arc::clone(&limited), Arc::clone(&respond));
        tokio::spawn(async move {
            let _permit = permit;
            let mut request = [0u8; 4096];
            let Ok(Ok(n)) = tokio::time::timeout(READ_TIMEOUT, conn.read(&mut request)).await
            else {
                return;
            };
            let request = &request[..n];
            let answer = if check_host && !host(request).is_some_and(|h| loopback_host(&h)) {
                limited("http_host");
                Answer::Bytes(reply(
                    "403 Forbidden",
                    "text/plain",
                    b"a loopback Host is required\n",
                ))
            } else {
                respond(request.to_vec()).await.into()
            };
            match answer {
                Answer::Bytes(response) => {
                    let _ = conn.write_all(&response).await;
                }
                Answer::Stream(stream) => stream(conn).await,
            }
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
