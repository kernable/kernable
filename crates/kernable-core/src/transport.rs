//! Client channels to a kernel at `http://host:port` (or `host:port`), optionally with mutual
//! TLS, or, on Unix, at `unix:/path`.

use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Error, Identity, Uri};
use tower::service_fn;

/// PEM files for mutual TLS: the CA that signed the kernel's certificate, and this
/// process's own certificate and key.
#[derive(Debug, Clone)]
pub struct Tls {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// Connects to a kernel at `host:port` over mutual TLS.
pub async fn connect_tls(
    addr: &str,
    tls: &Tls,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_tls_pinging(addr, tls, None).await
}

/// Pings on the connection every `every`, also when idle, and ends it when a ping is not
/// answered within as long: a peer the network dropped without a word (a partition) shows
/// within twice `every`, not after TCP's minutes.
fn pinging(endpoint: Endpoint, every: Option<std::time::Duration>) -> Endpoint {
    match every {
        Some(every) => endpoint
            .http2_keep_alive_interval(every)
            .keep_alive_timeout(every)
            .keep_alive_while_idle(true)
            .connect_timeout(every.max(std::time::Duration::from_secs(1)) * 2),
        None => endpoint,
    }
}

/// Like [`connect_tls`], pinging every `every` (see [`connect_pinging`]).
pub async fn connect_tls_pinging(
    addr: &str,
    tls: &Tls,
    every: Option<std::time::Duration>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(&tls.ca)?))
        .identity(Identity::from_pem(
            std::fs::read(&tls.cert)?,
            std::fs::read(&tls.key)?,
        ));
    Ok(pinging(
        Endpoint::from_shared(format!("https://{}", tcp_addr(addr)))?.tls_config(config)?,
        every,
    )
    .connect()
    .await?)
}

/// Connects over TLS to `https://host:port`: the server's certificate checked against the public
/// CAs (Mozilla's, built in) and `ca` if given, this process's certificate shown only if given.
/// For a server that knows its clients by their keys, not by certificates (a relay).
pub async fn connect_https_pinging(
    addr: &str,
    ca: Option<&std::path::Path>,
    identity: Option<(&std::path::Path, &std::path::Path)>,
    every: Option<std::time::Duration>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = ClientTlsConfig::new().with_webpki_roots();
    if let Some(ca) = ca {
        config = config.ca_certificate(Certificate::from_pem(std::fs::read(ca)?));
    }
    if let Some((cert, key)) = identity {
        config = config.identity(Identity::from_pem(
            std::fs::read(cert)?,
            std::fs::read(key)?,
        ));
    }
    Ok(pinging(
        Endpoint::from_shared(format!("https://{}", tcp_addr(addr)))?.tls_config(config)?,
        every,
    )
    .connect()
    .await?)
}

pub async fn connect(addr: &str) -> Result<Channel, Error> {
    connect_pinging(addr, None).await
}

/// Like [`connect`]; with `every`, the connection is pinged that often and ends when a ping is
/// not answered within as long: a standby notices a primary the network cut off.
pub async fn connect_pinging(
    addr: &str,
    every: Option<std::time::Duration>,
) -> Result<Channel, Error> {
    match unix_path(addr) {
        #[cfg(unix)]
        Some(path) => {
            let path = path.to_owned();
            // The URI is ignored by the connector but must be valid.
            pinging(Endpoint::from_static("http://kernable"), every)
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(TokioIo::new(stream))
                    }
                }))
                .await
        }
        // Unix sockets exist on Unix only; the invalid URI says so in the error.
        #[cfg(not(unix))]
        Some(_) => {
            Endpoint::from_shared("unix sockets need Unix")?
                .connect()
                .await
        }
        None => {
            pinging(
                Endpoint::from_shared(format!("http://{}", tcp_addr(addr)))?,
                every,
            )
            .connect()
            .await
        }
    }
}

/// How often a client of several kernels (a pair) pings each connection, and how long it
/// waits for the answer: one the network cut off without a word ends within twice this, and the
/// client turns to the other kernel (a single kernel's client has nowhere else to go, and pings
/// every 30 s).
pub const PAIR_PING: std::time::Duration = std::time::Duration::from_secs(2);

/// Pings on the connection, also when idle, so one the network dropped without a word (a dead
/// host, a NAT that forgot it) ends within 40 s, or within twice [`PAIR_PING`] for a client of
/// `several` kernels; and a bound on connecting.
fn sdk_endpoint(endpoint: Endpoint, several: bool) -> Endpoint {
    let (every, answer, connect) = if several {
        (PAIR_PING, PAIR_PING, PAIR_PING * 2)
    } else {
        (
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(10),
        )
    };
    endpoint
        .http2_keep_alive_interval(every)
        .keep_alive_timeout(answer)
        .keep_alive_while_idle(true)
        .connect_timeout(connect)
}

/// A channel to a kernel that connects on first use and again after the connection drops, so
/// a process can start before the kernel and survive its restarts (SDK clients). With `tls`,
/// `addr` is `host:port` over mutual TLS. Needs a tokio runtime.
pub fn connect_lazy(
    addr: &str,
    tls: Option<&Tls>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_lazy_among(addr, tls, false)
}

/// [`connect_lazy`] for a client of `several` kernels (a pair, a partition's kernels): a
/// connection the network cut off ends within seconds, so the client turns to another.
pub fn connect_lazy_among(
    addr: &str,
    tls: Option<&Tls>,
    several: bool,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tls) = tls {
        let config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(std::fs::read(&tls.ca)?))
            .identity(Identity::from_pem(
                std::fs::read(&tls.cert)?,
                std::fs::read(&tls.key)?,
            ));
        return Ok(sdk_endpoint(
            Endpoint::from_shared(format!("https://{}", tcp_addr(addr)))?,
            several,
        )
        .tls_config(config)?
        .connect_lazy());
    }
    match unix_path(addr) {
        #[cfg(unix)]
        Some(path) => {
            let path = path.to_owned();
            Ok(
                sdk_endpoint(Endpoint::from_static("http://kernable"), several)
                    .connect_with_connector_lazy(service_fn(move |_: Uri| {
                        let path = path.clone();
                        async move {
                            let stream = tokio::net::UnixStream::connect(path).await?;
                            Ok::<_, std::io::Error>(TokioIo::new(stream))
                        }
                    })),
            )
        }
        #[cfg(not(unix))]
        Some(_) => Err("unix sockets need Unix".into()),
        None => Ok(sdk_endpoint(
            Endpoint::from_shared(format!("http://{}", tcp_addr(addr)))?,
            several,
        )
        .connect_lazy()),
    }
}

/// `http://host:port` and `https://host:port` both mean `host:port`.
pub fn tcp_addr(addr: &str) -> &str {
    let addr = addr
        .strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))
        .unwrap_or(addr);
    addr.strip_suffix('/').unwrap_or(addr)
}

/// `unix:/tmp/k.sock` and `unix:///tmp/k.sock` both mean `/tmp/k.sock`.
pub fn unix_path(addr: &str) -> Option<&str> {
    addr.strip_prefix("unix:")
        .map(|p| p.strip_prefix("//").unwrap_or(p))
}
