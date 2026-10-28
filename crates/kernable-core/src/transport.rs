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
    let config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(&tls.ca)?))
        .identity(Identity::from_pem(
            std::fs::read(&tls.cert)?,
            std::fs::read(&tls.key)?,
        ));
    Ok(
        Endpoint::from_shared(format!("https://{}", tcp_addr(addr)))?
            .tls_config(config)?
            .connect()
            .await?,
    )
}

pub async fn connect(addr: &str) -> Result<Channel, Error> {
    match unix_path(addr) {
        #[cfg(unix)]
        Some(path) => {
            let path = path.to_owned();
            // The URI is ignored by the connector but must be valid.
            Endpoint::from_static("http://kernable")
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
            Endpoint::from_shared(format!("http://{}", tcp_addr(addr)))?
                .connect()
                .await
        }
    }
}

/// A channel to a kernel that connects on first use and again after the connection drops, so
/// a process can start before the kernel and survive its restarts (SDK clients). With `tls`,
/// `addr` is `host:port` over mutual TLS. Needs a tokio runtime.
pub fn connect_lazy(
    addr: &str,
    tls: Option<&Tls>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tls) = tls {
        let config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(std::fs::read(&tls.ca)?))
            .identity(Identity::from_pem(
                std::fs::read(&tls.cert)?,
                std::fs::read(&tls.key)?,
            ));
        return Ok(
            Endpoint::from_shared(format!("https://{}", tcp_addr(addr)))?
                .tls_config(config)?
                .connect_lazy(),
        );
    }
    match unix_path(addr) {
        #[cfg(unix)]
        Some(path) => {
            let path = path.to_owned();
            Ok(
                Endpoint::from_static("http://kernable").connect_with_connector_lazy(service_fn(
                    move |_: Uri| {
                        let path = path.clone();
                        async move {
                            let stream = tokio::net::UnixStream::connect(path).await?;
                            Ok::<_, std::io::Error>(TokioIo::new(stream))
                        }
                    },
                )),
            )
        }
        #[cfg(not(unix))]
        Some(_) => Err("unix sockets need Unix".into()),
        None => Ok(Endpoint::from_shared(format!("http://{}", tcp_addr(addr)))?.connect_lazy()),
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
