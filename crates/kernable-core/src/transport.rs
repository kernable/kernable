//! Client channels to a kernel listening on `unix:/path` or `host:port`, the latter
//! optionally with mutual TLS.

use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
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
    Ok(Endpoint::from_shared(format!("https://{addr}"))?
        .tls_config(config)?
        .connect()
        .await?)
}

pub async fn connect(addr: &str) -> Result<Channel, Error> {
    match unix_path(addr) {
        Some(path) => {
            let path = path.to_owned();
            // The URI is ignored by the connector but must be valid.
            Endpoint::from_static("http://kernable")
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = path.clone();
                    async move {
                        Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?))
                    }
                }))
                .await
        }
        None => {
            Endpoint::from_shared(format!("http://{addr}"))?
                .connect()
                .await
        }
    }
}

/// `unix:/tmp/k.sock` and `unix:///tmp/k.sock` both mean `/tmp/k.sock`.
pub fn unix_path(addr: &str) -> Option<&str> {
    addr.strip_prefix("unix:")
        .map(|p| p.strip_prefix("//").unwrap_or(p))
}
