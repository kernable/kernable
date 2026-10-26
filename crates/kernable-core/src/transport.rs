//! Client channels to a kernel listening on `unix:/path` or `host:port`.

use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Error, Uri};
use tower::service_fn;

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
