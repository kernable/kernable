//! At most `--max-connections` connections to the gRPC address at once: an accept loop that
//! closes a connection beyond the limit right away, and holds a permit for every other one
//! until it closes.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

use crate::Kernel;

/// A listening socket the kernel accepts connections on.
pub(crate) trait Accept: Send + 'static {
    type Io: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static;
    fn accept(&self) -> impl Future<Output = io::Result<Self::Io>> + Send;
}

impl Accept for tokio::net::TcpListener {
    type Io = tokio::net::TcpStream;
    async fn accept(&self) -> io::Result<Self::Io> {
        tokio::net::TcpListener::accept(self)
            .await
            .map(|(io, _)| io)
    }
}

#[cfg(unix)]
impl Accept for tokio::net::UnixListener {
    type Io = tokio::net::UnixStream;
    async fn accept(&self) -> io::Result<Self::Io> {
        tokio::net::UnixListener::accept(self)
            .await
            .map(|(io, _)| io)
    }
}

/// The connections `listener` accepts, at most `--max-connections` open at once.
pub(crate) fn limited<L: Accept>(
    kernel: Arc<Kernel>,
    listener: L,
) -> ReceiverStream<io::Result<Limited<L::Io>>> {
    let (tx, rx) = mpsc::channel(16);
    let permits = Arc::new(Semaphore::new(kernel.config().limits.connections));
    tokio::spawn(async move {
        loop {
            let io = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = tx.closed() => return,
            };
            let io = match io {
                Ok(io) => io,
                Err(e) => {
                    // Out of file descriptors, or a connection reset while waiting: retry
                    // after a pause instead of spinning.
                    tracing::warn!(error = %e, "accepting a connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            match Arc::clone(&permits).try_acquire_owned() {
                Ok(permit) => {
                    let limited = Limited {
                        io,
                        _permit: permit,
                    };
                    if tx.send(Ok(limited)).await.is_err() {
                        return;
                    }
                }
                Err(_) => {
                    kernel.metrics.limited("connections");
                    tracing::warn!("connection refused: --max-connections reached");
                }
            }
        }
    });
    ReceiverStream::new(rx)
}

/// A connection that counts against `--max-connections` until it is dropped.
pub(crate) struct Limited<Io> {
    io: Io,
    _permit: OwnedSemaphorePermit,
}

impl<Io: Connected> Connected for Limited<Io> {
    type ConnectInfo = Io::ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for Limited<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for Limited<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
