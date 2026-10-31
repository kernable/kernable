//! A TCP proxy between every client and the kernel that can fail the network: reset every
//! connection, stall all traffic for a while (a partition: nothing is lost, it waits), or delay
//! it.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::AbortHandle,
};

#[derive(Clone, Copy)]
enum Net {
    Up,
    /// Nothing gets through until then.
    Down(Instant),
    /// Every chunk waits this long, until then.
    Slow(Duration, Instant),
}

#[derive(Clone)]
pub struct Proxy {
    net: Arc<Mutex<Net>>,
    connections: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Proxy {
    /// Listens on `listen` and forwards to `target`.
    pub async fn start(listen: SocketAddr, target: SocketAddr) -> std::io::Result<Proxy> {
        let listener = TcpListener::bind(listen).await?;
        let proxy = Proxy {
            net: Arc::new(Mutex::new(Net::Up)),
            connections: Arc::default(),
        };
        let p = proxy.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let net = Arc::clone(&p.net);
                let task = tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(target).await else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    tokio::select! {
                        _ = pipe(cr, sw, Arc::clone(&net)) => {}
                        _ = pipe(sr, cw, net) => {}
                    }
                });
                let mut connections = p.connections.lock().unwrap();
                connections.retain(|c| !c.is_finished());
                connections.push(task.abort_handle());
            }
        });
        Ok(proxy)
    }

    /// Closes every open connection.
    pub fn reset(&self) {
        for c in self.connections.lock().unwrap().drain(..) {
            c.abort();
        }
    }

    pub fn down(&self, how_long: Duration) {
        *self.net.lock().unwrap() = Net::Down(Instant::now() + how_long);
    }

    pub fn slow(&self, delay: Duration, how_long: Duration) {
        *self.net.lock().unwrap() = Net::Slow(delay, Instant::now() + how_long);
    }

    pub fn heal(&self) {
        *self.net.lock().unwrap() = Net::Up;
    }
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    net: Arc<Mutex<Net>>,
) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        loop {
            let state = *net.lock().unwrap();
            match state {
                Net::Down(until) if Instant::now() < until => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Net::Slow(delay, until) if Instant::now() < until => {
                    tokio::time::sleep(delay).await;
                    break;
                }
                _ => break,
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}
