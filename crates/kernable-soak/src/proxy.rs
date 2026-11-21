//! A TCP proxy between every client and the kernel that can fail the network: reset every
//! connection, stall all traffic for a while (a partition: nothing is lost, it waits), delay it,
//! or flip a byte of what goes to the kernel (a hostile network: signatures and the protocol
//! must refuse it).
//!
//! The proxies run on a runtime of their own: while the harness blocks (reading a store,
//! stopping a process), the network goes on as it would without the harness, so a stall the
//! kernels see is one of the faults, never the harness's.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    task::AbortHandle,
};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("proxy")
            .enable_all()
            .build()
            .expect("a runtime for the proxies")
    })
}

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
    /// How many chunks on their way to the kernel still get a flipped byte.
    corrupt: Arc<AtomicU64>,
}

impl Proxy {
    /// Listens on `listen` and forwards to `target`.
    pub async fn start(listen: SocketAddr, target: SocketAddr) -> std::io::Result<Proxy> {
        let listener = runtime().spawn(TcpListener::bind(listen)).await??;
        let proxy = Proxy {
            net: Arc::new(Mutex::new(Net::Up)),
            connections: Arc::default(),
            corrupt: Arc::default(),
        };
        let p = proxy.clone();
        runtime().spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let net = Arc::clone(&p.net);
                let corrupt = Arc::clone(&p.corrupt);
                let task = tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(target).await else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    tokio::select! {
                        _ = pipe(cr, sw, Arc::clone(&net), Some(corrupt)) => {}
                        _ = pipe(sr, cw, net, None) => {}
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

    /// Flips one byte in each of the next `chunks` chunks on their way to the kernel.
    pub fn corrupt(&self, chunks: u64) {
        self.corrupt.store(chunks, Ordering::SeqCst);
    }

    pub fn heal(&self) {
        *self.net.lock().unwrap() = Net::Up;
        self.corrupt.store(0, Ordering::SeqCst);
    }
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    net: Arc<Mutex<Net>>,
    corrupt: Option<Arc<AtomicU64>>,
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
        if let Some(c) = &corrupt
            && c.try_update(Ordering::SeqCst, Ordering::SeqCst, |k| k.checked_sub(1))
                .is_ok()
        {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos() as usize);
            buf[nanos % n] ^= 1 << (nanos % 8);
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}
