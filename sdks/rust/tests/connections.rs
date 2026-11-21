//! A refusal that a damaged connection can cause (an unreadable grant) is not tried again on
//! that connection: HTTP/2 keeps headers in a table both sides share, so a header damaged on
//! the way stays wrong there for every later request. The next call opens a new one.

mod common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use common::TestKernel;
use kernable::{Kernable, Submit};
use tokio::net::{UnixListener, UnixStream};

/// A proxy at `path` to the kernel's socket that counts the connections it accepts.
fn proxy(path: &std::path::Path, kernel: String) -> Arc<AtomicUsize> {
    let accepted = Arc::new(AtomicUsize::new(0));
    let listener = UnixListener::bind(path).unwrap();
    let count = Arc::clone(&accepted);
    tokio::spawn(async move {
        loop {
            let (mut inbound, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            let kernel = kernel.clone();
            tokio::spawn(async move {
                let mut outbound = UnixStream::connect(&kernel).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });
    accepted
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_grant_is_not_tried_again_on_the_same_connection() {
    let kernel = TestKernel::new(&["*"], &[]);
    kernel.start().await;
    let socket = kernel.url.strip_prefix("unix:").unwrap().to_owned();
    let path = kernel.dir.join("proxy.sock");
    let accepted = proxy(&path, socket);
    // Hex, but no Grant: the kernel refuses it as unreadable.
    let grant = kernel.dir.join("damaged.grant");
    std::fs::write(&grant, "ff00ff00").unwrap();
    let client = Kernable::new(
        kernel
            .options()
            .url(format!("unix:{}", path.display()))
            .grant(&grant),
    )
    .unwrap();
    for i in 0..3 {
        let refused = client
            .submit(Submit::new("shop", "Order", &format!("o{i}"), "Place"))
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("grant"), "{refused}");
        assert_eq!(accepted.load(Ordering::SeqCst), i + 1, "call {i}");
    }
}
