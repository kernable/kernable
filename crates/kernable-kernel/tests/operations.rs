//! Operations: mutual TLS on TCP, and Prometheus metrics.

mod common;

use std::path::Path;

use common::*;
use kernable_core::{certs, proto::Wait, transport::Tls};
use kernable_kernel::config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn client_tls(dir: &Path, name: &str) -> Tls {
    Tls {
        ca: dir.join("ca.tls.crt"),
        cert: dir.join(format!("{name}.tls.crt")),
        key: dir.join(format!("{name}.tls.key")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_with_mutual_tls_admits_only_clients_of_the_ca() {
    let env = start_custom("tls", |config, dir| {
        certs::create(dir, &["app", "worker"]).unwrap();
        certs::create(&dir.join("other"), &["stranger"]).unwrap();
        config.listen = free_addr();
        config.tls = Some(config::Tls {
            cert: dir.join("kernel.tls.crt"),
            key: dir.join("kernel.tls.key"),
            client_ca: dir.join("ca.tls.crt"),
        });
    })
    .await;
    let worker = TestWorker {
        tls: Some(client_tls(&env.dir, "worker")),
        ..TestWorker::honest()
    };
    tokio::spawn(run_worker(env.addr.clone(), worker));

    let cmd = || signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let ok = channel(&env.addr, Some(&client_tls(&env.dir, "app")))
        .await
        .unwrap();
    assert_eq!(
        committed_versions(&submit_on(ok, cmd(), Wait::Settled).await.unwrap()),
        [1]
    );

    // No certificate, a certificate of another CA, or no TLS at all: refused.
    let stranger = client_tls(&env.dir.join("other"), "stranger");
    let stranger = Tls {
        ca: env.dir.join("ca.tls.crt"),
        ..stranger
    };
    let refused = match channel(&env.addr, Some(&stranger)).await {
        Ok(channel) => submit_on(channel, cmd(), Wait::Settled).await.is_err(),
        Err(_) => true,
    };
    assert!(refused, "a client certificate of another CA was accepted");
    let plain = match channel(&env.addr, None).await {
        Ok(channel) => submit_on(channel, cmd(), Wait::Settled).await.is_err(),
        Err(_) => true,
    };
    assert!(plain, "a client without TLS was accepted");
}

async fn get(addr: &str, path: &str) -> String {
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_commands_queue_and_sessions() {
    let metrics = free_addr();
    let env = start_custom("metrics", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env);
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, place).await.unwrap();
    let again = signed(command("c2", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, again).await.unwrap(); // rejected: the order exists

    let body = get(&metrics, "/metrics").await;
    assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    for line in [
        "kernable_commands_total{outcome=\"committed\"} 1",
        "kernable_commands_total{outcome=\"rejected\"} 1",
        "kernable_queued_commands 0",
        "kernable_worker_sessions 1",
        "kernable_head_position 1",
        "kernable_execution_seconds_count 1",
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in\n{body}"
        );
    }
    assert!(get(&metrics, "/").await.starts_with("HTTP/1.1 404"));
}
