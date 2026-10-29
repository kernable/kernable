//! Operations: mutual TLS on TCP, and Prometheus metrics.

mod common;

use std::path::Path;

use common::*;
use kernable_core::{certs, proto::Wait, transport::Tls};
use kernable_kernel::config;

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
    start_worker(env.addr.clone(), worker).await;

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

#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_commands_queue_and_sessions() {
    let metrics = free_addr();
    let env = start_custom("metrics", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    spawn_worker(&env).await;
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

async fn slow_worker(env: &Env, delay_ms: u64) {
    let worker = TestWorker {
        behavior: Behavior::Slow(std::time::Duration::from_millis(delay_ms)),
        ..TestWorker::honest()
    };
    start_worker(env.addr.clone(), worker).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_lets_running_requests_finish_and_refuses_new_ones() {
    let env = start("drain").await;
    slow_worker(&env, 300).await;
    let addr = env.addr.clone();
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    let running = tokio::spawn(async move { submit(&addr, place).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let kernel = std::sync::Arc::clone(&env.kernel);
    let drained = tokio::spawn(async move { kernel.drain().await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let late = signed(command("c2", "PlaceOrder", "o2"), &key(1));
    let refused = submit(&env.addr, late).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    assert!(refused.message().contains("shutting down"), "{refused:?}");
    assert!(env.kernel.readiness().is_err());

    assert_eq!(committed_versions(&running.await.unwrap().unwrap()), [1]);
    assert!(drained.await.unwrap(), "everything running finished");
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_gives_up_after_its_timeout() {
    let env = start_custom("drain-timeout", |config, _| config.drain_timeout_ms = 200).await;
    slow_worker(&env, 1_000).await;
    let addr = env.addr.clone();
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    tokio::spawn(async move { submit(&addr, place).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    assert!(!env.kernel.drain().await);
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
}
