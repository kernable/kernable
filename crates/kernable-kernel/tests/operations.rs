//! Operations: mutual TLS on TCP, and Prometheus metrics.

mod common;

use std::{path::Path, time::Duration};

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
    // TLS that trusts the kernel, but shows no certificate of its own: refused; the same with
    // the app's certificate gets through, so it is the missing certificate that counts.
    let raw = |identity: Option<&str>| {
        let mut tls = tonic::transport::ClientTlsConfig::new().ca_certificate(
            tonic::transport::Certificate::from_pem(
                std::fs::read(env.dir.join("ca.tls.crt")).unwrap(),
            ),
        );
        if let Some(name) = identity {
            let read =
                |ext: &str| std::fs::read(env.dir.join(format!("{name}.tls.{ext}"))).unwrap();
            tls = tls.identity(tonic::transport::Identity::from_pem(
                read("crt"),
                read("key"),
            ));
        }
        tonic::transport::Endpoint::from_shared(format!("https://{}", env.addr))
            .unwrap()
            .tls_config(tls)
            .unwrap()
    };
    let with = raw(Some("app")).connect().await.unwrap();
    let again = signed(command("c2", "AddItem", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit_on(with, again, Wait::Settled).await.unwrap()),
        [2]
    );
    let anonymous = match raw(None).connect().await {
        Ok(channel) => submit_on(channel, cmd(), Wait::Settled).await.is_err(),
        Err(_) => true,
    };
    assert!(anonymous, "a client without a certificate was accepted");
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_commands_queue_and_sessions() {
    let metrics = free_addr();
    let env = start_custom("metrics", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    let worker = start_worker(env.addr.clone(), TestWorker::honest()).await;
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

    // A worker that goes away is no longer counted.
    worker.abort();
    let mut gone = false;
    for _ in 0..100 {
        if get(&metrics, "/metrics")
            .await
            .lines()
            .any(|l| l == "kernable_worker_sessions 0")
        {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(gone, "the session is still counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ui_shows_the_map_the_connections_and_an_aggregate_history() {
    let ui = free_addr();
    let env = start_custom("ui", |config, _| {
        config.ui_listen = Some(ui.clone());
    })
    .await;
    spawn_worker(&env).await;
    let place = signed(command("c1", "PlaceOrder", "o1"), &key(1));
    submit(&env.addr, place).await.unwrap();

    let page = get(&ui, "/").await;
    assert!(page.starts_with("HTTP/1.1 200 OK") && page.contains("<title>kernable</title>"));
    let json = |response: String| -> serde_json::Value {
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        serde_json::from_str(body).unwrap()
    };
    let state = json(get(&ui, "/api/state").await);
    assert_eq!(state["kernel"]["head"], 1);
    assert_eq!(state["aggregates"][0]["aggregate"], "orders.Order");
    assert_eq!(state["aggregates"][0]["workers"], 1);
    assert_eq!(state["instances"][0]["aggregates"][0], "orders.Order");
    assert_eq!(state["events"][0]["type"], "orders.OrderPlaced");
    assert_eq!(state["events"][0]["writers"][0], "orders.Order");

    let streams = json(get(&ui, "/api/streams").await);
    assert_eq!(streams["streams"][0]["id"], "o1");
    let history = json(get(&ui, "/api/history?tenant=t1&domain=orders&type=Order&id=o1").await);
    assert_eq!(history["events"][0]["type"], "OrderPlaced");
    assert_eq!(history["commands"]["c1"]["type"], "PlaceOrder");
    assert!(get(&ui, "/nope").await.starts_with("HTTP/1.1 404"));
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
