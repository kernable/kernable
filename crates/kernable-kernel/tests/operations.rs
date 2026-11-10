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

    // What an operator checks besides the services.
    let system = json(get(&ui, "/api/system").await);
    assert_eq!(system["role"], "primary");
    assert_eq!(system["addr"], env.addr.as_str());
    assert_eq!(system["store"]["head"], 1);
    assert_eq!(system["store"]["maxBytes"], 0);
    assert_eq!(system["counts"]["commands"], 1);
    assert_eq!(system["queue"]["queued"], 0);
    assert_eq!(system["replication"]["sync"], false);
    assert_eq!(system["backup"]["enabled"], false);
    assert!(system["partition"].is_null(), "{system}");
    assert!(system["startedAt"].as_i64().unwrap() <= system["now"].as_i64().unwrap());
    // The activity is sampled from the start, once a minute.
    let trend = json(get(&ui, "/api/trend").await);
    assert!(!trend["activity"].as_array().unwrap().is_empty(), "{trend}");
    // Without the process engine's events: the order's event stays.
    let business = json(get(&ui, "/api/events?business=1").await);
    assert_eq!(business["events"][0]["type"], "OrderPlaced");

    // The command log: the command, who sent it, and what came of it.
    let commands = json(get(&ui, "/api/commands").await);
    let c = &commands["commands"][0];
    assert_eq!(
        (&c["commandId"], &c["type"], &c["status"]),
        (
            &serde_json::json!("c1"),
            &serde_json::json!("PlaceOrder"),
            &serde_json::json!("committed")
        )
    );
    assert_eq!(c["events"][0]["type"], "OrderPlaced");
    assert!(c["tookMs"].as_i64().unwrap() >= 0, "{c}");
    assert!(commands["next"].is_null(), "{commands}");
    assert_eq!(
        json(get(&ui, "/api/commands?status=rejected").await)["commands"],
        serde_json::json!([])
    );
    assert_eq!(
        json(get(&ui, "/api/commands?command=c1&type=placeorder").await)["commands"][0]["commandId"],
        "c1"
    );

    // The verdict, computed by the kernel: nothing wrong here.
    let health = json(get(&ui, "/api/health").await);
    assert_eq!(
        (&health["level"], &health["issues"], &health["head"]),
        (
            &serde_json::json!("ok"),
            &serde_json::json!([]),
            &serde_json::json!(1)
        ),
        "{health}"
    );
    assert_eq!(health["incidents"], 0);
    assert_eq!(json(get(&ui, "/api/rejections?since=0").await)["count"], 0);
    // The aggregates, searched by the kernel.
    assert_eq!(
        json(get(&ui, "/api/streams?q=O1").await)["streams"][0]["id"],
        "o1"
    );
    assert_eq!(
        json(get(&ui, "/api/streams?q=nothing-like-it").await)["streams"],
        serde_json::json!([])
    );
    // The page is told when the log moves on: the head at once, then each new one.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut watch = tokio::net::TcpStream::connect(&ui).await.unwrap();
        watch
            .write_all(b"GET /api/watch HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut said = String::new();
        let mut buf = [0u8; 4096];
        let read_until = |said: &String, what: &str| said.contains(what);
        while !read_until(&said, "event: head\ndata: 1\n") {
            let n = tokio::time::timeout(Duration::from_secs(5), watch.read(&mut buf))
                .await
                .expect("told the head")
                .unwrap();
            assert!(n > 0, "{said}");
            said.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        assert!(
            said.starts_with("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream"),
            "{said}"
        );
        submit(
            &env.addr,
            signed(command("c2", "PlaceOrder", "o2"), &key(1)),
        )
        .await
        .unwrap();
        while !read_until(&said, "event: head\ndata: 2\n") {
            let n = tokio::time::timeout(Duration::from_secs(5), watch.read(&mut buf))
                .await
                .expect("told the new head")
                .unwrap();
            assert!(n > 0, "{said}");
            said.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }

    let streams = json(get(&ui, "/api/streams").await);
    assert_eq!(streams["streams"][0]["id"], "o2");
    let history = json(get(&ui, "/api/history?tenant=t1&domain=orders&type=Order&id=o1").await);
    assert_eq!(history["events"][0]["type"], "OrderPlaced");
    assert_eq!(history["commands"]["c1"]["type"], "PlaceOrder");
    assert!(get(&ui, "/nope").await.starts_with("HTTP/1.1 404"));
    // Each view has its own address, served the same page.
    for view in [
        "/processes/shop.fulfilment/cases/o-1/2?tenant=t2",
        "/operations",
        "/explore/events?type=x",
        "/search?q=o-1",
    ] {
        let page = get(&ui, view).await;
        assert!(
            page.starts_with("HTTP/1.1 200 OK") && page.contains("<title>kernable</title>"),
            "{view}"
        );
    }
    assert!(get(&ui, "/processesx").await.starts_with("HTTP/1.1 404"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ui_searches_and_tails_the_log() {
    let ui = free_addr();
    let env = start_custom("ui-events", |config, _| {
        config.ui_listen = Some(ui.clone());
    })
    .await;
    spawn_worker(&env).await;
    for (c, o) in [("c1", "o1"), ("c2", "o2"), ("c3", "o3")] {
        submit(&env.addr, signed(command(c, "PlaceOrder", o), &key(1)))
            .await
            .unwrap();
    }
    let json = |response: String| -> serde_json::Value {
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        serde_json::from_str(body).unwrap()
    };
    let positions = |found: &serde_json::Value| -> Vec<u64> {
        found["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["position"].as_u64().unwrap())
            .collect()
    };

    // Newest first, with the aggregate each event belongs to.
    let all = json(get(&ui, "/api/events").await);
    assert_eq!(all["head"], 3);
    assert_eq!(positions(&all), [3, 2, 1]);
    assert_eq!(all["events"][0]["aggregateId"], "o3");
    assert_eq!(all["events"][0]["domain"], "orders");
    assert!(all["next"].is_null(), "the whole log was read");

    // Filters: aggregate id, command id, type part in any case, a type that matches nothing.
    assert_eq!(positions(&json(get(&ui, "/api/events?id=o2").await)), [2]);
    assert_eq!(
        positions(&json(get(&ui, "/api/events?command=c1").await)),
        [1]
    );
    assert_eq!(
        positions(&json(get(&ui, "/api/events?type=ORDERS.orderplaced").await)),
        [3, 2, 1]
    );
    assert_eq!(
        positions(&json(get(&ui, "/api/events?type=Shipped").await)),
        Vec::<u64>::new()
    );

    // Pages: `next` continues with older events.
    let first = json(get(&ui, "/api/events?limit=2").await);
    assert_eq!(positions(&first), [3, 2]);
    assert_eq!(first["next"], 2);
    let rest = json(get(&ui, "/api/events?limit=2&before=2").await);
    assert_eq!(positions(&rest), [1]);
    assert!(rest["next"].is_null());

    // The tail: oldest first after a position, and `last` for the next call.
    let tail = json(get(&ui, "/api/events?after=1").await);
    assert_eq!(positions(&tail), [2, 3]);
    assert_eq!(tail["last"], 3);
    let idle = json(get(&ui, "/api/events?after=3").await);
    assert_eq!(positions(&idle), Vec::<u64>::new());
    assert_eq!(idle["last"], 3);
    submit(
        &env.addr,
        signed(command("c4", "PlaceOrder", "o4"), &key(1)),
    )
    .await
    .unwrap();
    let tail = json(get(&ui, "/api/events?after=3&id=o4").await);
    assert_eq!(positions(&tail), [4]);
    assert_eq!(tail["last"], 4);

    assert!(
        get(&ui, "/api/events?limit=x")
            .await
            .starts_with("HTTP/1.1 500")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ui_lists_rejected_commands_newest_first() {
    let ui = free_addr();
    let env = start_custom("ui-rejections", |config, _| {
        config.ui_listen = Some(ui.clone());
    })
    .await;
    spawn_worker(&env).await;
    let json = |response: String| -> serde_json::Value {
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        serde_json::from_str(body).unwrap()
    };
    assert_eq!(
        json(get(&ui, "/api/rejections").await)["rejections"],
        serde_json::json!([])
    );

    // An item for an order that does not exist is refused; placing an order is not.
    for (c, t, o) in [
        ("c1", "AddItem", "o1"),
        ("c2", "PlaceOrder", "o2"),
        ("c3", "AddItem", "o3"),
    ] {
        submit(&env.addr, signed(command(c, t, o), &key(1)))
            .await
            .unwrap();
    }
    let listed = json(get(&ui, "/api/rejections").await);
    let rejected = listed["rejections"].as_array().unwrap();
    let ids: Vec<&str> = rejected
        .iter()
        .map(|r| r["commandId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c3", "c1"]);
    assert_eq!(rejected[0]["type"], "AddItem");
    assert_eq!(rejected[0]["aggregateId"], "o3");
    assert_eq!(rejected[0]["domain"], "orders");
    assert_eq!(rejected[0]["code"], "invalid_state");
    assert_eq!(rejected[0]["message"], "AddItem not allowed now");
    assert!(rejected[0]["requirement"].is_null());

    let newest = json(get(&ui, "/api/rejections?limit=1").await);
    assert_eq!(newest["rejections"].as_array().unwrap().len(), 1);
    assert_eq!(newest["limit"], 1);
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

/// `<store>.usage`, signed: the kernel goes on from the counts the file holds, and writes the
/// sum at shutdown: committed commands across restarts, the store's head as its events.
#[tokio::test(flavor = "multi_thread")]
async fn the_usage_beside_the_store_counts_on_across_restarts() {
    let env = start_custom("usage", |config, _| {
        let before = kernable_kernel::usage::Usage {
            at: 1,
            events: 0,
            commands: 10,
            rejected: 1,
            queries: 5,
            ..Default::default()
        };
        let path = kernable_kernel::usage::path(&config.store).unwrap();
        std::fs::write(path, serde_json::to_vec(&before).unwrap()).unwrap();
    })
    .await;
    spawn_worker(&env).await;
    for i in 0..3 {
        let cmd = signed(
            command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
            &key(1),
        );
        submit(&env.addr, cmd).await.unwrap();
    }
    env.kernel.shutdown().unwrap();
    let usage = kernable_kernel::usage::read(&env.store).unwrap();
    assert_eq!(
        (usage.commands, usage.rejected, usage.queries, usage.events),
        (13, 1, 5, 3)
    );
    assert!(usage.at > 1);
    // Signed by the kernel key.
    assert!(usage.verify(&env.kernel_key.verifying_key()));
}
