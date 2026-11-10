//! Resource limits: beyond each one the kernel refuses with RESOURCE_EXHAUSTED (or
//! closes the connection), counts it in `kernable_refused_total`, and goes on serving.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, Event, Hello, ListenerMessage, Register,
        SubscribeRequest, Wait, WorkerMessage, command_result::Outcome,
        event_service_client::EventServiceClient, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
        worker_gateway_client::WorkerGatewayClient, worker_message::Msg as WorkerIn,
    },
    signing, transport,
};
use kernable_kernel::config::Limits;
use kernable_store::invariants::{Log, Observed};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Streaming};

/// A kernel with these limits and a metrics port; returns it and the metrics address.
async fn start_limited(name: &str, limits: Limits) -> (Env, String) {
    let metrics = free_addr();
    let env = start_custom(name, |config, _| {
        config.limits = limits;
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    (env, metrics)
}

/// How often a limit refused, by `kernable_refused_total{reason}`.
async fn refused(metrics: &str, reason: &str) -> u64 {
    let text = get(metrics, "/metrics").await;
    let prefix = format!("kernable_refused_total{{reason=\"{reason}\"}} ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map_or(0, |n| n.parse().unwrap())
}

fn with_payload(id: &str, command_type: &str, aggregate_id: &str, bytes: usize) -> Command {
    let mut cmd = command(id, command_type, aggregate_id);
    cmd.payload = vec![b'x'; bytes];
    signed(cmd, &key(1))
}

fn accepted(id: &str, aggregate_id: &str) -> Command {
    signed(command(id, "PlaceOrder", aggregate_id), &key(1))
}

/// Lists `orders.Order` in a membership, so its commands queue while no worker runs.
async fn queue_without_worker(env: &Env) {
    apply(&env.addr, membership("shop", &["orders.Order"], &[], &[]))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_full_queue_refuses_new_commands_and_still_answers_stored_ones() {
    let (env, metrics) = start_limited(
        "limit-queue",
        Limits {
            queued_commands: 3,
            queued_per_aggregate: 2,
            ..Limits::default()
        },
    )
    .await;
    queue_without_worker(&env).await;
    let submit = |cmd| submit_with(&env.addr, cmd, Wait::Accepted);

    for id in ["c1", "c2"] {
        submit(accepted(id, "o1")).await.unwrap();
    }
    let full = submit(accepted("c3", "o1")).await.unwrap_err();
    assert_eq!(full.code(), Code::ResourceExhausted, "{full:?}");
    assert!(full.message().contains("queued_per_aggregate"));
    submit(accepted("c1", "o1"))
        .await
        .expect("a queued command is answered");
    submit(accepted("c3", "o2")).await.unwrap();
    let full = submit(accepted("c4", "o3")).await.unwrap_err();
    assert_eq!(full.code(), Code::ResourceExhausted);
    assert!(full.message().contains("queued_commands"));
    assert_eq!(refused(&metrics, "queued_per_aggregate").await, 1);
    assert_eq!(refused(&metrics, "queued_commands").await, 1);

    // Once a worker runs the queue down, new commands are taken again, and a command that
    // completed is answered whatever the queue holds.
    start_worker(
        env.addr.clone(),
        TestWorker {
            service: "shop".into(),
            ..TestWorker::honest()
        },
    )
    .await;
    let done = submit_with(&env.addr, accepted("c3", "o2"), Wait::Committed)
        .await
        .unwrap();
    assert!(matches!(done.outcome, Some(Outcome::Committed(_))));
    submit_with(&env.addr, accepted("c4", "o3"), Wait::Committed)
        .await
        .unwrap();
}

#[tokio::test]
async fn the_queue_counts_bytes() {
    let (env, metrics) = start_limited(
        "limit-queue-bytes",
        Limits {
            queued_bytes: 10_000,
            ..Limits::default()
        },
    )
    .await;
    queue_without_worker(&env).await;
    let submit = |cmd| submit_with(&env.addr, cmd, Wait::Accepted);
    submit(with_payload("c1", "PlaceOrder", "o1", 6_000))
        .await
        .unwrap();
    let full = submit(with_payload("c2", "PlaceOrder", "o2", 6_000))
        .await
        .unwrap_err();
    assert_eq!(full.code(), Code::ResourceExhausted);
    assert_eq!(refused(&metrics, "queued_bytes").await, 1);
    submit(with_payload("c3", "PlaceOrder", "o3", 100))
        .await
        .unwrap();
}

async fn subscribe(addr: &str) -> Result<Streaming<Event>, tonic::Status> {
    let issued_at = now_ms();
    let msg = signing::subscribe_message(0, &[], issued_at);
    let request = SubscribeRequest {
        after_position: 0,
        event_types: Vec::new(),
        issued_at,
        client_sig: Some(signing::sign(&key(1), &msg)),
        protocol: kernable_core::PROTOCOL,
    };
    let channel = transport::connect(addr).await.unwrap();
    EventServiceClient::new(channel)
        .subscribe(with_grant(request, &key(1)))
        .await
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn requests_and_subscriptions_per_key_are_bounded() {
    let (env, metrics) = start_limited(
        "limit-per-key",
        Limits {
            requests_per_key: 1,
            subscriptions_per_key: 1,
            ..Limits::default()
        },
    )
    .await;
    queue_without_worker(&env).await;

    // One submit waits for a worker; a second one of the same key is refused meanwhile.
    let addr = env.addr.clone();
    let waiting =
        tokio::spawn(
            async move { submit_with(&addr, accepted("c1", "o1"), Wait::Committed).await },
        );
    let started = Instant::now();
    while !get(&metrics, "/metrics")
        .await
        .contains("kernable_queued_commands 1\n")
    {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "c1 never queued"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let busy = submit_with(&env.addr, accepted("c2", "o2"), Wait::Accepted)
        .await
        .unwrap_err();
    assert_eq!(busy.code(), Code::ResourceExhausted);
    assert!(busy.message().contains("--max-requests-per-key"));
    start_worker(
        env.addr.clone(),
        TestWorker {
            service: "shop".into(),
            ..TestWorker::honest()
        },
    )
    .await;
    waiting.await.unwrap().unwrap();
    submit_with(&env.addr, accepted("c3", "o3"), Wait::Committed)
        .await
        .expect("the slot is free again");
    assert!(refused(&metrics, "requests_per_key").await >= 1);

    let first = subscribe(&env.addr).await.unwrap();
    let second = subscribe(&env.addr).await.unwrap_err();
    assert_eq!(second.code(), Code::ResourceExhausted);
    drop(first);
    let started = Instant::now();
    while subscribe(&env.addr).await.is_err() {
        assert!(started.elapsed() < Duration::from_secs(3), "slot not freed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(refused(&metrics, "subscriptions_per_key").await, 1);
}

/// Connects listener `name` with key 5, recording into `seen` and acknowledging every batch.
/// Returns once the kernel sent `Ready`; abort the handle to disconnect.
async fn listen(
    addr: &str,
    name: &str,
    from_position: Option<u64>,
    seen: Arc<Mutex<Observed>>,
) -> Result<JoinHandle<()>, tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: id(&key(5)),
        from_position,
        ..Default::default()
    };
    tx.send(send(In::Hello(hello))).await.unwrap();
    let channel = transport::connect(addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(with_grant(ReceiverStream::new(rx), &key(5)))
        .await?
        .into_inner();
    loop {
        match inbound.message().await?.and_then(|m| m.msg) {
            Some(Out::Challenge(c)) => {
                let sig = signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig;
                let response = In::ChallengeResponse(ChallengeResponse { sig });
                tx.send(send(response)).await.unwrap();
            }
            Some(Out::Ready(r)) => {
                seen.lock().unwrap().connected(name, &[], r.after_position);
                break;
            }
            Some(_) => {}
            None => return Err(tonic::Status::aborted("the stream ended")),
        }
    }
    let name = name.to_owned();
    Ok(tokio::spawn(async move {
        while let Ok(Some(message)) = inbound.message().await {
            if let Some(Out::Batch(b)) = message.msg {
                {
                    let mut seen = seen.lock().unwrap();
                    seen.delivered(&name, &b);
                    seen.acked(&name, b.up_to);
                }
                let ack = In::Ack(Ack {
                    position: b.up_to,
                    ..Default::default()
                });
                if tx.send(send(ack)).await.is_err() {
                    return;
                }
            }
        }
    }))
}

/// Connects `name` again until the kernel noticed its last connection ended.
async fn reconnect(addr: &str, name: &str, seen: Arc<Mutex<Observed>>) -> JoinHandle<()> {
    let started = Instant::now();
    loop {
        match listen(addr, name, None, Arc::clone(&seen)).await {
            Ok(handle) => return handle,
            Err(e) if e.code() == Code::AlreadyExists => {}
            Err(e) => panic!("{name}: {e:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(3));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_key_registers_a_bounded_number_of_plainly_named_listeners() {
    let (env, metrics) = start_limited(
        "limit-listeners",
        Limits {
            listeners_per_key: 1,
            ..Limits::default()
        },
    )
    .await;
    let seen = Arc::new(Mutex::new(Observed::default()));
    let a = listen(&env.addr, "a", None, Arc::clone(&seen))
        .await
        .unwrap();
    a.abort();
    let b = listen(&env.addr, "b", None, Arc::clone(&seen))
        .await
        .unwrap_err();
    assert_eq!(b.code(), Code::ResourceExhausted, "{b:?}");
    assert!(b.message().contains("--max-listeners-per-key"));
    reconnect(&env.addr, "a", Arc::clone(&seen)).await.abort();
    assert_eq!(refused(&metrics, "listeners_per_key").await, 1);

    for bad in ["a b", "x\"y", "", &"n".repeat(129)] {
        let e = listen(&env.addr, bad, None, Arc::clone(&seen))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{bad:?}: {e:?}");
    }
}

#[tokio::test]
async fn a_stream_that_stalls_its_handshake_ends() {
    let (env, metrics) = start_limited(
        "limit-handshake",
        Limits {
            handshake_timeout_ms: 300,
            ..Limits::default()
        },
    )
    .await;
    let started = Instant::now();

    // A listener stream that never says Hello.
    let (_silent, rx) = mpsc::channel::<ListenerMessage>(1);
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let ended = inbound.message().await.unwrap_err();
    assert_eq!(ended.code(), Code::DeadlineExceeded, "{ended:?}");

    // A worker that registers but never answers the challenge.
    let (tx, rx) = mpsc::channel(4);
    let register = Register {
        worker_id: "w".into(),
        key_id: id(&key(2)),
        aggregates: aggregates(&["orders.PlaceOrder".into()]),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    tx.send(WorkerMessage {
        msg: Some(WorkerIn::Register(register)),
    })
    .await
    .unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = WorkerGatewayClient::new(channel)
        .session(with_grant(ReceiverStream::new(rx), &key(2)))
        .await
        .unwrap()
        .into_inner();
    let ended = loop {
        match inbound.message().await {
            Ok(Some(_challenge)) => {}
            Ok(None) => panic!("ended without a status"),
            Err(e) => break e,
        }
    };
    assert_eq!(ended.code(), Code::DeadlineExceeded, "{ended:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(refused(&metrics, "handshake_timeout").await, 2);
}

#[tokio::test]
async fn connections_beyond_the_limit_are_closed() {
    let (env, metrics) = start_limited(
        "limit-connections",
        Limits {
            connections: 2,
            ..Limits::default()
        },
    )
    .await;
    let path = transport::unix_path(&env.addr).unwrap().to_owned();
    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(tokio::net::UnixStream::connect(&path).await.unwrap());
    }
    // Give the kernel time to accept both before the third arrives.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let refused_submit = async {
        let channel = transport::connect(&env.addr).await?;
        submit_on(channel, accepted("c1", "o1"), Wait::Accepted)
            .await
            .map_err(Into::into)
    };
    let third: Result<_, Box<dyn std::error::Error + Send + Sync>> = refused_submit.await;
    assert!(third.is_err(), "a third connection was served");
    assert!(refused(&metrics, "connections").await >= 1);

    drop(held);
    spawn_worker(&env).await;
    let started = Instant::now();
    loop {
        let channel = transport::connect(&env.addr).await;
        if let Ok(channel) = channel
            && submit_on(channel, accepted("c1", "o1"), Wait::Committed)
                .await
                .is_ok()
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "connections not freed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A kernel whose messages hold at most 64 KiB, and a worker that reports the
/// snapshot version and history length of every command it runs.
async fn small_messages(
    name: &str,
) -> (Env, String, mpsc::UnboundedReceiver<(Option<u64>, usize)>) {
    let (env, metrics) = start_limited(
        name,
        Limits {
            message_bytes: 64 << 10,
            ..Limits::default()
        },
    )
    .await;
    let (tx, executions) = mpsc::unbounded_channel();
    start_worker(
        env.addr.clone(),
        TestWorker {
            executions: Some(tx),
            ..TestWorker::honest()
        },
    )
    .await;
    (env, metrics, executions)
}

fn committed(result: &CommandResult) -> bool {
    matches!(result.outcome, Some(Outcome::Committed(_)))
}

#[tokio::test]
async fn a_history_longer_than_one_message_reaches_the_worker_in_parts() {
    let (env, metrics, mut executions) = small_messages("limit-parts").await;
    // Twelve events of 20 KiB: about 240 KiB of history, four times the limit.
    for n in 0..12 {
        let kind = if n == 0 { "PlaceOrder" } else { "AddItem" };
        let cmd = with_payload(&format!("c{n}"), kind, "o1", 20 << 10);
        let result = submit(&env.addr, cmd).await.unwrap();
        assert!(committed(&result), "c{n}: {result:?}");
    }
    let last = submit(&env.addr, with_payload("c12", "AddItem", "o1", 10))
        .await
        .unwrap();
    assert!(committed(&last), "{last:?}");
    let mut seen = Vec::new();
    while let Ok(execution) = executions.try_recv() {
        seen.push(execution);
    }
    assert_eq!(
        seen.last(),
        Some(&(None, 12)),
        "the whole history: {seen:?}"
    );
    assert_eq!(refused(&metrics, "too_large").await, 0);
}

#[tokio::test]
async fn listener_batches_fit_the_message_limit() {
    let (env, _) = start_limited(
        "limit-batches",
        Limits {
            message_bytes: 64 << 10,
            ..Limits::default()
        },
    )
    .await;
    spawn_worker(&env).await;
    let mut seen = Observed::default();
    for (i, order) in ["o1", "o2", "o3", "o4"].iter().enumerate() {
        let cmd = with_payload(&format!("c{i}"), "PlaceOrder", order, 30 << 10);
        let result = submit_with(&env.addr, cmd, Wait::Committed).await.unwrap();
        seen.outcome(&result);
    }
    let seen = Arc::new(Mutex::new(seen));
    let listener = listen(&env.addr, "big", Some(0), Arc::clone(&seen))
        .await
        .unwrap();
    let started = Instant::now();
    while seen.lock().unwrap().delivered_positions("big").len() < 4 {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "not every event arrived"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    listener.abort();
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let kernel_id = id(&env.kernel_key);
    let ids = seen.lock().unwrap().command_ids();
    let log = Log::read_with(store.as_ref(), &[kernel_id], &ids).unwrap();
    let mut violations = log.check();
    violations.extend(seen.lock().unwrap().check(&log));
    assert!(violations.is_empty(), "{violations:#?}");
}

/// The raw response to `GET path` at `addr` with this `Host` header.
async fn get_as(addr: &str, path: &str, host: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n");
    conn.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test]
async fn http_on_loopback_answers_loopback_hosts_only() {
    let ui = free_addr();
    let (_env, metrics) = start_limited("limit-http", Limits::default()).await;
    let evil = get_as(&metrics, "/health", "evil.example").await;
    assert!(evil.starts_with("HTTP/1.1 403"), "{evil}");
    let ok = get_as(&metrics, "/health", "localhost").await;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    assert_eq!(refused(&metrics, "http_host").await, 1);

    let _ui_env = start_custom("limit-http-ui", |config, _| {
        config.ui_listen = Some(ui.clone());
    })
    .await;
    let started = Instant::now();
    let evil = loop {
        // The UI starts with the kernel; give it a moment.
        if let Ok(mut conn) = tokio::net::TcpStream::connect(&ui).await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let request = "GET /api/state HTTP/1.1\r\nHost: rebind.evil.example:7234\r\n\r\n";
            conn.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            conn.read_to_string(&mut response).await.unwrap();
            break response;
        }
        assert!(started.elapsed() < Duration::from_secs(3));
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(evil.starts_with("HTTP/1.1 403"), "{evil}");
    let ok = get_as(&ui, "/api/state", &ui).await;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
}

#[tokio::test]
async fn after_the_snapshots_are_lost_a_long_history_comes_in_parts_with_every_vault_value() {
    let setup = |config: &mut kernable_kernel::config::Config, dir: &std::path::Path| {
        config.limits = Limits {
            message_bytes: 64 << 10,
            ..Limits::default()
        };
        with_vault(config, dir);
    };
    let env = start_custom("limit-parts-vault", setup).await;
    // Each event refers to a personal value; snapshots after every command.
    let writes: VaultWriter = std::sync::Arc::new(|cmd: &Command| {
        let value = vault_value(&cmd.aggregate_id, &format!("value of {}", cmd.command_id));
        (vec![value], String::new())
    });
    let writer = TestWorker {
        behavior: Behavior::Snapshots,
        writes_vault: Some(std::sync::Arc::clone(&writes)),
        ..TestWorker::honest()
    };
    start_worker(env.addr.clone(), writer).await;
    for n in 0..12 {
        let kind = if n == 0 { "PlaceOrder" } else { "AddItem" };
        let cmd = with_payload(&format!("c{n}"), kind, "o1", 20 << 10);
        let result = submit(&env.addr, cmd).await.unwrap();
        assert!(committed(&result), "c{n}: {result:?}");
    }

    // A restart without the snapshot store: the whole history, in parts, with its values.
    let from = env.dir.clone();
    let after = start_custom("limit-parts-vault-restarted", move |config, dir| {
        for file in ["k.db", "k.db-wal", "k.log", "vault.db", "vault.db-wal"] {
            if from.join(file).exists() {
                std::fs::copy(from.join(file), dir.join(file)).unwrap();
            }
        }
        setup(config, dir);
    })
    .await;
    let (executions, mut executed) = mpsc::unbounded_channel();
    let (vaults, mut values) = mpsc::unbounded_channel();
    let reader = TestWorker {
        executions: Some(executions),
        vaults: Some(vaults),
        writes_vault: Some(writes),
        ..TestWorker::honest()
    };
    start_worker(after.addr.clone(), reader).await;
    let last = submit(&after.addr, with_payload("c12", "AddItem", "o1", 10))
        .await
        .unwrap();
    assert!(committed(&last), "{last:?}");
    assert_eq!(
        executed.recv().await.unwrap(),
        (None, 12),
        "no snapshot, every event"
    );
    let got = values.recv().await.unwrap();
    let mut seen: Vec<String> = got
        .iter()
        .map(|v| String::from_utf8_lossy(&v.value).into_owned())
        .collect();
    seen.sort();
    // One per event of the history (c12's own is written with its result).
    let mut want: Vec<String> = (0..12).map(|n| format!("\"value of c{n}\"")).collect();
    want.sort();
    assert_eq!(seen, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_store_refuses_new_commands_but_not_the_exempt_ones() {
    // One byte: the store is over it from its first measurement on.
    let limits = Limits {
        store_bytes: 1,
        quota_exempt: vec!["orders.AddItem".into()],
        ..Limits::default()
    };
    let (env, metrics) = start_limited("limits-quota", limits).await;
    spawn_worker(&env).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let refused_now = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap_err();
    assert_eq!(refused_now.code(), Code::ResourceExhausted);
    assert!(
        refused_now.message().contains("quota_storage"),
        "{}",
        refused_now.message()
    );
    assert_eq!(refused(&metrics, "quota_storage").await, 1);
    // The exempt command goes on (here it is rejected by the worker: the order does not exist).
    let exempt = submit(&env.addr, signed(command("c2", "AddItem", "o1"), &key(1))).await;
    assert!(exempt.is_ok(), "{exempt:?}");
    let shown = get(&metrics, "/metrics").await;
    assert!(shown.contains("kernable_store_bytes_max 1"), "{shown}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_beyond_its_rate_is_refused_and_another_key_is_not() {
    let limits = Limits {
        requests_per_second_per_key: 3,
        ..Limits::default()
    };
    let (env, metrics) = start_limited("limits-rate", limits).await;
    spawn_worker(&env).await;
    let mut codes = Vec::new();
    for i in 0..5 {
        let cmd = signed(
            command(&format!("r{i}"), "PlaceOrder", &format!("o{i}")),
            &key(1),
        );
        codes.push(submit(&env.addr, cmd).await.err().map(|s| s.code()));
    }
    assert_eq!(
        &codes[..3],
        [None, None, None],
        "a burst of one second's worth"
    );
    assert!(
        codes[3..]
            .iter()
            .all(|c| *c == Some(Code::ResourceExhausted)),
        "{codes:?}"
    );
    assert!(refused(&metrics, "rate").await >= 2);
    // Another key has its own bucket; and a second later the first has tokens again.
    env.kernel
        .grant(grant(&key(9), &[("submit", &["*"])]))
        .unwrap();
    let other = signed(command("x1", "PlaceOrder", "x1"), &key(9));
    let other = submit(&env.addr, other).await;
    assert!(other.is_ok(), "{other:?}");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let again = signed(command("r9", "PlaceOrder", "o9"), &key(1));
    assert!(submit(&env.addr, again).await.is_ok());
}

/// A worker that goes while commands keep coming: every command it took or was about to
/// take goes to the next worker at once, not after its lease ran out. The soak on four servers
/// saw a client wait 10 s, a lease, once in about twenty kills of the workers: dispatch took a
/// command in the moment the worker's in-flight commands were handed back.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_goes_hands_back_every_command_at_once() {
    let env = start_with("worker-goes", 30_000).await;
    for round in 0..20u32 {
        let gone = start_worker(env.addr.clone(), TestWorker::honest()).await;
        let mut submits = Vec::new();
        for i in 0..32u32 {
            let addr = env.addr.clone();
            submits.push(tokio::spawn(async move {
                let cmd = signed(
                    command(
                        &format!("g{round}-{i}"),
                        "PlaceOrder",
                        &format!("g{round}-{i}"),
                    ),
                    &key(1),
                );
                submit(&addr, cmd).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(u64::from(round % 5))).await;
        gone.abort();
        let _next = start_worker(env.addr.clone(), TestWorker::honest()).await;
        let started = Instant::now();
        for s in submits {
            let result = tokio::time::timeout(Duration::from_secs(10), s)
                .await
                .unwrap_or_else(|_| panic!("round {round}: a command waited for its lease"));
            result.unwrap().unwrap();
        }
        assert!(started.elapsed() < Duration::from_secs(10), "round {round}");
    }
}
