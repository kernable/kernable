//! Scheduled commands (design/processes.md, "Timers"): a command with a delivery time is
//! accepted at once, waits beside its aggregate's queue until due, is checked again then, and
//! can be cancelled until it is due.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, CancelRequest, CancelResult, ChallengeResponse, Command, CommandResult, Hello,
        ListenerMessage, Middleware, MiddlewareCall, PartitionMap, Rejected, TenantMove,
        TenantPartition, Verdict, Wait, When, cancel_result, command_result::Outcome,
        command_service_client::CommandServiceClient, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
        middleware_call::Request,
    },
    signing, transport,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// `cmd` due at `at` and, unless 0, rejected if not run by `until`; signed by `key`.
fn due_at(mut cmd: Command, at: i64, until: i64, key: &SigningKey) -> Command {
    cmd.deliver_at = at;
    cmd.deliver_until = until;
    signed(cmd, key)
}

/// An `AddItem` of order `order`, due in `in_ms`.
fn later(id: &str, order: &str, in_ms: i64) -> Command {
    due_at(command(id, "AddItem", order), now() + in_ms, 0, &key(1))
}

/// The outcome of a submit: `accepted`, `committed` or the rejection's code.
fn outcome(result: &CommandResult) -> String {
    match &result.outcome {
        Some(Outcome::Accepted(_)) => "accepted".into(),
        Some(Outcome::Committed(_)) => "committed".into(),
        Some(Outcome::Rejected(r)) => r.code.clone(),
        other => panic!("unexpected outcome {other:?}"),
    }
}

/// The stored result of `command_id`, once it is completed (within 10 s).
async fn completed(store: &str, command_id: &str) -> CommandResult {
    for _ in 0..500 {
        let store = kernable_store::open_read_only(store).unwrap();
        if let Some(result) = store.result(command_id).unwrap() {
            return result;
        }
        drop(store);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{command_id} was not completed");
}

fn stored_result(store: &str, command_id: &str) -> Option<CommandResult> {
    kernable_store::open_read_only(store)
        .unwrap()
        .result(command_id)
        .unwrap()
}

async fn cancel_as(
    addr: &str,
    command_id: &str,
    tenant: &str,
    signer: &SigningKey,
    request: impl FnOnce(CancelRequest) -> tonic::Request<CancelRequest>,
) -> Result<CancelResult, tonic::Status> {
    let mut r = CancelRequest {
        command_id: command_id.into(),
        tenant_id: tenant.into(),
        context: None,
        issued_at: issued_now(),
        client_sig: None,
        protocol: kernable_core::PROTOCOL,
    };
    r.client_sig = Some(signing::sign(signer, &signing::cancel_message(&r)));
    CommandServiceClient::new(transport::connect(addr).await.unwrap())
        .cancel(request(r))
        .await
        .map(tonic::Response::into_inner)
}

/// Cancels `command_id` of tenant `t1` with client key 1; returns `cancelled` or the code.
async fn cancel(addr: &str, command_id: &str) -> String {
    let result = cancel_as(addr, command_id, "t1", &key(1), |r| with_grant(r, &key(1)))
        .await
        .unwrap();
    assert_eq!(result.command_id, command_id);
    match result.outcome {
        Some(cancel_result::Outcome::Cancelled(_)) => "cancelled".into(),
        Some(cancel_result::Outcome::Rejected(r)) => r.code,
        None => panic!("no outcome"),
    }
}

fn refuse(code: &str) -> Option<Rejected> {
    Some(Rejected {
        code: code.into(),
        message: "refused by a test middleware".into(),
        ..Default::default()
    })
}

type Decide = Arc<dyn Fn(&MiddlewareCall) -> Option<Rejected> + Send + Sync>;

/// A middleware before every request (key 5), deciding with `decide`; logs every call it gets.
async fn middleware(env: &Env, decide: Decide, log: Arc<Mutex<Vec<MiddlewareCall>>>) {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: "guard".into(),
        key_id: id(&key(5)),
        event_types: vec!["orders.OrderPlaced".into()],
        from_position: Some(0),
        middlewares: vec![Middleware {
            order: 0,
            types: vec!["*".into()],
            when: When::Before.into(),
        }],
        ..Default::default()
    };
    tx.send(send(In::Hello(hello))).await.unwrap();
    let channel = transport::connect(&env.addr).await.unwrap();
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tokio::spawn(async move {
        while let Ok(Some(m)) = inbound.message().await {
            let reply = match m.msg.unwrap() {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Batch(b) => In::Ack(Ack {
                    position: b.up_to,
                    ..Default::default()
                }),
                Out::Check(call) => {
                    let rejected = decide(&call);
                    let call_id = call.call_id.clone();
                    log.lock().unwrap().push(call);
                    In::Verdict(Verdict { call_id, rejected })
                }
                _ => continue,
            };
            if tx.send(send(reply)).await.is_err() {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_command_waits_for_its_time_while_its_aggregate_goes_on() {
    let metrics = free_addr();
    let env = start_custom("timer-runs", |config, _| {
        config.metrics_listen = Some(metrics.clone());
    })
    .await;
    let (seen, mut executed) = mpsc::unbounded_channel();
    start_worker(env.addr.clone(), TestWorker::with(Behavior::Honest, seen)).await;
    let placed = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    assert_eq!(executed.recv().await.unwrap(), "c1");

    // Accepted at once, though the client waits for the commit.
    let submitted = Instant::now();
    let s1 = later("s1", "o1", 600);
    let accepted = submit_with(&env.addr, s1.clone(), Wait::Committed)
        .await
        .unwrap();
    assert_eq!(outcome(&accepted), "accepted");
    assert!(submitted.elapsed() < Duration::from_millis(500));
    let gauge = get(&metrics, "/metrics").await;
    assert!(gauge.contains("kernable_scheduled_commands 1"), "{gauge}");

    // The aggregate's other commands pass meanwhile.
    let added = submit(&env.addr, signed(command("c2", "AddItem", "o1"), &key(1)))
        .await
        .unwrap();
    assert_eq!(committed_versions(&added), [2]);
    assert_eq!(executed.recv().await.unwrap(), "c2");
    assert!(stored_result(&env.store, "s1").is_none());
    // A resubmit before it is due is accepted again.
    let again = submit_with(&env.addr, s1.clone(), Wait::Committed)
        .await
        .unwrap();
    assert_eq!(outcome(&again), "accepted");

    // Then it runs, at its time, behind what the aggregate holds.
    assert_eq!(executed.recv().await.unwrap(), "s1");
    assert!(submitted.elapsed() >= Duration::from_millis(600));
    let result = completed(&env.store, "s1").await;
    assert_eq!(committed_versions(&result), [3]);
    let resubmitted = submit_with(&env.addr, s1, Wait::Committed).await.unwrap();
    assert_eq!(committed_versions(&resubmitted), [3]);
    let gauge = get(&metrics, "/metrics").await;
    assert!(gauge.contains("kernable_scheduled_commands 0"), "{gauge}");
}

#[tokio::test(flavor = "multi_thread")]
async fn delivery_times_are_checked_at_submit() {
    let env = start_custom("timer-checked", |config, _| {
        config.limits.schedule_max_days = 1;
    })
    .await;
    spawn_worker(&env).await;
    let day = 86_400_000;
    let too_far = due_at(
        command("f1", "PlaceOrder", "o1"),
        now() + 2 * day,
        0,
        &key(1),
    );
    let refused = submit(&env.addr, too_far).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    assert!(
        refused.message().contains("--schedule-max-days"),
        "{refused:?}"
    );

    let at = now() + 1_000;
    let never = due_at(command("f2", "PlaceOrder", "o1"), at, at - 1, &key(1));
    let refused = submit(&env.addr, never).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    assert!(refused.message().contains("deliver_until"), "{refused:?}");

    // A delivery time that has passed: an ordinary command, run at once.
    let past = due_at(command("f3", "PlaceOrder", "o1"), now() - 1_000, 0, &key(1));
    let result = submit_with(&env.addr, past, Wait::Committed).await.unwrap();
    assert_eq!(committed_versions(&result), [1]);

    // The times are signed: changed after signing, the command is refused.
    let mut moved = later("f4", "o1", 1_000);
    moved.deliver_at += 1;
    let refused = submit(&env.addr, moved).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated, "{refused:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_due_commands_run_late_unless_past_their_limit() {
    let env = start("timer-restart").await;
    let worker = start_worker(env.addr.clone(), TestWorker::honest()).await;
    let placed = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    let t = now();
    for cmd in [
        due_at(command("late", "AddItem", "o1"), t + 200, 0, &key(1)),
        due_at(command("gone", "AddItem", "o1"), t + 200, t + 400, &key(1)),
        due_at(command("future", "AddItem", "o1"), t + 2_500, 0, &key(1)),
    ] {
        let accepted = submit(&env.addr, cmd).await.unwrap();
        assert_eq!(outcome(&accepted), "accepted");
    }
    // No worker runs anything here any more; the kernel is down, as far as the commands go.
    worker.abort();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(stored_result(&env.store, "late").is_none());

    // Back after both were due: the late one runs, the other is past its limit.
    let from = env.dir.clone();
    let back = start_custom("timer-restart-back", move |_, dir| {
        for file in ["k.db", "k.db-wal", "k.log"] {
            if from.join(file).exists() {
                std::fs::copy(from.join(file), dir.join(file)).unwrap();
            }
        }
    })
    .await;
    spawn_worker(&back).await;
    let late = completed(&back.store, "late").await;
    assert_eq!(committed_versions(&late), [2]);
    let gone = completed(&back.store, "gone").await;
    assert_eq!(outcome(&gone), "expired");
    // The one not due yet waits for its time.
    assert!(now() < t + 2_500);
    assert!(stored_result(&back.store, "future").is_none());
    let future = completed(&back.store, "future").await;
    assert!(now() >= t + 2_500);
    assert_eq!(committed_versions(&future), [3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_command_can_be_cancelled_until_it_is_due() {
    let env = start("timer-cancel").await;
    spawn_worker(&env).await;
    let placed = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    for cmd in [later("s1", "o1", 3_000), later("s2", "o1", 300)] {
        assert_eq!(outcome(&submit(&env.addr, cmd).await.unwrap()), "accepted");
    }

    // Only a key that may submit the command's type cancels it.
    let narrow = grant(&key(11), &[("submit", &["orders.PlaceOrder"])]);
    env.kernel.grant(narrow.clone()).unwrap();
    let denied = cancel_as(&env.addr, "s1", "t1", &key(11), |r| presenting(r, &narrow))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied:?}");
    // Signed for another command, it is refused.
    let forged = cancel_as(&env.addr, "s1", "t1", &key(1), |mut r| {
        r.command_id = "s2".into();
        with_grant(r, &key(1))
    })
    .await
    .unwrap_err();
    assert_eq!(forged.code(), Code::Unauthenticated, "{forged:?}");

    assert_eq!(cancel(&env.addr, "nope").await, "not_found");
    let other_tenant = cancel_as(&env.addr, "s1", "t2", &key(1), |r| with_grant(r, &key(1)))
        .await
        .unwrap();
    assert!(
        matches!(&other_tenant.outcome, Some(cancel_result::Outcome::Rejected(r)) if r.code == "not_found"),
        "{other_tenant:?}"
    );
    assert_eq!(cancel(&env.addr, "c1").await, "too_late");

    assert_eq!(cancel(&env.addr, "s1").await, "cancelled");
    assert_eq!(cancel(&env.addr, "s1").await, "cancelled", "idempotent");
    let s1 = completed(&env.store, "s1").await;
    assert_eq!(outcome(&s1), "cancelled");
    let resubmitted = submit(&env.addr, later("s1", "o1", 3_000)).await.unwrap();
    assert_eq!(outcome(&resubmitted), "cancelled");

    // Once it ran, it is too late.
    let s2 = completed(&env.store, "s2").await;
    assert_eq!(committed_versions(&s2), [2]);
    assert_eq!(cancel(&env.addr, "s2").await, "too_late");
}

#[tokio::test(flavor = "multi_thread")]
async fn middlewares_check_a_cancel_and_a_command_again_when_it_is_due() {
    let env = start("timer-middleware").await;
    spawn_worker(&env).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let decide: Decide = Arc::new(|call: &MiddlewareCall| match &call.request {
        // A cancel of order `kept` is refused.
        Some(Request::Cancel(_)) => match &call.scheduled {
            Some(c) if c.aggregate_id == "kept" => refuse("keep_it"),
            _ => None,
        },
        // Order `late` is closed by the time its scheduled command is due.
        Some(Request::Command(c))
            if c.aggregate_id == "late" && c.deliver_at != 0 && c.deliver_at <= now() =>
        {
            refuse("closed")
        }
        _ => None,
    });
    middleware(&env, decide, Arc::clone(&log)).await;
    for order in ["kept", "late", "free"] {
        let placed = submit(
            &env.addr,
            signed(command(order, "PlaceOrder", order), &key(1)),
        )
        .await
        .unwrap();
        assert_eq!(committed_versions(&placed), [1], "{order}");
    }
    for cmd in [
        later("k1", "kept", 3_000),
        later("l1", "late", 300),
        later("f1", "free", 3_000),
    ] {
        assert_eq!(outcome(&submit(&env.addr, cmd).await.unwrap()), "accepted");
    }

    // A cancel is checked as one, with the command it cancels.
    assert_eq!(cancel(&env.addr, "k1").await, "keep_it");
    assert_eq!(cancel(&env.addr, "f1").await, "cancelled");
    let cancels: Vec<(String, String)> = log
        .lock()
        .unwrap()
        .iter()
        .filter_map(|call| match &call.request {
            Some(Request::Cancel(c)) => Some((
                c.command_id.clone(),
                call.scheduled.as_ref().unwrap().command_type.clone(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        cancels,
        [
            ("k1".into(), "AddItem".into()),
            ("f1".into(), "AddItem".into())
        ]
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|c| c.r#type == "orders.AddItem" && matches!(c.request, Some(Request::Cancel(_)))),
        "a cancel is checked under the command's type"
    );

    // Due, the command is checked again, and the middleware's rejection completes it.
    let l1 = completed(&env.store, "l1").await;
    match &l1.outcome {
        Some(Outcome::Rejected(r)) => assert_eq!(r.code, "closed"),
        other => panic!("not rejected: {other:?}"),
    }
    let checked = log
        .lock()
        .unwrap()
        .iter()
        .filter(|c| matches!(&c.request, Some(Request::Command(x)) if x.command_id == "l1"))
        .count();
    assert_eq!(checked, 2, "at submit and when due");
    assert_eq!(cancel(&env.addr, "k1").await, "keep_it", "still waiting");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_revoked_before_its_command_is_due_does_not_run_it() {
    let env = start("timer-revoked").await;
    spawn_worker(&env).await;
    let placed = submit(
        &env.addr,
        signed(command("c1", "PlaceOrder", "o1"), &key(1)),
    )
    .await
    .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    assert_eq!(
        outcome(&submit(&env.addr, later("s1", "o1", 500)).await.unwrap()),
        "accepted"
    );
    // Stolen after the command was scheduled: revoked from now on.
    env.kernel.revoke(&id(&key(1)), now()).unwrap();
    let s1 = completed(&env.store, "s1").await;
    assert_eq!(outcome(&s1), "key_revoked");
}

/// An order of `tenant`, signed by the client key.
fn of_tenant(mut cmd: Command, tenant: &str) -> Command {
    cmd.tenant_id = tenant.into();
    signed(cmd, &key(1))
}

async fn move_status(addr: &str, tenant: &str) -> kernable_core::proto::MoveStatus {
    kernable_core::proto::partition_service_client::PartitionServiceClient::new(
        transport::connect(addr).await.unwrap(),
    )
    .moving(with_grant(
        kernable_core::proto::MoveStatusRequest {
            tenant: tenant.into(),
            protocol: kernable_core::PROTOCOL,
            partition: String::new(),
        },
        &key(1),
    ))
    .await
    .unwrap()
    .into_inner()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_command_moves_with_its_tenant_and_runs_there() {
    let p1 = start_custom("timer-mv-p1", |config, dir| {
        config.partition = Some(kernable_kernel::config::PartitionConfig {
            name: "p1".into(),
            control: None,
            node_key: node_key(dir),
        });
    })
    .await;
    let p1_addr = p1.addr.clone();
    let p2 = start_custom("timer-mv-p2", |config, dir| {
        config.partition = Some(kernable_kernel::config::PartitionConfig {
            name: "p2".into(),
            control: Some(p1_addr.clone()),
            node_key: node_key(dir),
        });
    })
    .await;
    for env in [&p1, &p2] {
        env.kernel
            .grant(grant(&key(13), &[("replicate", &["*"])]))
            .unwrap();
    }
    let partition = |name: &str, addr: &str| kernable_core::proto::Partition {
        name: name.into(),
        addresses: vec![addr.into()],
    };
    let mut map = PartitionMap {
        epoch: 1,
        partitions: vec![partition("p1", &p1.addr), partition("p2", &p2.addr)],
        default: "p1".into(),
        control: "p1".into(),
        ..Default::default()
    };
    apply_map(&p1.addr, map.clone()).await.unwrap();
    let (seen1, mut ran_on_p1) = mpsc::unbounded_channel();
    start_worker(p1.addr.clone(), TestWorker::with(Behavior::Honest, seen1)).await;
    until_async("p2 holds the map", async || {
        kernable_core::proto::partition_service_client::PartitionServiceClient::new(
            transport::connect(&p2.addr).await.unwrap(),
        )
        .map(with_grant(
            kernable_core::proto::MapRequest {
                protocol: kernable_core::PROTOCOL,
            },
            &key(1),
        ))
        .await
        .is_ok()
    })
    .await;
    let (seen2, mut ran_on_p2) = mpsc::unbounded_channel();
    start_worker(p2.addr.clone(), TestWorker::with(Behavior::Honest, seen2)).await;

    let placed = submit(&p1.addr, of_tenant(command("c1", "PlaceOrder", "o1"), "t1"))
        .await
        .unwrap();
    assert_eq!(committed_versions(&placed), [1]);
    assert_eq!(ran_on_p1.recv().await.unwrap(), "c1");
    let t = now();
    let s1 = due_at(
        of_tenant(command("s1", "AddItem", "o1"), "t1"),
        t + 4_000,
        0,
        &key(1),
    );
    assert_eq!(outcome(&submit(&p1.addr, s1).await.unwrap()), "accepted");

    // A copy while t1 runs, then the move: the scheduled command does not hold it up.
    let t1 = |epoch| TenantMove {
        tenant: "t1".into(),
        from: "p1".into(),
        to: "p2".into(),
        epoch,
        ..Default::default()
    };
    map.epoch = 2;
    map.copying = vec![t1(2)];
    apply_map(&p1.addr, map.clone()).await.unwrap();
    until_async("p2 copied t1 while it runs", async || {
        let s = move_status(&p2.addr, "t1").await;
        s.done && s.precopy && s.epoch == 2
    })
    .await;
    map.epoch = 3;
    map.copying.clear();
    map.moving = vec![t1(3)];
    apply_map(&p1.addr, map.clone()).await.unwrap();
    until_async("p2 took t1", async || {
        let s = move_status(&p2.addr, "t1").await;
        s.done && !s.precopy && s.epoch == 3
    })
    .await;
    let status = move_status(&p2.addr, "t1").await;
    map.epoch = 4;
    map.moving.clear();
    map.tenants = vec![TenantPartition {
        tenant: "t1".into(),
        partition: "p2".into(),
    }];
    map.moved = vec![TenantMove {
        source_head: status.source_head,
        target_after: status.target_after,
        ..t1(3)
    }];
    apply_map(&p1.addr, map).await.unwrap();
    assert!(now() < t + 4_000, "moved before it was due");
    let pending = kernable_store::open_read_only(&p2.store)
        .unwrap()
        .pending()
        .unwrap();
    assert!(
        pending
            .iter()
            .any(|p| p.command.command_id == "s1" && p.command.deliver_at == t + 4_000),
        "{pending:?}"
    );

    // It runs on p2, at its time, after the copied history; p1 lets it be.
    assert_eq!(ran_on_p2.recv().await.unwrap(), "s1");
    assert!(now() >= t + 4_000);
    let s1 = completed(&p2.store, "s1").await;
    assert_eq!(committed_versions(&s1), [2]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(ran_on_p1.try_recv().is_err());
    assert!(stored_result(&p1.store, "s1").is_none());
}
