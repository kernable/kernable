//! Crash points of moving a tenant (`--features crash-points`): the target or the source
//! aborts at each point during a move, is started again, and the move completes: every command
//! of the tenant is on the target once, at its version, its global keys moved with it, and both
//! logs keep every store invariant. The move starts with a copy while the tenant runs (an order
//! comes after it), so each point is also reached in that copy or in the rest after it.

mod common;

use std::{
    path::Path,
    process::{Command as Process, Stdio},
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{
        Command, CommandResult, MoveStatusRequest, Partition, PartitionMap, TenantMove,
        TenantPartition, Wait, command_result::Outcome,
        partition_service_client::PartitionServiceClient,
    },
    transport,
};
use kernable_kernel::crash::MOVE_POINTS;
use kernable_store::invariants::Log;
use prost::Message;

fn store(dir: &Path, name: &str) -> String {
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => format!("file:{}", dir.join(format!("{name}.log")).display()),
        _ => format!("sqlite:{}", dir.join(format!("{name}.db")).display()),
    }
}

fn sock(dir: &Path, name: &str) -> String {
    format!("unix:{}", dir.join(format!("{name}.sock")).display())
}

fn args(dir: &Path, name: &str) -> Vec<String> {
    let at = |f: &str| dir.join(f).display().to_string();
    let mut args: Vec<String> = vec![
        "start".into(),
        "--root".into(),
        id(&root()),
        "--store".into(),
        store(dir, name),
        "--listen".into(),
        sock(dir, name),
        "--kernel-key".into(),
        at(&format!("{name}-kernel.key")),
        "--snapshots".into(),
        format!("sqlite:{}", at(&format!("{name}-snapshots.db"))),
        "--data-store".into(),
        format!("file:{}", at(&format!("{name}-data"))),
        "--partition".into(),
        name.into(),
        "--node-key".into(),
        at("node.key"),
    ];
    if name == "p2" {
        args.extend(["--control".into(), sock(dir, "p1")]);
    }
    args
}

fn start(dir: &Path, name: &str, crash_at: Option<&str>) -> KillOnDrop {
    let mut process = Process::new(env!("CARGO_BIN_EXE_kernable"));
    process
        .current_dir(dir)
        .args(args(dir, name))
        .stdout(Stdio::null())
        .stderr(match std::env::var_os("KERNABLE_TEST_LOGS") {
            Some(_) => Stdio::from(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join(format!("{name}.log.txt")))
                    .unwrap(),
            ),
            None => Stdio::null(),
        });
    if let Some(point) = crash_at {
        process.env("KERNABLE_CRASH_AT", point);
    }
    KillOnDrop::spawn(&mut process)
}

fn map(dir: &Path, epoch: u64, tenants: Vec<TenantPartition>) -> PartitionMap {
    PartitionMap {
        epoch,
        partitions: ["p1", "p2"]
            .map(|name| Partition {
                name: name.into(),
                addresses: vec![sock(dir, name)],
            })
            .into(),
        tenants,
        default: "p1".into(),
        control: "p1".into(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    }
}

/// The status of t1's move or copy on p2, once its copy of `epoch` is done.
async fn copied(dir: &Path, epoch: u64) -> Option<kernable_core::proto::MoveStatus> {
    let channel = transport::connect(&sock(dir, "p2")).await.ok()?;
    let status = PartitionServiceClient::new(channel)
        .moving(with_grant(
            MoveStatusRequest {
                tenant: "t1".into(),
                protocol: kernable_core::PROTOCOL,
                partition: String::new(),
            },
            &key(1),
        ))
        .await
        .ok()?
        .into_inner();
    (status.done && status.epoch == epoch).then_some(status)
}

/// An order of t1 with a global unique key `email`.
fn order(id: &str, kind: &str, aggregate: &str, email: &str) -> Command {
    let mut cmd = command(id, kind, aggregate);
    cmd.tenant_id = "t1".into();
    cmd.payload = serde_json::json!({ "email": email })
        .to_string()
        .into_bytes();
    signed(cmd, &key(1))
}

fn by_email(
    cmd: &Command,
) -> (
    Vec<kernable_core::proto::UniqueKey>,
    Vec<String>,
    Vec<kernable_core::proto::Requirement>,
) {
    use sha2::Digest;
    let p: serde_json::Value = serde_json::from_slice(&cmd.payload).unwrap_or_default();
    let keys = p["email"]
        .as_str()
        .map(|email| kernable_core::proto::UniqueKey {
            name: "email".into(),
            value_hash: sha2::Sha256::digest(email).to_vec(),
            global: true,
            subject: String::new(),
        })
        .into_iter()
        .collect();
    (keys, Vec::new(), Vec::new())
}

fn workers(addr: String) {
    tokio::spawn(async move {
        loop {
            let worker = TestWorker {
                index: Some(std::sync::Arc::new(by_email)),
                ..TestWorker::honest()
            };
            run_worker(addr.clone(), worker).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

/// The command, until a kernel answers it.
async fn submit_until(addr: &str, cmd: Command, started: Instant, what: &str) -> CommandResult {
    loop {
        if let Ok(channel) = transport::connect(addr).await
            && let Ok(Ok(result)) = tokio::time::timeout(
                Duration::from_secs(3),
                submit_on(channel, cmd.clone(), Wait::Committed),
            )
            .await
        {
            return result;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{what}: no answer"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn version(result: &CommandResult) -> u64 {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.last().map_or(0, |e| e.version),
        other => panic!("{}: {other:?}", result.command_id),
    }
}

/// Starts `process` again (without a crash point) once it has stopped; whether it had.
async fn revive(dir: &Path, name: &str, process: &mut KillOnDrop) -> bool {
    if process.child.try_wait().unwrap().is_none() {
        return false;
    }
    let _ = std::fs::remove_file(dir.join(format!("{name}.sock")));
    *process = start(dir, name, None);
    wait_for(&sock(dir, name)).await;
    true
}

async fn crash_and_recover(point: &str) {
    if std::env::var("KERNABLE_MOVE_POINT").is_ok_and(|p| p != point) {
        return;
    }
    let dir = std::env::temp_dir().join(format!("kn-mc-{point}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (name, k) in [("p1", 3), ("p2", 8)] {
        std::fs::write(
            dir.join(format!("{name}-kernel.key")),
            hex(key(k).as_bytes()),
        )
        .unwrap();
    }
    std::fs::write(dir.join("node.key"), hex(key(13).as_bytes())).unwrap();
    let node_grant = grant(&key(13), &[("replicate", &["*"])]);
    std::fs::write(dir.join("node.grant"), hex(&node_grant.encode_to_vec())).unwrap();
    let (p1_addr, p2_addr) = (sock(&dir, "p1"), sock(&dir, "p2"));
    let on_source = point == "move.dropped";
    let mut p1 = start(&dir, "p1", on_source.then_some(point));
    wait_for(&p1_addr).await;
    let mut p2 = start(&dir, "p2", (!on_source).then_some(point));
    wait_for(&p2_addr).await;
    apply_map(&p1_addr, map(&dir, 1, Vec::new())).await.unwrap();
    workers(p1_addr.clone());
    workers(p2_addr.clone());
    let started = Instant::now();

    // t1 on p1: three orders, each with an email and items.
    let mut told = Vec::new();
    for o in 0..3 {
        let email = format!("o{o}@x");
        for (n, kind) in ["PlaceOrder", "AddItem", "AddItem"].iter().enumerate() {
            let cmd = order(&format!("m-{o}-{n}"), kind, &format!("o{o}"), &email);
            let result = submit_until(&p1_addr, cmd, started, point).await;
            told.push((result.command_id.clone(), version(&result)));
        }
    }

    // A copy while t1 runs, then the move, with p1 or p2 crashing and coming back on the way.
    let t1_move = |epoch| TenantMove {
        tenant: "t1".into(),
        from: "p1".into(),
        to: "p2".into(),
        epoch,
        ..Default::default()
    };
    let mut copying = map(&dir, 2, Vec::new());
    copying.copying.push(t1_move(2));
    apply_map(&p1_addr, copying).await.unwrap();
    let mut crashed = false;
    loop {
        crashed |= revive(&dir, "p1", &mut p1).await;
        crashed |= revive(&dir, "p2", &mut p2).await;
        if copied(&dir, 2).await.is_some_and(|s| s.precopy) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{point}: the copy while t1 runs never finished"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // t1 runs on p1 meanwhile: this order only reaches p2 with the rest.
    let late = order("m-late", "PlaceOrder", "o3", "o3@x");
    let late = submit_until(&p1_addr, late, started, point).await;
    told.push((late.command_id.clone(), version(&late)));
    let mut moving = map(&dir, 3, Vec::new());
    moving.moving.push(t1_move(3));
    apply_map(&p1_addr, moving).await.unwrap();
    let status = loop {
        crashed |= revive(&dir, "p1", &mut p1).await;
        crashed |= revive(&dir, "p2", &mut p2).await;
        if let Some(status) = copied(&dir, 3).await {
            assert!(!status.precopy, "{point}");
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{point}: the copy never finished"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut moved = map(
        &dir,
        4,
        vec![TenantPartition {
            tenant: "t1".into(),
            partition: "p2".into(),
        }],
    );
    moved.moved.push(TenantMove {
        tenant: "t1".into(),
        from: "p1".into(),
        to: "p2".into(),
        epoch: 3,
        source_head: status.source_head,
        target_after: status.target_after,
    });
    apply_map(&p1_addr, moved).await.unwrap();

    // The source drops t1's keys within a second or two, crashing on the way at move.dropped.
    let deadline = Instant::now() + Duration::from_secs(10);
    while on_source && !crashed && Instant::now() < deadline {
        crashed |= revive(&dir, "p1", &mut p1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // t1 goes on at p2.
    let next = submit_until(
        &p2_addr,
        order("m-next", "AddItem", "o0", "o0@x"),
        started,
        point,
    )
    .await;
    assert_eq!(version(&next), 4, "{point}");
    // Its global key moved with t1: p1's t9 cannot take it.
    let mut cmd = command("t9-take", "PlaceOrder", "o9");
    cmd.tenant_id = "t9".into();
    cmd.payload = br#"{"email":"o1@x"}"#.to_vec();
    let taken = submit_until(&p1_addr, signed(cmd, &key(1)), started, point).await;
    match &taken.outcome {
        Some(Outcome::Rejected(r)) if r.code == "unique_taken" => {}
        other => panic!("{point}: o1@x is held on p2: {other:?}"),
    }
    assert!(crashed, "{point}: never reached");

    // p2 holds every command of t1 once, at the version its client was told.
    let p2_store = kernable_store::open_read_only(&store(&dir, "p2")).unwrap();
    let t1: Vec<_> = p2_store
        .read_all(0, 10_000)
        .unwrap()
        .into_iter()
        .filter(|e| e.tenant_id == "t1")
        .collect();
    assert_eq!(t1.len(), told.len() + 1, "{point}");
    for (command, version) in &told {
        let held: Vec<_> = t1.iter().filter(|e| &e.command_id == command).collect();
        assert_eq!(held.len(), 1, "{point}: {command}");
        assert_eq!(held[0].version, *version, "{point}: {command}");
        assert_eq!(held[0].origin_partition, "p1", "{point}: {command}");
    }
    drop(p2_store);
    for (name, k) in [("p1", 3), ("p2", 8)] {
        let read = kernable_store::open_read_only(&store(&dir, name)).unwrap();
        let violations = Log::read(read.as_ref(), &[id(&key(k))]).unwrap().check();
        assert!(violations.is_empty(), "{point}, {name}: {violations:?}");
    }
    drop((p1, p2));
    if std::env::var_os("KERNABLE_TEST_LOGS").is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_move_crash_point_completes_the_move() {
    for point in MOVE_POINTS {
        crash_and_recover(point).await;
    }
}
