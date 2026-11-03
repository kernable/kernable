//! Crash points of replication (`--features crash-points`): the primary or the standby
//! aborts at each point under load, is started again, and afterwards everything a client was
//! told is on the standby (P1), the standby holds exactly what the primary holds, and every
//! store invariant holds on both.
//!
//! `KERNABLE_CRASH_ROUNDS=<n>` repeats every point n times (`make torture`).

mod common;

use std::{
    path::Path,
    process::{Command as Process, Stdio},
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{Command, CommandResult, Event, Wait, command_result::Outcome},
    transport,
};
use kernable_kernel::crash::REPLICATION_POINTS;
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
    vec![
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
        "--checkpoint-interval-ms".into(),
        "20".into(),
    ]
}

fn start(dir: &Path, args: &[String], crash_at: Option<&str>) -> KillOnDrop {
    let mut process = Process::new(env!("CARGO_BIN_EXE_kernable"));
    process
        .current_dir(dir)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(point) = crash_at {
        process.env("KERNABLE_CRASH_AT", point);
    }
    KillOnDrop::spawn(&mut process)
}

fn workload(round: u32) -> Vec<Command> {
    (0..12)
        .map(|i| {
            let order = format!("r{round}-o{}", i % 3);
            let t = if i < 3 { "PlaceOrder" } else { "AddItem" };
            signed(command(&format!("r{round}-c{i}"), t, &order), &key(1))
        })
        .collect()
}

fn events(result: &CommandResult) -> Vec<Event> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => c.events.clone(),
        other => panic!("{}: {other:?}", result.command_id),
    }
}

async fn crash_and_recover(point: &str, round: u32) {
    let dir = std::env::temp_dir().join(format!("kn-rc-{point}-{round}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("p-kernel.key"), hex(key(3).as_bytes())).unwrap();
    std::fs::write(dir.join("s-kernel.key"), hex(key(8).as_bytes())).unwrap();
    std::fs::write(dir.join("standby.key"), hex(key(7).as_bytes())).unwrap();
    let grant = grant(&key(7), &[("replicate", &["*"])]);
    std::fs::write(dir.join("standby.grant"), hex(&grant.encode_to_vec())).unwrap();
    let imaging = point == "standby.imaging";
    if imaging {
        if store(&dir, "p").starts_with("file:") {
            return; // a file log's standby follows from the start: no image
        }
        // Written before replication was on: the standby needs an image.
        let s = kernable_store::open(&store(&dir, "p")).unwrap();
        for i in 0..40 {
            let cmd = signed(
                command(&format!("pre-{i}"), "PlaceOrder", &format!("pre-{i}")),
                &key(1),
            );
            s.enqueue(&cmd, 0).unwrap();
            let events = order(false, &cmd).unwrap();
            s.commit(&cmd.command_id, 0, &events, &Default::default(), None, 1)
                .unwrap();
        }
    }
    let mut primary_args = args(&dir, "p");
    primary_args.extend(["--replication".into(), "sync".into()]);
    let mut standby_args = args(&dir, "s");
    standby_args.extend([
        "--follow".into(),
        sock(&dir, "p"),
        "--follow-key".into(),
        dir.join("standby.key").display().to_string(),
        "--primary-kernel-key".into(),
        id(&key(3)),
    ]);
    let on_primary = point.starts_with("replication.");
    let mut primary = start(&dir, &primary_args, on_primary.then_some(point));
    wait_for(&sock(&dir, "p")).await;
    let mut standby = start(&dir, &standby_args, (!on_primary).then_some(point));
    let addr = sock(&dir, "p");
    tokio::spawn({
        let addr = addr.clone();
        async move {
            loop {
                run_worker(addr.clone(), TestWorker::honest()).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    // The workload; whichever process crashes is started again, without the crash point.
    let mut told = Vec::new();
    let mut crashed = false;
    let started = Instant::now();
    for cmd in workload(round) {
        loop {
            for (process, args, name) in [
                (&mut primary, &primary_args, "p"),
                (&mut standby, &standby_args, "s"),
            ] {
                if process.child.try_wait().unwrap().is_some() {
                    crashed = true;
                    let _ = std::fs::remove_file(dir.join(format!("{name}.sock")));
                    *process = start(&dir, args, None);
                    if name == "p" {
                        wait_for(&addr).await;
                    }
                }
            }
            let answer = match transport::connect(&addr).await {
                Ok(channel) => tokio::time::timeout(
                    Duration::from_secs(3),
                    submit_on(channel, cmd.clone(), Wait::Committed),
                )
                .await
                .ok()
                .and_then(Result::ok),
                Err(_) => None,
            };
            if let Some(result) = answer {
                told.push(result);
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "{point}: no answer"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert!(
        crashed,
        "{point}: the workload never reached the crash point"
    );

    // P1: the standby holds what the clients were told; then it holds exactly the primary's log.
    let held = || kernable_store::open_read_only(&store(&dir, "s")).unwrap();
    for result in &told {
        let stored = held()
            .result(&result.command_id)
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "{point}: {} was told, not on the standby",
                    result.command_id
                )
            });
        assert_eq!(events(&stored), events(result), "{point}");
    }
    let primary_log = || {
        let read = kernable_store::open_read_only(&store(&dir, "p")).unwrap();
        Log::read(read.as_ref(), &[id(&key(3))]).unwrap()
    };
    let started = Instant::now();
    loop {
        let (p, s) = (
            primary_log(),
            Log::read(held().as_ref(), &[id(&key(3))]).unwrap(),
        );
        if p.events == s.events && p.results == s.results {
            // The commands written into the store before replication carry no worker signature.
            let real = |log: &Log| -> Vec<String> {
                log.check()
                    .into_iter()
                    .map(|v| v.detail)
                    .filter(|d| !d.contains("command pre-"))
                    .collect()
            };
            assert!(real(&p).is_empty(), "{point}: primary {:?}", real(&p));
            assert!(real(&s).is_empty(), "{point}: standby {:?}", real(&s));
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{point}: the standby does not catch up ({} of {} events)",
            s.events.len(),
            p.events.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop((primary, standby));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_replication_crash_point_loses_nothing_told() {
    let rounds: u32 = std::env::var("KERNABLE_CRASH_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(1);
    for round in 0..rounds {
        for point in REPLICATION_POINTS {
            crash_and_recover(point, round).await;
        }
    }
}
