//! Continuous backup: a backup follower writes a kernel started with `--backup on` to a
//! target, continues where it left off after a restart, and a restore builds the same store
//! again, whole or up to a record.

mod common;

use std::{sync::Arc, time::Duration};

use common::*;
use kernable_kernel::{
    backup::{self, Follower, Until, crypt::BackupKey, target},
    standby::Stores,
};
use prost::Message;

/// The follower's key, granted `backup`.
fn follower(env: &Env, to: &str) -> Follower {
    let key = key(9);
    let grant = grant(&key, &[("backup", &["*"])]);
    env.kernel.grant(grant.clone()).unwrap();
    Follower {
        addrs: vec![env.addr.clone()],
        key,
        grant: hex(&grant.encode_to_vec()),
        target: Arc::from(target::open(to).unwrap()),
        crypt: Arc::new(BackupKey::new(&[7; 32])),
        segment_every: Duration::from_millis(100),
        base_every: Duration::from_secs(3600),
        keep: 3,
    }
}

/// Where the test's backup goes: a directory, or `KERNABLE_TEST_BACKUP_TARGET` (an
/// `s3://<bucket>/<prefix>` with the `KERNABLE_S3_*` and `AWS_*` variables), under a prefix of
/// the test's own.
fn target_of(env: &Env) -> String {
    let name = env.dir.file_name().unwrap().to_string_lossy();
    match std::env::var("KERNABLE_TEST_BACKUP_TARGET") {
        Ok(base) if !base.is_empty() => format!("{}/{name}", base.trim_end_matches('/')),
        _ => format!("file:{}", env.dir.join("backup").display()),
    }
}

/// How far the backup in `to` reaches, and in how many generations.
fn reach(to: &str) -> (u64, usize) {
    let t = target::open(to).unwrap();
    let gens = backup::generations(t.as_ref(), &BackupKey::new(&[7; 32])).unwrap();
    let last = gens.iter().rev().find_map(|g| {
        let b = g.base.as_ref()?;
        Some(g.segments.last().map_or(b.seq, |s| s.last))
    });
    (last.unwrap_or(0), gens.len())
}

async fn backed_up(env: &Env, to: &str) {
    let head = env.kernel_head();
    for _ in 0..500 {
        if reach(to).0 >= head {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the backup did not reach record {head}: {:?}", reach(to));
}

trait Head {
    fn kernel_head(&self) -> u64;
}

impl Head for Env {
    fn kernel_head(&self) -> u64 {
        let store = kernable_store::open_read_only(&self.store).unwrap();
        store.record_head().unwrap().seq
    }
}

async fn place(env: &Env, from: usize, to: usize) {
    for i in from..to {
        let cmd = signed(
            command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
            &key(1),
        );
        submit(&env.addr, cmd).await.unwrap();
    }
}

/// New stores in `dir`, named after `name`.
fn stores(env: &Env, name: &str) -> Stores {
    let path = env.dir.join(name);
    std::fs::create_dir_all(&path).unwrap();
    Stores {
        store: kernable_store::open(&format!("sqlite:{}", path.join("k.db").display())).unwrap(),
        vault: None,
        data: kernable_store::data::open(&format!("file:{}", path.join("data").display())).unwrap(),
        snapshots: kernable_store::snapshots::open("memory:").unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backup_restores_the_same_store_and_continues_after_a_restart() {
    let env = start_custom("backup", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = target_of(&env);
    place(&env, 0, 10).await;
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    backed_up(&env, &to).await;

    // Stopped and started again, it continues the same generation.
    running.abort();
    place(&env, 10, 20).await;
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    backed_up(&env, &to).await;
    running.abort();
    assert_eq!(reach(&to).1, 1, "one generation");

    let restored = stores(&env, "restored");
    let t = target::open(&to).unwrap();
    let done = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &restored,
        Until::End,
        &[id(&key(3))],
    )
    .unwrap();
    assert_eq!(done.record, env.kernel_head());
    let source = kernable_store::open_read_only(&env.store).unwrap();
    assert_eq!(
        restored.store.read_all(0, 1_000).unwrap(),
        source.read_all(0, 1_000).unwrap(),
        "the same events"
    );
    assert!(restored.store.verify(&[id(&key(3))]).unwrap().is_ok());

    // Another key opens nothing.
    let wrong = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[8; 32]),
        &stores(&env, "wrong"),
        Until::End,
        &[],
    );
    assert!(wrong.unwrap_err().to_string().contains("another key"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_stops_at_a_record() {
    let env = start_custom("backup-at", |c, _| c.replication.backup = true).await;
    spawn_worker(&env).await;
    let to = target_of(&env);
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    place(&env, 0, 5).await;
    backed_up(&env, &to).await;
    let middle = env.kernel_head();
    place(&env, 5, 10).await;
    backed_up(&env, &to).await;
    running.abort();

    let restored = stores(&env, "restored");
    let t = target::open(&to).unwrap();
    let done = backup::restore(
        t.as_ref(),
        &BackupKey::new(&[7; 32]),
        &restored,
        Until::Record(middle),
        &[id(&key(3))],
    )
    .unwrap();
    assert_eq!(done.record, middle);
    assert_eq!(restored.store.record_head().unwrap().seq, middle);
    let events = restored.store.read_all(0, 1_000).unwrap();
    let source = kernable_store::open_read_only(&env.store).unwrap();
    let all = source.read_all(0, 1_000).unwrap();
    assert!(events.len() < all.len(), "later orders are not restored");
    assert_eq!(
        events,
        all[..events.len()],
        "what is restored is the beginning"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_without_backup_on_refuses_the_follower() {
    let env = start("backup-off").await;
    let to = target_of(&env);
    let running = tokio::spawn(backup::run(follower(&env, &to)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.abort();
    assert_eq!(reach(&to), (0, 0), "nothing written");
}
