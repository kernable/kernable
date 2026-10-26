//! Tamper tests: every manipulation of the stored log must show up in `verify`.

use std::path::PathBuf;

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{Command, NewEvent},
    signing::{self, GENESIS},
};
use kernable_store_sqlite::{Report, SqliteStore};
use rusqlite::{Connection, params};

struct Fixture {
    path: PathBuf,
    store: SqliteStore,
    worker: SigningKey,
    kernel: SigningKey,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

fn ev(event_type: &str, payload: &str) -> NewEvent {
    NewEvent {
        event_type: event_type.into(),
        payload: payload.as_bytes().to_vec(),
    }
}

impl Fixture {
    /// Positions 1–4 are anchored by a checkpoint; position 5 is not.
    ///
    /// | pos | command | aggregate | event        |
    /// |-----|---------|-----------|--------------|
    /// | 1   | c1      | o1 v1     | OrderPlaced  |
    /// | 2   | c1      | o1 v2     | ItemAdded    |
    /// | 3   | c2      | o2 v1     | OrderPlaced  |
    /// | 4   | c3      | o1 v3     | ItemAdded    |
    /// | 5   | c4      | o2 v2     | OrderShipped |
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kernable-integrity-{name}-{}.db",
            std::process::id()
        ));
        let f = Fixture {
            store: SqliteStore::open(&path).unwrap(),
            path,
            worker: SigningKey::from_bytes(&[2; 32]),
            kernel: SigningKey::from_bytes(&[3; 32]),
        };
        f.run(
            "c1",
            "o1",
            &[ev("OrderPlaced", "{}"), ev("ItemAdded", r#"{"sku":"a"}"#)],
        );
        f.run("c2", "o2", &[ev("OrderPlaced", "{}")]);
        f.run("c3", "o1", &[ev("ItemAdded", r#"{"sku":"b"}"#)]);
        f.store.checkpoint(&f.kernel, 10).unwrap().unwrap();
        f.run("c4", "o2", &[ev("OrderShipped", "{}")]);
        f
    }

    fn run(&self, id: &str, aggregate_id: &str, events: &[NewEvent]) {
        let mut cmd = Command {
            command_id: id.into(),
            tenant_id: "t1".into(),
            aggregate_type: "Order".into(),
            aggregate_id: aggregate_id.into(),
            command_type: "Do".into(),
            payload: b"{}".to_vec(),
            client_sig: None,
        };
        let client = SigningKey::from_bytes(&[1; 32]);
        cmd.client_sig = Some(signing::sign(&client, &signing::command_message(&cmd)));
        self.store.enqueue(&cmd, 0).unwrap();

        let lease = self.store.lease(&["Do"], 0, 1_000).unwrap().unwrap();
        let version = self
            .store
            .read_stream("t1", "Order", aggregate_id)
            .unwrap()
            .len() as u64;
        let msg = signing::result_message(&lease.command, version, events);
        let sig = signing::sign(&self.worker, &msg);
        self.store
            .commit(&lease.lease_id, version, events, &sig, 0)
            .unwrap();
    }

    fn raw(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    fn verify(&self) -> Report {
        let kernel_id = signing::key_id(&self.kernel.verifying_key());
        self.store.verify(&kernel_id).unwrap()
    }
}

fn assert_problem(report: &Report, needle: &str) {
    assert!(
        report.problems.iter().any(|p| p.contains(needle)),
        "expected a problem containing {needle:?}, got {:?}",
        report.problems
    );
}

#[test]
fn intact_log_verifies() {
    let f = Fixture::new("intact");
    let r = f.verify();
    assert!(r.is_ok(), "{:?}", r.problems);
    assert_eq!(
        (r.events, r.commands, r.checkpoints, r.unanchored),
        (5, 4, 1, 1)
    );
}

#[test]
fn modified_event_is_detected() {
    let f = Fixture::new("modified");
    f.raw()
        .execute(
            "UPDATE events SET payload = CAST('{\"sku\":\"z\"}' AS BLOB) WHERE global_position = 2",
            [],
        )
        .unwrap();
    let r = f.verify();
    assert_problem(&r, "event 2: hash mismatch");
    assert_problem(&r, "command c1: invalid worker signature");
}

#[test]
fn deleted_event_is_detected() {
    let f = Fixture::new("deleted");
    f.raw()
        .execute("DELETE FROM events WHERE global_position = 3", [])
        .unwrap();
    assert_problem(&f.verify(), "event 4: link to previous event broken");
}

#[test]
fn reordered_events_are_detected() {
    let f = Fixture::new("reordered");
    let raw = f.raw();
    raw.execute_batch(
        "UPDATE events SET global_position = 99 WHERE global_position = 3;
         UPDATE events SET global_position = 3 WHERE global_position = 4;
         UPDATE events SET global_position = 4 WHERE global_position = 99;",
    )
    .unwrap();
    let r = f.verify();
    assert_problem(&r, "event 3: link to previous event broken");
    assert_problem(&r, "event 3: hash mismatch");
}

#[test]
fn truncated_log_is_detected_by_checkpoint() {
    let f = Fixture::new("truncated");
    f.raw()
        .execute("DELETE FROM events WHERE global_position >= 4", [])
        .unwrap();
    assert_problem(&f.verify(), "checkpoint 4: event missing");
}

/// An attacker with DB write access and their own worker key rewrites history consistently:
/// every hash and worker signature checks out, only the kernel-signed checkpoint exposes it.
#[test]
fn consistently_rewritten_chain_is_detected_by_checkpoint() {
    let f = Fixture::new("rewritten");
    let attacker = SigningKey::from_bytes(&[66; 32]);
    let mut events = f.store.read_all(0, 100).unwrap();
    events[1].payload = br#"{"sku":"stolen"}"#.to_vec();

    let c1 = Command {
        command_id: "c1".into(),
        tenant_id: "t1".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o1".into(),
        ..Default::default()
    };
    let c1_events: Vec<NewEvent> = events[..2]
        .iter()
        .map(|e| NewEvent {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
        })
        .collect();
    let forged = signing::sign(&attacker, &signing::result_message(&c1, 0, &c1_events));

    let raw = f.raw();
    let mut prev = GENESIS.to_vec();
    for e in &mut events {
        if e.command_id == "c1" {
            e.producer_sig = Some(forged.clone());
        }
        e.hash = signing::event_hash(&prev, e).to_vec();
        let sig = e.producer_sig.as_ref().unwrap();
        raw.execute(
            "UPDATE events SET payload = ?2, producer_key_id = ?3, producer_sig = ?4, prev_hash = ?5, hash = ?6
             WHERE global_position = ?1",
            params![e.global_position, e.payload, sig.key_id, sig.sig, prev, e.hash],
        )
        .unwrap();
        prev = e.hash.clone();
    }

    let r = f.verify();
    assert_eq!(
        r.problems,
        ["checkpoint 4: chain hash differs (log rewritten)"]
    );
}

#[test]
fn forged_checkpoint_is_detected() {
    let f = Fixture::new("forged-checkpoint");
    f.raw()
        .execute("UPDATE checkpoints SET chain_hash = zeroblob(32)", [])
        .unwrap();
    assert_problem(&f.verify(), "checkpoint 4: invalid kernel signature");

    let other = signing::key_id(&SigningKey::from_bytes(&[4; 32]).verifying_key());
    assert_problem(&f.store.verify(&other).unwrap(), "signed by unknown key");
}

#[test]
fn unsigned_command_is_reported() {
    let f = Fixture::new("unsigned");
    let cmd = Command {
        command_id: "c5".into(),
        tenant_id: "t1".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o3".into(),
        command_type: "Do".into(),
        payload: b"{}".to_vec(),
        client_sig: None,
    };
    f.store.enqueue(&cmd, 1).unwrap();
    assert_problem(
        &f.verify(),
        "command c5: missing or invalid client signature",
    );
}
