use std::{collections::HashSet, path::PathBuf, sync::Arc, thread};

use kernable_core::proto::{Command, NewEvent, Rejected, Signature, command_result::Outcome};
use kernable_store_sqlite::{Enqueued, Error, SqliteStore};

const TTL: i64 = 1_000;

fn cmd(id: &str, command_type: &str) -> Command {
    Command {
        command_id: id.into(),
        tenant_id: "t1".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o1".into(),
        command_type: command_type.into(),
        payload: b"{}".to_vec(),
        client_sig: None,
    }
}

fn ev(event_type: &str) -> NewEvent {
    NewEvent {
        event_type: event_type.into(),
        payload: b"{}".to_vec(),
    }
}

/// The store does not check signatures; the kernel does.
fn sig() -> Signature {
    Signature::default()
}

fn versions(store: &SqliteStore) -> Vec<u64> {
    let events = store.read_stream("t1", "Order", "o1").unwrap();
    events.iter().map(|e| e.version).collect()
}

#[test]
fn commit_appends_events_and_completes_command() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert_eq!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap(),
        Enqueued::New
    );

    let lease = store.lease(&["PlaceOrder"], 0, TTL).unwrap().unwrap();
    let stored = store
        .commit(
            &lease.lease_id,
            0,
            &[ev("OrderPlaced"), ev("ItemAdded")],
            &sig(),
            1,
        )
        .unwrap();

    assert_eq!(stored.iter().map(|e| e.version).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(versions(&store), [1, 2]);
    assert_eq!(store.read_all(0, 10).unwrap().len(), 2);
    assert_eq!(
        store.read_all(stored[0].global_position, 10).unwrap().len(),
        1
    );
    let result = store.result("c1").unwrap().unwrap();
    assert!(matches!(result.outcome, Some(Outcome::Committed(c)) if c.events.len() == 2));
    assert!(store.lease(&["PlaceOrder"], 2, TTL).unwrap().is_none());
}

#[test]
fn stale_version_conflicts_and_lease_stays_usable() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store.enqueue(&cmd("c2", "AddItem"), 0).unwrap();

    let l1 = store.lease(&["PlaceOrder"], 0, TTL).unwrap().unwrap();
    let l2 = store.lease(&["AddItem"], 0, TTL).unwrap().unwrap();
    store
        .commit(&l1.lease_id, 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();

    let err = store
        .commit(&l2.lease_id, 0, &[ev("ItemAdded")], &sig(), 1)
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VersionConflict {
            expected: 0,
            actual: 1
        }
    ));
    store
        .commit(&l2.lease_id, 1, &[ev("ItemAdded")], &sig(), 2)
        .unwrap();
    assert_eq!(versions(&store), [1, 2]);
}

#[test]
fn enqueue_is_idempotent() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    assert_eq!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap(),
        Enqueued::Duplicate { result: None }
    );

    let lease = store.lease(&["PlaceOrder"], 0, TTL).unwrap().unwrap();
    store
        .commit(&lease.lease_id, 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();
    assert!(matches!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 2).unwrap(),
        Enqueued::Duplicate { result: Some(_) }
    ));
    assert_eq!(versions(&store), [1]);
}

#[test]
fn expired_lease_is_redelivered_and_old_lease_is_lost() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();

    let first = store.lease(&["PlaceOrder"], 0, TTL).unwrap().unwrap();
    assert!(
        store
            .lease(&["PlaceOrder"], TTL - 1, TTL)
            .unwrap()
            .is_none()
    );
    let second = store.lease(&["PlaceOrder"], TTL, TTL).unwrap().unwrap();
    assert_ne!(first.lease_id, second.lease_id);

    let late = store.commit(&first.lease_id, 0, &[ev("OrderPlaced")], &sig(), TTL);
    assert!(matches!(late, Err(Error::LeaseLost)));
    store
        .commit(&second.lease_id, 0, &[ev("OrderPlaced")], &sig(), TTL)
        .unwrap();
    assert!(matches!(
        store.commit(&second.lease_id, 1, &[ev("OrderPlaced")], &sig(), TTL),
        Err(Error::LeaseLost)
    ));
    assert_eq!(versions(&store), [1]);
}

#[test]
fn lease_only_returns_requested_command_types() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    assert!(store.lease(&["ShipOrder"], 0, TTL).unwrap().is_none());
    assert!(store.lease(&[], 0, TTL).unwrap().is_none());
    assert!(
        store
            .lease(&["ShipOrder", "PlaceOrder"], 0, TTL)
            .unwrap()
            .is_some()
    );
}

#[test]
fn reject_completes_command_without_events() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enqueue(&cmd("c1", "CancelOrder"), 0).unwrap();
    let lease = store.lease(&["CancelOrder"], 0, TTL).unwrap().unwrap();
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "no order".into(),
    };
    store.reject(&lease.lease_id, reason.clone()).unwrap();

    let result = store.result("c1").unwrap().unwrap();
    assert_eq!(result.outcome, Some(Outcome::Rejected(reason)));
    assert!(versions(&store).is_empty());
}

struct TempDb(PathBuf);

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

/// Several connections on one file race for leases and commits on a single aggregate:
/// every command must be committed exactly once, with no lost updates or version gaps.
#[test]
fn concurrent_connections_lose_no_updates() {
    const THREADS: usize = 4;
    const COMMANDS: usize = 50;

    let db = TempDb(std::env::temp_dir().join(format!("kernable-test-{}.db", std::process::id())));
    let setup = SqliteStore::open(&db.0).unwrap();
    for i in 0..THREADS * COMMANDS {
        setup
            .enqueue(&cmd(&format!("c{i}"), "AddItem"), i as i64)
            .unwrap();
    }

    let path = Arc::new(db.0.clone());
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let path = Arc::clone(&path);
            thread::spawn(move || {
                let store = SqliteStore::open(path.as_path()).unwrap();
                while let Some(lease) = store.lease(&["AddItem"], 0, i64::MAX / 2).unwrap() {
                    loop {
                        let current = store.read_stream("t1", "Order", "o1").unwrap().len() as u64;
                        match store.commit(&lease.lease_id, current, &[ev("ItemAdded")], &sig(), 0)
                        {
                            Ok(_) => break,
                            Err(Error::VersionConflict { .. }) => continue,
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let events = setup.read_stream("t1", "Order", "o1").unwrap();
    let expected: Vec<u64> = (1..=(THREADS * COMMANDS) as u64).collect();
    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        expected
    );
    let committed: HashSet<_> = events.iter().map(|e| e.command_id.as_str()).collect();
    assert_eq!(committed.len(), THREADS * COMMANDS);
}
