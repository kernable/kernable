//! Store contract, run against every backend.

use std::{collections::HashSet, path::PathBuf, sync::Arc, thread};

use kernable_core::proto::{Command, NewEvent, Rejected, Signature, command_result::Outcome};
use kernable_store::{Enqueued, Error, FileStore, SqliteStore, Store};

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

fn versions(store: &dyn Store) -> Vec<u64> {
    let events = store.read_stream("t1", "Order", "o1").unwrap();
    events.iter().map(|e| e.version).collect()
}

fn commit_appends_events_and_completes_command(store: &dyn Store) {
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
    assert_eq!(versions(store), [1, 2]);
    assert_eq!(store.read_all(0, 10).unwrap().len(), 2);
    assert_eq!(
        store.read_all(stored[0].global_position, 10).unwrap().len(),
        1
    );
    let result = store.result("c1").unwrap().unwrap();
    assert!(matches!(result.outcome, Some(Outcome::Committed(c)) if c.events.len() == 2));
    assert!(store.lease(&["PlaceOrder"], 2, TTL).unwrap().is_none());
}

fn stale_version_conflicts_and_lease_stays_usable(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    let lease = store.lease(&["PlaceOrder"], 0, TTL).unwrap().unwrap();

    let err = store
        .commit(&lease.lease_id, 3, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VersionConflict {
            expected: 3,
            actual: 0
        }
    ));
    store
        .commit(&lease.lease_id, 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();
    assert_eq!(versions(store), [1]);
}

fn one_command_per_aggregate_at_a_time_in_submission_order(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store.enqueue(&cmd("c2", "AddItem"), 1).unwrap();
    let mut other = cmd("c3", "AddItem");
    other.aggregate_id = "o2".into();
    store.enqueue(&other, 2).unwrap();

    // A worker for AddItem may not overtake c1 on o1, but o2 is free.
    let l3 = store.lease(&["AddItem"], 0, TTL).unwrap().unwrap();
    assert_eq!(l3.command.command_id, "c3");
    let l1 = store
        .lease(&["PlaceOrder", "AddItem"], 0, TTL)
        .unwrap()
        .unwrap();
    assert_eq!(l1.command.command_id, "c1");
    // o1 is busy while c1 is leased: c2 waits.
    assert!(store.lease(&["AddItem"], 0, TTL).unwrap().is_none());

    // An expired lease gives the aggregate back to the same, oldest command.
    let again = store
        .lease(&["PlaceOrder", "AddItem"], TTL, TTL)
        .unwrap()
        .unwrap();
    assert_eq!(again.command.command_id, "c1");
    store
        .commit(&again.lease_id, 0, &[ev("OrderPlaced")], &sig(), TTL)
        .unwrap();

    let l2 = store.lease(&["AddItem"], TTL, TTL).unwrap().unwrap();
    assert_eq!(l2.command.command_id, "c2");
}

fn enqueue_is_idempotent(store: &dyn Store) {
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
    assert_eq!(versions(store), [1]);
}

fn expired_lease_is_redelivered_and_old_lease_is_lost(store: &dyn Store) {
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
    assert_eq!(versions(store), [1]);
}

fn lease_only_returns_requested_command_types(store: &dyn Store) {
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

fn reject_completes_command_without_events(store: &dyn Store) {
    store.enqueue(&cmd("c1", "CancelOrder"), 0).unwrap();
    let lease = store.lease(&["CancelOrder"], 0, TTL).unwrap().unwrap();
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "no order".into(),
    };
    store.reject(&lease.lease_id, reason.clone()).unwrap();

    let result = store.result("c1").unwrap().unwrap();
    assert_eq!(result.outcome, Some(Outcome::Rejected(reason)));
    assert!(versions(store).is_empty());
}

struct TempDb(PathBuf);

impl TempDb {
    fn new(name: &str) -> Self {
        TempDb(std::env::temp_dir().join(format!("kernable-{name}-{}", std::process::id())))
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

/// Several connections on one file race for leases and commits on a single aggregate:
/// every command must be committed exactly once, with no lost updates or version gaps.
/// SQLite only: several connections (as from several processes) on one file.
#[test]
fn sqlite_concurrent_connections_lose_no_updates() {
    const THREADS: usize = 4;
    const COMMANDS: usize = 50;

    let db = TempDb::new("sqlite-connections.db");
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

/// Several threads race for leases and commits on one store and one aggregate.
fn concurrent_threads_lose_no_updates(store: &dyn Store) {
    const THREADS: usize = 4;
    const COMMANDS: usize = 50;
    for i in 0..THREADS * COMMANDS {
        store
            .enqueue(&cmd(&format!("c{i}"), "AddItem"), i as i64)
            .unwrap();
    }
    let done = std::sync::atomic::AtomicUsize::new(0);
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                while done.load(std::sync::atomic::Ordering::SeqCst) < THREADS * COMMANDS {
                    let Some(lease) = store.lease(&["AddItem"], 0, i64::MAX / 2).unwrap() else {
                        thread::yield_now();
                        continue;
                    };
                    let current = store.read_stream("t1", "Order", "o1").unwrap().len() as u64;
                    store
                        .commit(&lease.lease_id, current, &[ev("ItemAdded")], &sig(), 0)
                        .unwrap();
                    done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
    });
    let expected: Vec<u64> = (1..=(THREADS * COMMANDS) as u64).collect();
    assert_eq!(versions(store), expected);
}

fn file_store(name: &str) -> (TempDb, Box<dyn Store>) {
    let db = TempDb::new(&format!("{name}.log"));
    let store = FileStore::open(&db.0).unwrap();
    (db, Box::new(store))
}

macro_rules! for_each_backend {
    ($($name:ident),* $(,)?) => {
        mod sqlite {
            $(#[test]
            fn $name() {
                super::$name(&super::SqliteStore::open_in_memory().unwrap());
            })*
        }
        mod file {
            $(#[test]
            fn $name() {
                let (_db, store) = super::file_store(stringify!($name));
                super::$name(&*store);
            })*
        }
    };
}

for_each_backend!(
    commit_appends_events_and_completes_command,
    stale_version_conflicts_and_lease_stays_usable,
    one_command_per_aggregate_at_a_time_in_submission_order,
    enqueue_is_idempotent,
    expired_lease_is_redelivered_and_old_lease_is_lost,
    lease_only_returns_requested_command_types,
    reject_completes_command_without_events,
    concurrent_threads_lose_no_updates,
);
