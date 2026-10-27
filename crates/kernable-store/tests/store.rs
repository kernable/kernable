//! Store contract, run against every backend.

use std::{collections::HashSet, path::PathBuf, sync::Arc, thread};

use kernable_core::proto::{
    Command, ListenerRecord, Middleware, NewEvent, Rejected, Signature, command_result::Outcome,
};
use kernable_store::{Enqueued, Error, FileStore, SqliteStore, Store};

fn cmd(id: &str, command_type: &str) -> Command {
    Command {
        vault_refs: Vec::new(),
        command_id: id.into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o1".into(),
        command_type: command_type.into(),
        payload: b"{}".to_vec(),
        client_sig: None,
        context: None,
    }
}

fn ev(event_type: &str) -> NewEvent {
    NewEvent {
        erases: String::new(),
        vault_refs: Vec::new(),
        event_type: event_type.into(),
        payload: b"{}".to_vec(),
        schema: 1,
    }
}

/// The store does not check signatures; the kernel does.
fn sig() -> Signature {
    Signature::default()
}

fn versions(store: &dyn Store) -> Vec<u64> {
    let events = store.read_stream("t1", "orders", "Order", "o1").unwrap();
    events.iter().map(|e| e.version).collect()
}

fn commit_appends_events_and_completes_command(store: &dyn Store) {
    assert_eq!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap(),
        Enqueued::New
    );

    let stored = store
        .commit("c1", 0, &[ev("OrderPlaced"), ev("ItemAdded")], &sig(), 1)
        .unwrap();

    assert_eq!(stored.iter().map(|e| e.version).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(versions(store), [1, 2]);
    assert_eq!(store.read_all(0, 10).unwrap().len(), 2);
    assert_eq!(store.head_position().unwrap(), 2);
    assert_eq!(
        store.read_all(stored[0].global_position, 10).unwrap().len(),
        1
    );
    let result = store.result("c1").unwrap().unwrap();
    assert!(matches!(result.outcome, Some(Outcome::Committed(c)) if c.events.len() == 2));
    assert!(store.pending().unwrap().is_empty());
}

fn stale_version_conflicts_and_command_stays_pending(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();

    let err = store
        .commit("c1", 3, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VersionConflict {
            expected: 3,
            actual: 0
        }
    ));
    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();
    assert_eq!(versions(store), [1]);
}

fn a_command_completes_at_most_once(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();
    assert!(matches!(
        store.commit("c1", 1, &[ev("OrderPlaced")], &sig(), 2),
        Err(Error::NotPending)
    ));
    let reason = Rejected::default();
    assert!(matches!(
        store.reject("c1", reason.clone()),
        Err(Error::NotPending)
    ));
    assert!(matches!(
        store.reject("unknown", reason),
        Err(Error::NotPending)
    ));
    assert_eq!(versions(store), [1]);
}

fn pending_lists_open_commands_in_submission_order(store: &dyn Store) {
    for (i, id) in ["c1", "c2", "c3"].into_iter().enumerate() {
        store.enqueue(&cmd(id, "AddItem"), i as i64).unwrap();
    }
    store
        .commit("c2", 0, &[ev("ItemAdded")], &sig(), 3)
        .unwrap();
    let pending: Vec<(String, i64)> = store
        .pending()
        .unwrap()
        .into_iter()
        .map(|p| (p.command.command_id, p.received_at))
        .collect();
    assert_eq!(pending, [("c1".into(), 0), ("c3".into(), 2)]);
}

fn enqueue_is_idempotent(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    assert_eq!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap(),
        Enqueued::Duplicate { result: None }
    );

    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), 1)
        .unwrap();
    assert!(matches!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 2).unwrap(),
        Enqueued::Duplicate { result: Some(_) }
    ));
    assert_eq!(versions(store), [1]);
}

fn reject_completes_command_without_events(store: &dyn Store) {
    store.enqueue(&cmd("c1", "CancelOrder"), 0).unwrap();
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "no order".into(),
    };
    store.reject("c1", reason.clone()).unwrap();

    let result = store.result("c1").unwrap().unwrap();
    assert_eq!(result.outcome, Some(Outcome::Rejected(reason)));
    assert!(versions(store).is_empty());
}

fn listener(name: &str, order: i32, position: u64) -> ListenerRecord {
    ListenerRecord {
        name: name.into(),
        key_id: "k".into(),
        order,
        event_types: vec!["OrderPlaced".into()],
        position,
        removed: false,
        middleware: None,
    }
}

fn listeners_are_saved_replaced_and_removed(store: &dyn Store) {
    assert!(store.listeners().unwrap().is_empty());
    let auth = ListenerRecord {
        middleware: Some(Middleware {
            order: 0,
            types: vec!["*".into()],
        }),
        ..listener("auth", -30, 0)
    };
    store
        .save_listeners(&[listener("orders", 0, 0), auth.clone()])
        .unwrap();
    store.save_listeners(&[listener("orders", 10, 7)]).unwrap();
    assert_eq!(
        store.listeners().unwrap(),
        [auth, listener("orders", 10, 7)]
    );

    let removed = ListenerRecord {
        removed: true,
        ..listener("auth", 0, 0)
    };
    store.save_listeners(&[removed]).unwrap();
    assert_eq!(store.listeners().unwrap(), [listener("orders", 10, 7)]);
}

fn reading_after_a_version_returns_only_later_events(store: &dyn Store) {
    for (i, id) in ["c1", "c2", "c3"].into_iter().enumerate() {
        store.enqueue(&cmd(id, "AddItem"), i as i64).unwrap();
        store
            .commit(id, i as u64, &[ev("ItemAdded")], &sig(), 0)
            .unwrap();
    }
    let after = |v| -> Vec<u64> {
        let events = store
            .read_stream_after("t1", "orders", "Order", "o1", v)
            .unwrap();
        events.iter().map(|e| e.version).collect()
    };
    assert_eq!(
        (after(0), after(2), after(3), after(9)),
        (vec![1, 2, 3], vec![3], vec![], vec![])
    );
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

/// Several connections on one file race to commit commands on a single aggregate: every
/// command must be committed exactly once, with no lost updates or version gaps.
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
        .map(|t| {
            let path = Arc::clone(&path);
            thread::spawn(move || {
                let store = SqliteStore::open(path.as_path()).unwrap();
                for i in (t..THREADS * COMMANDS).step_by(THREADS) {
                    loop {
                        let current = store
                            .read_stream("t1", "orders", "Order", "o1")
                            .unwrap()
                            .len() as u64;
                        match store.commit(&format!("c{i}"), current, &[ev("ItemAdded")], &sig(), 0)
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

    let events = setup.read_stream("t1", "orders", "Order", "o1").unwrap();
    let expected: Vec<u64> = (1..=(THREADS * COMMANDS) as u64).collect();
    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        expected
    );
    let committed: HashSet<_> = events.iter().map(|e| e.command_id.as_str()).collect();
    assert_eq!(committed.len(), THREADS * COMMANDS);
}

/// Several threads race to commit commands on one store and one aggregate; the version
/// check makes the losers retry.
fn concurrent_threads_lose_no_updates(store: &dyn Store) {
    const THREADS: usize = 4;
    const COMMANDS: usize = 50;
    for i in 0..THREADS * COMMANDS {
        store
            .enqueue(&cmd(&format!("c{i}"), "AddItem"), i as i64)
            .unwrap();
    }
    thread::scope(|s| {
        for t in 0..THREADS {
            s.spawn(move || {
                for i in (t..THREADS * COMMANDS).step_by(THREADS) {
                    loop {
                        let current = store
                            .read_stream("t1", "orders", "Order", "o1")
                            .unwrap()
                            .len() as u64;
                        match store.commit(&format!("c{i}"), current, &[ev("ItemAdded")], &sig(), 0)
                        {
                            Ok(_) => break,
                            Err(Error::VersionConflict { .. }) => continue,
                            Err(e) => panic!("{e}"),
                        }
                    }
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
        mod memory {
            $(#[test]
            fn $name() {
                super::$name(&super::FileStore::in_memory());
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
    stale_version_conflicts_and_command_stays_pending,
    a_command_completes_at_most_once,
    pending_lists_open_commands_in_submission_order,
    enqueue_is_idempotent,
    reject_completes_command_without_events,
    concurrent_threads_lose_no_updates,
    listeners_are_saved_replaced_and_removed,
    reading_after_a_version_returns_only_later_events,
);
