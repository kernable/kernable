//! Store contract, run against every backend.

use std::{collections::HashSet, path::PathBuf, sync::Arc, thread};

use kernable_core::proto::{
    AggregateIndex, Attribute, Command, Grant, ListenerRecord, Membership, MembershipRecord,
    Middleware, NewEvent, ParkedEvent, Permission, Rejected, RequestContext, Requirement,
    RevocationRecord, Signature, StoredKey, When, WorkerRecord, command_result::Outcome,
};
use kernable_store::{Enqueued, Error, FileStore, SqliteStore, Store, StoredCommand, StreamId};

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
        deliver_at: 0,
        deliver_until: 0,
    }
}

fn ev(event_type: &str) -> NewEvent {
    NewEvent {
        erases: String::new(),
        vault_refs: Vec::new(),
        event_type: event_type.into(),
        payload: b"{}".to_vec(),
        schema: 1,
        origin_partition: String::new(),
        origin_position: 0,
        deployment: Default::default(),
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
        .commit(
            "c1",
            0,
            &[ev("OrderPlaced"), ev("ItemAdded")],
            &sig(),
            None,
            1,
        )
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
        .commit("c1", 3, &[ev("OrderPlaced")], &sig(), None, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VersionConflict {
            expected: 3,
            actual: 0
        }
    ));
    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), None, 1)
        .unwrap();
    assert_eq!(versions(store), [1]);
}

fn a_command_completes_at_most_once(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), None, 1)
        .unwrap();
    assert!(matches!(
        store.commit("c1", 1, &[ev("OrderPlaced")], &sig(), None, 2),
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
        .commit("c2", 0, &[ev("ItemAdded")], &sig(), None, 3)
        .unwrap();
    let pending: Vec<(String, i64)> = store
        .pending()
        .unwrap()
        .into_iter()
        .map(|p| (p.command.command_id, p.received_at))
        .collect();
    assert_eq!(pending, [("c1".into(), 0), ("c3".into(), 2)]);
}

/// A scheduled command keeps its delivery time and limit wherever the store hands it out.
fn delivery_times_come_back_as_stored(store: &dyn Store) {
    let scheduled = Command {
        deliver_at: 1_000,
        deliver_until: 2_000,
        ..cmd("c1", "AddItem")
    };
    store.enqueue(&scheduled, 5).unwrap();
    store.enqueue(&cmd("c2", "AddItem"), 6).unwrap();
    let pending = store.pending().unwrap();
    assert_eq!(pending[0].command, scheduled);
    assert_eq!(
        (
            pending[1].command.deliver_at,
            pending[1].command.deliver_until
        ),
        (0, 0)
    );
    assert_eq!(store.command("c1").unwrap().unwrap().command, scheduled);
    let mut imaged = Vec::new();
    let read = store.snapshot(&mut |record| {
        if let kernable_core::proto::log_record::Record::Enqueued(e) = record {
            imaged.extend(e.command);
        }
        Ok(())
    });
    if read.is_ok() {
        assert_eq!(imaged[0], scheduled);
    }
}

fn enqueue_is_idempotent(store: &dyn Store) {
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    assert_eq!(
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap(),
        Enqueued::Duplicate { result: None }
    );

    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), None, 1)
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
        ..Default::default()
    };
    store.reject("c1", reason.clone()).unwrap();

    let result = store.result("c1").unwrap().unwrap();
    assert_eq!(result.outcome, Some(Outcome::Rejected(reason)));
    assert!(versions(store).is_empty());
}

fn rejections_list_the_newest_first_with_why(store: &dyn Store) {
    for (id, at) in [("c1", 10), ("c2", 20), ("c3", 30)] {
        store.enqueue(&cmd(id, "PlaceOrder"), at).unwrap();
    }
    let early = Rejected {
        code: "invalid_state".into(),
        message: "no order".into(),
        ..Default::default()
    };
    let late = Rejected {
        code: "requirement_missing".into(),
        message: "the customer is not active".into(),
        key: "email".into(),
        requirement: Some(Box::new(Requirement {
            domain: "crm".into(),
            aggregate_type: "Customer".into(),
            fact: "active".into(),
            ..Default::default()
        })),
    };
    store.reject("c1", early.clone()).unwrap();
    store.reject("c3", late.clone()).unwrap();

    // c2 is still pending: not a rejection.
    let all = store.rejections(10).unwrap();
    let seen: Vec<(&str, i64, &Rejected)> = all
        .iter()
        .map(|(p, r)| (p.command.command_id.as_str(), p.received_at, r))
        .collect();
    assert_eq!(seen, [("c3", 30, &late), ("c1", 10, &early)]);
    assert_eq!(all[0].0.command.command_type, "PlaceOrder");
    let newest = store.rejections(1).unwrap();
    assert_eq!(newest.len(), 1);
    assert_eq!(newest[0].0.command.command_id, "c3");
}

fn commands_list_the_newest_first_with_their_results(store: &dyn Store) {
    for (id, at) in [("c1", 10), ("c2", 20), ("c3", 30), ("c4", 40)] {
        store.enqueue(&cmd(id, "PlaceOrder"), at).unwrap();
    }
    store
        .reject(
            "c2",
            Rejected {
                code: "invalid_state".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let ids = |list: &[StoredCommand]| -> Vec<String> {
        list.iter()
            .map(|c| c.pending.command.command_id.clone())
            .collect()
    };
    let all = store.commands(None, 10).unwrap();
    assert_eq!(ids(&all), ["c4", "c3", "c2", "c1"]);
    assert_eq!(all[0].pending.received_at, 40);
    assert!(all[0].result.is_none(), "c4 is pending");
    assert!(all[2].result.is_some(), "c2 completed, rejected");
    assert!(all.windows(2).all(|w| w[0].seq > w[1].seq), "{all:?}");
    // Page by page, from where the last one ended.
    let first = store.commands(None, 2).unwrap();
    assert_eq!(ids(&first), ["c4", "c3"]);
    let rest = store.commands(Some(first[1].seq), 2).unwrap();
    assert_eq!(ids(&rest), ["c2", "c1"]);
    assert!(store.commands(Some(rest[1].seq), 2).unwrap().is_empty());
}

fn listener(name: &str, order: i32, position: u64) -> ListenerRecord {
    ListenerRecord {
        name: name.into(),
        key_id: "k".into(),
        order,
        event_types: vec!["OrderPlaced".into()],
        position,
        removed: false,
        middlewares: Vec::new(),
        service: String::new(),
        last_seen: 1_000 + position as i64,
        parked: Vec::new(),
        parked_changed: false,
    }
}

fn worker(aggregate_type: &str, command_types: &[&str]) -> WorkerRecord {
    WorkerRecord {
        aggregate_type: aggregate_type.into(),
        key_id: "k".into(),
        command_types: command_types.iter().map(|t| t.to_string()).collect(),
        service: "shop".into(),
        last_seen: 42,
        removed: false,
    }
}

/// Process versions are kept once each; the newest change of state per process wins.
fn process_versions_and_states_are_kept(store: &dyn Store) {
    use kernable_core::proto::ProcessRecord;
    assert!(store.processes().unwrap().is_empty());
    let version = |name: &str, version: u32, model: &str| ProcessRecord {
        name: name.into(),
        version,
        model: model.as_bytes().to_vec(),
        canonical: model.as_bytes().to_vec(),
        issued_at: 10 + i64::from(version),
        sig: Some(Signature {
            key_id: "k".into(),
            sig: vec![version as u8],
        }),
        recorded_at: 20,
        removed: false,
        acting: None,
    };
    let state = |name: &str, removed: bool, at: i64| ProcessRecord {
        name: name.into(),
        issued_at: at,
        sig: Some(Signature {
            key_id: "op".into(),
            sig: vec![9],
        }),
        recorded_at: at,
        removed,
        acting: (at == 31).then(|| RequestContext {
            sender_tenant: "platform".into(),
            sender_identity: "process-1".into(),
            sender_session: "key-1".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    store.save_process(&version("shop.b", 1, "{}")).unwrap();
    store
        .save_process(&version("shop.a", 2, "{\"v\":2}"))
        .unwrap();
    store
        .save_process(&version("shop.a", 1, "{\"v\":1}"))
        .unwrap();
    // A version kept already stays as it was.
    store
        .save_process(&version("shop.a", 1, "{\"other\":1}"))
        .unwrap();
    store.save_process(&state("shop.a", true, 30)).unwrap();
    store.save_process(&state("shop.a", false, 31)).unwrap();
    store.save_process(&state("shop.b", true, 32)).unwrap();
    assert_eq!(
        store.processes().unwrap(),
        [
            version("shop.a", 1, "{\"v\":1}"),
            version("shop.a", 2, "{\"v\":2}"),
            version("shop.b", 1, "{}"),
            state("shop.a", false, 31),
            state("shop.b", true, 32),
        ]
    );
}

/// The mark of a masked copy: the newest is kept, and an image carries it to another store.
fn a_copy_mark_is_kept_and_carried_by_an_image(store: &dyn Store) {
    use kernable_core::proto::{CopyRecord, log_record::Record};
    assert_eq!(store.copy_mark().unwrap(), None);
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit("c1", 0, &[ev("OrderPlaced")], &sig(), None, 1)
        .unwrap();
    let mark = |n: i64| CopyRecord {
        copied_at: n,
        position: 1,
        source: format!("store {n}"),
        vault_key: "k".into(),
        source_kernel_keys: vec![format!("{n:064}")],
    };
    store.mark_copy(&mark(1)).unwrap();
    store.mark_copy(&mark(2)).unwrap();
    assert_eq!(store.copy_mark().unwrap(), Some(mark(2)));
    let mut records = Vec::new();
    match store.snapshot(&mut |r| {
        records.push(r);
        Ok(())
    }) {
        Err(Error::NoRecordLog) => return, // `memory:` makes no image
        other => other.unwrap(),
    }
    assert!(records.contains(&Record::Copy(mark(2))));
    static TARGETS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = TARGETS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (_db, file) = file_store(&format!("copy-mark-target-{n}"));
    for target in [
        Box::new(SqliteStore::open_in_memory().unwrap()) as Box<dyn Store>,
        file,
    ] {
        target.apply_image(&records).unwrap();
        assert_eq!(target.copy_mark().unwrap(), Some(mark(2)));
        assert_eq!(target.head_position().unwrap(), 1);
    }
}

/// The process engine's position, and the streams of one domain that hold something.
fn the_engine_position_and_a_domain_index_are_kept(store: &dyn Store) {
    assert_eq!(store.process_position().unwrap(), 0);
    store.save_process_position(7).unwrap();
    store.save_process_position(9).unwrap();
    assert_eq!(store.process_position().unwrap(), 9);
    let in_domain = |domain: &str, id: &str| Command {
        domain: domain.into(),
        aggregate_type: "shop.fulfilment".into(),
        aggregate_id: id.into(),
        ..cmd(id, "Advance")
    };
    let running = AggregateIndex {
        facts: vec!["running".into()],
        ..Default::default()
    };
    for (domain, id) in [("process", "o1#1"), ("process", "o2#1"), ("orders", "x")] {
        store.enqueue(&in_domain(domain, id), 0).unwrap();
        store
            .commit(id, 0, &[ev("StepReached")], &sig(), Some(&running), 1)
            .unwrap();
    }
    // Ended: o1#1 holds nothing any more.
    let mut end = in_domain("process", "o1#1");
    end.command_id = "o1#1-end".into();
    store.enqueue(&end, 0).unwrap();
    store
        .commit(
            "o1#1-end",
            1,
            &[ev("ProcessEnded")],
            &sig(),
            Some(&AggregateIndex::default()),
            2,
        )
        .unwrap();
    let held: Vec<String> = store
        .domain_index("process")
        .unwrap()
        .into_iter()
        .map(|(s, _)| s.aggregate_id)
        .collect();
    assert_eq!(held, ["o2#1"]);
}

fn memberships_are_replaced_per_service(store: &dyn Store) {
    assert!(store.memberships().unwrap().is_empty());
    let membership = |service: &str, listeners: &[&str], at: i64| MembershipRecord {
        membership: Some(Membership {
            processes: vec![format!("{service}.fulfilment")],
            service: service.into(),
            workers: vec!["shop.Cart".into()],
            listeners: listeners.iter().map(|l| l.to_string()).collect(),
            middlewares: vec!["guard".into()],
            metadata: vec![
                Attribute {
                    key: "version".into(),
                    value: "1.0.0".into(),
                },
                Attribute {
                    key: "description".into(),
                    value: "Carts, \"quoted\"".into(),
                },
            ],
            issued_at: at,
            deployment: Default::default(),
        }),
        sig: Some(Signature {
            key_id: "k".into(),
            sig: vec![1, 2, 3],
        }),
        applied_at: at + 1,
    };
    store
        .save_membership(&membership("shop", &["carts", "mails"], 1))
        .unwrap();
    store
        .save_membership(&membership("billing", &[], 2))
        .unwrap();
    store
        .save_membership(&membership("shop", &["carts"], 3))
        .unwrap();
    assert_eq!(
        store.memberships().unwrap(),
        [
            membership("billing", &[], 2),
            membership("shop", &["carts"], 3)
        ]
    );
}

fn grant(key_id: &str, action: &str, issued_at: i64) -> Grant {
    Grant {
        key_id: key_id.into(),
        permissions: vec![Permission {
            action: action.into(),
            types: vec!["*".into()],
        }],
        succeeds: String::new(),
        issued_at,
        root_sig: Some(sig()),
        issuer: None,
    }
}

fn the_newest_grant_per_key_is_kept(store: &dyn Store) {
    assert!(store.grants().unwrap().is_empty());
    store.save_grant(&grant("b", "submit", 5)).unwrap();
    store.save_grant(&grant("a", "submit", 1)).unwrap();
    store.save_grant(&grant("a", "query", 2)).unwrap();
    // Older, or as old: kept out.
    store.save_grant(&grant("a", "*", 1)).unwrap();
    store.save_grant(&grant("b", "*", 5)).unwrap();
    assert_eq!(
        store.grants().unwrap(),
        [grant("a", "query", 2), grant("b", "submit", 5)]
    );
}

fn workers_are_saved_replaced_and_removed(store: &dyn Store) {
    assert!(store.workers().unwrap().is_empty());
    let order = worker("orders.Order", &["orders.PlaceOrder"]);
    store
        .save_workers(&[worker("shop.Cart", &["shop.AddItem"]), order.clone()])
        .unwrap();
    let cart = worker("shop.Cart", &["shop.AddItem", "shop.RemoveItem"]);
    store.save_workers(std::slice::from_ref(&cart)).unwrap();
    assert_eq!(store.workers().unwrap(), [order.clone(), cart]);

    let removed = WorkerRecord {
        removed: true,
        ..worker("shop.Cart", &[])
    };
    store.save_workers(&[removed]).unwrap();
    assert_eq!(store.workers().unwrap(), [order]);
}

fn listeners_are_saved_replaced_and_removed(store: &dyn Store) {
    assert!(store.listeners().unwrap().is_empty());
    let auth = ListenerRecord {
        middlewares: vec![
            Middleware {
                order: 0,
                types: vec!["*".into()],
                when: When::Before.into(),
            },
            Middleware {
                order: 5,
                types: vec!["orders.*".into(), "billing.GetInvoice".into()],
                when: When::After.into(),
            },
        ],
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

fn parked_events_change_only_with_their_flag(store: &dyn Store) {
    let parked = |position: u64, retry: bool| ParkedEvent {
        position,
        event_type: "orders.OrderPlaced".into(),
        code: "not_found".into(),
        message: "no such order".into(),
        attempts: 5,
        parked_at: 2_000,
        retry,
    };
    let with = |position: u64, events: Vec<ParkedEvent>, changed: bool| ListenerRecord {
        parked: events,
        parked_changed: changed,
        ..listener("mailer", 10, position)
    };
    store
        .save_listeners(&[with(3, vec![parked(2, false), parked(3, true)], true)])
        .unwrap();
    // A position flush carries none and keeps them.
    store.save_listeners(&[with(9, Vec::new(), false)]).unwrap();
    assert_eq!(
        store.listeners().unwrap(),
        [with(9, vec![parked(2, false), parked(3, true)], false)]
    );
    store
        .save_listeners(&[with(9, vec![parked(3, false)], true)])
        .unwrap();
    assert_eq!(
        store.listeners().unwrap(),
        [with(9, vec![parked(3, false)], false)]
    );
    store.save_listeners(&[with(9, Vec::new(), true)]).unwrap();
    assert_eq!(store.listeners().unwrap(), [with(9, Vec::new(), false)]);

    // Removing the listener removes them.
    store
        .save_listeners(&[with(9, vec![parked(4, false)], true)])
        .unwrap();
    let removed = ListenerRecord {
        removed: true,
        ..listener("mailer", 0, 0)
    };
    store.save_listeners(&[removed]).unwrap();
    store.save_listeners(&[with(1, Vec::new(), false)]).unwrap();
    assert_eq!(store.listeners().unwrap(), [with(1, Vec::new(), false)]);
}

fn reading_after_a_version_returns_only_later_events(store: &dyn Store) {
    for (i, id) in ["c1", "c2", "c3"].into_iter().enumerate() {
        store.enqueue(&cmd(id, "AddItem"), i as i64).unwrap();
        store
            .commit(id, i as u64, &[ev("ItemAdded")], &sig(), None, 0)
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
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

/// One writer at a time: a second kernel on the same SQLite file would run queued commands
/// twice. Readers (`kernable verify`) open it unlocked.
#[test]
fn sqlite_admits_one_writer() {
    let db = TempDb::new("sqlite-one-writer.db");
    let writer = SqliteStore::open(&db.0).unwrap();
    let second = SqliteStore::open(&db.0)
        .err()
        .expect("a second writer is refused");
    assert!(second.to_string().contains("in use"), "{second}");
    let reader = SqliteStore::open_unlocked(&db.0).unwrap();
    writer.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    assert_eq!(reader.pending().unwrap().len(), 1);
    drop(writer);
    SqliteStore::open(&db.0).expect("free again once the writer closed it");
}

/// Commits only hand the WAL to the OS; `Full` flushes it in `sync` (group commit) and makes
/// SQLite's own flushes real ones on macOS.
#[test]
fn sqlite_durability_sets_how_commits_sync() {
    use kernable_store::Durability;
    for (durability, fullfsync) in [(Durability::Full, 1), (Durability::Normal, 0)] {
        let db = TempDb::new(&format!("sqlite-durability-{fullfsync}.db"));
        let store = SqliteStore::open_with(&db.0, durability).unwrap();
        assert_eq!(store.pragma("synchronous").unwrap(), 1, "{durability:?}");
        assert_eq!(
            store.pragma("fullfsync").unwrap(),
            fullfsync,
            "{durability:?}"
        );
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
        store.sync().unwrap();
        store.sync().unwrap();
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
                // Unlocked: a second writer is refused (see below), so this tests the version
                // checks of SQLite itself.
                let store = SqliteStore::open_unlocked(path.as_path()).unwrap();
                for i in (t..THREADS * COMMANDS).step_by(THREADS) {
                    loop {
                        let current = store
                            .read_stream("t1", "orders", "Order", "o1")
                            .unwrap()
                            .len() as u64;
                        match store.commit(
                            &format!("c{i}"),
                            current,
                            &[ev("ItemAdded")],
                            &sig(),
                            None,
                            0,
                        ) {
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
                        match store.commit(
                            &format!("c{i}"),
                            current,
                            &[ev("ItemAdded")],
                            &sig(),
                            None,
                            0,
                        ) {
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

fn contexts_come_back_as_stored(store: &dyn Store) {
    let full = RequestContext {
        sender_tenant: "t1".into(),
        sender_identity: "i-1".into(),
        sender_account: "a-1".into(),
        sender_session: "s-1".into(),
        on_behalf_of: "i-2".into(),
        expected_version: Some(0),
        correlation_id: "r-1".into(),
        causation_id: "c-0".into(),
        trace_id: "tr-1".into(),
        bypass_authorization: true,
        attributes: vec![
            Attribute {
                key: "channel".into(),
                value: "web \"quoted\" ü".into(),
            },
            Attribute {
                key: "channel".into(),
                value: "app".into(),
            },
        ],
    };
    for (id, context) in [
        ("c1", Some(full)),
        ("c2", Some(RequestContext::default())),
        ("c3", None),
    ] {
        let command = Command {
            context: context.clone(),
            aggregate_id: id.into(),
            ..cmd(id, "PlaceOrder")
        };
        store.enqueue(&command, 0).unwrap();
        let pending = store.pending().unwrap();
        let stored = pending.iter().find(|p| p.command.command_id == id).unwrap();
        assert_eq!(stored.command.context, context, "{id}");
        let events = store
            .commit(id, 0, &[ev("OrderPlaced")], &sig(), None, 1)
            .unwrap();
        assert_eq!(events[0].context, context, "{id}");
        let read = store.read_stream("t1", "orders", "Order", id).unwrap();
        assert_eq!(read[0].context, context, "{id}");
    }
}

/// The overviews the UI and `kernable schemas` show, which the file backend derives from the
/// events and SQLite queries for.
fn overviews_of_the_stored_events(store: &dyn Store) {
    assert_eq!(store.streams(10).unwrap(), []);
    assert_eq!(store.event_sources().unwrap(), []);
    assert_eq!(store.event_schemas().unwrap(), []);
    let on = |id: &str, aggregate_type: &str, aggregate_id: &str| Command {
        aggregate_type: aggregate_type.into(),
        aggregate_id: aggregate_id.into(),
        ..cmd(id, "Do")
    };
    let schema = |event_type: &str, schema: u32| NewEvent {
        schema,
        ..ev(event_type)
    };
    store.enqueue(&on("c1", "Order", "o1"), 0).unwrap();
    store
        .commit(
            "c1",
            0,
            &[schema("OrderPlaced", 2), ev("ItemAdded")],
            &sig(),
            None,
            10,
        )
        .unwrap();
    store.enqueue(&on("c2", "Invoice", "i1"), 0).unwrap();
    store
        .commit("c2", 0, &[schema("InvoiceSent", 3)], &sig(), None, 20)
        .unwrap();
    store.enqueue(&on("c3", "Order", "o1"), 0).unwrap();
    store
        .commit("c3", 2, &[schema("OrderPlaced", 1)], &sig(), None, 30)
        .unwrap();

    let stream = |aggregate_type: &str, aggregate_id: &str| kernable_store::StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: aggregate_type.into(),
        aggregate_id: aggregate_id.into(),
    };
    let summary = |aggregate_type: &str, aggregate_id: &str, version, last_at| {
        kernable_store::StreamSummary {
            stream: stream(aggregate_type, aggregate_id),
            version,
            last_at,
        }
    };
    assert_eq!(
        store.streams(10).unwrap(),
        [
            summary("Order", "o1", 3, 30),
            summary("Invoice", "i1", 1, 20)
        ],
        "the most recently changed first"
    );
    assert_eq!(store.streams(1).unwrap(), [summary("Order", "o1", 3, 30)]);
    let source = |aggregate_type: &str, event_type: &str| {
        (
            "orders".to_owned(),
            aggregate_type.to_owned(),
            event_type.to_owned(),
        )
    };
    assert_eq!(
        store.event_sources().unwrap(),
        [
            source("Invoice", "InvoiceSent"),
            source("Order", "ItemAdded"),
            source("Order", "OrderPlaced"),
        ]
    );
    assert_eq!(
        store.event_schemas().unwrap(),
        [
            ("orders.InvoiceSent".to_owned(), 3),
            ("orders.ItemAdded".to_owned(), 1),
            ("orders.OrderPlaced".to_owned(), 2),
        ],
        "the highest schema, not the latest"
    );
}

fn revocation(key_id: &str, since: i64) -> RevocationRecord {
    RevocationRecord {
        key_id: key_id.into(),
        since,
        created_at: since,
        kernel_sig: Some(sig()),
    }
}

fn a_key_is_revoked_once_as_it_was_first(store: &dyn Store) {
    store.revoke(&revocation("a", 1)).unwrap();
    store.revoke(&revocation("b", 2)).unwrap();
    store.revoke(&revocation("a", 5)).unwrap();
    assert_eq!(
        store.revocations().unwrap(),
        [revocation("a", 1), revocation("b", 2)]
    );
}

fn index(key: u8, facts: &[&str]) -> AggregateIndex {
    AggregateIndex {
        keys: vec![StoredKey {
            name: "number".into(),
            global: false,
            id: vec![key; 32],
            vault_ref: String::new(),
        }],
        facts: facts.iter().map(|f| f.to_string()).collect(),
    }
}

fn o1() -> StreamId {
    StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: "o1".into(),
    }
}

fn an_index_is_replaced_with_its_commit_and_dropped_when_empty(store: &dyn Store) {
    assert!(store.index().unwrap().is_empty());
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit(
            "c1",
            0,
            &[ev("OrderPlaced")],
            &sig(),
            Some(&index(1, &["open"])),
            1,
        )
        .unwrap();
    assert_eq!(store.index().unwrap(), [(o1(), index(1, &["open"]))]);

    // None leaves it as it is; a commit without events replaces it all the same.
    store.enqueue(&cmd("c2", "AddItem"), 2).unwrap();
    store
        .commit("c2", 1, &[ev("ItemAdded")], &sig(), None, 2)
        .unwrap();
    store.enqueue(&cmd("c3", "Reindex"), 3).unwrap();
    store
        .commit("c3", 2, &[], &sig(), Some(&index(2, &[])), 3)
        .unwrap();
    assert_eq!(store.index().unwrap(), [(o1(), index(2, &[]))]);

    // A commit that conflicts changes nothing.
    store.enqueue(&cmd("c4", "AddItem"), 4).unwrap();
    let conflict = store.commit("c4", 0, &[ev("ItemAdded")], &sig(), Some(&index(3, &[])), 4);
    assert!(matches!(conflict, Err(Error::VersionConflict { .. })));
    assert_eq!(store.index().unwrap(), [(o1(), index(2, &[]))]);

    store
        .commit(
            "c4",
            2,
            &[ev("ItemAdded")],
            &sig(),
            Some(&AggregateIndex::default()),
            5,
        )
        .unwrap();
    assert!(store.index().unwrap().is_empty(), "holds nothing");
}

/// The kernel reads the index per stream and per key as commits need it.
fn keys_are_found_by_identity_and_freed_with_their_stream(store: &dyn Store) {
    let o2 = || Command {
        aggregate_id: "o2".into(),
        ..cmd("c2", "PlaceOrder")
    };
    assert_eq!(store.key_holder(&[1; 32]).unwrap(), None);
    assert_eq!(store.stream_index(&o1()).unwrap(), None);
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit(
            "c1",
            0,
            &[ev("OrderPlaced")],
            &sig(),
            Some(&index(1, &["open"])),
            1,
        )
        .unwrap();
    assert_eq!(store.key_holder(&[1; 32]).unwrap(), Some(o1()));
    assert_eq!(
        store.stream_index(&o1()).unwrap(),
        Some(index(1, &["open"]))
    );

    // Another stream filing the same key writes nothing.
    store.enqueue(&o2(), 2).unwrap();
    let taken = store.commit(
        "c2",
        0,
        &[ev("OrderPlaced")],
        &sig(),
        Some(&index(1, &[])),
        2,
    );
    assert!(matches!(taken, Err(Error::KeyTaken)), "{taken:?}");
    assert_eq!(store.head_position().unwrap(), 1);

    // A new value frees the old one, which another stream may then take.
    store.enqueue(&cmd("c3", "Renumber"), 3).unwrap();
    store
        .commit(
            "c3",
            1,
            &[ev("Renumbered")],
            &sig(),
            Some(&index(2, &["open"])),
            3,
        )
        .unwrap();
    assert_eq!(store.key_holder(&[1; 32]).unwrap(), None);
    assert_eq!(store.key_holder(&[2; 32]).unwrap(), Some(o1()));
    store
        .commit(
            "c2",
            0,
            &[ev("OrderPlaced")],
            &sig(),
            Some(&index(1, &[])),
            4,
        )
        .unwrap();
    let o2_stream = StreamId {
        aggregate_id: "o2".into(),
        ..o1()
    };
    assert_eq!(store.key_holder(&[1; 32]).unwrap(), Some(o2_stream));

    // A personal key is filed by its vault value only; the store just knows there is one.
    assert!(!store.holds_personal_keys().unwrap());
    let personal = AggregateIndex {
        keys: vec![StoredKey {
            name: "email".into(),
            global: false,
            id: Vec::new(),
            vault_ref: "ref-1".into(),
        }],
        facts: Vec::new(),
    };
    store.enqueue(&cmd("c4", "SetEmail"), 5).unwrap();
    store
        .commit("c4", 2, &[ev("EmailSet")], &sig(), Some(&personal), 5)
        .unwrap();
    assert!(store.holds_personal_keys().unwrap());
    assert_eq!(
        store.key_holder(&[2; 32]).unwrap(),
        None,
        "o1 dropped its number"
    );
}

#[test]
fn an_index_survives_reopening_the_store() {
    for backend in ["sqlite", "file"] {
        let db = TempDb::new(&format!("index-reopened.{backend}"));
        let open = || -> Box<dyn Store> {
            match backend {
                "sqlite" => Box::new(SqliteStore::open(&db.0).unwrap()),
                _ => Box::new(FileStore::open(&db.0).unwrap()),
            }
        };
        let store = open();
        store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
        store
            .commit(
                "c1",
                0,
                &[ev("OrderPlaced")],
                &sig(),
                Some(&index(1, &["open"])),
                1,
            )
            .unwrap();
        store.enqueue(&cmd("c2", "Reindex"), 2).unwrap();
        store
            .commit("c2", 1, &[], &sig(), Some(&index(2, &["open"])), 2)
            .unwrap();
        // Read back over the readers of the open store, and after reopening it.
        let check = |store: &dyn Store| {
            assert_eq!(store.key_holder(&[2; 32]).unwrap(), Some(o1()), "{backend}");
            assert_eq!(store.key_holder(&[1; 32]).unwrap(), None, "{backend}");
            assert_eq!(
                store.stream_index(&o1()).unwrap(),
                Some(index(2, &["open"])),
                "{backend}"
            );
        };
        check(&*store);
        drop(store);
        check(&*open());
        assert_eq!(
            open().index().unwrap(),
            [(o1(), index(2, &["open"]))],
            "{backend}"
        );
    }
}

fn file_store(name: &str) -> (TempDb, Box<dyn Store>) {
    let db = TempDb::new(&format!("{name}.log"));
    let store = FileStore::open(&db.0).unwrap();
    (db, Box::new(store))
}

/// Events keep the deployment that wrote them, in their hash too; the store answers what each
/// deployment wrote and which wrote a type first.
fn events_keep_their_deployment(store: &dyn Store) {
    let deployed = |event_type: &str, deployment: &str| NewEvent {
        deployment: deployment.into(),
        ..ev(event_type)
    };
    store.enqueue(&cmd("c1", "PlaceOrder"), 0).unwrap();
    store
        .commit(
            "c1",
            0,
            &[deployed("OrderPlaced", "v1"), deployed("ItemAdded", "")],
            &sig(),
            None,
            1,
        )
        .unwrap();
    store.enqueue(&cmd("c2", "AddItem"), 0).unwrap();
    store
        .commit(
            "c2",
            2,
            &[deployed("ItemAdded", "v2"), deployed("ItemRemoved", "v2")],
            &sig(),
            None,
            2,
        )
        .unwrap();
    let all = store.read_all(0, 10).unwrap();
    let deployments: Vec<&str> = all.iter().map(|e| e.deployment.as_str()).collect();
    assert_eq!(deployments, ["v1", "", "v2", "v2"]);
    let mut prev = vec![0u8; 32];
    for e in &all {
        assert_eq!(
            kernable_core::signing::event_hash(&prev, e).to_vec(),
            e.hash
        );
        prev = e.hash.clone();
    }
    assert_eq!(
        store.deployment_written("v2").unwrap(),
        (
            Some(3),
            vec!["orders.ItemAdded".into(), "orders.ItemRemoved".into()]
        )
    );
    assert_eq!(store.deployment_written("v3").unwrap(), (None, Vec::new()));
    assert_eq!(
        store.first_of_type("orders", "ItemAdded").unwrap(),
        Some((2, String::new()))
    );
    assert_eq!(
        store.first_of_type("orders", "ItemRemoved").unwrap(),
        Some((4, "v2".into()))
    );
    assert_eq!(store.first_of_type("orders", "Nothing").unwrap(), None);
    let membership = Membership {
        service: "shop".into(),
        deployment: "v2".into(),
        ..Default::default()
    };
    store
        .save_membership(&MembershipRecord {
            membership: Some(membership.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(store.memberships().unwrap()[0].membership, Some(membership));
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
    events_keep_their_deployment,
    stale_version_conflicts_and_command_stays_pending,
    a_command_completes_at_most_once,
    pending_lists_open_commands_in_submission_order,
    delivery_times_come_back_as_stored,
    enqueue_is_idempotent,
    reject_completes_command_without_events,
    rejections_list_the_newest_first_with_why,
    commands_list_the_newest_first_with_their_results,
    concurrent_threads_lose_no_updates,
    listeners_are_saved_replaced_and_removed,
    parked_events_change_only_with_their_flag,
    workers_are_saved_replaced_and_removed,
    memberships_are_replaced_per_service,
    process_versions_and_states_are_kept,
    the_engine_position_and_a_domain_index_are_kept,
    a_copy_mark_is_kept_and_carried_by_an_image,
    the_newest_grant_per_key_is_kept,
    reading_after_a_version_returns_only_later_events,
    contexts_come_back_as_stored,
    overviews_of_the_stored_events,
    a_key_is_revoked_once_as_it_was_first,
    an_index_is_replaced_with_its_commit_and_dropped_when_empty,
    keys_are_found_by_identity_and_freed_with_their_stream,
);

#[test]
fn inspecting_a_store_that_is_not_there_fails_and_creates_nothing() {
    let path = std::env::temp_dir().join(format!("kernable-missing-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    for url in [
        format!("sqlite:{}", path.display()),
        path.display().to_string(),
    ] {
        assert!(kernable_store::open_read_only(&url).is_err(), "{url}");
        assert!(!path.exists(), "{url} created a store");
    }
}
