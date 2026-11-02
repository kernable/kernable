//! The invariant checks: silent on a healthy store of every backend, and each one fires on the
//! damage it is there to catch.

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        AggregateIndex, Batch, Command, CommandResult, Committed, ListenerRecord, NewEvent,
        Rejected, StoredKey, command_result::Outcome,
    },
    signing,
};
use kernable_store::{
    FileStore, SqliteStore, Store, StreamId,
    invariants::{self, Log, Observed, Violation},
};

fn ev(event_type: &str, schema: u32) -> NewEvent {
    NewEvent {
        event_type: event_type.into(),
        payload: b"{}".to_vec(),
        schema,
        ..NewEvent::default()
    }
}

fn cmd(id: &str, aggregate_id: &str) -> Command {
    let mut cmd = Command {
        command_id: id.into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: aggregate_id.into(),
        command_type: "Do".into(),
        payload: b"{}".to_vec(),
        ..Command::default()
    };
    let client = SigningKey::from_bytes(&[1; 32]);
    cmd.client_sig = Some(signing::sign(&client, &signing::command_message(&cmd)));
    cmd
}

fn kernel() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn kernel_ids() -> Vec<String> {
    vec![signing::key_id(&kernel().verifying_key()).to_string()]
}

fn run(store: &dyn Store, id: &str, aggregate_id: &str, events: &[NewEvent]) {
    run_indexed(store, id, aggregate_id, events, None);
}

fn run_indexed(
    store: &dyn Store,
    id: &str,
    aggregate_id: &str,
    events: &[NewEvent],
    index: Option<&AggregateIndex>,
) {
    let cmd = cmd(id, aggregate_id);
    store.enqueue(&cmd, 0).unwrap();
    let version = store
        .read_stream("t1", "orders", "Order", aggregate_id)
        .unwrap()
        .len() as u64;
    let worker = SigningKey::from_bytes(&[2; 32]);
    let sig = signing::sign(&worker, &signing::result_message(&cmd, version, events));
    store.commit(id, version, events, &sig, index, 0).unwrap();
}

/// A non-personal key `number` of the ID `[n; 32]`, and the fact `open`.
fn index(n: u8) -> AggregateIndex {
    AggregateIndex {
        keys: vec![StoredKey {
            name: "number".into(),
            global: false,
            id: vec![n; 32],
            vault_ref: String::new(),
        }],
        facts: vec!["open".into()],
    }
}

fn stream(aggregate_id: &str) -> StreamId {
    StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: aggregate_id.into(),
    }
}

/// | pos | command | aggregate | event                   |
/// |-----|---------|-----------|-------------------------|
/// | 1   | c1      | o1 v1     | OrderPlaced             |
/// | 2   | c1      | o1 v2     | ItemAdded               |
/// | 3   | c2      | o2 v1     | OrderPlaced             |
/// | 4   | c4      | o1 v3     | ItemAdded (schema 2)    |
///
/// c3 is rejected, c5 pending, listener `audit` at 3; o1 holds the key [`index`]`(1)`.
fn workload(store: &dyn Store) {
    run(
        store,
        "c1",
        "o1",
        &[ev("OrderPlaced", 1), ev("ItemAdded", 1)],
    );
    run(store, "c2", "o2", &[ev("OrderPlaced", 1)]);
    store.enqueue(&cmd("c3", "o2"), 0).unwrap();
    let rejected = Rejected {
        code: "invalid_state".into(),
        message: "no".into(),
        ..Default::default()
    };
    store.reject("c3", rejected).unwrap();
    run_indexed(store, "c4", "o1", &[ev("ItemAdded", 2)], Some(&index(1)));
    store.enqueue(&cmd("c5", "o3"), 0).unwrap();
    store
        .save_listeners(&[ListenerRecord {
            name: "audit".into(),
            position: 3,
            ..ListenerRecord::default()
        }])
        .unwrap();
    store.checkpoint(&kernel(), 1).unwrap();
}

fn healthy() -> Log {
    let store = FileStore::in_memory();
    workload(&store);
    Log::read(&store, &kernel_ids()).unwrap()
}

/// Asserts that `violations` hold exactly one, of `invariant`.
fn fires(violations: Vec<Violation>, invariant: &str) {
    assert!(
        violations.len() == 1 && violations[0].invariant == invariant,
        "expected one {invariant}, got {violations:#?}"
    );
}

#[test]
fn a_healthy_store_holds_every_invariant() {
    let file = FileStore::in_memory();
    workload(&file);
    let path = std::env::temp_dir().join(format!("kernable-invariants-{}.db", std::process::id()));
    let sqlite = SqliteStore::open(&path).unwrap();
    workload(&sqlite);
    for store in [&file as &dyn Store, &sqlite] {
        let log = Log::read(store, &kernel_ids()).unwrap();
        assert_eq!(log.events.len(), 4);
        assert_eq!(log.pending.len(), 1);
        assert_eq!(
            store.command_ids().unwrap(),
            ["c1", "c2", "c3", "c4", "c5"],
            "every command, the rejected one too, in the order stored"
        );
        assert_eq!(log.commands.len(), 5);
        assert_eq!(log.index, [(stream("o1"), index(1))]);
        assert_eq!(log.check(), []);
    }
    drop(sqlite);
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn s1_integrity_reports_what_verify_finds() {
    let mut log = healthy();
    log.integrity
        .push("event 2: hash mismatch (modified)".into());
    fires(log.check(), invariants::S1);
}

#[test]
fn s1_fires_on_a_store_whose_event_was_changed() {
    let path =
        std::env::temp_dir().join(format!("kernable-invariants-s1-{}.db", std::process::id()));
    let store = SqliteStore::open(&path).unwrap();
    workload(&store);
    drop(store);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE events SET payload = '{\"x\":1}' WHERE global_position = 2",
        [],
    )
    .unwrap();
    drop(conn);
    let store = SqliteStore::open(&path).unwrap();
    // The checkpoint over the changed event no longer matches either: S1 at least once.
    let violations = Log::read(&store, &kernel_ids()).unwrap().check();
    assert!(
        violations.iter().any(|v| v.invariant == invariants::S1),
        "{violations:#?}"
    );
    drop(store);
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn s2_positions_have_no_gaps_and_match_the_head() {
    let mut log = healthy();
    log.events.remove(2);
    log.events.truncate(2);
    let mut v = log.check();
    v.retain(|v| v.invariant == invariants::S2);
    assert_eq!(v.len(), 1, "{v:#?}");

    let mut log = healthy();
    log.head = 9;
    fires(log.check(), invariants::S2);
}

#[test]
fn s3_stream_versions_have_no_gaps() {
    let mut log = healthy();
    log.events[3].version = 4;
    fires(log.check(), invariants::S3);
}

#[test]
fn s4_a_command_is_one_run_of_events() {
    let mut log = healthy();
    // c1's second event moves behind c2's, keeping positions and versions intact.
    log.events.swap(1, 2);
    for (i, e) in log.events.iter_mut().enumerate() {
        e.global_position = i as u64 + 1;
    }
    let v = log.check();
    assert!(v.iter().any(|v| v.invariant == invariants::S4), "{v:#?}");
}

#[test]
fn s5_events_carry_their_command_s_stream_and_context() {
    let mut log = healthy();
    log.commands
        .get_mut("c2")
        .unwrap()
        .as_mut()
        .unwrap()
        .tenant_id = "t2".into();
    fires(log.check(), invariants::S5);
}

#[test]
fn s6_a_command_has_one_outcome() {
    let mut log = healthy();
    log.pending.insert("c2".into());
    let v = log.check();
    assert!(
        v.len() == 2 && v.iter().all(|v| v.invariant == invariants::S6),
        "{v:#?}"
    );

    let mut log = healthy();
    log.results.insert("c2".into(), None);
    fires(log.check(), invariants::S6);

    let mut log = healthy();
    log.commands.insert("c2".into(), None);
    fires(log.check(), invariants::S6);
}

#[test]
fn s7_schemas_never_go_down() {
    let mut log = healthy();
    log.events[1].schema = 3; // ItemAdded at 3, then at 2
    fires(log.check(), invariants::S7);
}

#[test]
fn s8_listeners_are_not_beyond_the_head() {
    let mut log = healthy();
    log.listeners[0].position = 5;
    fires(log.check(), invariants::S8);
}

#[test]
fn s9_a_key_is_held_once_and_only_by_streams_with_events() {
    let mut log = healthy();
    log.index.push((stream("o2"), index(1)));
    fires(log.check(), invariants::S9);

    // A personal key: its ID is in the vault, so another stream's equal one is not seen.
    let mut log = healthy();
    let mut personal = index(1);
    personal.keys[0].id.clear();
    personal.keys[0].vault_ref = "ab".into();
    log.index.push((stream("o2"), personal.clone()));
    assert_eq!(log.check(), []);

    let mut log = healthy();
    personal.keys[0].vault_ref.clear();
    log.index.push((stream("o2"), personal));
    fires(log.check(), invariants::S9);

    // A reindex of an ID never written holds nothing.
    let mut log = healthy();
    log.index.push((stream("o3"), index(2)));
    fires(log.check(), invariants::S9);
}

fn committed(log: &Log, id: &str, positions: &[u64]) -> CommandResult {
    CommandResult {
        command_id: id.into(),
        outcome: Some(Outcome::Committed(Committed {
            events: positions
                .iter()
                .map(|p| log.event(*p).unwrap().clone())
                .collect(),
            settled: true,
        })),
    }
}

#[test]
fn o1_clients_are_told_what_the_store_holds() {
    let log = healthy();
    let mut seen = Observed::default();
    seen.outcome(&committed(&log, "c1", &[1, 2]));
    seen.outcome(&committed(&log, "c1", &[1, 2]));
    seen.outcome(&log.results["c3"].clone().unwrap());
    assert_eq!(seen.check(&log), []);

    seen.outcome(&committed(&log, "c1", &[1]));
    fires(seen.check(&log), invariants::O1);

    let mut seen = Observed::default();
    seen.outcome(&CommandResult {
        command_id: "c5".into(),
        outcome: Some(Outcome::Rejected(Rejected::default())),
    });
    fires(seen.check(&log), invariants::O1);
}

fn batch(log: &Log, positions: &[u64], up_to: u64) -> Batch {
    Batch {
        events: positions
            .iter()
            .map(|p| log.event(*p).unwrap().clone())
            .collect(),
        up_to,
        vault: Vec::new(),
    }
}

#[test]
fn o2_to_o4_listeners_get_what_they_acknowledge_in_order() {
    let log = healthy();
    let placed = ["orders.OrderPlaced".to_string()];

    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1, 3], 3));
    seen.acked("audit", 3);
    assert_eq!(seen.check(&log), []);

    // O2: an acknowledgement covers an event that never came.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1], 3));
    seen.acked("audit", 3);
    fires(seen.check(&log), invariants::O2);

    // O2: the stored position is ahead of every acknowledgement.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1], 1));
    seen.acked("audit", 1);
    fires(seen.check(&log), invariants::O2);

    // O3: an event twice on one connection.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1, 1, 3], 3));
    seen.acked("audit", 3);
    fires(seen.check(&log), invariants::O3);

    // O4: an event the listener did not ask for, and one that was changed.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1, 2, 3], 3));
    seen.acked("audit", 3);
    fires(seen.check(&log), invariants::O4);

    let mut changed = batch(&log, &[1, 3], 3);
    changed.events[0].payload = b"{\"x\":1}".to_vec();
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &changed);
    seen.acked("audit", 3);
    fires(seen.check(&log), invariants::O4);

    // O2: a reconnect resumes beyond what was acknowledged.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1], 1));
    seen.acked("audit", 1);
    seen.connected("audit", &placed, 3);
    fires(seen.check(&log), invariants::O2);

    // A reconnect resumes after its stored position: a new connection, checked on its own.
    let mut seen = Observed::default();
    seen.connected("audit", &placed, 0);
    seen.delivered("audit", &batch(&log, &[1], 1));
    seen.acked("audit", 1);
    seen.connected("audit", &placed, 1);
    seen.delivered("audit", &batch(&log, &[3], 3));
    seen.acked("audit", 3);
    assert_eq!(seen.check(&log), []);
    assert_eq!(seen.delivered_positions("audit"), [1, 3].into());
}
