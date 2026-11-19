//! Property tests: random and damaged input never crashes a store or reads back wrong data,
//! and every backend behaves the same for the same operations.
//! `PROPTEST_CASES=<n>` sets the number of cases (default 64; `make torture` runs many more).

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{Command, NewEvent, Rejected, Signature},
    signing,
};
use kernable_store::{Error, SqliteStore, Store};
use proptest::prelude::*;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(64);
    ProptestConfig::with_cases(cases)
}

fn command(id: usize, aggregate: usize) -> Command {
    Command {
        command_id: format!("c{id}"),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: format!("o{aggregate}"),
        command_type: "Do".into(),
        payload: format!(r#"{{"n":{id}}}"#).into_bytes(),
        ..Default::default()
    }
}

fn events(n: usize) -> Vec<NewEvent> {
    (0..n)
        .map(|i| NewEvent {
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: format!("Done{i}"),
            payload: format!(r#"{{"i":{i}}}"#).into_bytes(),
            schema: 1,
            origin_partition: String::new(),
            origin_position: 0,
            deployment: Default::default(),
        })
        .collect()
}

#[derive(Debug, Clone)]
enum Op {
    Enqueue {
        id: usize,
        aggregate: usize,
    },
    Commit {
        id: usize,
        expected: u64,
        events: usize,
    },
    Reject {
        id: usize,
    },
    Checkpoint,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..8usize, 0..3usize).prop_map(|(id, aggregate)| Op::Enqueue { id, aggregate }),
        (0..8usize, 0..4u64, 0..3usize).prop_map(|(id, expected, events)| Op::Commit {
            id,
            expected,
            events
        }),
        (0..8usize).prop_map(|id| Op::Reject { id }),
        Just(Op::Checkpoint),
    ]
}

/// What a store answered, in a form every backend must agree on.
fn outcome<T: std::fmt::Debug>(result: Result<T, Error>) -> String {
    match result {
        Ok(value) => format!("ok {value:?}"),
        Err(Error::VersionConflict { expected, actual }) => format!("conflict {expected} {actual}"),
        Err(Error::NotPending) => "not pending".into(),
        Err(e) => format!("error {e}"),
    }
}

fn apply(store: &dyn Store, ops: &[Op]) -> Vec<String> {
    let kernel = SigningKey::from_bytes(&[3; 32]);
    let sig = Signature {
        key_id: "w".into(),
        sig: vec![1, 2, 3],
    };
    let mut log: Vec<String> = ops
        .iter()
        .enumerate()
        .map(|(t, op)| match op {
            Op::Enqueue { id, aggregate } => {
                outcome(store.enqueue(&command(*id, *aggregate), t as i64))
            }
            Op::Commit {
                id,
                expected,
                events: n,
            } => outcome(store.commit(
                &format!("c{id}"),
                *expected,
                &events(*n),
                &sig,
                None,
                t as i64,
            )),
            Op::Reject { id } => outcome(store.reject(&format!("c{id}"), Rejected::default())),
            Op::Checkpoint => outcome(store.checkpoint(&kernel, t as i64)),
        })
        .collect();
    log.push(format!("{:?}", store.read_all(0, 1000).unwrap()));
    log.push(format!("{:?}", store.pending().unwrap()));
    log.extend((0..8).map(|id| format!("{:?}", store.result(&format!("c{id}")).unwrap())));
    log.push(format!(
        "{:?}",
        store
            .verify(&[signing::key_id(&kernel.verifying_key()).to_string()])
            .unwrap()
            .problems
    ));
    log
}

proptest! {
    #![proptest_config(config())]

    /// SQLite on disk (with its readers' connections) and in memory (`memory:`) give the same
    /// answers to the same operations.
    #[test]
    fn on_disk_and_in_memory_behave_the_same(ops in proptest::collection::vec(op(), 1..40)) {
        let memory = apply(&SqliteStore::open_in_memory().unwrap(), &ops);
        let path = std::env::temp_dir().join(format!("kernable-props-diff-{}.db", std::process::id()));
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        let disk = apply(&SqliteStore::open(&path).unwrap(), &ops);
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        prop_assert_eq!(&memory, &disk);
    }
}
