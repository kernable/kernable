//! Property tests: random and damaged input never crashes a store or reads back wrong data,
//! and every backend behaves the same for the same operations.
//! `PROPTEST_CASES=<n>` sets the number of cases (default 64; `make torture` runs many more).

use std::sync::OnceLock;

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{Command, Event, NewEvent, Rejected, Signature},
    signing,
};
use kernable_store::{Error, FileStore, SqliteStore, Store};
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
        })
        .collect()
}

/// The bytes of a small, valid file log with commits, a rejection, a pending command, a
/// listener, a snapshot-free history and checkpoints.
fn valid_log() -> &'static (Vec<u8>, Vec<Event>) {
    static LOG: OnceLock<(Vec<u8>, Vec<Event>)> = OnceLock::new();
    LOG.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("kernable-props-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let store = FileStore::open(&path).unwrap();
            let kernel = SigningKey::from_bytes(&[3; 32]);
            for i in 0..6 {
                let cmd = command(i, i % 2);
                store.enqueue(&cmd, 1).unwrap();
                let version = store
                    .read_stream("t1", "orders", "Order", &cmd.aggregate_id)
                    .unwrap()
                    .len() as u64;
                let new = events(1 + i % 2);
                let sig = signing::sign(&kernel, &signing::result_message(&cmd, version, &new));
                store
                    .commit(&cmd.command_id, version, &new, &sig, None, 2)
                    .unwrap();
                if i == 3 {
                    store.checkpoint(&kernel, 3).unwrap();
                }
            }
            store.enqueue(&command(6, 0), 4).unwrap();
            store.reject("c6", Rejected::default()).unwrap();
            store.enqueue(&command(7, 1), 5).unwrap();
            store.checkpoint(&kernel, 6).unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let events = FileStore::open_bytes(&bytes)
            .unwrap()
            .read_all(0, 100)
            .unwrap();
        (bytes, events)
    })
}

proptest! {
    #![proptest_config(config())]

    /// Any bytes: an error or a store, never a panic.
    #[test]
    fn random_bytes_never_crash_the_file_log(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(store) = FileStore::open_bytes(&bytes) {
            let _ = store.read_all(0, 1000);
            let _ = store.verify(&["00".to_string()]);
        }
    }

    /// A damaged log is refused or read as a prefix of what was written; it is never misread.
    #[test]
    fn a_damaged_log_is_refused_or_read_as_a_prefix(
        at in any::<proptest::sample::Index>(),
        flip in 1u8..=255,
        cut in any::<bool>(),
    ) {
        let (log, written) = valid_log();
        let at = at.index(log.len());
        let mut bytes = log.clone();
        if cut {
            bytes.truncate(at);
        } else {
            bytes[at] ^= flip;
        }
        match FileStore::open_bytes(&bytes) {
            Err(Error::Corrupt(_) | Error::Format(_) | Error::Decode(_)) => {}
            Err(e) => prop_assert!(false, "unexpected error {e}"),
            Ok(store) => {
                let read = store.read_all(0, 1000).unwrap();
                prop_assert!(read.len() <= written.len());
                prop_assert_eq!(&read[..], &written[..read.len()]);
            }
        }
    }
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

    /// SQLite, the file log and memory give the same answers to the same operations.
    #[test]
    fn every_backend_behaves_the_same(ops in proptest::collection::vec(op(), 1..40)) {
        let sqlite = apply(&SqliteStore::open_in_memory().unwrap(), &ops);
        let memory = apply(&FileStore::in_memory(), &ops);
        prop_assert_eq!(&sqlite, &memory);
        let path = std::env::temp_dir().join(format!("kernable-props-diff-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let file = apply(&FileStore::open(&path).unwrap(), &ops);
        let _ = std::fs::remove_file(&path);
        prop_assert_eq!(&sqlite, &file);
    }
}
