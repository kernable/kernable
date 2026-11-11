//! File backend specifics: replay on open, crash recovery, corruption, tampering, locking.

use std::{fs, io::Write, path::PathBuf};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Command, ListenerRecord, LogRecord, NewEvent, command_result::Outcome, log_record::Record,
    },
    signing,
};
use kernable_store::{Enqueued, Error, FileStore, Store};
use prost::Message;

struct TempLog(PathBuf);

impl TempLog {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("kernable-file-{name}-{}.log", std::process::id()));
        let _ = fs::remove_file(&path);
        TempLog(path)
    }

    fn torn_files(&self) -> Vec<PathBuf> {
        let dir = self.0.parent().unwrap();
        let prefix = format!("{}.torn-", self.0.file_name().unwrap().to_string_lossy());
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(&prefix)
            })
            .collect()
    }
}

impl Drop for TempLog {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        for p in self.torn_files() {
            let _ = fs::remove_file(p);
        }
    }
}

fn kernel() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

/// Signed command `id` on aggregate `agg`, committed with one event by a signing worker.
fn run(store: &dyn Store, id: &str, agg: &str) {
    let client = SigningKey::from_bytes(&[1; 32]);
    let mut cmd = Command {
        deliver_at: 0,
        deliver_until: 0,
        vault_refs: Vec::new(),
        command_id: id.into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: agg.into(),
        command_type: "Do".into(),
        payload: format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        client_sig: None,
        context: None,
    };
    cmd.client_sig = Some(signing::sign(&client, &signing::command_message(&cmd)));
    store.enqueue(&cmd, 0).unwrap();
    let version = store
        .read_stream("t1", "orders", "Order", agg)
        .unwrap()
        .len() as u64;
    let events = [NewEvent {
        erases: String::new(),
        vault_refs: Vec::new(),
        event_type: "Done".into(),
        payload: cmd.payload.clone(),
        schema: 1,
        origin_partition: String::new(),
        origin_position: 0,
        deployment: Default::default(),
    }];
    let worker = SigningKey::from_bytes(&[2; 32]);
    let sig = signing::sign(&worker, &signing::result_message(&cmd, version, &events));
    store
        .commit(&cmd.command_id, version, &events, &sig, None, 0)
        .unwrap();
}

fn kernel_id() -> String {
    signing::key_id(&kernel().verifying_key())
}

#[test]
fn reopening_replays_events_results_queue_checkpoints_and_listeners() {
    let log = TempLog::new("reopen");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
        run(&store, "c2", "o1");
        store.checkpoint(&kernel(), 1).unwrap().unwrap();
        let pending = Command {
            command_id: "c3".into(),
            command_type: "Do".into(),
            aggregate_id: "o2".into(),
            ..Default::default()
        };
        store.enqueue(&pending, 2).unwrap();
        let mut orders = ListenerRecord {
            name: "orders".into(),
            position: 1,
            ..Default::default()
        };
        store.save_listeners(&[orders.clone()]).unwrap();
        orders.position = 2;
        store.save_listeners(&[orders]).unwrap();
    }

    let store = FileStore::open(&log.0).unwrap();
    let versions: Vec<u64> = store
        .read_stream("t1", "orders", "Order", "o1")
        .unwrap()
        .iter()
        .map(|e| e.version)
        .collect();
    assert_eq!(versions, [1, 2]);
    assert!(matches!(
        store.enqueue(&Command { command_id: "c1".into(), ..Default::default() }, 3).unwrap(),
        Enqueued::Duplicate { result: Some(r) } if matches!(r.outcome, Some(Outcome::Committed(_)))
    ));
    let pending: Vec<String> = store
        .pending()
        .unwrap()
        .into_iter()
        .map(|p| p.command.command_id)
        .collect();
    assert_eq!(pending, ["c3"]);
    let listeners = store.listeners().unwrap();
    assert_eq!(
        (listeners[0].name.as_str(), listeners[0].position),
        ("orders", 2)
    );
    let report = store.verify(&[kernel_id().to_string()]).unwrap();
    // c3 is unsigned on purpose; everything else must check out.
    assert_eq!(
        report.problems,
        ["command c3: missing or invalid client signature"]
    );
    assert_eq!(
        (report.events, report.checkpoints, report.unanchored),
        (2, 1, 0)
    );
}

#[test]
fn checkpointing_an_unchanged_head_writes_nothing() {
    let log = TempLog::new("checkpoint");
    let store = FileStore::open(&log.0).unwrap();
    run(&store, "c1", "o1");
    let first = store.checkpoint(&kernel(), 1).unwrap().unwrap();
    let len = fs::metadata(&log.0).unwrap().len();
    assert_eq!(store.checkpoint(&kernel(), 2).unwrap().unwrap(), first);
    assert_eq!(fs::metadata(&log.0).unwrap().len(), len);
}

#[test]
fn torn_last_record_is_cut_off_and_kept_aside() {
    let log = TempLog::new("torn");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
    }
    let intact = fs::metadata(&log.0).unwrap().len();
    // A crash in the middle of a write: the header promises 100 bytes, 10 arrived.
    let mut f = fs::OpenOptions::new().append(true).open(&log.0).unwrap();
    f.write_all(&FileStore::frame(&[7; 100])[..12 + 10])
        .unwrap();
    drop(f);

    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(fs::metadata(&log.0).unwrap().len(), intact);
    assert_eq!(log.torn_files().len(), 1);
    run(&store, "c2", "o1");
    drop(store);
    let reopened = FileStore::open(&log.0).unwrap();
    assert_eq!(
        reopened
            .read_stream("t1", "orders", "Order", "o1")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn a_last_record_that_fails_its_checksum_is_cut_off_as_torn() {
    let log = TempLog::new("torn-checksum");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
    }
    let intact = fs::metadata(&log.0).unwrap().len();
    {
        // One record.
        let store = FileStore::open(&log.0).unwrap();
        store
            .revoke(&kernable_core::proto::RevocationRecord {
                key_id: "a".into(),
                ..Default::default()
            })
            .unwrap();
    }
    // The last record arrived whole in length, but not in content.
    let mut bytes = fs::read(&log.0).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&log.0, bytes).unwrap();

    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(fs::metadata(&log.0).unwrap().len(), intact);
    assert_eq!(log.torn_files().len(), 1);
    assert_eq!(store.revocations().unwrap(), []);
    assert_eq!(store.read_all(0, 10).unwrap().len(), 1);
}

#[test]
fn revocations_come_back_after_reopening() {
    let log = TempLog::new("revocations");
    let revocation = |key_id: &str, since| kernable_core::proto::RevocationRecord {
        key_id: key_id.into(),
        since,
        created_at: since,
        kernel_sig: None,
    };
    {
        let store = FileStore::open(&log.0).unwrap();
        store.revoke(&revocation("a", 1)).unwrap();
        store.revoke(&revocation("b", 2)).unwrap();
    }
    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(
        store.revocations().unwrap(),
        [revocation("a", 1), revocation("b", 2)]
    );
}

#[test]
fn damaged_record_in_the_middle_refuses_to_open() {
    let log = TempLog::new("damaged");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
        run(&store, "c2", "o1");
    }
    let mut bytes = fs::read(&log.0).unwrap();
    let second = 12 + len_at(&bytes, 0);
    bytes[second + 12] ^= 0xff; // inside the second record's body
    fs::write(&log.0, bytes).unwrap();
    assert!(matches!(FileStore::open(&log.0), Err(Error::Corrupt(_))));
    assert!(log.torn_files().is_empty());
}

/// The length of the record at byte `at`.
fn len_at(bytes: &[u8], at: usize) -> usize {
    u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
}

/// A damaged length in the middle of the log that points past its end is not a torn tail:
/// the records after it were committed, so the log refuses to open and keeps every byte.
#[test]
fn a_damaged_length_in_the_middle_refuses_to_open() {
    let log = TempLog::new("damaged-length");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
        run(&store, "c2", "o1");
    }
    let mut bytes = fs::read(&log.0).unwrap();
    let second = 12 + len_at(&bytes, 0);
    bytes[second..second + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    fs::write(&log.0, &bytes).unwrap();

    assert!(matches!(FileStore::open(&log.0), Err(Error::Corrupt(_))));
    assert!(matches!(
        FileStore::open_read_only(&log.0),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(fs::read(&log.0).unwrap(), bytes);
    assert!(log.torn_files().is_empty());
}

/// A crash can leave zeros where a write did not reach the disk: a header of zeros at the end
/// is a torn tail.
#[test]
fn a_zeroed_tail_is_cut_off_as_torn() {
    let log = TempLog::new("torn-zeros");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
    }
    let intact = fs::metadata(&log.0).unwrap().len();
    let mut f = fs::OpenOptions::new().append(true).open(&log.0).unwrap();
    f.write_all(&[0u8; 40]).unwrap();
    drop(f);

    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(fs::metadata(&log.0).unwrap().len(), intact);
    assert_eq!(log.torn_files().len(), 1);
    assert_eq!(store.read_all(0, 10).unwrap().len(), 1);
}

/// A log torn before its first record ended starts again, and keeps the torn bytes aside.
#[test]
fn a_torn_first_record_is_kept_aside() {
    let log = TempLog::new("torn-first");
    fs::write(&log.0, &FileStore::frame(&[7; 100])[..12 + 10]).unwrap();

    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(store.head_position().unwrap(), 0);
    let torn = log.torn_files();
    assert_eq!(torn.len(), 1);
    assert_eq!(fs::read(&torn[0]).unwrap().len(), 22);
    run(&store, "c1", "o1");
    drop(store);
    assert_eq!(
        FileStore::open(&log.0)
            .unwrap()
            .read_all(0, 10)
            .unwrap()
            .len(),
        1
    );
}

/// An attacker rewrites the log with valid framing but a changed event payload:
/// the file opens fine, and `verify` exposes the change.
#[test]
fn consistently_reframed_tampering_is_caught_by_verify() {
    let log = TempLog::new("tamper");
    {
        let store = FileStore::open(&log.0).unwrap();
        run(&store, "c1", "o1");
        run(&store, "c2", "o1");
        store.checkpoint(&kernel(), 1).unwrap();
    }
    let bytes = fs::read(&log.0).unwrap();
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let len = len_at(&bytes, at);
        let mut record = LogRecord::decode(&bytes[at + 12..at + 12 + len]).unwrap();
        if let Some(Record::Committed(c)) = &mut record.record
            && c.command_id == "c1"
        {
            c.events[0].event.as_mut().unwrap().payload = br#"{"id":"forged"}"#.to_vec();
        }
        out.extend(FileStore::frame(&record.encode_to_vec()));
        at += 12 + len;
    }
    fs::write(&log.0, out).unwrap();

    let report = FileStore::open_read_only(&log.0)
        .unwrap()
        .verify(&[kernel_id().to_string()])
        .unwrap();
    assert!(
        report
            .problems
            .iter()
            .any(|p| p == "event 1: hash mismatch (modified)"),
        "{:?}",
        report.problems
    );
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.starts_with("command c1: invalid worker signature"))
    );
}

#[test]
fn one_writer_but_readers_may_open_concurrently() {
    let log = TempLog::new("lock");
    let writer = FileStore::open(&log.0).unwrap();
    run(&writer, "c1", "o1");
    assert!(matches!(FileStore::open(&log.0), Err(Error::Io(_))));

    let reader = FileStore::open_read_only(&log.0).unwrap();
    assert_eq!(
        reader
            .read_stream("t1", "orders", "Order", "o1")
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        reader.enqueue(&Command::default(), 0),
        Err(Error::ReadOnly)
    ));
}
