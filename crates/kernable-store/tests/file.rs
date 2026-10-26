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
use sha2::{Digest, Sha256};

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
        command_id: id.into(),
        tenant_id: "t1".into(),
        aggregate_type: "Order".into(),
        aggregate_id: agg.into(),
        command_type: "Do".into(),
        payload: format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        client_sig: None,
    };
    cmd.client_sig = Some(signing::sign(&client, &signing::command_message(&cmd)));
    store.enqueue(&cmd, 0).unwrap();
    let version = store.read_stream("t1", "Order", agg).unwrap().len() as u64;
    let events = [NewEvent {
        event_type: "Done".into(),
        payload: cmd.payload.clone(),
    }];
    let worker = SigningKey::from_bytes(&[2; 32]);
    let sig = signing::sign(&worker, &signing::result_message(&cmd, version, &events));
    store
        .commit(&cmd.command_id, version, &events, &sig, 0)
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
        .read_stream("t1", "Order", "o1")
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
        .map(|c| c.command_id)
        .collect();
    assert_eq!(pending, ["c3"]);
    let listeners = store.listeners().unwrap();
    assert_eq!(
        (listeners[0].name.as_str(), listeners[0].position),
        ("orders", 2)
    );
    let report = store.verify(&kernel_id()).unwrap();
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
    // A crash in the middle of a write: header promises 100 bytes, 10 arrived.
    let mut f = fs::OpenOptions::new().append(true).open(&log.0).unwrap();
    f.write_all(&100u32.to_be_bytes()).unwrap();
    f.write_all(&[0u8; 14]).unwrap();
    drop(f);

    let store = FileStore::open(&log.0).unwrap();
    assert_eq!(fs::metadata(&log.0).unwrap().len(), intact);
    assert_eq!(log.torn_files().len(), 1);
    run(&store, "c2", "o1");
    drop(store);
    let reopened = FileStore::open(&log.0).unwrap();
    assert_eq!(reopened.read_stream("t1", "Order", "o1").unwrap().len(), 2);
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
    bytes[20] ^= 0xff; // inside the first record's body
    fs::write(&log.0, bytes).unwrap();
    assert!(matches!(FileStore::open(&log.0), Err(Error::Corrupt(_))));
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
        let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let mut record = LogRecord::decode(&bytes[at + 8..at + 8 + len]).unwrap();
        if let Some(Record::Committed(c)) = &mut record.record
            && c.command_id == "c1"
        {
            c.events[0].event.as_mut().unwrap().payload = br#"{"id":"forged"}"#.to_vec();
        }
        let body = record.encode_to_vec();
        out.extend((body.len() as u32).to_be_bytes());
        out.extend(&Sha256::digest(&body)[..4]);
        out.extend(&body);
        at += 8 + len;
    }
    fs::write(&log.0, out).unwrap();

    let report = FileStore::open_read_only(&log.0)
        .unwrap()
        .verify(&kernel_id())
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
    assert_eq!(reader.read_stream("t1", "Order", "o1").unwrap().len(), 1);
    assert!(matches!(
        reader.enqueue(&Command::default(), 0),
        Err(Error::ReadOnly)
    ));
}
