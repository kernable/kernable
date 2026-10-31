//! Data formats: a new store records the format it is written in, and a store of another
//! format is refused instead of being misread.

use std::{fs, path::PathBuf};

use kernable_core::proto::{
    Command, EnqueuedRecord, FormatRecord, Grant, LogRecord, log_record::Record,
};
use kernable_store::{Error, FORMAT, FileStore, SqliteStore, Store};
use prost::Message;
use sha2::{Digest, Sha256};

struct Temp(PathBuf);

impl Temp {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("kernable-format-{name}-{}", std::process::id()));
        let _ = fs::remove_file(&path);
        Temp(path)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

/// A file log of exactly these records.
fn log(path: &PathBuf, records: Vec<Record>) {
    let mut out = Vec::new();
    for record in records {
        let body = LogRecord {
            record: Some(record),
        }
        .encode_to_vec();
        out.extend((body.len() as u32).to_be_bytes());
        out.extend(&Sha256::digest(&body)[..4]);
        out.extend(&body);
    }
    fs::write(path, out).unwrap();
}

fn format(version: u32) -> Record {
    Record::Format(FormatRecord { version })
}

fn enqueued() -> Record {
    Record::Enqueued(EnqueuedRecord {
        command: Some(Command {
            command_id: "c1".into(),
            ..Default::default()
        }),
        created_at: 0,
    })
}

fn format_error<T>(opened: Result<T, Error>) -> u32 {
    match opened {
        Err(Error::Format(found)) => found,
        Err(e) => panic!("expected a format error, got {e}"),
        Ok(_) => panic!("expected a format error, the store opened"),
    }
}

#[test]
fn a_new_file_log_starts_with_its_format() {
    let path = Temp::new("file-new");
    drop(FileStore::open(&path.0).unwrap());
    let bytes = fs::read(&path.0).unwrap();
    let first = LogRecord::decode(&bytes[8..]).unwrap().record;
    assert_eq!(first, Some(format(FORMAT)));
    // Reopening and writing keeps it the only format record.
    let store = FileStore::open(&path.0).unwrap();
    assert_eq!(store.head_position().unwrap(), 0);
}

#[test]
fn a_file_log_of_another_format_is_refused() {
    let path = Temp::new("file-other");
    log(&path.0, vec![enqueued()]); // from before formats were recorded
    assert_eq!(format_error(FileStore::open(&path.0)), 0);
    assert_eq!(format_error(FileStore::open_read_only(&path.0)), 0);

    log(&path.0, vec![format(FORMAT + 1), enqueued()]);
    let err = FileStore::open(&path.0).err().unwrap();
    assert!(err.to_string().contains("newer kernable"), "{err}");

    log(&path.0, vec![format(FORMAT), enqueued(), format(FORMAT)]);
    assert!(matches!(FileStore::open(&path.0), Err(Error::Corrupt(_))));
}

#[test]
fn a_new_sqlite_database_records_its_format_and_others_are_refused() {
    let path = Temp::new("sqlite");
    drop(SqliteStore::open(&path.0).unwrap());
    let conn = rusqlite::Connection::open(&path.0).unwrap();
    let version: u32 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, FORMAT);
    assert!(SqliteStore::open(&path.0).is_ok());

    // Tables without a recorded format: written before formats were recorded.
    conn.pragma_update(None, "user_version", 0).unwrap();
    let err = SqliteStore::open(&path.0).err().unwrap();
    assert!(err.to_string().contains("kernable migrate"), "{err}");
    assert_eq!(format_error(SqliteStore::open(&path.0)), 0);

    conn.pragma_update(None, "user_version", FORMAT + 1)
        .unwrap();
    assert_eq!(format_error(SqliteStore::open(&path.0)), FORMAT + 1);
}

fn grant(issued_at: i64) -> Grant {
    Grant {
        key_id: "k".into(),
        issued_at,
        ..Default::default()
    }
}

#[test]
fn a_file_log_of_format_1_migrates_to_format_2() {
    let path = Temp::new("file-migrate");
    log(&path.0, vec![format(1), enqueued()]);
    assert_eq!(format_error(FileStore::open(&path.0)), 1);
    let url = format!("file:{}", path.0.display());
    assert_eq!(kernable_store::migrate(&url).unwrap(), 1);
    assert_eq!(
        kernable_store::migrate(&url).unwrap(),
        FORMAT,
        "nothing left"
    );
    let kept = PathBuf::from(format!("{}.format-1", path.0.display()));
    assert_eq!(
        format_error(FileStore::open_read_only(&kept)),
        1,
        "the old log is kept as it was"
    );
    fs::remove_file(&kept).unwrap();

    let store = FileStore::open(&path.0).unwrap();
    assert_eq!(store.pending().unwrap()[0].command.command_id, "c1");
    store.save_grant(&grant(7)).unwrap();
    drop(store);
    let store = FileStore::open(&path.0).unwrap();
    assert_eq!(store.grants().unwrap(), [grant(7)]);
}

#[test]
fn migrate_leaves_a_file_log_it_has_no_migration_for() {
    let path = Temp::new("file-no-migration");
    let url = format!("file:{}", path.0.display());
    for (first, found) in [(Some(format(FORMAT + 1)), FORMAT + 1), (None, 0)] {
        let records = first.into_iter().chain([enqueued()]).collect();
        log(&path.0, records);
        let before = fs::read(&path.0).unwrap();
        assert_eq!(format_error(kernable_store::migrate(&url)), found);
        assert_eq!(
            fs::read(&path.0).unwrap(),
            before,
            "format {found}: unchanged"
        );
        let kept = PathBuf::from(format!("{}.format-{found}", path.0.display()));
        assert!(!kept.exists());
    }
}

#[test]
fn a_sqlite_database_of_format_1_migrates_to_format_2() {
    let path = Temp::new("sqlite-migrate");
    let store = SqliteStore::open(&path.0).unwrap();
    store
        .enqueue(
            &Command {
                command_id: "c1".into(),
                ..Default::default()
            },
            0,
        )
        .unwrap();
    drop(store);
    // As format 1 wrote it: no grants table.
    let conn = rusqlite::Connection::open(&path.0).unwrap();
    conn.execute_batch("DROP TABLE grants; PRAGMA user_version = 1;")
        .unwrap();
    drop(conn);
    assert_eq!(format_error(SqliteStore::open(&path.0)), 1);
    let url = format!("sqlite:{}", path.0.display());
    assert_eq!(kernable_store::migrate(&url).unwrap(), 1);
    assert_eq!(
        kernable_store::migrate(&url).unwrap(),
        FORMAT,
        "nothing left"
    );

    let store = SqliteStore::open(&path.0).unwrap();
    assert_eq!(store.pending().unwrap()[0].command.command_id, "c1");
    store.save_grant(&grant(7)).unwrap();
    drop(store);
    let store = SqliteStore::open(&path.0).unwrap();
    assert_eq!(store.grants().unwrap(), [grant(7)]);
}
