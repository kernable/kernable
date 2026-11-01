//! Data formats: a new store records the format it is written in, and a store of another
//! format is refused instead of being misread.

use std::{fs, path::PathBuf};

use kernable_core::proto::{Command, EnqueuedRecord, FormatRecord, LogRecord, log_record::Record};
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
    assert!(err.to_string().contains("development build"), "{err}");
    assert_eq!(format_error(SqliteStore::open(&path.0)), 0);

    conn.pragma_update(None, "user_version", FORMAT + 1)
        .unwrap();
    assert_eq!(format_error(SqliteStore::open(&path.0)), FORMAT + 1);
}
