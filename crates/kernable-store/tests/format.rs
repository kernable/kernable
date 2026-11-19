//! Data formats: a new store records the format it is written in, and a store of another
//! format is refused instead of being misread.

use std::{fs, path::PathBuf};

use kernable_store::{Error, FORMAT, SqliteStore};

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

fn format_error<T>(opened: Result<T, Error>) -> u32 {
    match opened {
        Err(Error::Format(found)) => found,
        Err(e) => panic!("expected a format error, got {e}"),
        Ok(_) => panic!("expected a format error, the store opened"),
    }
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
