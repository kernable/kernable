//! Snapshots: kept per stream, replaced and removed; a SQLite store keeps them across a
//! restart and drops those of another format, as they can be rebuilt from the log.

use kernable_core::proto::{Signature, Snapshot, SnapshotRecord};
use kernable_store::{StreamId, snapshots};

fn record(id: &str, version: u64) -> SnapshotRecord {
    SnapshotRecord {
        tenant_id: "t".into(),
        domain: "shop".into(),
        aggregate_type: "Order".into(),
        aggregate_id: id.into(),
        snapshot: Some(Snapshot {
            version,
            schema: "v1".into(),
            state: br#"{"lines":2}"#.to_vec(),
            worker_sig: Some(Signature {
                key_id: "w".into(),
                sig: vec![version as u8; 64],
            }),
        }),
        event_hash: vec![version as u8; 32],
    }
}

fn stream(id: &str) -> StreamId {
    StreamId {
        tenant_id: "t".into(),
        domain: "shop".into(),
        aggregate_type: "Order".into(),
        aggregate_id: id.into(),
    }
}

#[test]
fn both_backends_keep_replace_and_remove_snapshots() {
    let path = std::env::temp_dir().join(format!("kn-snapshots-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    for url in ["memory:".to_owned(), format!("sqlite:{}", path.display())] {
        let store = snapshots::open(&url).unwrap();
        assert_eq!(store.get(&stream("o-1")).unwrap(), None, "{url}");
        store.save(&record("o-1", 3)).unwrap();
        store.save(&record("o-2", 1)).unwrap();
        assert_eq!(
            store.get(&stream("o-1")).unwrap(),
            Some(record("o-1", 3)),
            "{url}"
        );
        store.save(&record("o-1", 5)).unwrap();
        assert_eq!(
            store.get(&stream("o-1")).unwrap(),
            Some(record("o-1", 5)),
            "{url}"
        );
        store.remove(&stream("o-1")).unwrap();
        assert_eq!(store.get(&stream("o-1")).unwrap(), None, "{url}");
        assert_eq!(
            store.get(&stream("o-2")).unwrap(),
            Some(record("o-2", 1)),
            "{url}"
        );
    }

    // A restart keeps them; another format drops them.
    let url = format!("sqlite:{}", path.display());
    assert_eq!(
        snapshots::open(&url).unwrap().get(&stream("o-2")).unwrap(),
        Some(record("o-2", 1))
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    drop(conn);
    assert_eq!(
        snapshots::open(&url).unwrap().get(&stream("o-2")).unwrap(),
        None
    );
    let _ = std::fs::remove_file(&path);
}
