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
        deployment: String::new(),
    }
}

fn of(deployment: &str, id: &str, version: u64) -> SnapshotRecord {
    SnapshotRecord {
        deployment: deployment.into(),
        ..record(id, version)
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
        assert_eq!(store.get(&stream("o-1"), "").unwrap(), None, "{url}");
        store.save(&record("o-1", 3)).unwrap();
        store.save(&record("o-2", 1)).unwrap();
        assert_eq!(
            store.get(&stream("o-1"), "").unwrap(),
            Some(record("o-1", 3)),
            "{url}"
        );
        store.save(&record("o-1", 5)).unwrap();
        assert_eq!(
            store.get(&stream("o-1"), "").unwrap(),
            Some(record("o-1", 5)),
            "{url}"
        );
        store.remove(&stream("o-1")).unwrap();
        assert_eq!(store.get(&stream("o-1"), "").unwrap(), None, "{url}");
        assert_eq!(
            store.get(&stream("o-2"), "").unwrap(),
            Some(record("o-2", 1)),
            "{url}"
        );
    }

    // A restart keeps them; another format drops them.
    let url = format!("sqlite:{}", path.display());
    assert_eq!(
        snapshots::open(&url)
            .unwrap()
            .get(&stream("o-2"), "")
            .unwrap(),
        Some(record("o-2", 1))
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 2).unwrap();
    drop(conn);
    assert_eq!(
        snapshots::open(&url)
            .unwrap()
            .get(&stream("o-2"), "")
            .unwrap(),
        None
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn each_deployment_has_its_own_snapshots_and_drops_them_alone() {
    let path = std::env::temp_dir().join(format!("kn-snapshots-dep-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    for url in ["memory:".to_owned(), format!("sqlite:{}", path.display())] {
        let store = snapshots::open(&url).unwrap();
        store.save(&of("v1", "o-1", 3)).unwrap();
        store.save(&of("v2", "o-1", 5)).unwrap();
        store.save(&of("v2", "o-2", 1)).unwrap();
        // Each deployment gets its own; one without any gets none.
        assert_eq!(
            store.get(&stream("o-1"), "v1").unwrap(),
            Some(of("v1", "o-1", 3)),
            "{url}"
        );
        assert_eq!(
            store.get(&stream("o-1"), "v2").unwrap(),
            Some(of("v2", "o-1", 5)),
            "{url}"
        );
        assert_eq!(store.get(&stream("o-1"), "").unwrap(), None, "{url}");
        let mut infos: Vec<_> = store
            .infos()
            .unwrap()
            .into_iter()
            .map(|i| (i.deployment, i.version))
            .collect();
        infos.sort();
        assert_eq!(
            infos,
            [("v1".into(), 3), ("v2".into(), 1), ("v2".into(), 5)],
            "{url}"
        );
        // Dropping a deployment leaves the others'.
        assert_eq!(store.drop_deployment("v2").unwrap(), 2, "{url}");
        assert_eq!(store.get(&stream("o-1"), "v2").unwrap(), None, "{url}");
        assert_eq!(
            store.get(&stream("o-1"), "v1").unwrap(),
            Some(of("v1", "o-1", 3)),
            "{url}"
        );
        // Removing a stream's snapshot (an erasure) removes every deployment's.
        store.save(&of("v2", "o-1", 6)).unwrap();
        store.remove(&stream("o-1")).unwrap();
        assert_eq!(store.get(&stream("o-1"), "v1").unwrap(), None, "{url}");
        assert_eq!(store.get(&stream("o-1"), "v2").unwrap(), None, "{url}");
    }
    let _ = std::fs::remove_file(&path);
}
