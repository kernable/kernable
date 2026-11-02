//! A read model's store on its own: values by namespace and key, removed and reset.

use kernable::Store;

#[test]
fn values_are_set_read_removed_and_reset() {
    let store = Store::open("memory:", "1").unwrap();
    for (ns, key, value) in [("carts", "c1", 1), ("carts", "c2", 2), ("users", "u1", 3)] {
        store.set(ns, key, &value).unwrap();
    }
    assert_eq!(store.get::<i64>("carts", "c1").unwrap(), Some(1));
    store.remove("carts", "c1").unwrap();
    assert_eq!(store.get::<i64>("carts", "c1").unwrap(), None);
    let all = store.all::<i64>("carts").unwrap();
    assert_eq!(all, [("c2".to_owned(), 2)]);

    store.reset(Some("carts")).unwrap();
    assert!(store.all::<i64>("carts").unwrap().is_empty());
    assert_eq!(store.get::<i64>("users", "u1").unwrap(), Some(3));
    store.set("carts", "c3", &4).unwrap();
    store.reset(None).unwrap();
    assert!(store.all::<i64>("carts").unwrap().is_empty());
    assert!(store.all::<i64>("users").unwrap().is_empty());
}

#[test]
fn opening_a_store_empties_the_log_a_crash_left_behind() {
    let dir = std::env::temp_dir().join(format!("kn-store-wal-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("s.db");
    let url = format!("sqlite:{}", path.display());
    Store::open(&url, "1").unwrap().close();
    // A process that removed an erased value and crashed before the log was emptied: its
    // connection is never closed, so the old page with the value stays in the log.
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA secure_delete = ON; PRAGMA wal_autocheckpoint = 0;
         INSERT INTO data VALUES ('emails', 'a1', '\"max@firma.de\"');
         DELETE FROM data;",
    )
    .unwrap();
    std::mem::forget(db);
    let found = || {
        ["", "-wal"].into_iter().any(|suffix| {
            let bytes = std::fs::read(format!("{}{suffix}", path.display())).unwrap_or_default();
            bytes.windows(12).any(|w| w == b"max@firma.de")
        })
    };
    assert!(found(), "the log holds the removed value");
    let store = Store::open(&url, "1").unwrap();
    assert!(
        !found(),
        "the removed value is still in a file of the store"
    );
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
}
