use kernable_store::data::{self, Data, DataKey};

fn key(k: &str) -> DataKey {
    DataKey {
        tenant_id: "t".into(),
        domain: "shop".into(),
        key: k.into(),
    }
}

#[test]
fn both_backends_store_replace_and_delete_files() {
    let dir = std::env::temp_dir().join(format!("kn-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for url in ["memory:".to_owned(), format!("file:{}", dir.display())] {
        let store = data::open(&url).unwrap();
        // A key may hold anything, also what looks like a path out of the directory.
        let k = key("../invoices/o-1.pdf");
        assert_eq!(store.get(&k).unwrap(), None, "{url}");
        let pdf = Data {
            content_type: "application/pdf".into(),
            data: b"%PDF-1.7".to_vec(),
        };
        store.put(&k, &pdf).unwrap();
        assert_eq!(store.get(&k).unwrap(), Some(pdf), "{url}");
        let empty = Data {
            content_type: String::new(),
            data: Vec::new(),
        };
        store.put(&k, &empty).unwrap();
        assert_eq!(store.get(&k).unwrap(), Some(empty), "{url}");
        assert_eq!(store.get(&key("other")).unwrap(), None, "{url}");
        assert!(store.delete(&k).unwrap(), "{url}");
        assert!(!store.delete(&k).unwrap(), "{url}");
        assert_eq!(store.get(&k).unwrap(), None, "{url}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_unknown_backend_is_refused() {
    assert!(data::open("s3://bucket").is_err());
    assert!(data::open("file:").is_err());
}

#[test]
fn a_file_that_cannot_be_read_is_an_error_not_a_missing_value() {
    let dir = std::env::temp_dir().join(format!("kn-data-unreadable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = data::open(&format!("file:{}", dir.display())).unwrap();
    // A directory where the value's file belongs: reading and removing it fail.
    let path = dir
        .join(hex::encode("t"))
        .join(hex::encode("shop"))
        .join(hex::encode("o-1"));
    std::fs::create_dir_all(&path).unwrap();
    assert!(store.get(&key("o-1")).is_err());
    assert!(store.delete(&key("o-1")).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn keys_of_any_allowed_length_are_stored_apart() {
    let dir = std::env::temp_dir().join(format!("kn-data-long-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = data::open(&format!("file:{}", dir.display())).unwrap();
    // File names hold 255 bytes: hex names up to 123-byte keys, hashed names beyond.
    let lengths = [1, 123, 124, 127, 128, 511, 512];
    for n in lengths {
        let d = Data {
            content_type: "text/plain".into(),
            data: n.to_string().into_bytes(),
        };
        store.put(&key(&"k".repeat(n)), &d).unwrap();
    }
    for n in lengths {
        let got = store.get(&key(&"k".repeat(n))).unwrap().unwrap();
        assert_eq!(got.data, n.to_string().into_bytes());
    }
    // A long tenant and domain too.
    let long = DataKey {
        tenant_id: "t".repeat(300),
        domain: "d".repeat(300),
        key: "k".into(),
    };
    let d = Data {
        content_type: String::new(),
        data: b"x".to_vec(),
    };
    store.put(&long, &d).unwrap();
    assert_eq!(store.get(&long).unwrap(), Some(d));
    assert!(store.delete(&long).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn both_backends_list_by_prefix_and_copy_long_names() {
    let dir = std::env::temp_dir().join(format!("kn-data-list-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let long = format!("docs/{}", "é".repeat(200));
    let tenant = "t".repeat(300);
    for url in ["memory:".to_owned(), format!("file:{}", dir.display())] {
        let store = data::open(&url).unwrap();
        let file = |bytes: &[u8]| Data {
            content_type: "text/plain".into(),
            data: bytes.to_vec(),
        };
        for (k, bytes) in [
            ("docs/b", &b"bb"[..]),
            ("a", b"a"),
            (long.as_str(), b"long"),
        ] {
            store.put(&key(k), &file(bytes)).unwrap();
        }
        // Another tenant (with a name too long to be spelled out) and another domain.
        let elsewhere = DataKey {
            tenant_id: tenant.clone(),
            domain: "shop".into(),
            key: "docs/x".into(),
        };
        store.put(&elsewhere, &file(b"x")).unwrap();
        store
            .put(
                &DataKey {
                    domain: "other".into(),
                    ..key("docs/y")
                },
                &file(b"y"),
            )
            .unwrap();

        let keys = |prefix: &str, after: &str, limit: usize| -> Vec<(String, u64)> {
            store
                .list("t", "shop", prefix, after, limit)
                .unwrap()
                .into_iter()
                .map(|f| (f.key, f.size))
                .collect()
        };
        assert_eq!(
            keys("", "", 10),
            [("a".into(), 1), ("docs/b".into(), 2), (long.clone(), 4)],
            "{url}"
        );
        assert_eq!(keys("docs/", "", 1), [("docs/b".into(), 2)], "{url}");
        assert_eq!(keys("docs/", "docs/b", 10), [(long.clone(), 4)], "{url}");
        assert_eq!(keys("nothing", "", 10), [], "{url}");
        assert_eq!(
            store.list(&tenant, "shop", "", "", 10).unwrap().len(),
            1,
            "{url}"
        );

        // A copy (a standby's image, a move) knows every name, also the hashed ones.
        let copy = data::open("memory:").unwrap();
        store
            .dump(&mut |place, stored| {
                assert!(data::key_of(stored).is_ok(), "{url}");
                copy.restore(place, stored)
            })
            .unwrap();
        assert_eq!(copy.get(&key(&long)).unwrap(), Some(file(b"long")), "{url}");
        assert_eq!(copy.get(&elsewhere).unwrap(), Some(file(b"x")), "{url}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
