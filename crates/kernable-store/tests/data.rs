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
