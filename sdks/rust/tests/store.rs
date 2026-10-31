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
