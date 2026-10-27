//! Any bytes as a file log: the store refuses them or reads them, and never crashes.
#![no_main]

use kernable_store::{FileStore, Store};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(store) = FileStore::open_bytes(data) {
        let _ = store.read_all(0, 10_000);
        let _ = store.pending();
        let _ = store.listeners();
        let _ = store.event_schemas();
        let _ = store.verify(&["00".to_string()]);
    }
});
