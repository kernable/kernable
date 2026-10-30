//! Any bytes as a file log: the store refuses them or reads them, and never crashes; neither do
//! the invariant checks on what it read.
#![no_main]

use kernable_store::{FileStore, Store, invariants::Log};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(store) = FileStore::open_bytes(data) {
        let _ = store.read_all(0, 10_000);
        let _ = store.pending();
        let _ = store.listeners();
        let _ = store.event_schemas();
        let _ = store.verify(&["00".to_string()]);
        let _ = store.command_ids();
        // The invariant checks read whatever a damaged log holds, and never crash on it.
        if let Ok(log) = Log::read(&store, &["00".to_string()]) {
            let _ = log.check();
        }
    }
});
