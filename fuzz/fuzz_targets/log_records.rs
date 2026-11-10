//! Any records with valid framing and checksums, as someone who rewrites the log would write
//! them: replaying them refuses inconsistent ones and never crashes, and `verify` runs.
#![no_main]

use kernable_store::{FileStore, Store};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The input is a sequence of `u8 length || body`, framed like the real log.
    let mut log = Vec::new();
    let mut rest = data;
    while let Some((&len, tail)) = rest.split_first() {
        let body = &tail[..(len as usize).min(tail.len())];
        rest = &tail[body.len()..];
        log.extend(FileStore::frame(body));
    }
    if let Ok(store) = FileStore::open_bytes(&log) {
        let _ = store.read_all(0, 10_000);
        let _ = store.pending();
        let _ = store.event_schemas();
        let _ = store.index();
        let _ = store.verify(&["00".to_string()]);
    }
});
