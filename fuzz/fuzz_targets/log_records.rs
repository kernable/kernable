//! Any records with valid framing and checksums, as someone who rewrites the log would write
//! them: replaying them refuses inconsistent ones and never crashes, and `verify` runs.
#![no_main]

use kernable_store::{FileStore, Store};
use libfuzzer_sys::fuzz_target;
use sha2::{Digest, Sha256};

fuzz_target!(|data: &[u8]| {
    // The input is a sequence of `u8 length || body`, framed like the real log.
    let mut log = Vec::new();
    let mut rest = data;
    while let Some((&len, tail)) = rest.split_first() {
        let body = &tail[..(len as usize).min(tail.len())];
        rest = &tail[body.len()..];
        log.extend((body.len() as u32).to_be_bytes());
        log.extend(&Sha256::digest(body)[..4]);
        log.extend(body);
    }
    if let Ok(store) = FileStore::open_bytes(&log) {
        let _ = store.read_all(0, 10_000);
        let _ = store.pending();
        let _ = store.event_schemas();
        let _ = store.verify("00");
    }
});
