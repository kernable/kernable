//! Any bytes as a SQLite file where the kernel expects its event store, its vault or its
//! snapshot store, as a damaged disk or a hostile backup would hand them over: refused or
//! read, never a crash, and the invariant checks never crash on what is read.
#![no_main]

use kernable_store::{SqliteStore, Store, StreamId, invariants::Log, vault::Vault};
use libfuzzer_sys::fuzz_target;

/// The key of the vault fixtures, so seeds from them decrypt.
const VAULT_KEY: &str = include_str!("../../crates/kernable-kernel/tests/fixtures/format-1/vault.key");

fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kernable-fuzz-store-{}.db", std::process::id()))
}

fn reset(path: &std::path::Path, data: &[u8]) {
    for suffix in ["-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    std::fs::write(path, data).expect("a temporary file");
}

fuzz_target!(|data: &[u8]| {
    let path = path();
    let url = format!("sqlite:{}", path.display());

    reset(&path, data);
    if let Ok(store) = SqliteStore::open_unlocked(&path) {
        let _ = store.read_all(0, 1000);
        let _ = store.pending();
        let _ = store.command_ids();
        let _ = store.listeners();
        let _ = store.workers();
        let _ = store.memberships();
        let _ = store.revocations();
        let _ = store.event_schemas();
        let _ = store.streams(100);
        if let Ok(log) = Log::read(&store, &["00".repeat(32)]) {
            let _ = log.check();
        }
    }
    let _ = kernable_store::migrate(&url);

    reset(&path, data);
    let mut key = [0u8; 32];
    hex_into(VAULT_KEY.trim(), &mut key);
    if let Ok(vault) = Vault::open(&url, &key) {
        let _ = vault.secret_commands();
        let _ = vault.describe(&["00".repeat(32)]);
        let _ = vault.rekey(&key); // decrypts every value
    }

    reset(&path, data);
    if let Ok(snapshots) = kernable_store::snapshots::open(&url) {
        for id in ["o1", "o2"] {
            let stream = StreamId {
                tenant_id: "t1".into(),
                domain: "orders".into(),
                aggregate_type: "Order".into(),
                aggregate_id: id.into(),
            };
            let _ = snapshots.get(&stream);
        }
    }
});

fn hex_into(hex: &str, out: &mut [u8; 32]) {
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2).unwrap_or("00"), 16).unwrap_or(0);
    }
}
