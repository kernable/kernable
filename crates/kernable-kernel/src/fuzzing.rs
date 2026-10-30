//! Entry points for the fuzz targets in `fuzz/` (feature `fuzzing`, never in a release): the
//! parts of the kernel that take outside input but are private to the crate.

use std::sync::{Arc, OnceLock};

use kernable_core::{
    proto::{Command, Membership, NewEvent, Register, Signature},
    signing,
};

use crate::{Config, Kernel};

/// A kernel on in-memory stores with a few events, made once per process.
pub fn kernel() -> &'static Arc<Kernel> {
    static KERNEL: OnceLock<Arc<Kernel>> = OnceLock::new();
    KERNEL.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("kernable-fuzz-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temporary directory");
        let key = dir.join("kernel.key");
        std::fs::write(&key, hex::encode(signing::generate_key().as_bytes())).expect("a key file");
        let root = signing::key_id(&signing::generate_key().verifying_key());
        let args = [
            "--root",
            &root,
            "--store",
            "memory:",
            "--snapshots",
            "memory:",
            "--data-store",
            "memory:",
            "--kernel-key",
            key.to_str().expect("a UTF-8 path"),
        ];
        let config = Config::from_args(&args, |_| None).expect("a valid configuration");
        let store = kernable_store::open("memory:").expect("a memory store");
        for (i, aggregate) in ["o1", "o1", "o2"].iter().enumerate() {
            let cmd = Command {
                command_id: format!("c{i}"),
                tenant_id: "t1".into(),
                domain: "orders".into(),
                aggregate_type: "Order".into(),
                aggregate_id: (*aggregate).into(),
                command_type: "PlaceOrder".into(),
                payload: br#"{"sku":"a"}"#.to_vec(),
                ..Command::default()
            };
            store.enqueue(&cmd, 0).expect("enqueued");
            let version = store
                .read_stream("t1", "orders", "Order", aggregate)
                .expect("a stream")
                .len() as u64;
            let event = NewEvent {
                event_type: "OrderPlaced".into(),
                payload: br#"{"sku":"a"}"#.to_vec(),
                schema: 1,
                ..NewEvent::default()
            };
            store
                .commit(&cmd.command_id, version, &[event], &Signature::default(), 0)
                .expect("committed");
        }
        Kernel::open_on(config, store).expect("a kernel")
    })
}

/// A request to the UI and to the metrics endpoint, and its `Host` check.
pub fn http(request: &[u8]) {
    let kernel = kernel();
    let _ = crate::ui::respond(kernel, request);
    let _ = crate::metrics::respond(kernel, request);
    if let Some(host) = crate::http::host(request) {
        let _ = crate::http::loopback_host(&host);
    }
}

/// The checks of a membership as `kernable apply` sends it.
pub fn membership(m: &Membership) {
    let _ = crate::memberships::validate(m);
    let _ = signing::membership_message(m);
}

/// The checks of a worker's registration.
pub fn register(reg: &Register) {
    let _ = crate::workers::validate(reg);
}
