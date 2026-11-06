//! Usage beside the store: `<store>.usage`, JSON, written every minute and at shutdown,
//! for whoever bills or watches the machine. Its counts go on across restarts: a kernel starts
//! from the file's, so a crash loses at most the last minute of them.
//!
//! ```json
//! {"at": 1793996832409, "events": 205200, "commands": 205211, "rejected": 3, "queries": 1200,
//!  "store_bytes": 241172480}
//! ```
//!
//! `events` is the store's head (it never goes back), `store_bytes` what the stores take on the
//! disk now; `commands` (committed), `rejected` and `queries` (answered) are counted since the
//! file began.

use std::{path::PathBuf, sync::atomic::Ordering::Relaxed, time::Duration};

use serde::{Deserialize, Serialize};

use crate::Kernel;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// When it was written, ms since epoch.
    pub at: i64,
    pub events: u64,
    pub commands: u64,
    pub rejected: u64,
    pub queries: u64,
    pub store_bytes: u64,
}

/// Where the store at `url` keeps its usage; `None` for a store in memory.
pub fn path(url: &str) -> Option<PathBuf> {
    crate::standby::marker(url, "usage")
}

/// The usage the file at `url`'s store holds, if it can be read.
pub fn read(url: &str) -> Option<Usage> {
    serde_json::from_slice(&std::fs::read(path(url)?).ok()?).ok()
}

/// The usage now: the counts the kernel started from, and its own since.
pub(crate) fn now(kernel: &Kernel) -> Usage {
    let (base, m) = (&kernel.usage_base, &kernel.metrics);
    Usage {
        at: crate::now_ms(),
        events: kernel.store.head_position().unwrap_or(0),
        commands: base.commands + m.committed.load(Relaxed),
        rejected: base.rejected + m.rejected.load(Relaxed),
        queries: base.queries + m.queries_answered.load(Relaxed),
        store_bytes: kernel.stored_bytes.load(Relaxed),
    }
}

/// Writes the usage now, replacing the file in one step.
pub(crate) fn write(kernel: &Kernel) {
    let Some(path) = path(&kernel.config().store) else {
        return;
    };
    let usage = now(kernel);
    let next = path.with_extension("usage.next");
    let written = std::fs::write(&next, serde_json::to_vec(&usage).expect("serializes"))
        .and_then(|()| std::fs::rename(&next, &path));
    if let Err(e) = written {
        tracing::warn!(path = %path.display(), error = %e, "writing the usage failed");
    }
}

/// Writes the usage every minute.
pub(crate) async fn keep(kernel: &Kernel) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.tick().await;
    loop {
        tick.tick().await;
        write(kernel);
    }
}
