//! Crash points for robustness tests. Built with `--features crash-points` and started with
//! `KERNABLE_CRASH_AT=<point>`, the kernel aborts the first time it reaches that point, as a
//! power cut or `kill -9` would stop it. In a normal build, [`point`] does nothing.

/// Every crash point, roughly in the order a command passes them.
pub const POINTS: &[&str] = &[
    "submit.enqueued",   // the command is stored, the client has no answer
    "execute.sent",      // a worker got the command, nothing is stored for it
    "commit.stored",     // the events are stored, nobody has been told
    "snapshot.stored",   // the snapshot is stored after the events
    "reject.stored",     // a rejection is stored, nobody has been told
    "listeners.acked",   // a listener acknowledged, its position is not stored
    "listeners.flushed", // listener positions are stored
    "settle.done",       // every listener processed the events, the client has no answer
    "checkpoint.stored", // a checkpoint is stored
];

#[cfg(feature = "crash-points")]
pub(crate) fn point(name: &str) {
    use std::sync::OnceLock;
    static AT: OnceLock<Option<String>> = OnceLock::new();
    debug_assert!(POINTS.contains(&name), "unknown crash point {name}");
    if AT
        .get_or_init(|| std::env::var("KERNABLE_CRASH_AT").ok())
        .as_deref()
        == Some(name)
    {
        eprintln!("kernable: crash point {name}");
        std::process::abort();
    }
}

#[cfg(not(feature = "crash-points"))]
#[inline(always)]
pub(crate) fn point(_name: &str) {}
