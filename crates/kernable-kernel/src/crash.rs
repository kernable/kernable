//! Crash points for robustness tests. Built with `--features crash-points` and started with
//! `KERNABLE_CRASH_AT=<point>`, the kernel aborts the first time it reaches that point, as a
//! power cut or `kill -9` would stop it. In a normal build, [`point`] does nothing.

/// The crash points a command passes, roughly in order.
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

/// The crash points of the kernel's other state: vault, memberships, revocations.
pub const STATE_POINTS: &[&str] = &[
    "vault.stored", // a client's vault values are stored, the command is not queued yet
    "erasure.intended", // a subject's erasure is noted, its event is not committed
    "erasure.pending", // the erasing event is committed, the subject's values are not erased
    "erasure.done", // the subject's values are erased, nobody was told
    "membership.stored", // a membership is stored, not yet in effect
    "revocation.stored", // a revocation is stored, the key is not refused yet
];

/// The crash points of replication: on the primary, and on the standby.
pub const REPLICATION_POINTS: &[&str] = &[
    "replication.sent", // the primary sent records to the standby, nobody was told of them
    "standby.applied",  // the standby applied and synced records, not acknowledged, vault not swept
    "standby.imaging",  // the standby applied part of an image
];

/// The crash points of moving a tenant: on the target, and on the source.
pub const MOVE_POINTS: &[&str] = &[
    "move.batch",   // the target stored a part of the copy, not all of it
    "move.copied",  // the target copied everything, the control partition does not know
    "move.handed", // the control partition handed over the global keys, the target has not noted the move
    "move.dropped", // the source dropped the tenant's keys, not its vault values and files
];

#[cfg(feature = "crash-points")]
pub(crate) fn point(name: &str) {
    use std::sync::OnceLock;
    static AT: OnceLock<Option<String>> = OnceLock::new();
    debug_assert!(
        POINTS.contains(&name)
            || STATE_POINTS.contains(&name)
            || REPLICATION_POINTS.contains(&name)
            || MOVE_POINTS.contains(&name),
        "unknown crash point {name}"
    );
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
