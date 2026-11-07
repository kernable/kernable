# Rewinding a diverged log instead of starting over

Status: proposal, 2026-11-07, open to choose. Nothing built.

## The problem

A primary that loses its standby and the witness (a network cut) goes on for a moment: commits under way are written to its own store before the standby could acknowledge them, and checkpoints, listener positions and worker registrations follow until it is fenced (after 1 s at most for commits). Meanwhile the standby takes over. When the old primary comes back as a standby, its record log holds a **tail** the new primary never had, and P4 finds it diverged.

Today it sets all its stores aside (`<file>.diverged-<ms>`, only the newest copy kept since 3a) and copies everything from the new primary as an image. That is correct and loses nothing acknowledged, but its cost grows with the store: seconds for megabytes, hours for hundreds of gigabytes, and the pair runs without a standby in sync all that time. The soak on four servers saw it after every cut of the primary (2026-11-07).

The tail itself is small: records nobody was told about, after the last record both logs share. **Rewinding** removes exactly those and follows on from there.

## What a rewind needs

1. **The common point**: the newest record both logs hold with the same hash. The primary already compares the standby's head; it would search back (both logs keep hashes per record) until they match, within what the old primary can still undo.
2. **Undoing the tail** in the old primary's stores, newest record first, to exactly the state after the common point.
3. **What the tail touched outside the store**: files of the data store (`DataChanged`), vault values, the snapshot store. The standby already clears snapshots when it follows; files named in the undone records are fetched again from the new primary; vault values are below.
4. **Crash safety**: a crash during a rewind must not leave a half-undone store. A marker, like the image marker: on restart, either finish the rewind or fall back to starting over.

**The file log is easy**: it is the record log, and its state is rebuilt by reading it, so a rewind truncates the file at the common record (the torn-tail handling already cuts files back). Only SQLite needs an undo.

## Options for SQLite

### A. Undo changesets (SQLite's session extension) — recommended, after a spike

Every write transaction that files a record also records its changes with SQLite's session extension (`rusqlite` feature `session`), and stores the **inverted changeset** in an `undo (seq, changeset)` table in the same transaction. Rows are dropped once the standby acknowledges the record (the same moment the record log may prune). A rewind applies the undo rows newest-first and deletes the records after the common point, in one transaction.

- Generic: every record kind, every table, without writing an inverse per kind.
- Costs on every write: capturing the session, and writing and later deleting a changeset. **To measure first** (commit p50/p99 and throughput at 100 and 400 commands/s, with and without), and a no-go if it is more than about 10 %.
- The vault is its own database: the same mechanism there, or vault values written by the tail stay behind without a reference (encrypted, and nobody was told of them) until something drops values without a reference; no such sweep exists yet.
- Undo rows exist only for records not yet acknowledged: a few at a time, so the table stays tiny.

### B. Inverses per record kind

Hand-written undo for each kind: an enqueued command removed, a commit's events, keys and facts removed and its command pending again, a checkpoint dropped, a listener's position put back, … Fallback to starting over for kinds without one.

- No overhead on writes.
- Every kind needs an exact inverse, also for indexes kept beside the events (keys, facts, requirements); a mistake leaves a standby that differs from its primary without anyone noticing. Many kinds, two backends' worth of state, high risk. Not recommended.

### C. Replicate before committing

The primary writes nothing that its standby has not acknowledged: the record goes to the standby first, the local transaction commits after the acknowledgement. No tail exists, so nothing needs undoing.

- The cleanest invariant.
- SQLite has one writer: holding the write transaction open across a network round trip serializes commits behind it. Throughput would drop to roughly one batch per round trip; group commit softens it but changes the write path deeply. Committing alone (as the witness allows) still needs the old path. Not recommended now.

### D. Keep starting over (with 3a)

No change. Fine while stores are small; the cost is felt by the first customer with a big store after a network cut.

## Independent of the option: write less while fenced

A fenced primary answers nothing, but its background writes go on: checkpoints, listener flushes, worker saves. Sealing its record log while fenced (as during a handover) shrinks the tail to the commits that were under way when it lost its standby. It does not remove the need to rewind them, but it makes the tail a handful of records. Cheap; worth doing with any option.

## Proposed steps (for A)

1. Seal the record log while fenced (small, independent).
2. Spike: session capture and undo rows in `SqliteStore`, measured with `kernable-bench load` at 100 and 400/s against the plain store. Go or no-go with the numbers.
3. The common point: the primary searches back from the standby's head; the answer `rewind { to }` beside `follow`, `image` and `diverged`.
4. The rewind in both stores (file log: truncate; SQLite: undo rows), with its marker and crash points; data files of the undone tail fetched again.
5. Tests: the store's replication tests with a diverged tail of every record kind (the rewound standby must equal the primary, as the existing comparison checks); crash points during a rewind; the model-based pair test with partitions; the soak on four servers, where every cut of the primary should then end without a start-over.

## Questions, answered

1. Option A (with a spike first), B, C or D?
2. Seal the record log while fenced now, whatever the answer to 1?
3. Vault values from a rewound tail: undone with a session of their own, or left behind (and a sweep for values without a reference built)?
