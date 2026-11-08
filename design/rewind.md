# Rewinding a diverged log instead of starting over

Status: **built 2026-11-08** as way on 2 below (chosen after the spike): a hand-written undo per frequent write, filed in the same transaction and dropped as the standby acknowledges; anything else is irreversible and still means starting over. Cost (bench/replication.sh, a primary and its standby on one machine): about 0 at full durability and one client, about -3 % saturated; -4 % and -8 % at normal durability. The log is sealed while fenced.

## As built

- **SQLite** (`kernable-store/src/undo.rs`): an `undo (first, last, steps)` table beside the record log. An enqueue undoes by deleting its command; a commit by deleting its events and putting back its command (pending), each event type's schema and the stream's index as they were; a rejection by putting its command back; a checkpoint by deleting it; a listener or a worker by saving it as it was. Every other write files `Irreversible`. Also irreversible: a commit or rejection of a command with vault values (its secrets are dropped once it completes) and a commit with an erasure. Steps go once the standby acknowledges the records (every 1,000), and beyond the newest 100,000 records without a standby.
- **The file log** cuts the file back and replays it: everything in it can be taken back but data files, erasures, commands completed with vault values and the log's start.
- **The protocol**: the standby's hello carries the records it could go back to (`rewind_points`, newest first, at most 1,000); a primary that holds one answers `REWIND` to the newest it holds. The standby takes back the records after it (one transaction), drops the vault values of the commands it took back, and follows on when it connects again. If it cannot after all, it starts over.
- **Tests**: the store compares a rewound store with a standby that never had the tail, field by field, on both stores (`kernable-store/tests/replication.rs`); a standby with its own tail rewinds and sets nothing aside, one with an irreversible write starts over (`kernable-kernel/tests/replication.rs`); a real partition under a client ends with the old primary taking its tail back (`tests/replication_kills.rs`).
- **A gap**: a crash after the store was rewound and before the vault values of the commands it took back were dropped leaves those values behind, encrypted, referred to by nothing.

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

## The spike (2026-11-07)

`bench/undo-spike.patch` on `bench/replication.sh` (a primary and a standby on one machine, SQLite, 1 KB payloads, wait=committed), commits only; results in `bench/results-2026-11-07-m2max-undo-spike.txt`.

| What the session holds | durability | concurrency 1 | concurrency 64 (saturated) |
|---|---|---|---|
| every table | normal | -15 % | -29 % |
| every table | full | -4 % | -15 % |
| all but events and the record log | normal | -7 % | -15 % |
| all but events and the record log | full | 0 to -10 % (noisy) | -6 % to -14 % |

Leaving out events and records (a rewind deletes them by position) halved the cost; deleting old undo rows in batches changed nothing: what remains is the session's capture itself, on the commands and index rows a commit changes. At the load customers will run (far below saturation) the cost is a few hundredths of a millisecond per commit; at saturation it is 6–15 % of throughput, above the 10 % agreed.

### Ways on

1. **Accept it**: 6–15 % at saturation, about nothing below it. Rewind as designed.
2. **A hand-written inverse for the commit only** (the hot path, which is also what a tail holds: commits under way), the session for every other kind of record (rare). No cost on commits; one inverse to get exactly right, checked by the same tests (a rewound standby must equal its primary).
3. **Keep starting over** (D) with the changes above and 3a: the tail is now only the commits under way, but any tail still costs a full copy.

Recommendation: **2**. The commit's own writes are few and known (its events, its command's outcome, the stream's index entries), the tests compare whole stores, and the cost stays off the hot path.

## Questions, answered

1. Answered: A, with a spike first. The spike missed the bar: accept the cost (1), an inverse for commits only with the session for the rest (2, recommended), or keep starting over (3)?
2. Answered: yes; built.
3. Answered: undone too.
