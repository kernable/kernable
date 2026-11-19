# Rewinding a diverged log instead of starting over

Status: **built**: a hand-written undo per frequent write, filed in the same transaction and dropped as the standby acknowledges; anything else is irreversible and still means starting over. Cost (bench/replication.sh, a primary and its standby on one machine): about 0 at full durability and one client, about -3 % saturated; -4 % and -8 % at normal durability. The log is sealed while fenced.

> The `file:` store (an append-only log file) this design also covers was removed before 0.1.0: the store is SQLite. What it says about the file log is history.

## As built

- **SQLite** (`kernable-store/src/undo.rs`): an `undo (first, last, steps)` table beside the record log. An enqueue undoes by deleting its command; a commit by deleting its events and putting back its command (pending), each event type's schema and the stream's index as they were; a rejection by putting its command back; a checkpoint by deleting it; a listener or a worker by saving it as it was. Every other write files `Irreversible`, and so does a commit with an erasure (the values are destroyed). A commit or rejection of a command with vault values notes them: a rewind puts the command back only if the vault still holds them, else the standby starts over. Their secrets stay until a standby confirms the completion (when stores open too), then go: on the primary as the standby acknowledges, on a standby once it follows on or has rewound. Steps go once the standby acknowledges the records (every 1,000), and beyond the newest 100,000 records without a standby.
- **The file log** cuts the file back and replays it: everything in it can be taken back but data files, erasures and the log's start. It keeps no undo window, so the secrets of completed commands go when its stores open: a tail with such a completion starts over there.
- **The protocol**: the standby's hello carries the records it could go back to (`rewind_points`, newest first, at most 1,000); a primary that holds one answers `REWIND` to the newest it holds. The standby takes back the records after it (one transaction), drops the vault values of the commands it took back, and follows on when it connects again. If it cannot after all, it starts over.
- **Tests**: the store compares a rewound store with a standby that never had the tail, field by field, on both stores (`kernable-store/tests/replication.rs`); a standby with its own tail rewinds and sets nothing aside, one with an irreversible write starts over (`kernable-kernel/tests/replication.rs`); a real partition under a client ends with the old primary taking its tail back (`tests/replication_kills.rs`).
- **A crash during a rewind**: the commands whose vault values go are noted beside the store (`<store>.rewinding`) before the store rewinds; when the stores open again, the values of those the store no longer holds go.

## The problem

A primary that loses its standby and the witness (a network cut) goes on for a moment: commits under way are written to its own store before the standby could acknowledge them, and checkpoints, listener positions and worker registrations follow until it is fenced (after 1 s at most for commits). Meanwhile the standby takes over. When the old primary comes back as a standby, its record log holds a **tail** the new primary never had, and P4 finds it diverged.

Without a rewind it sets all its stores aside (`<file>.diverged-<ms>`, only the newest copy kept) and copies everything from the new primary as an image. That is correct and loses nothing acknowledged, but its cost grows with the store: seconds for megabytes, hours for hundreds of gigabytes, and the pair runs without a standby in sync all that time. The soak on four servers saw it after every cut of the primary.

The tail itself is small: records nobody was told about, after the last record both logs share. **Rewinding** removes exactly those and follows on from there.

## What a rewind needs

1. **The common point**: the newest record both logs hold with the same hash. The primary already compares the standby's head; it would search back (both logs keep hashes per record) until they match, within what the old primary can still undo.
2. **Undoing the tail** in the old primary's stores, newest record first, to exactly the state after the common point.
3. **What the tail touched outside the store**: files of the data store (`DataChanged`), vault values, the snapshot store. The standby already clears snapshots when it follows; files named in the undone records are fetched again from the new primary; vault values are below.
4. **Crash safety**: a crash during a rewind must not leave a half-undone store. A marker, like the image marker: on restart, either finish the rewind or fall back to starting over.

**The file log is easy**: it is the record log, and its state is rebuilt by reading it, so a rewind truncates the file at the common record (the torn-tail handling already cuts files back). Only SQLite needs an undo.

## Rejected alternatives

- **Undo changesets from SQLite's session extension**, recorded with every write: generic, every table without an inverse per kind. A spike on `bench/replication.sh` measured the session's capture at 6–15 % of throughput at saturation even with events and the record log left out (a rewind deletes those by position), above the 10 % allowed. A hand-written inverse for the frequent writes keeps the hot path free; every other write is irreversible and still means starting over.
- **Replicating before committing**, so no tail exists: SQLite has one writer, so holding the write transaction open across a network round trip would serialize commits behind it, and committing alone (as the witness allows) still needs the old path.
- **Keeping starting over**: fine while stores are small, but its cost grows with the store.

Independent of these, a fenced primary seals its record log (as during a handover), so its background writes (checkpoints, listener flushes, worker saves) stop and the tail is only the commits under way when it lost its standby.
