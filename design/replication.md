# Replication and the standby

Status: S1a built 2026-11-03: the store's record log (increment 1), the primary and the standby (2, 3), `kernable promote` and the docs (4, [Run a standby](../docs/how-to/standby.md)). Tests: `crates/kernable-store/tests/replication.rs`, `crates/kernable-kernel/tests/replication.rs`. S1b built the same day: the witness and pairs (below). S1c and S1d followed: address lists in the SDKs, crash points during replication, `kill -9` tests, soak runs with failover faults (2026-11-04) and the model-based test of a pair under seeded kills and pauses (2026-11-04). Invariants P1–P4 are in [invariants.md](invariants.md). 2026-11-07: a handing-over primary and a fenced one seal their record log, so a handover leaves no tail; a diverged standby keeps only its newest set-aside copy; a diverged standby takes its own newest records back instead of starting over (2026-11-08, [rewind.md](rewind.md)).

## Goal

A second kernel, the **standby**, holds every write the primary acknowledged. If the primary dies, an operator promotes the standby (`kernable promote <address> <key>`); it starts like a restarted kernel and loses nothing. Automatic failover through a witness is S1b, the SDKs' address lists are S1c.

## Decisions (2026-11-03)

| Question | Choice |
|---|---|
| What is sent | the store's own `LogRecord`s (`storage.proto`), for every backend; the two nodes may use different backends |
| Catch-up after a gap | incremental, from the standby's last record; a full image only for an empty standby or one whose log diverged |
| Vault key | one per node: values travel in plaintext inside the mutual-TLS stream and the standby encrypts them with its own key |

## Records and the record log

Every store write already has a `LogRecord`: enqueued, committed, rejected, checkpoint, listener, worker, membership, grant, revocation. Replication adds one kind, `DataChanged { tenant_id, domain, key }`, written after a file of the data store was put or deleted.

Each record gets a **sequence number** (1, 2, …, without gaps) and a **record hash**:

```text
hash(n) = SHA-256("kernable.v1.replication" || hash(n-1) || u64 BE n || SHA-256(body))
hash(0) = 32 zero bytes
```

The body's SHA-256 is the one the file log already computes for its record checksum, so the file log hashes each record once more, over 32 bytes.

Two stores at the same sequence number with the same hash hold the same history. That is how the primary checks a standby's log when it reconnects, and how P4 finds a diverged one.

The record log lives in the store, written in the same transaction as the write itself:

- **file log:** it is the log. The sequence number is the record's index after the format record; the offset and hash of every 1,024th record are kept in memory, so a record is found without reading the whole file.
- **SQLite:** a table `replication (seq INTEGER PRIMARY KEY, hash BLOB, record BLOB)`, written only while replication is on (`--sync-standby` on a primary, always on a standby). It is pruned to what the standby has not acknowledged plus the newest 100,000 records, so it does not double the store. A standby that falls further behind gets a full image. Turned on in a store that already holds data, the log starts with a `ReplicationHead` record of a random hash: no standby continues it, each takes an image first. Turned off, the table is dropped.

A standby's record log is a copy of its primary's: it files each record it applies with the primary's sequence number and hash, in the same transaction (SQLite) or as the same bytes (file log). After an image, a `ReplicationHead { seq, hash }` record starts its log where the image stands.

## What is not in the record log

- **Vault values.** No personal or secret value is ever written to a record log: an erasure could not reach it there. The primary attaches the values to the records that refer to them (an enqueued command's `vault_refs`, a commit's events and personal keys), read from its vault when it sends them, as vault rows with what the vault keeps about them (owner, stream, time; `Vault::export`). A value erased or dropped in the meantime is not sent. The standby encrypts each with its own key (`Vault::import`). It cannot check a value against its reference, because the vault keeps no salts: it takes them from its authenticated primary. It derives every deletion from the records, as the kernel does: an erasing event's subject is noted as an intent before the commit and erased after it (so a crash between still erases), a completed command drops its secrets, a commit that releases a personal key frees its value.
- **File contents.** `DataChanged` names the file; the primary reads it when it sends the record. A file overwritten in the meantime travels in its newer version, a deleted one as absent; the following records converge the standby.
- **Snapshots, the cache, leases, `live` listeners and the replay window.** A promoted standby rebuilds or refuses them as after a restart. A standby clears its snapshot store when it starts following.

## The stream

A new kernel service, `Replication.Follow`, a bidirectional stream from the standby to the primary:

1. The standby says who it is (key ID, challenge as for workers and listeners) and where it stands: its last sequence number and hash, or none. Its key needs the new grant permission `replicate`, which gives as much as `vault_read`.
2. The primary answers one of:
   - **follow:** the standby's record exists in the primary's record log with the same hash; the records after it follow;
   - **image:** the standby is empty, or so far behind that the record is pruned; a full image follows (below), then the records after it;
   - **diverged:** the standby holds a record the primary does not have, or the hash differs. The standby moves its stores aside (`<path>.diverged-<ms>`, kept like `<path>.torn-<offset>`) and starts again, empty (P4).
3. Records follow in order, in batches of up to 512 records or 1 MiB, with the vault values and files they need, at most 8,192 records ahead of the standby's acknowledgements. The standby applies each batch, syncs its store with its own `--durability`, and acknowledges the last sequence number.

The standby's own grant, presented on this stream, is stored without waiting for a standby: it arrives before the standby follows, and reaches it with the records.

**Full image.** From one consistent view of the store. From SQLite, one read transaction gives the records in an order that applies: grants, revocations, memberships, workers, every command as enqueued, every commit in log order (each stream's newest keys and facts filed with the commit of its last event), commits without events, rejections, listeners with their parked events, checkpoints. The file log only grows, so its records up to an offset are its image. Either way it ends with the sequence number and hash it stands for, which the standby takes as the start of its own record log. The files of the data store follow from the kernel (increment 2). The primary keeps records from that point until the standby has acknowledged them.

## Applying records on the standby

`Store::apply(records)` does what the writes did on the primary and checks each record before it writes it: it follows the standby's head and its hash chains; a command is enqueued once; a commit completes a pending command, its event positions follow without gaps, every event hash matches its content and its predecessor, and committing them through the store's own commit gives exactly them (SQLite) or they continue the command's stream (file log); a checkpoint names an event the standby holds. A record that fails is refused as diverged and nothing of it is written (SQLite refuses the whole batch). The kernel checks the checkpoints' signatures against the primary's kernel key (`--primary-kernel-pub`, increment 3). A record the standby holds already has a sequence number at or below its head, so a resend after a crash between applying and acknowledging is refused as not following, and the standby asks again from its head.

## When a write counts (P1)

`Kernel::sync` already runs before anyone is told of a write, and the `durable` position keeps listeners, subscribers and workers from getting an event before it is on the disk. With `--sync-standby`, `sync` also waits until the standby has acknowledged every record written so far; `durable` advances only then. So no client, listener or worker hears of a write the standby does not hold.

Without a standby in sync, the primary waits: commits stay queued, submits time out (`--submit-timeout-ms`, the command still runs later), `/ready` answers 503, and the log says why every 10 s. The wait blocks its thread (`block_in_place` on the multi-threaded runtime, so other tasks go on), and so does a request that waits for a lock held across it (the one that keeps memberships in order). In S1a the primary never commits alone; restarting it without `--replication sync` is the operator's explicit choice, after which that standby must not be promoted. This also means a standby that takes an image holds up the primary's writes for as long as the image takes. S1b replaces both with the witness's in-sync set: a primary registered alone commits alone, and a standby catches up outside it. Without a witness, the kernel does the same for a primary's first standby (below).

## Without a witness: catching up and switching over

Decided (2026-11-06), so that a single kernel can be moved to another machine without a witness and without its writes waiting while the new standby takes its image.

**Catching up.** A primary without a witness starts *catching up* unless `<store>.standby-caught-up` exists: `Primary::wait` lets writes through without the standby, and raises `told_alone` to the newest one it let through. `ack` ends catching up, under the same lock, once the standby is connected, acknowledged the head it was sent, and holds `told_alone`: a write that passed `wait` alone after that head was read raised `told_alone` above it, so no write told alone is missing. Every write from then on waits. The stream notes it (`<store>.standby-caught-up`, synced) and only then sends the standby `StandbyInSync`, which notes `<store>.in-sync` beside its own store. A crash between the two leaves the standby without the note: not promotable, never wrong.

**Promotion** without a witness needs the standby's note: a standby never told it is in sync may lack writes told alone. The note stays across the standby's restarts (a standby whose primary was lost must stay promotable) and goes when it reaches a primary again (whose stream sends it anew once it caught up), takes an image, or starts over. Once caught up, the primary never commits alone again: without a witness nobody could tell later whether a standby promoted by hand holds everything. Deleting the primary's note to let a replacement catch up alone is the operator's explicit choice, after which the old standby must not be promoted.

**Switchover.** `Switchover` on a primary without a witness: refused unless the standby is in sync; then `kernel.switching` refuses new requests, running ones finish, the primary waits until the standby holds the head (at most 10 s, else it serves on), notes `<store>.handed-over` (synced), and sends `TakeOver { seq }` on the stream. The standby, holding exactly `seq`, answers `TookOver` and promotes itself. The old primary stops; started as a primary it refuses while the note exists, and starting it with `--follow` removes it. Handed over, it writes nothing at shutdown (no checkpoint, no listener flush), so its log stays the new primary's beginning; a record written after the handover anyway was never told (no standby acknowledges it), and makes it take an image. If the primary dies before `TakeOver` leaves, or the standby before it serves, the standby waits for its primary and is promoted by hand: it holds every record told.

**Tested** in `tests/replication.rs` (serving alone until caught up, P1 from then on, promotion refused without the note and allowed with it after a restart; a switchover, the old primary refusing to serve and following the new one) and in `tests/replication_crashes.rs` (crash points between catching up, the note and telling the standby; a switchover with a crash on either side after `TakeOver`).

## The standby while it follows

`kernable start --follow <address> --follow-key <key> --primary-kernel-key <pub>` with the usual store, vault, data, key and TLS flags (the connection to the primary uses `KERNABLE_TLS_*`, as the SDKs do). It serves no requests: every RPC but `ReplicationService` answers `UNAVAILABLE` with `not_primary` and the primary's address (a tower layer in front of the server); `/ready` answers 503; `/metrics` shows the record it applied and how far it is behind. It clears its snapshot store when it starts. While it applies an image, a marker file `<store>.image` exists; a standby that finds one starts over, empty.

## Promotion

`kernable promote <standby address> <key>`, signed over `kernable.v1.promote` by a key whose grant (presented, or replicated from the primary) allows `manage` for `*`. The standby refuses if it never followed a primary or its image is not complete. A standby that lost its primary can be promoted: the primary told nobody of a write the standby did not acknowledge. Then it stops following, closes its stores and opens them as a kernel does after a restart (the queue from `pending()`, listener positions, a new replay window, the vault's intents carried out); its checkpoints from then on are signed with its own kernel key (`verify` needs both public keys).

The old primary, if it still runs, gets no more acknowledgements, so it tells nobody of anything it writes from then on: it is fenced by P1. It rejoins only as a standby of the new primary; its unacknowledged tail makes its log diverge, so it is moved aside and filled from an image.

## The witness and pairs

Decided (2026-11-03): the witness assigns the roles (both kernels start with the same flags), and a primary whose standby is away or behind goes alone after `--standby-timeout-ms`.

**The witness** (`kernable witness`, `crates/kernable-kernel/src/witness.rs`) keeps `WitnessState { term, primary, primary_address, in_sync }` in one file, written atomically and flushed before it answers. It decides four requests (`WitnessService.Ask`), each signed by the node's key and presenting its grant (`replicate`); it keeps no grants, and does not see revocations.

| Request | Granted when | Effect |
|---|---|---|
| `LEASE` | the node is the primary, or nobody is (term 0: the first node becomes the primary of term 1) | the lease runs `--lease-ms` (3,000) from now |
| `ALONE` | the primary of the term asks, under its lease | `in_sync` empty: no standby may take over |
| `IN_SYNC` | the same | `in_sync` names the standby |
| `CLAIM` | the node is `in_sync`, and the lease expired a margin ago (a third of the lease) | term + 1, the node is the primary, `in_sync` empty |

A restarted witness grants no claim for a lease and the margin.

**A node** (`kernable start --witness`, `node.rs`) asks `LEASE` at start: granted, it opens as the primary; otherwise it follows `primary_address` as a standby, and claims once its stream to the primary has been lost for a lease and the margin; granted, it asks again and opens as the primary. The primary renews every third of the lease and counts its lease from when it asked, a margin short.

**Committing alone.** The primary's lease keeper watches its standby. When the standby is away or its acknowledgements stand still behind the head for `--standby-timeout-ms`, the keeper asks `ALONE`; only once granted does `Primary::wait` let writes through without the standby, and only while the lease holds. When the standby has caught up, the primary first waits for it again, then asks `IN_SYNC`: nothing told alone is missing on the standby the witness names.

**Fencing.** In sync, the standby's acknowledgements fence the primary: promoted, the standby no longer acknowledges, so the old primary tells nothing more, reachable by the witness or not. Alone, the lease fences it: it stops telling a margin before the witness could grant a claim. A primary whose renewal is refused was deposed: its lease future ends with an error and the process stops (exit code 1); a supervisor starts it again, and it follows the new primary.

**Tested** in `crates/kernable-kernel/tests/pair.rs`, with plugs in front of the sockets that cut every connection as a lost machine does: failover in about a second at a 600 ms lease, the new primary alone, the old primary back as its standby and in sync; a primary alone that tells nothing while it cannot renew its lease. The witness's decisions have unit tests.

## Tests under kill -9, and what they found

`crates/kernable-kernel/tests/replication_kills.rs` runs real kernel processes: a primary and a standby killed at random under load, then the primary lost for good: everything a client was told is on the standby, which is promoted and serves every order exactly once; and a pair with a witness whose primary is killed at random (30 kills: 14 to 15 failovers) with clients that try both kernels: every order exactly once, every answer the one a client was told, every store invariant. `make torture` runs them with more kills. `tests/pair.rs` also checks that a primary the witness replaced stops.

Found and fixed on the way:

- **A deadlock under load.** A grant was stored and waited for the standby under the grant lock; under load, every runtime thread blocked on that lock, and with them the stream to the standby. The lock is now taken only to look and to insert.
- **A primary that stopped answering at start** (found by the soak's pair, 2026-11-04). A second membership waited for the lock that keeps memberships in order, blocking its runtime thread in place, while the first held the lock and waited for the standby. Was that thread the last to poll the network, the others asleep, nobody read the standby's acknowledgement: the primary answered nothing more, for good. Under CPU and disk load about one start of the soak in five hung. A request that waits for that lock now hands its thread's tasks over first, as the wait for the standby does (`replication::blocking`); `tests/replication.rs` reproduces it on a primary of one runtime thread.
- **Streams ended by a race in SQLite.** A reader could see a record committed a moment before the head in memory moved past it; the next request then looked like one beyond the log, and the stream ended (with a reconnecting standby it could even be judged diverged). The rows decide now.
- **No failover under load.** The primary reported its standby in sync only when it had acknowledged the very head, which under steady load never happens: the witness never named it, and a claim was always refused. It now reports it once the standby holds everything that may have been told without it (the head when the primary stopped committing alone, or when it started); a primary alone waits for its standby again once it holds what existed at the last look.
- **Slow lookups in the file log.** Each read of the records after a standby's head read forward from the last mark, up to 1,023 records; the newest records' places are now kept as well.

Crash points (`replication.sent` on the primary, `standby.applied` and `standby.imaging` on the standby; `tests/replication_crashes.rs`, 8 rounds each without a loss) built the same evening. Revocations: a standby whose key is revoked loses its stream, and a primary whose in-sync standby is revoked commits alone first, so the witness never lets a revoked key take over (the witness itself keeps no revocations). Not built: replication in the model-based test and the soak, a second standby.

## Cost

Measured in `bench/results-2026-11-03-m2max-replication.txt` (both kernels on one machine and disk); the summary is in [Run a standby](../docs/how-to/standby.md#what-it-costs). Overlapping the primary's flush with the standby's was tried and dropped: on one disk it halved throughput, and it would send a standby to an image after an unlucky crash.

## Building it

| Increment | Content |
|---|---|
| 1 | Store: record log and hashes for both backends, `DataChanged`, `Store::apply`, records after a sequence number, the full image, `DataStore::keys`. In-process tests: random writes on one store, applied to another of each backend, give equal stores (`Log::check` and a comparison), also through an image |
| 2 | Proto and the primary: `Replication.Follow`, the `replicate` permission, vault values and files attached, `sync` waiting for acknowledgements, retention, metrics, `/ready` |
| 3 | The standby: `kernable standby`, applying and acknowledging, derived vault deletions, `not_primary`, divergence moved aside |
| 4 | `kernable promote`, the docs (a how-to, failures, architecture limits, compliance), the first P1–P4 tests |

## Cost

With `--sync-standby`, each commit waits for one more round trip; group commit shares it among concurrent commits, as it shares the flush. SQLite writes each record a second time into the record log while replication is on. Both are to be measured after increment 2.
