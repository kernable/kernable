# Replication and the standby

Status: designed 2026-11-03. Increment 1 (the store layer) built 2026-11-03; increments 2–4 not yet. Invariants P1–P4 are in [invariants.md](invariants.md).

## Goal

A second kernel, the **standby**, holds every write the primary acknowledged. If the primary dies, an operator promotes the standby (`kernable promote`); it starts like a restarted kernel and loses nothing. Automatic failover through a witness is S1b, the SDKs' address lists are S1c.

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

- **Vault values.** No personal or secret value is ever written to a record log: an erasure could not reach it there. The primary attaches the values to the records that refer to them (an enqueued command's `vault_refs`, a commit's events and personal keys), read from its vault when it sends them. A value erased or dropped in the meantime travels as absent. The standby checks every value against its reference, encrypts it with its own key, and derives every deletion from the records, as the kernel does: a completed command drops its secrets, a committed erasing event erases its subject in its tenant, a commit that changes a personal key frees the old value.
- **File contents.** `DataChanged` names the file; the primary reads it when it sends the record. A file overwritten in the meantime travels in its newer version, a deleted one as absent; the following records converge the standby.
- **Snapshots, the cache, leases, `live` listeners and the replay window.** A promoted standby rebuilds or refuses them as after a restart. A standby clears its snapshot store when it starts following.

## The stream

A new kernel service, `Replication.Follow`, a bidirectional stream from the standby to the primary:

1. The standby says who it is (key ID, challenge as for workers and listeners) and where it stands: its last sequence number and hash, or none. Its key needs the new grant permission `replicate`, which gives as much as `vault_read`.
2. The primary answers one of:
   - **follow:** the standby's record exists in the primary's record log with the same hash; the records after it follow;
   - **image:** the standby is empty, or so far behind that the record is pruned; a full image follows (below), then the records after it;
   - **diverged:** the standby holds a record the primary does not have, or the hash differs. The standby moves its stores aside (`<path>.diverged-<ms>`, kept like `<path>.torn-<offset>`) and starts again, empty (P4).
3. Records follow in order, in batches within `--max-message-bytes`, with the vault values and files they need. The standby applies each batch, syncs its store with its own `--durability`, and acknowledges the last sequence number.

**Full image.** From one consistent view of the store. From SQLite, one read transaction gives the records in an order that applies: grants, revocations, memberships, workers, every command as enqueued, every commit in log order (each stream's newest keys and facts filed with the commit of its last event), commits without events, rejections, listeners with their parked events, checkpoints. The file log only grows, so its records up to an offset are its image. Either way it ends with the sequence number and hash it stands for, which the standby takes as the start of its own record log. The files of the data store follow from the kernel (increment 2). The primary keeps records from that point until the standby has acknowledged them.

## Applying records on the standby

`Store::apply(records)` does what the writes did on the primary and checks each record before it writes it: it follows the standby's head and its hash chains; a command is enqueued once; a commit completes a pending command, its event positions follow without gaps, every event hash matches its content and its predecessor, and committing them through the store's own commit gives exactly them (SQLite) or they continue the command's stream (file log); a checkpoint names an event the standby holds. A record that fails is refused as diverged and nothing of it is written (SQLite refuses the whole batch). The kernel checks the checkpoints' signatures against the primary's kernel key (`--primary-kernel-pub`, increment 3). A record the standby holds already has a sequence number at or below its head, so a resend after a crash between applying and acknowledging is refused as not following, and the standby asks again from its head.

## When a write counts (P1)

`Kernel::sync` already runs before anyone is told of a write, and the `durable` position keeps listeners, subscribers and workers from getting an event before it is on the disk. With `--sync-standby`, `sync` also waits until the standby has acknowledged every record written so far; `durable` advances only then. So no client, listener or worker hears of a write the standby does not hold.

Without a standby in sync, the primary waits: commits stay queued, submits time out (`--submit-timeout-ms`, the command still runs later), `/ready` answers 503, and the log says why. In S1a the primary never commits alone; restarting it without `--sync-standby` is the operator's explicit choice, after which that standby must not be promoted. S1b replaces this with the witness's in-sync set.

## The standby while it follows

`kernable standby --primary <address> --primary-kernel-pub <file>` with the usual store, vault, data, key and TLS flags. It serves no requests: every RPC answers `UNAVAILABLE` with `not_primary` and the primary's address; `/ready` answers 503; `/metrics` shows how far it is behind (records and milliseconds).

## Promotion

`kernable promote --kernel <standby address>`, signed by a key with `manage`. The standby refuses unless it is in sync: it completed a catch-up, applied every record it received, and was not following a diverged log. Then it stops following, opens its stores as a kernel does after a restart (the queue from `pending()`, listener positions, a new replay window), writes a checkpoint signed with its own kernel key (`verify` needs both public keys from then on), and starts serving.

The old primary, if it still runs, gets no more acknowledgements, so it tells nobody of anything it writes from then on: it is fenced by P1. It rejoins only as a standby of the new primary; its unacknowledged tail makes its log diverge, so it is moved aside and filled from an image.

## Building it

| Increment | Content |
|---|---|
| 1 | Store: record log and hashes for both backends, `DataChanged`, `Store::apply`, records after a sequence number, the full image, `DataStore::keys`. In-process tests: random writes on one store, applied to another of each backend, give equal stores (`Log::check` and a comparison), also through an image |
| 2 | Proto and the primary: `Replication.Follow`, the `replicate` permission, vault values and files attached, `sync` waiting for acknowledgements, retention, metrics, `/ready` |
| 3 | The standby: `kernable standby`, applying and acknowledging, derived vault deletions, `not_primary`, divergence moved aside |
| 4 | `kernable promote`, the docs (a how-to, failures, architecture limits, compliance), the first P1–P4 tests |

## Cost

With `--sync-standby`, each commit waits for one more round trip; group commit shares it among concurrent commits, as it shares the flush. SQLite writes each record a second time into the record log while replication is on. Both are to be measured after increment 2.
