# Run read-only copies

A **reader** is a further kernel that holds a full copy of the writer's store and answers queries from it. Each reader has its own copy of your code beside it (read models, middlewares, the HTTP endpoints of your services), so queries, live subscriptions and the reads of your API spread over as many machines as you run readers, while every write still goes through the one writer.

What you get:

- **One writer, any number of read-only copies.** The writer never waits for a reader; a [standby](./standby) stays the failover partner. Add and remove readers while everything runs.
- **Saved means visible, on every copy.** A command sent to a reader runs on the writer, and the reader answers once its own copy holds the events. A query carries the client's newest position, and a reader answers it only once it holds that position.
- **Nothing happens twice.** Reactors, workers, timers, scheduled commands and business processes act on the writer only. The same code runs beside a reader; there its reactors and workers wait.
- **Rights and personal data are checked where the copy is.** Grants, revocations, memberships and the vault values come with the records: a reader checks a key and its middlewares run on its own.

## How it works

A reader follows the writer over the same stream a standby uses: every record of the writer's store, in order, chained by a hash, with the vault values and files they refer to. It checks each record before it writes it to its own SQLite store, and encrypts the vault values with its own vault key. It gets only what the writer may tell: with a standby, only what the standby holds, so a failover never takes back anything a reader has.

| Request on a reader | What it does |
|---|---|
| Query | answered by a read model connected to the reader, after the reader's middlewares checked it; waits until the reader holds `min_position` (up to `--query-timeout-ms`) |
| Event subscription, live listener | from the reader's copy |
| Read model, middleware | connect to the reader as to any kernel; positions are kept in memory (a read model with a store resumes from its own) |
| Reactor | waits, connected: the reader answers `Waiting` with `reader_of`, the writer's address |
| Worker (command handler) | registers and gets no commands (`Registered.reader_of`) |
| Command | forwarded to the writer as it came (signature, ID, vault values); answered once the reader holds the events, and for a settling submit once the reader's listeners processed them too: the writer's listeners settle first, then the reader's, so a settling submit through a reader may take up to two `--settle-timeout-ms`. `Committed.position` says where they are. |
| Files, cache, memberships, revocations, processes, partition maps, exports | forwarded to the writer as they come |

A reader is never promoted and keeps no followers of its own (no standby, backup follower or readers).

## 1. Let the writer keep readers

Start the writer with `--readers on`; with a standby, give both kernels of the pair the flag, since either may be the writer:

```sh
kernable start --root root.pub --readers on --replication sync \
  --listen 10.0.0.1:7233 --tls-cert … --tls-key … --tls-client-ca … --vault-key vault.key
```

The writer then keeps its record log (as for a standby or a backup), and SQLite keeps what a reader that is away needs, a million records at most. A reader further behind starts anew.

## 2. Keys

Each reader needs a key granted `read`, and keys of its own for its vault and its checkpoints:

```sh
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*'
kernable keys new reader-1-kernel
kernable keys new reader-1-vault
```

`read` lets a key receive everything, every vault value in plaintext included: keep the reader's key as carefully as the root key, and run readers over mutual TLS only (see [Mutual TLS](./tls)). Revoke it and its stream ends. Unlike `replicate`, a standby's action, `read` never lets the key follow as a standby, so a reader's key cannot take the standby's place in a pair (and a standby's key does not follow as a reader: each role has its own key).

## 3. Start a reader

```sh
KERNABLE_TLS_CA=ca.tls.crt KERNABLE_TLS_CERT=reader-1.tls.crt KERNABLE_TLS_KEY=reader-1.tls.key \
kernable start --root root.pub \
  --reader 10.0.0.1:7233,10.0.0.2:7233 --follow-key reader-1.key \
  --primary-kernel-key kernel-a.pub --primary-kernel-key kernel-b.pub \
  --kernel-key reader-1-kernel.key --vault-key reader-1-vault.key \
  --listen 10.0.0.5:7233 --tls-cert … --tls-key … --tls-client-ca …
```

- `--reader` names the writer, or both kernels of a pair: the reader follows whichever is the primary (a standby answers `not_primary`, and it turns to the next address).
- `--primary-kernel-key`: the writers' kernel keys, as for a standby; the reader checks their checkpoints with them.
- The connection to the writer uses the `KERNABLE_TLS_*` variables, as the SDKs do.

A new reader first catches up: it takes a full image of the writer (or a backup, below), then follows, and only then serves. Meanwhile every request gets `UNAVAILABLE` `reader_catching_up` and `/ready` answers 503.

### Start from a backup

A full image comes from the writer and loads it. A new reader can start from the writer's [continuous backup](./backup) instead and take only the rest from the writer:

```sh
kernable start … --reader 10.0.0.1:7233,10.0.0.2:7233 \
  --from-backup s3://backups/shop --from-backup-key backup.key --from-backup-vault-key vault.key
```

- `--from-backup` takes a target as `kernable restore` does, with the same `KERNABLE_S3_*` and `AWS_*` variables. With a [backup signer](./backup#continuous-backup) (`KERNABLE_S3_SIGNER`, and `KERNABLE_S3_SIGNER_CA` for its certificate) the reader holds no S3 keys: it asks the signer with `KERNABLE_S3_SIGNER_KEY`, or else with its own `--follow-key`, which the signer must sign for.
- `--from-backup-vault-key` is the writer's vault key of the time the backup was taken: its values are encrypted with it. The reader encrypts them with its own `--vault-key` right after the restore, so the writer's key stays only where you keep it.
- It is used only by a reader whose store is empty, or one the writer no longer keeps the records for. If the backup cannot be read, or the writer no longer holds the records after it, the reader takes a full image from the writer instead.

## 4. Your code beside a reader

Run the same processes beside each reader as beside the writer, and point them at their reader:

```sh
KERNABLE_INSTANCE=reader-1 KERNABLE_URL=https://10.0.0.5:7233 node app.ts
```

- **Commands go through the reader**, not straight to the writer: the reader answers once its own copy holds the events, so the query that follows sees them, on this reader. Going straight to the writer would be as correct but leaves the next query on the reader waiting for the copy.
- **A settling command takes longer through a reader.** With `wait: settled` the writer waits for its listeners (up to `--settle-timeout-ms` on the writer), then the reader for its own (up to `--settle-timeout-ms` on the reader): up to two settle timeouts in all. Give the client's own deadline room for both.
- **Read your writes holds across copies.** Every SDK keeps the newest position its own commands committed at and sends it with its queries (`min_position`), so a query that a different copy answers waits until that copy holds it. Across processes (a browser that talks to two of your servers), pass the position on yourself: it is the last event's `globalPosition` (`global_position`) of the outcome.
- **Address lists still work.** `KERNABLE_URL=https://10.0.0.5:7233,https://10.0.0.6:7233` lets a process turn to another reader (or the writer) when its own is away or behind: a reader answers `UNAVAILABLE` (`reader_behind`, `writer_away`, `reader_catching_up`), and the SDKs try the next address.
- Read models and live listeners connect to the reader and build their state from its copy. Give them an instance ID (`KERNABLE_INSTANCE`) as on a [pair](./instances); a read model in memory rebuilds from the first event, one with a store resumes from its own position.
- **Middlewares** run on the reader too: a reader knows the writer's middlewares and refuses what they check until the same middleware is connected to it (they fail closed, as on the writer).
- Reactors and workers that connect to a reader wait; the SDKs log `waiting: this kernel is a reader` and `connected to a reader`. Those beside the writer do the work.

## Watch it

| Where | What |
|---|---|
| `/ready` on a reader | 200 once it follows its writer (heard from it within 5 s: the writer sends a beat every second) and every listener and middleware its writer's memberships list has connected here and caught up (a reactor counts once it connected and waits); 503 with why otherwise, so no query reaches a reader before the code that answers it |
| `/health` on a reader | `ok: a reader of <writer>, following, <n> records behind` |
| `kernable_reader_following`, `kernable_reader_lag`, `kernable_reader_applied`, `kernable_reader_silent_ms` | on the reader |
| `kernable_readers`, `kernable_readers_lag_max` | on the writer |
| `GET /api/role` on the local socket (`--admin-socket`) | `{"role": "reader", "state": "following", "writer": …, "lag": …}`, or on the writer `{"role": "writer", "standby": …, "readers": [...]}` |

A load balancer in front of the readers routes by `/ready`.

## When something fails

| When | What happens |
|---|---|
| The writer is away | The reader answers queries from its copy and refuses commands with `UNAVAILABLE` `writer_away` (the SDKs try again). It keeps trying every address. `/ready` answers 503 after 5 s. |
| The writer fails over to its standby | The reader turns to the new primary and follows on from where it is: it never had anything the standby lacked. Commands sent meanwhile are answered `UNAVAILABLE`, and resubmitted by the SDKs (idempotent by ID). |
| A reader falls behind | Queries with a position it does not hold wait, then get `UNAVAILABLE` `reader_behind` after `--query-timeout-ms`; with several addresses the SDKs try the next copy. |
| The writer or the relay refuses the reader (its grant, its root not in a relay's access list, its key revoked) | It logs the reason once as an error, keeps asking (at most every 60 s) and follows as soon as it is let in, without a restart; meanwhile `/ready` answers 503 with the reason. A new reader that has no copy key yet asks for it the same way. |
| A reader stays behind | Once it has not held everything its writer may tell for longer than `--reader-max-lag-ms` (30 s by default), it refuses every query with `UNAVAILABLE` `reader_behind` and `/ready` answers 503, until it caught up again: no client reads data that old. `0` turns the limit off. |
| A reader's copy no longer continues the writer's log (it was away beyond what the writer keeps, or its log differs) | The reader stops with an error. Started again (by `kernable node` or your supervisor), it catches up anew: from its backup, if given, otherwise from an image. |
| A reader is lost | Nothing on the writer waits for it. Start a new one. |

## What it costs

The writer sends each record once more per reader, from its record log (SQLite writes each record a second time while it keeps one). Nothing on the writer waits for a reader. Measure on your own machines: a reader that keeps up uses about what the writer's read side does.
