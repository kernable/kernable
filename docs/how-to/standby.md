# Run a standby

A **standby** is a second kernel that holds every write the primary told anyone about. If the primary's machine is lost, you promote the standby and it serves on, without losing a committed command, an acknowledged listener position or a confirmed file.

What you get:

- **Nothing told is lost.** The primary answers a client, delivers an event, or confirms a file only once the standby holds it on its disk.
- **Automatic failover with a witness**, a small third process: the standby takes the primary's place within seconds, and the old primary comes back as its standby by itself. Without a witness, `kernable promote` does it by hand.
- **Clients find the primary themselves.** Give every process both kernels' addresses, comma-separated (`KERNABLE_URL=https://10.0.0.1:7233,https://10.0.0.2:7233`): a request or stream that meets the standby (`not_primary`) or a kernel that is gone goes to the other one. Commands are idempotent by ID, so a resubmit after a failover gets the original outcome.

Two ways to run it: [a pair with a witness](#a-pair-with-a-witness) (recommended), or [a fixed primary and standby](#a-fixed-primary-and-standby) that you promote by hand.

## How it works

The primary sends the standby every record of its store, in order and chained by a hash, with the vault values and files they refer to. The standby checks every record before it writes it, and acknowledges what is on its disk. With `--replication sync` the primary waits for that acknowledgement before it tells anyone of a write. Design: `design/replication.md`.

The standby serves no requests: every call gets `UNAVAILABLE` with `not_primary`, and its `/ready` answers 503. The SDKs retry `UNAVAILABLE` by themselves.

## A pair with a witness

Three machines: two kernels and the witness. The witness keeps only who is the primary and whether its standby is in sync, in one small file; it needs no store and little of anything.

```sh
kernable keys new node-a && kernable grant root.key node-a.pub 'replicate=*'
kernable keys new node-b && kernable grant root.key node-b.pub 'replicate=*'

# machine W
kernable witness --root root.pub --listen 10.0.0.9:7300 --state witness.state \
  --tls-cert … --tls-key … --tls-client-ca …

# machines A and B, the same flags but their own keys, stores and addresses
kernable start --root root.pub --witness 10.0.0.9:7300 \
  --node-key node-a.key --advertise 10.0.0.1:7233 \
  --primary-kernel-key kernel-a.pub --primary-kernel-key kernel-b.pub \
  --kernel-key kernel-a.key --vault-key vault-a.key \
  --listen 10.0.0.1:7233 --tls-cert … --tls-key … --tls-client-ca …
```

- Start the kernel that holds the data first: on a new witness, the first kernel to ask becomes the primary of term 1. The other one follows it, taking an image if it is empty.
- `--advertise` is where the other kernel reaches this one; `--primary-kernel-key` names both kernels' kernel keys, since either may be the primary.
- The kernels reach the witness and each other with the `KERNABLE_TLS_*` variables, as the SDKs do.

**What happens:**

| When | What the pair does |
|---|---|
| Both run | The primary renews its lease with the witness every second (`--lease-ms`, 3,000 by default, at the witness). Writes wait for the standby. |
| The primary's machine is lost | The standby loses its stream. Once the primary's lease has expired at the witness (3 s, plus 1 s for clocks), the standby claims its place, becomes the primary of the next term, and serves. |
| The standby is away or behind for `--standby-timeout-ms` (5 s) | The primary tells the witness, then commits alone. The witness no longer lets the standby take over: it lacks those writes. Back, the standby catches up; the primary waits for it again, then tells the witness it is in sync. |
| The old primary comes back | The witness names the new primary; the old one follows it. Writes it never told anyone of make its log differ; it moves its stores aside and takes an image. |
| The primary loses the witness | In sync, it goes on: the standby's acknowledgements fence it. Alone, it stops telling writes once its lease has run out (a margin before the witness would let anyone take over), until it reaches the witness again. |
| The witness is down | No failover, and the primary cannot go alone; writes with the standby in sync go on. |
| A primary learns it was replaced | It stops with an error. Run kernels under a supervisor (systemd, Kubernetes): started again, it follows the new primary. |

`kernable promote` on the standby of a pair asks the witness, too: it is refused while the primary holds its lease.

Keep the witness on a third machine, so a lost machine never takes the witness and a kernel down together. Its state file decides who is the primary: back it up, and never start a pair against a new, empty witness while the old primary may still run.

### Update a pair without downtime

With `kernable switchover`, the primary hands its role over to the standby at once, without waiting for its lease to run out:

1. **Update the standby**: stop it, install the new binary, start it again; it catches up and is in sync again (`kernable_standby_in_sync` on the primary).
2. **Switch over**, with a key granted `manage` for `*`:

   ```sh
   kernable switchover https://10.0.0.1:7233 admin.key
   ```

   The primary refuses new requests (`UNAVAILABLE`: clients with both addresses turn to the other kernel), lets running ones finish, waits until the standby holds every record, and tells the witness. The standby serves at once, without losing a told write; the old primary stops.
3. **Update the old primary** and start it: it follows the new primary as its standby.

If the standby is not in sync, or does not hold everything within 10 s, the switchover is refused and the primary serves on. Under [`kernable node`](./node), the stopped kernel is started again by itself.

## A fixed primary and standby

Without a witness, the roles stay as you start them, and you promote the standby by hand.

## 1. Keys

The standby needs three keys of its own:

```sh
kernable keys new standby          # proves the standby to the primary
kernable grant root.key standby.pub 'replicate=*'
kernable keys new standby-kernel   # signs the checkpoints it writes once promoted
kernable keys new standby-vault    # encrypts its vault
```

`replicate` lets a key receive everything: every event, command and file, and every vault value in plaintext. Keep `standby.key` as carefully as the root key, and run replication over mutual TLS only (see [Mutual TLS](./tls)). Revoke it, and the standby's stream ends at once; in a pair, the primary commits alone, so the revoked key can never take its place.

The standby encrypts its vault with its own key; no secret is copied between the machines.

## 2. Start the primary with `--replication sync`

```sh
kernable start --root root.pub --replication sync \
  --listen 10.0.0.1:7233 --tls-cert … --tls-key … --tls-client-ca … \
  --vault-key vault.key
```

Until a standby is in sync, writes wait and `/ready` answers `waiting for a standby in sync`.

## 3. Start the standby

On the second machine, with its own stores:

```sh
KERNABLE_TLS_CA=ca.crt KERNABLE_TLS_CERT=standby.crt KERNABLE_TLS_KEY=standby.tls.key \
kernable start --root root.pub \
  --follow 10.0.0.1:7233 --follow-key standby.key --primary-kernel-key kernel.pub \
  --kernel-key standby-kernel.key --vault-key standby-vault.key \
  --listen 10.0.0.2:7233 --tls-cert … --tls-key … --tls-client-ca …
```

- `--follow` is the primary's address; the connection uses the `KERNABLE_TLS_*` variables, as the SDKs do.
- `--primary-kernel-key` is the primary's kernel public key: the standby checks the primary's checkpoints with it. Give every key the primary had, if it changed its key.
- Start it with the flags it should have as the primary later, such as `--replication sync` if it should wait for a standby of its own once promoted.

An **empty** standby takes a full image of the primary first, then follows. While it takes the image, the primary's writes wait. With a large store, add the standby in a quiet hour. A standby that was away only briefly catches up from where it stopped: the file log keeps every record, and SQLite keeps 100,000 records beyond what the standby acknowledged.

## 4. Watch it

| Metric | On | Meaning |
|---|---|---|
| `kernable_standby_in_sync` | primary | 1 while a standby is connected and has caught up |
| `kernable_standby_acknowledged`, `kernable_record_head` | primary | the newest record the standby holds, and the newest written; writes in between wait |
| `kernable_standby_lag` | standby | records behind the primary's head as last seen |

Alert when `kernable_standby_in_sync` is 0: the primary's writes wait. The primary also logs `a write waits for the standby` every 10 s while one waits.

## When the standby is away

The primary's writes wait. Commands stay queued and submits time out (`--submit-timeout-ms`), but nothing is lost: once the standby is back, it catches up and the queued commands run.

If the standby is lost for good and the primary must serve alone, restart the primary without `--replication sync`. From then on, that standby must not be promoted: it lacks what the primary writes alone. When you start it again (or a new one), it notices and takes an image.

## Promote the standby

Only when the primary is gone, or must be replaced:

```sh
kernable promote 10.0.0.2:7233 ops.key    # ops.key: granted manage for *
# promoted 10.0.0.2:7233: it serves as the primary from record 48213
```

The standby stops following, opens its stores as a kernel does after a restart, and serves: queued commands run, listeners resume after their stored positions, and a client that resubmits a command gets the original outcome. Snapshots start empty: the first command per aggregate reads its whole history.

Processes given both addresses turn to it by themselves. Others: change the address they use (DNS, a load balancer that checks `/ready`), and restart them if they do not pick up the change.

The standby refuses promotion if it never followed a primary, or if it is in the middle of an image.

## Bring the old primary back

Never start the old primary as a primary again. Start it as a standby of the new one, with `--follow` and the new primary's kernel key. It may hold writes it never told anyone of; then its log differs from the new primary's. It moves its stores aside (`<file>.diverged-<ms>`) and takes an image. The files it moves aside can hold personal data: delete them once you know nothing in them is needed.

A primary that still ran when you promoted its standby tells nobody of anything it writes from then on: no standby acknowledges it any more.

## What it costs

Every write waits for one more round trip: to the standby, its own flush, and back. Measured on one Apple M2 Max with both kernels on the same machine and disk, a Rust worker, 1 KB payloads, `wait: committed` (`bench/replication.sh`, results in `bench/`):

| Store, durability | Alone, 1 client | With a standby, 1 client | Alone, 64 clients | With a standby, 64 clients |
|---|---|---|---|---|
| SQLite, `full` | 167/s | 84/s | 3,094/s | 659/s |
| SQLite, `full`, standby `normal` | 161/s | 153/s | 3,042/s | 2,018/s |
| SQLite, `normal` | 2,823/s | 1,702/s | 7,154/s | 4,090/s |
| file log, `normal` | 3,711/s | 2,535/s | 12,833/s | 7,253/s |

With `full` durability on one disk, the two kernels' flushes queue behind each other; the row with a `normal` standby shows the cost without that, which is closer to two machines with their own disks. Measure on your own machines before you size them.

## A standby is not a backup

A standby copies every write, mistakes included, within milliseconds. Keep your [backups](./backup).
