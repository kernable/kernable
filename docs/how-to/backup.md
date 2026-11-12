# Back up and restore

Read this before you run a kernel with data you cannot lose. The event store is the system of record: everything else either depends on it (the vault, the keys) or is rebuilt from it (snapshots, read models). To copy a whole kernel into one file, for another machine or a self-hosted kernel, use [export and import](./export).

## What to back up

| What | Flag (default) | Why | If lost |
|---|---|---|---|
| the event store | `--store` (`sqlite:kernable.db`) | commands, events, checkpoints, revocations, registrations of listeners and workers, applied memberships | everything is lost |
| the vault | `--vault-store` (`sqlite:vault.db`) | personal and secret values | personal data reads as `null` everywhere |
| the vault key | `--vault-key` | decrypts the vault | the vault cannot be read: same as losing it |
| files | `--data-store` (`file:data`) | files your processes put ([Store files](./data-and-cache)) | the files are gone |
| the kernel key | `--kernel-key` (`kernel.key`) | signs checkpoints and revocations | change it ([Keys](./keys#the-kernel-s-key)); keep every old public key for `kernable verify` |
| the root key | `--root` takes its `.pub` | signs grants and revocations | start with a new root and grant every key again ([Keys](./keys#the-root-key)) |
| process keys and `.grant` files | — | sign requests; say what each key may do | create new keys and grant them with the root key |
| snapshots | `--snapshots` (`sqlite:kernable-snapshots.db`, next to the event store) | a cache | nothing: workers rebuild them, the first command per aggregate reads all its events |
| the cache | `--cache-store` (`memory:`) | values that expire | nothing: it is in memory and empty after every restart |
| read model stores | in your processes | caches of your views | nothing: read models rebuild from the events |

With `kernable start --dev`, all of these are in the working directory: `kernable.db`, `kernable-snapshots.db`, `vault.db`, `data/`, and the keys `root`, `kernel`, `vault` and `app` (`.key` and `.pub`), with `app.grant`. Where each kind of data lives and how long it is kept: [Data over time](../concepts/data#where-kernable-keeps-what).

## Back up

1. **Back up the event store, then the vault**, right after each other. In that order, the worst case is a vault value that no event refers to yet, which does no harm.

   With SQLite, use SQLite's online backup; it is safe while the kernel runs:

   ```sh
   sqlite3 kernable.db ".backup backup/kernable.db"
   sqlite3 vault.db ".backup backup/vault.db"
   ```

   Do not `cp` a running SQLite store: the event store runs in WAL mode, and the latest commits are still in `kernable.db-wal`. To copy plain files, stop the kernel first.

   A `file:` event log can be copied while the kernel runs. The copy may end in a torn record; the kernel cuts it off when it opens the copy and keeps the cut bytes in `<path>.torn-<offset>`.

2. **Copy the data directory** (`data/` by default). Each file is written next to its place and then renamed, so a copy never holds half a file.

3. **Check the backup** of the event store:

   ```sh
   kernable verify sqlite:backup/kernable.db kernel.pub
   ```

   Name every public key the kernel ever had. It works on a copy, without a running kernel:

   ```text
   2311 events, 1903 commands, 41 checkpoints, 0 events after the last checkpoint
   OK
   ```

   `0 events` means the path is wrong: `verify` opens a missing SQLite file as an empty store. `OK, 1 suspect` (exit code 2) means the log is intact but something was signed by a revoked key; `TAMPERED` (exit code 1) means it is not ([Keys](./keys#revoking-a-stolen-key)).

Keep keys apart from the data, as you would keep any secret, and the vault key apart from the vault backups. Keep the root key offline; it is needed only to issue grants and revocations.

## Restore

1. **Stop the kernel.**
2. **Put back** the event store, the vault, the data directory, the keys and the `.grant` files.
3. **Delete the snapshot store** if it is a SQLite file (`--snapshots sqlite:…`). Snapshots newer than the restored log would not match it; workers rebuild them.
4. **Check the store:** `kernable verify <store> <kernel.pub>...` must say `OK`.
5. **Start the kernel** of the build that made the backup. A backup from an earlier development build does not carry over: before the first release, formats change in place and there are no migrations.
6. **Rebuild the read models that have a store** ([Rebuild a read model](./rebuild-read-model)): their stores may be ahead of the restored log. Read models in memory read every event again on start anyway.

Everything written after the backup was taken is lost: commands, events, erasures, files.

## Continuous backup

The steps above copy the stores now and then: what happened since the last copy is lost. A **backup follower** writes every record to object storage seconds after it is written, and `kernable restore` builds the stores again, up to the newest record or a point in time before something went wrong, such as a faulty deploy at 13:40.

1. **Start the kernel with `--backup on`.** It keeps its record log for the follower, as for a standby.
2. **Make a key for the follower and a backup key:**

   ```sh
   kernable keys new backup && kernable grant root.key backup.pub 'backup=*'
   kernable backup key backup-encryption.key
   ```

   The grant lets the key read every record and file; vault values reach it sealed with the kernel's vault key. The backup key encrypts everything the follower writes (AES-256-GCM): a bucket that leaks reveals nothing. Keep the backup key apart from the bucket, as you keep the root key; without it, no backup can be read.

3. **Run the follower**, next to the kernel or on another machine:

   ```sh
   AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… KERNABLE_S3_ENDPOINT=https://fsn1.your-objectstorage.com \
   kernable backup --follow https://10.0.0.1:7233,https://10.0.0.2:7233 --key backup.key \
     --to s3://acme-backups/shop --backup-key backup-encryption.key
   ```

   `--follow` names both kernels of a pair: the follower turns to whichever is the primary. `--to` is `s3://<bucket>/<prefix>` for AWS S3 and S3-compatible storage (Hetzner, MinIO with `KERNABLE_S3_PATH_STYLE=1`), or `file:<directory>`. `KERNABLE_S3_ENDPOINT` may carry a path (`https://storage.example.com/s3`); a bucket name with dots needs `KERNABLE_S3_PATH_STYLE=1` over HTTPS, as its name does not match the endpoint's certificate otherwise. The connection uses `KERNABLE_TLS_*`, as the SDKs do. To keep storage credentials off the machine, give it a [signer](#without-credentials-on-the-server-a-signer) instead.

What it writes, in **generations**: a base (a full image of the kernel, or nothing if the record log reaches back to the start), then a segment every `--segment-seconds` (default 5), 16 MiB or 4,096 records. The follower acknowledges records to the kernel only once their segment is in the target, so the kernel keeps them until then. A new generation starts every `--base-hours` (default 24), and the newest `--keep` (default 7) are kept. So a lost primary loses at most the last few seconds that were not uploaded yet, unless a [standby](./standby) holds them. The follower never holds up the kernel: it is not a standby, and a write never waits for it. Stopped and started again, it continues where its last segment ends; if the kernel no longer keeps those records, or its log differs from the backup's, a new generation begins.

`kernable backup list s3://acme-backups/shop --backup-key backup-encryption.key` shows the generations, each base's record and time, and how far its segments reach.

### Without credentials on the server: a signer

Storage credentials on a machine let whoever takes the machine read, overwrite or delete every backup they reach. To keep them off it, give the follower a **signer**: a small HTTP service, run where the credentials are, that presigns each request for it. The follower asks it right before every request to object storage, then sends the presigned request to the storage itself; the backup's bytes never pass through the signer.

```sh
kernable backup --follow https://10.0.0.1:7233,https://10.0.0.2:7233 --key backup.key \
  --to s3://acme-backups/shop --backup-key backup-encryption.key \
  --signer https://signer.example.com/backup
```

`--signer` (or `KERNABLE_S3_SIGNER`) takes the place of `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `KERNABLE_S3_ENDPOINT` and the other `KERNABLE_S3_*` variables: the follower needs none of them. It signs what it asks with its own key (`--key`; another one with `--signer-key <file>` or `KERNABLE_S3_SIGNER_KEY`), so the signer knows which instance asks. The signer's certificate is checked against the public roots, or only against the CA certificates in the PEM file `KERNABLE_S3_SIGNER_CA`. In a node's `[backup]`: `signer = "https://…"`. `kernable backup list`, `plan`, `check` and `kernable restore` take `--signer <url> --signer-key <file>` as well.

**While the signer is away** (or the storage), nothing is uploaded and nothing acknowledged, so the kernel keeps the records: up to a million beyond what a standby needs; a follower further behind takes a full image when it is back, a new generation. The follower logs, as an error, `the backup is falling behind: its target cannot be written`, with why (`the backup signer … cannot be reached`, `… refused …`, or the storage's answer), tries again after 1 s and then up to every 30 s, and notes `upload_error` and `failing_since` in its `--status-file`: alert on the log line or on `failing_since`. Once the signer answers again, the follower takes what the kernel kept, logs `the backup's target can be written again`, and clears both.

**The protocol.** For each request to object storage, the follower `POST`s JSON to the signer's URL:

```json
{
  "op": "put",
  "bucket": "acme-backups",
  "prefix": "shop",
  "name": "g1762862400000/seg-00000000000000000001-00000000000000000412-9f2c…",
  "continuation": "",
  "sha256": "<the body's SHA-256, hex>",
  "length": 52311,
  "issued_at": 1762862412345,
  "key_id": "<the key's ID>",
  "sig": "<the Ed25519 signature, hex>"
}
```

- `op`: `put` (a PUT of the object), `get` (a GET), `delete` (a DELETE), or `list`: a GET of the bucket with `list-type=2` (ListObjectsV2) for the objects whose keys start with `<prefix>/<name>` (`name` may be empty), after `continuation` (the `NextContinuationToken` of the page before; empty for the first page). Objects go whole: the follower sends no parts and asks for no ranges (a base part is at most 64 MiB).
- `name` is relative to `prefix`: the object's key is `<prefix>/<name>`.
- `sha256` and `length` describe the body a put sends; the other operations send none (the SHA-256 of nothing, 0).
- `sig` signs the message `kernable.v1.backup-object` ([Signatures](../reference/signing)) with the key `key_id` names; `issued_at` (ms) must be within 60 s of the signer's clock.

The signer checks the signature and the time, and decides whether that key may do `op` under that bucket and prefix: the kernel's grants play no part. It answers `200`:

```json
{
  "method": "PUT",
  "url": "https://fsn1.your-objectstorage.com/acme-backups/shop/g1762862400000/seg-…?X-Amz-Algorithm=AWS4-HMAC-SHA256&…&X-Amz-Signature=…",
  "headers": {},
  "expires_at": 1762862712345
}
```

The follower sends `method` to `url`, with `headers` (if any) and the body. `method` must be the operation's, and `expires_at` (ms) later than now; the follower keeps no answer, it asks again for every request. The URL may name the bucket in its path or its host. Any other status (with `{"error": "…"}`) means the signer refused (4xx) or failed (5xx): the follower logs it and tries again later.

**A reference signer** answers this for tests and for a trusted network: `kernable backup signer --listen 127.0.0.1:7390 --to s3://acme-backups/shop --allow backup.pub` signs with the credentials of its environment (as for `--to s3://`), for that prefix and the ones below it, for the keys `--allow` names (comma-separated `.pub` files), each request valid for `--expires-seconds` (default 300). It speaks plain HTTP: put TLS in front of it beyond one machine. A signing service for many instances does the same with one prefix per key.

### Restore from a continuous backup

```sh
kernable restore s3://acme-backups/shop --backup-key backup-encryption.key \
  --at 2026-11-04T13:39:00Z --store sqlite:kernable.db --vault-store sqlite:vault.db --vault-key vault.key
```

It fills new, empty stores from the newest generation that begins before `--at` (or `--at-record <n>`; without either, everything), and stops before the first batch the follower took after that time. If the backup ends earlier, or holds nothing between the end of that generation and the base of the next, it says so in a `note:` line and restores up to there. Then it checks the store like `kernable verify`. `--vault-key` must be the source's vault key of that generation's time: each generation's values are sealed with one vault key, which `kernable backup list` names, and a restore with another is refused before it writes anything. Then start the kernel on the restored stores and rebuild the read models that have a store, as in steps 5 and 6 of [Restore](#restore).

**A copy for a preview.** `--mask-vault --vault-key <new file>` restores a masked copy instead: the backup's events without its vault values, so personal data reads as `null`, with a new vault of its own; the source's vault key is not needed ([Previews and copies](./previews)).

**Erasures after the point.** Before it writes anything, a restore to a point lists the erasures the backup holds after it:

```text
2 erasures since 2026-11-04T13:39:00Z, in the backup after the point:
  2026-11-04T13:52:10Z  acme/c1  shop.customer c1 CustomerErased (command 5f0c…)
  2026-11-04T14:03:44Z  acme/c7  shop.customer c7 CustomerErased (command 91aa…)
```

Restored as it was, the history is exact: at that point, those people's data was not erased yet, so their values are back in the vault. Whether to erase them again is your decision, not the kernel's. By default the restore keeps them (and warns); answer `y` when it asks, or pass `--reapply-erasures erasures.json`, to apply them again: the restore writes the list to that file (readable by its owner only), and once the kernel runs,

```sh
kernable erasures apply 127.0.0.1:7233 support.key erasures.json
```

submits each erasing command again, signed with a key granted `submit` for those commands, with its original context (so the domain authorizes it as before) and the original command as its cause. The kernel commits new erasing events and erases, as for any [erasure request](./erasure). An erasing command that was queued before the point is in the restored store already, and the kernel runs it; one whose command carries vault values is listed, not submitted: send it again from your application. A privileged one (its context sets `bypass_authorization`) is marked (`"privileged": true` in the list): the key that submits it again needs a grant for `privileged=bypass_authorization` too. `kernable backup plan <target> --backup-key <file> --at <time>` shows the same list without restoring (`--json <file>` for scripts).

A **vault key that changes** (`kernable vault rekey`, or a kernel started with another key) begins a new backup generation: no generation needs two keys. Files you keep per vault key next to the backup, under `vault-keys/<key id>/` (an escrow of the key, say; the key ID is the one `kernable backup list` prints, the key ID of the key's `.pub`), are deleted by the follower with the last generation that needs that key. In a bucket with object lock or versioning, a delete adds a delete marker and the old versions stay until their retention ends, as the generations' objects do.

**After a restore**, start the backup follower against the restored kernel, with the same target. A generation only ever holds one history:

- **Restored to the end** of the newest generation, the store holds all of it, so the follower continues that generation: the new kernel's records chain on from its last one, in segments after the old ones.
- **Restored to an earlier point** (`--at` or `--at-record` before the end, or a generation that is not the newest), the generation holds records after the point that the restored kernel never gets, and the kernel writes other records at those positions. The follower finds that the kernel's log differs from the backup's (the kernel's log does not reach the generation's last record yet, or holds another record there) and begins a new generation; it never writes into the old one. The old one stays as it was, for a restore to a point in it, until `--keep` prunes it.
- **A new kernel key** (the restored kernel runs on a new server with a key of its own) begins no generation: the history goes on, and its later checkpoints are signed by the new key, as after a [change of the kernel key](./keys#the-kernel-s-key). A new generation would not help: its base holds the old checkpoints with their old signatures. A later restore names every kernel key with `--kernel-pub` (or none, to take the ones the backup names); one left out is named in the refusal.

### Check a backup

A backup is only as good as its last restore. `kernable backup check` reads the newest generation all through without writing a store: every object opens with the backup key and decodes, the base holds the records its marker names, and the segments' records chain from the base's hash to the last one without a gap. It prints the record a restore would reach and how old it is, and exits 1 if the backup does not check out:

```sh
kernable backup check s3://acme-backups/shop --backup-key backup-encryption.key
```

The follower runs it by itself with `--check-hours <h>` (`check-hours` in a node's `[backup]`) and logs `the backup checks out` or, as an error, why not: alert on that line. Restore into scratch stores now and then as well (`bench/backup-drill.sh` does it end to end): a check proves the bytes, a restore also the time it takes.

### What it loses, and how long the way back takes

Measured with `bench/backup-drill.sh` on a 2-core cloud server, the backup in object storage in the same region (results in `bench/results-2026-11-07-backup-drill.txt`): a kernel on a store of 200,000 commands takes 200 commands a second, and at a random moment the kernel, the follower and the clients are killed together.

| | Segments every 5 s | Segments every 1 s |
|---|---|---|
| Commands stored but not in the backup | 126 (0.6 s); 759 (3.8 s) in another run | 127 (0.6 s) |
| Restore of about 206,000 events, 250 MB, verify included | 63 s | 68 s |
| From the restore's start until a kernel on it answers | 64 s | 69 s |

A store five times as large (1,006,043 events, 1.17 GB) lost 559 commands (2.8 s) and was back after 319 s: the way back grows with the store, about linearly. So the point in time a restore reaches is at most `--segment-seconds` and an upload behind the loss. The restore's time grows with the store: most of it goes to applying the records and checking every signature (on every core). A [standby](./standby) loses nothing a client was told and takes over in seconds; the backup is for the machine and the standby lost together, for mistakes, and for keeping history elsewhere.

## Backups and erasure

A backup keeps personal data that was erased after it was taken. Keep vault backups only as long as your retention rules allow.

A restore from a continuous backup lists the erasures after its point and can apply them again ([above](#restore-from-a-continuous-backup)). For copies made by hand, the kernel does not repeat erasures by itself. If you restore a vault that is older than the event store, or older than an erasure that happened since, delete the erased values again. The log names every erased subject in the `erases` column of its erasing events. With the kernel stopped:

```sh
sqlite3 vault.db "ATTACH 'kernable.db' AS log;
  DELETE FROM vault WHERE secret = 0 AND EXISTS (
    SELECT 1 FROM log.events e WHERE e.erases = vault.subject AND e.tenant_id = vault.tenant
                                  AND e.created_at >= vault.created_at);"
```

This deletes the values of each erased subject that were written up to its erasure, and nothing written later. Then delete the snapshot store, as in step 3 above.

Files are never erased with a person; delete them yourself ([Handle an erasure request](./erasure)).
