# Back up and restore

Read this before you run a kernel with data you cannot lose. The event store is the system of record: everything else either depends on it (the vault, the keys) or is rebuilt from it (snapshots, read models).

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
5. **Start the kernel.** It refuses a store of another data format: a backup from an earlier development build of kernable does not carry over, as there are no migrations before the first release.
6. **Rebuild the read models that have a store** ([Rebuild a read model](./rebuild-read-model)): their stores may be ahead of the restored log. Read models in memory read every event again on start anyway.

Everything written after the backup was taken is lost: commands, events, erasures, files.

## Backups and erasure

A backup keeps personal data that was erased after it was taken. Keep vault backups only as long as your retention rules allow.

The kernel does not repeat erasures by itself. If you restore a vault that is older than the event store, or older than an erasure that happened since, delete the erased values again. The log names every erased subject in the `erases` column of its erasing events. With the kernel stopped:

```sh
sqlite3 vault.db "ATTACH 'kernable.db' AS log;
  DELETE FROM vault WHERE secret = 0 AND EXISTS (
    SELECT 1 FROM log.events e WHERE e.erases = vault.subject AND e.tenant_id = vault.tenant
                                  AND e.created_at >= vault.created_at);"
```

This deletes the values of each erased subject that were written up to its erasure, and nothing written later. Then delete the snapshot store, as in step 3 above.

Files are never erased with a person; delete them yourself ([Handle an erasure request](./erasure)).
