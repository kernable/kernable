# Previews and copies

Use this page to give a preview of a new deployment, or a test, a copy of a production kernel's stores. A **masked copy** holds the events and the whole history, and none of the personal data: every personal value of the source reads as `null`, as after an [erasure](./erasure), and nothing in the copy can decrypt the source's vault. The preview writes freely to its copy, with personal data of its own, and is thrown away afterwards.

For a full copy that keeps the personal data, to move a kernel, use [export and import](./export) or a [restore](./backup#restore-from-a-continuous-backup) instead.

## What a masked copy holds

| Part | In the copy |
|---|---|
| commands, events, rejections, checkpoints, revocations, grants, memberships, registrations of workers and listeners with their positions, processes | all of them, as the source has them: the hash chain and the signatures verify |
| the vault | **none of the source's values**: a new, empty vault with a new vault key that the copy makes |
| files | none, unless you ask for them (`--with-files`, `--files-from`) |
| snapshots, the cache | none: workers rebuild snapshots from the copy's events, the cache starts empty |

The log only ever refers to vault values by salted hashes ([Signatures](../reference/signing)), so without the source's vault nothing in the copy leads back to a value. The copy is consistent at one position: the moment it was read (`kernable copy`, an export), or the record a restore reaches.

The copy's store is **marked** as a masked copy, with where it came from, when, up to which event, its own vault key's ID and the source's keys it was checked with. `kernable verify` says so, and so does the kernel UI.

## Make one

From any of three sources, into new, empty stores. `--instance-key` names a file that does not exist yet: the copy writes its own new instance key there (readable by its owner only, its `.pub` beside it), and creates the vault at `--vault-store` with the vault key derived from it. The other flags name the copy's stores as for `kernable start`, with its defaults.

**From a continuous backup**, the usual way for production: it does not touch the running kernel and needs no key of it, only the backup key, and works [through a signer](./backup#without-credentials-on-the-server-a-signer):

```sh
kernable restore s3://acme-backups/shop --backup-key backup-encryption.key \
  --signer https://signer.example.com/backup --signer-key preview.key \
  --mask-vault --store sqlite:preview/kernable.db --vault-store sqlite:preview/vault.db \
  --instance-key preview/instance.key --data-store file:preview/data
```

`--at <time>` or `--at-record <n>` copies the state at a point, as for any restore. The source's vault key is not needed: the backup's vault values are left out unread.

**From a store on this machine**, read in one consistent view, also while its kernel runs (a SQLite store):

```sh
kernable copy sqlite:kernable.db --mask-vault --store sqlite:preview/kernable.db \
  --vault-store sqlite:preview/vault.db --instance-key preview/instance.key
```

`--files-from file:data` copies the files of that data store too. The source's vault is never opened.

**From an export** of a running kernel ([export](./export#export-a-running-kernel)):

```sh
kernable export 127.0.0.1:7233 ops.key shop.kx
kernable import shop.kx --mask-vault --store sqlite:preview/kernable.db \
  --vault-store sqlite:preview/vault.db --instance-key preview/instance.key
rm shop.kx
```

The export file holds the vault values sealed with the source's vault key; the masked import leaves them out. Delete the file afterwards, as you would an old backup.

Each prints what it copied and the mark, then checks the copy like `kernable verify`, with the same output and exit codes:

```text
restored generation g1762862400000 as a masked copy: base at record 41200, up to record 48112 (taken 2026-11-12T09:14:03Z)
masked copy of backup s3://acme-backups/shop, generation g1762862400000, record 48112, copied 2026-11-12T09:15:40Z up to event 41870: no vault value of the source, so its personal values read as null. ...
41870 events, 31402 commands, 412 checkpoints, 0 events after the last checkpoint
OK
```

Files are left out by default because files may hold personal data that neither an erasure nor masking reaches ([Personal data in files](./data-and-cache#personal-data-in-files)). Add `--with-files` (`restore`, `import`) or `--files-from` (`copy`) only for files you know hold none.

## Run the preview

Start a kernel on the copy with the copy's instance key and a **new snapshot store**:

```sh
kernable start --root root.pub --store sqlite:preview/kernable.db \
  --vault-store sqlite:preview/vault.db --instance-key preview/instance.key \
  --snapshots sqlite:preview/snapshots.db --data-store file:preview/data
```

The kernel finds the mark and runs the store as a masked copy (it logs `running a masked copy`):

- **Personal values of the source read as `null`**, in workers' histories, for read models with `vault_read`, everywhere, exactly as after an erasure. Unique keys built from personal data are free, as after an erasure. Write handlers and read models that cope with an erased value (`Personal<string>` in TypeScript), and they cope with a copy too.
- **New personal values work end to end.** Commands and events of the preview keep their vault values in the copy's vault, under its key: sign-ups, emails, erasures of the preview's own test people.
- **Read models rebuild** from the copy's events. Run the preview's processes with a [deployment name](./deployments) of its own and new read model stores; a read model that kept its store from production would hold production's personal data.
- **Its own key.** The preview's kernel runs with the instance key the copy made, never production's. Production's checkpoints came along, signed by production's key; the preview's kernel goes on from them and signs its own with its key. The copy's mark names the keys it was checked with (production's, by key ID, which is the public key), so `kernable verify preview/kernable.db preview/instance.pub` checks both: the preview's checkpoints with the key you give, production's with the keys the mark names, and says which those are. Compare them with production's `instance.pub`; to check against production's file itself, give both. The mark is not signed: it tells which keys to check with, it does not vouch for them.
- **Reactors go on after production's position**: the registrations came along, so a reactor does not act again on what production handled. Point the preview's outbound connections (mail, payments, webhooks) at test accounts.

## What it refuses

A masked copy never becomes anything but a copy:

- **It runs alone.** The kernel refuses to start on it with `--follow`, `--witness`, `--replication sync`, `--backup on` or `--partition`: nothing replicates, backs up or moves a copy, and a copy is never a standby of anything.
- **Only with its own vault key.** A kernel started on it with another vault key (production's, say) refuses to start, naming the key the copy made.
- **Only with its own snapshots.** A snapshot store that holds snapshots from before the copy was made (production's, which may hold personal values in clear) is refused.
- **A copy of a copy is masked again.** An export of a copy imports, and a store that holds a copy's mark restores, only with `--mask-vault`, which leaves the copy's own vault values out too and marks it anew. A standby refuses records that carry a copy's mark.

## What masking does not reach

Masking removes the vault; it cannot remove what was never in it. These come along in the copy as they are in the source, as they stay after an [erasure](./erasure#what-stays):

- values that a client sent in clear because it lacked the domain's declarations, and fields not marked personal;
- IDs (aggregate, command, subject) and the request context of commands, such as `senderIdentity`;
- the reasons of rejections, if your code put personal data into them;
- files, if you copy them.

The source's erasures stay visible in the copy: the erasing events, with the subject they erased, are part of the history and verify. The proofs in the source's vault (`erasures`) are not copied; the copy has no value to erase. Erasures in the preview are logged in the preview's vault as anywhere.

## Size and time

A copy streams: it reads the source in batches (up to 512 records or 1 MiB each) and applies each to the new store, so memory stays at a batch whatever a SQLite store's size, and the disk needs the copy's size once (`import` also the export file's). It applies and checks every record as a restore does, so it takes about as long as a restore of the same store: about a minute for 200,000 events and five for a million on a 2-core server ([measured restores](./backup#what-it-loses-and-how-long-the-way-back-takes)), most of it applying the records and checking every signature.

- `kernable copy` holds one read transaction on a SQLite source for the whole copy: the running kernel goes on writing, but its write-ahead log cannot be folded back until the copy ends and grows meanwhile. For a large production store, copy from the backup instead.
- A copy needs new, empty target stores; delete a half-made copy and start again.
- A copy is not incremental: a fresh preview is a fresh copy. Copy-on-write snapshots of the file system may make copies near-instant later.

Throw a preview away by stopping its kernel and deleting its stores, its vault and its vault key. Nothing in production refers to it.
