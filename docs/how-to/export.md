# Move a kernel with export and import

`kernable export` writes everything a kernel holds into one file, and `kernable import` fills new stores from it. Use it to:

- **leave a hosted kernel** and run it yourself, with the same commands, events, vault values and files;
- **move to another machine**, or **change the store backend** (SQLite to the `file:` log, or back);
- **hand a copy to someone** who checks it with `kernable verify`, without access to your machines.

For regular backups, see [Back up and restore](./backup); an export is a full copy each time. For a preview, a copy without the personal data, import with `--mask-vault` ([Previews and copies](./previews)).

## What an export holds

| Part | In the export | Notes |
|---|---|---|
| the event store | every command, event, rejection, checkpoint, revocation, grant, membership, registration of listeners and workers, with their positions | as one consistent view |
| the vault | every value the store refers to | **still encrypted with the source's vault key**: the file holds no personal data in clear |
| files | every file of the data store | as stored |
| snapshots, the cache | no | workers rebuild snapshots; cache values expire |
| keys | no | the export names the kernel keys that signed its checkpoints, by key ID |

A file never holds a value that was erased before the export. It does hold values erased **after** it: delete an export once it is imported, as you would an old backup ([Backups and erasure](./backup#backups-and-erasure)).

## Export a running kernel

The kernel streams the export while it serves; nothing stops. The key needs a grant for `export`:

```sh
kernable keys new ops && kernable grant root.key ops.pub 'export=*'
kernable export 127.0.0.1:7233 ops.key kernable.kx
```

```text
exported 48112 records, 913 vault values and 27 files to kernable.kx
signed by the kernel keys 3f9c…
```

The kernel signs a checkpoint first, so every event in the file is anchored. Run it over mutual TLS (set `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT`, `KERNABLE_TLS_KEY`): the stream carries everything. A standby does not export; ask the primary.

The file is created readable by its owner only, never overwrites one, and is deleted if the export does not finish.

## Export stopped stores

With the kernel stopped, read the stores directly. The flags are those of `kernable start`, with its defaults:

```sh
kernable export kernable.kx --store sqlite:kernable.db --data-store file:data \
  --vault-store sqlite:vault.db --vault-key vault.key
```

Without `--vault-key` the vault is left out, which fails if the store refers to any vault value.

## Import

Import into **new, empty** stores, with the source's vault key:

```sh
kernable import kernable.kx --store file:kernable.log --data-store file:data \
  --vault-store sqlite:vault.db --vault-key vault.key --kernel-pub old-kernel.pub
```

```text
imported 48112 records, 913 vault values and 27 files
41870 events, 31402 commands, 412 checkpoints, 0 events after the last checkpoint
OK
```

The import reads the whole file once before it writes anything: a copy cut short or changed (the file ends with a SHA-256 of everything before it), a file of another format, or a vault key that does not open its values is refused. Then it checks the new store like `kernable verify`, with the same output and exit codes: `OK, 1 suspect` (exit 2) is what the source would show too.

Name the source's kernel keys with `--kernel-pub`. Without them, the store is checked with the keys the export names, which proves the file is consistent but not who signed it: compare the printed key IDs with the source's `.pub` files.

## Start the new kernel

```sh
kernable start --root root.pub --store file:kernable.log --vault-key vault.key ...
```

- **The root key:** grants are imported with the store and stay valid only under the root key that signed them. Start with the same `--root`, or grant every key again with a new one ([Keys](./keys)).
- **The kernel key:** use the source's, or a new one ([Keys](./keys#the-kernel-s-key)). Keep the source's `.pub` files: `kernable verify` needs every key that ever signed a checkpoint.
- **The vault key:** the import kept the values encrypted with the source's key, so the new kernel uses it. To change it afterwards, run `kernable vault rekey` ([CLI](../reference/cli)).
- **Listeners** keep their positions. Read models with their own store carry on where they were if they come along; new ones read every event from the start.
