# Link your kernels

Your project's writer sends a sealed copy to kernable Cloud; readers elsewhere follow it. This
page sets up both with the open `kernable` binary.

No certificates: the relay knows your pusher and your readers by their keys, and lets in only the
keys you name for each partition.

## 1. Keys

Every kernel has one key of its own, made on its machine; only its public half (`.pub`, 64 hex
digits) ever reaches kernable Cloud. The kernel signs with it, follows and pushes with it, and
its vault key is derived from it. On the writer's machine, its key, granted `read` by your root
key (which stays offline) so its pusher may follow it, and the partition's copy key:

```sh
kernable keys new writer && kernable grant root.key writer.pub 'read=*'
kernable relay key copy.key
```

`copy.key` seals everything that leaves the writer. It goes to the pusher and to every reader of
the partition, **by your own means** (a secrets manager, configuration management, a file copy):
kernable Cloud never sees it and never hands it out. Whoever holds it reads everything the writer
tells a reader: keep it as carefully as a reader's key. One copy key per partition.

## 2. The project's root key

A project has one root key: grants signed by it decide what every key may do in your kernels,
and it is what kernable Cloud gives the project's [hosted kernel](/cloud/hosted-kernels). The
relay does not use it: it knows keys only by the list you keep for each partition (below). Tell
kernable Cloud its public half once (admins and owners):

```sh
kernable cloud projects root shop set root.pub
# shop: root 81c2d4e5f6a7…
```

Or in the console, on the project's page under **Root key**. Only `root.pub` (64 hex digits)
leaves your machine; the command refuses a `.key`.

### Replacing it

When the root key changes (someone who held it leaves, or it was lost), set the new one. The old
root stays valid beside it while you sign your grants anew with the new root, until you finish
the replacement, or for 7 days at most; then only the new one counts:

```sh
kernable cloud projects root shop set new-root.pub
#   the old root 81c2d4e5f6a7… stays valid until 2026-12-01 10:31 UTC, or until: kernable cloud projects root shop finish
kernable grant new-root.key pusher.pub 'read=*'     # and every other key, anew
kernable cloud projects root shop finish
```

If the root key **leaked**, end it at once: `kernable cloud projects root shop set new-root.pub
--now` (in the console: **Replace now**). Every change is in the team's audit log.

## 3. Link a partition

Tell kernable Cloud which kernel may push a partition's copy, by its public key:

```sh
kernable cloud projects link shop eu --pusher writer.pub
# acme.shop.eu: linked; push it with: kernable push … --to https://relay-eu.kernable.cloud:7400 --copy acme.shop.eu
```

Or in the console, on the project's page under **Linked kernels**. Developers and up link; the
relay lets the pusher in within seconds. Only the `.pub` files or the key IDs (64 hex digits)
leave your machine; the command refuses a `.key`.

`kernable cloud projects links shop` lists the links, each key with its role and state.
`kernable cloud projects unlink shop eu` (admins and owners) ends one at once: nothing pushes to
or reads the copy from then on. The copy and its backups are kept 30 days, and linking the
partition again brings it back; after that they are deleted.

## 4. The writer's node

The writer runs with `--readers on` and `--instance-key writer.key`; a `[push]` section in its
`project.toml` runs the pusher beside it, with the same key:

```toml
[push]
to = "https://relay-eu.kernable.cloud:7400"
copy = "acme.shop.eu"                # <team>.<project>.<partition>: `kernable cloud projects show` names it
copy-key = "copy.key"
```

The connection is TLS, the relay's certificate a public one: nothing to install. The pusher
reconnects by itself after any failure. If the writer's history does not continue the copy (a
writer restored from an older backup, say), the pusher pushes nothing and says so: the copy keeps
its history. See [backups](/cloud/backups#if-your-writer-was-restored).

## 5. Readers at the copy

A reader needs its one key, granted `read`, named for the partition, and the copy key:

```sh
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*'
kernable cloud projects key shop eu reader-1.pub read
# eu: 5e0c… read active
kernable start --root root.pub \
  --reader https://relay-eu.kernable.cloud:7400 --reader-copy acme.shop.eu --reader-copy-key copy.key \
  --instance-key reader-1.key --primary-kernel-key writer.pub
```

The keys: `--instance-key`, the reader's own, granted `read`, which also signs its checkpoints and
seals its vault; `--reader-copy-key`, the partition's copy key, which you brought there;
`--primary-kernel-key`, the writer's public key (name both of a pair), to check its signed
checkpoints. Every flag: [readers](/how-to/readers#_3-start-a-reader).

A reader refuses queries while it is further behind the writer than `--reader-max-lag-ms` (30 s
by default), and reports itself not ready while the writer is away.

## 6. Stop or revoke a key

Each key of a partition is **active**, **stopped** or **revoked**; the relay lets in only active
ones, and ends the streams of the others within seconds:

```sh
kernable cloud projects key shop eu reader-1.pub stop       # refused until: … activate
kernable cloud projects key shop eu reader-1.pub activate
kernable cloud projects key shop eu reader-1.pub revoke     # refused for good
```

Or in the console, on each key under **Linked kernels**. A partition has one active pusher at a
time: a new writer's pusher is linked only once the current one is stopped or revoked, so two
writers never push the same partition. A revoked key is never let in again; use a new key.

A stopped or revoked reader still holds the copy key and what it read. If the copy key itself
leaked, make a new one, give it to the pusher and the remaining readers, and start the copy anew
under it.

## 7. A writer from its copy

If the writer's machine is lost, or the writer moves to another site, a new kernel takes the copy
over: it catches up from the copy, then becomes the writer on the same history, and its pusher
continues the copy where it stands. On the new machine, its one key (`kernable keys new
writer-c`, granted `read`); then tell kernable Cloud which key it uses:

```sh
kernable cloud projects key shop eu writer-c.pub read          # it follows the copy first
kernable approve project.key acme.shop.eu writer-c.pub         # where the project key is kept
kernable start --root root.pub --readers on \
  --reader https://relay-eu.kernable.cloud:7400 --reader-copy acme.shop.eu --reader-copy-key copy.key \
  --instance-key writer-c.key --primary-kernel-key writer.pub \
  --become-writer on --approval acme.shop.eu.approval
kernable cloud projects key shop eu writer.pub revoke          # the old writer (or stop)
kernable cloud projects link shop eu --pusher writer-c.pub     # now the new one pushes
```

The new kernel waits, and says why, until the relay takes its key as the pusher; then it becomes
the writer and writes a **takeover** with your project key's approval. Start its pusher with
the same `writer-c.key` (`[push]` on its node). Readers started with `--project-key project.pub` take the
new writer from the takeover on, and refuse the old one's records from there: nothing to change
on them. The **project key** is one per project, made once (`kernable keys new project`) and kept
with you, never on an instance and never with kernable: it only approves which kernel key may
write a partition.

## 8. Managed nodes (optional)

A node can be linked to kernable Cloud, which then sets its role, whom it follows, the copy it
pushes to, its kernel version and a reader's maximum lag, and nothing else: never keys, grants,
secrets, your code or your services.

Owners and admins make a one-time join token (valid 24 hours) per node, in the console under
**Managed nodes** or with:

```sh
kernable cloud projects node-token shop eu-1
kernable node join https://control.kernable.cloud <join token>   # on the node's machine
```

The node keeps its link in `managed.toml` beside its project file, pulls its settings every few
seconds and reports how it stands. The protocol is open:
[managed nodes](/reference/managed-node).
