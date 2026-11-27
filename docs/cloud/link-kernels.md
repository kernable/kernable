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

`copy.key` seals everything that leaves the writer; one per partition, made once on the writer's
machine. Readers do not need it copied to them: your writer's pusher hands it to each reader whose
grant from your root key allows `copy=<copy>`, **sealed to that reader's own key** and signed by
the writer's (section 5). kernable Cloud carries the sealed key and can never open it, and it
cannot grant a key, so only whoever holds your root key decides who gets it. Whoever holds the
copy key reads everything the writer tells a reader. You can keep it by hand instead
(`copy-key-handover = false` under `[push]`, then put the file on every reader yourself).

## 2. The project's root key

A project has one root key, the **project root key**: grants signed by it decide what every
key may do in your kernels, its approvals decide which kernel may write a partition (below), and
it is what kernable Cloud gives the project's [hosted kernel](/cloud/hosted-kernels). The
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
kernable grant new-root.key instance.pub 'read=*'   # and every other key, anew
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

The node gives the pusher the kernel's `root`, so it hands the copy key to new readers granted
`copy=<copy>`. The connection is TLS, the relay's certificate a public one: nothing to install. The pusher
reconnects by itself after any failure. If the writer's history does not continue the copy (a
writer restored from an older backup, say), the pusher pushes nothing and says so: the copy keeps
its history. See [backups](/cloud/backups#if-your-writer-was-restored).

## 5. Readers at the copy

A reader needs its one key, granted `read` and `copy=<copy>`, and named for the partition:

```sh
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*' 'copy=acme.shop.eu'
kernable cloud projects key shop eu reader-1.pub read
# eu: 5e0c… read active
kernable start --root root.pub \
  --reader https://relay-eu.kernable.cloud:7400 --reader-copy acme.shop.eu --reader-copy-key copy.key \
  --instance-key reader-1.key --primary-kernel-key writer.pub
```

The keys: `--instance-key`, the reader's own, granted `read` and `copy`, which also signs its
checkpoints and seals its vault; `--reader-copy-key`, where the partition's copy key goes: the
first time, the reader asks the writer's pusher for it through the relay and writes it there
(mode 0600) once it arrives sealed to its key and signed by the writer's; `--primary-kernel-key`,
the writer's public key (name both of a pair), to check its signed checkpoints. Every flag: [readers](/how-to/readers#_3-start-a-reader).

A reader refuses queries while it is further behind the writer than `--reader-max-lag-ms` (30 s
by default), and reports itself not ready while the writer is away.

## 6. Stop or revoke a key

Each key of a partition is **active**, **stopped** or **revoked**; the relay lets in only active
ones, and ends the streams of the others within seconds. Developers and up name keys and let them
in again; admins and owners stop and revoke them:

```sh
kernable cloud projects key shop eu reader-1.pub stop       # refused until: … activate
kernable cloud projects key shop eu reader-1.pub activate
kernable cloud projects key shop eu reader-1.pub revoke     # refused for good
```

Or in the console, on each key under **Linked kernels**. A partition has one active pusher at a
time: a new writer's pusher is linked only once the current one is stopped or revoked, so two
writers never push the same partition. A revoked key is never let in again; use a new key.

A stopped or revoked reader still holds the copy key and what it read. The pusher hands the key
to nobody whose key the writer revoked. To shut it out of what comes next, **rotate the copy
key** on the writer's machine:

```sh
kernable relay rotate copy.key
# the copy key 4c1e… replaces 3f9a… (kept as copy.key.3f9a….old): the pusher pushes with it within seconds …
```

The pusher takes the new key within seconds and the copy begins anew under it. Each reader whose
grant allows `copy=<copy>` finds another key, asks for the new one and is handed it, sealed to its
own key; started again, it catches up anew. The console shows, per reader, whether its node holds
the writer's current copy key, and an alert names a reader that does not.

### An instance that may be compromised

```sh
kernable cloud projects compromised shop reader-1.pub
```

Its key is revoked at the relay for every partition at once (audited), and the command prints
what only you can do where your keys are: revoke it on the writer (`kernable revoke`, so no kernel
takes it and no pusher hands it a copy key), rotate each copy key it held (`kernable relay
rotate`), and treat what it read as disclosed. Rotating is your decision, never automatic. If it
was a partition's writer, it says how to approve a new one (below).

## 7. A writer from its copy

If the writer's machine is lost, or the writer moves to another site, a new kernel takes the copy
over: it catches up from the copy, then becomes the writer on the same history, and its pusher
continues the copy where it stands. On the new machine, its one key (`kernable keys new
writer-c`, granted `read`); then tell kernable Cloud which key it uses:

```sh
kernable cloud projects approve-writer shop eu root.key writer-c.pub    # --revoke-old for a lost machine
kernable start --root root.pub --readers on \
  --reader https://relay-eu.kernable.cloud:7400 --reader-copy acme.shop.eu --reader-copy-key copy.key \
  --instance-key writer-c.key --approvals approvals --become-writer on
```

`approve-writer` signs the approval with your root key on your machine, hands it to kernable
Cloud, stops the old writer's key at the relay (revokes it with `--revoke-old`) and names the new
one the partition's pusher, in one step. A joined node gets the approval in `approvals/` by
itself; without one, `kernable approve root.key acme.shop.eu writer-c.pub` writes it as a file for
`--approval`.

The new kernel waits, and says why, until the relay takes its key as the pusher; then it becomes
the writer and writes a **takeover** with your root key's approval. Start its pusher with the
same `writer-c.key` (`[push]` on its node). Every reader of the project trusts its root already,
so it takes the new writer from the takeover on, and refuses the old one's records from there:
nothing to change on them. The approval is signed in a context of its own, so a grant never
passes for one; only whoever holds the project's root key, never kernable, can make it.

## 8. Managed nodes: join instead of copying files

A node joined to kernable Cloud makes its key on its machine, and from then on gets from kernable
Cloud everything else it needs that is public or signed by your root key: its role, whom it
follows, the copy it pushes to, its kernel version and a reader's maximum lag, **its grant** and
**the writers your root approved**. Never a secret: kernable carries what your root signed and
can make none of it, and the node checks every signature against the root it pinned when it
joined.

Owners and admins make a one-time join token (valid 24 hours) per node, in the console under
**Managed nodes** or with the CLI, which prints the whole join command, your root included:

```sh
kernable cloud projects node-token shop eu-1
# on the node's machine:
kernable node join https://control.kernable.cloud <join token> --root 81c2d4e5f6a7…
# joined as n-… with the instance key 5e0c… (compare it in the console)
```

Then sign its grant where your root key is kept; kernable carries it to the node, which writes it
beside its key within seconds:

```sh
kernable cloud projects grant shop root.key 5e0c… 'read=*' 'copy=acme.shop.eu'
kernable cloud projects key shop eu 5e0c… read                 # the relay lets it in
```

A reader joined this way needs no file by hand: its key and vault are its own, its grant comes
signed, the writer it trusts comes as your root's approval, and the copy key comes from the
writer's pusher, sealed to its key (section 5). Approve the writer of a partition once, its first
or a new one:

```sh
kernable cloud projects approve shop eu root.key writer.pub
kernable cloud projects signed shop                            # what kernable carries
```

The node keeps its link in `managed.toml` beside its project file, pulls every few seconds and
reports how it stands. The protocol is open: [managed nodes](/reference/managed-node).
