# Link your kernels

Your project's writer sends a sealed copy to kernable Cloud; readers elsewhere follow it. This
page sets up both with the open `kernable` binary.

No certificates: the relay knows your pusher and your readers by their keys.

## 1. Keys

Your root key stays offline. From it, a key for the pusher and the copy key:

```sh
kernable keys new pusher && kernable grant root.key pusher.pub 'read=*'
kernable relay key copy.key
```

`copy.key` goes to the pusher and to your readers, never to kernable. Whoever holds it reads
everything the writer tells a reader: keep it as carefully as a reader's key.

For new readers without the root key at hand, a narrow **enrol key** that may grant reading and
nothing else; it can live in your CI or your vault:

```sh
kernable keys new enrol && kernable grant root.key enrol.pub 'grant=read'
```

## 2. The project's root key

A project has one root key: grants signed by it decide what every key may do in its kernels and
copies. Tell kernable Cloud its public half once, before the first link (admins and owners):

```sh
kernable cloud projects root shop set root.pub
# shop: root 81c2d4e5f6a7…
```

Or in the console, on the project's page under **Root key**. Only `root.pub` (64 hex digits)
leaves your machine; the command refuses a `.key`. (`kernable cloud projects root` comes with the
next kernable release; until then, use the console or the [API](/cloud/reference/api#projects).)

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
--now` (in the console: **Replace now**). Every grant the old root signed stops counting at the
relay at once, until you grant those keys again with the new root.

Every linked partition takes each change within seconds, and every change is in the team's
audit log.

## 3. Link a partition

Tell kernable Cloud which pusher may push a partition's copy, by its public key; readers granted
by the project's root follow it:

```sh
kernable cloud projects link shop eu --pusher pusher.pub --root root.pub
# acme.shop.eu: linked; push it with: kernable push … --to https://relay-eu.kernable.cloud:7400 --copy acme.shop.eu
```

`--root` must name the project's root (newer kernable versions leave it out). Or in the console,
on the project's page under **Linked kernels**. Developers and up link; the
relay lets the pusher in within seconds. Only the `.pub` files or the key IDs (64 hex digits)
leave your machine; the command refuses a `.key`.

`kernable cloud projects links shop` lists the links. `kernable cloud projects unlink shop eu`
(admins and owners) ends one at once: nothing pushes to or reads the copy from then on. The copy
and its backups are kept 30 days, and linking the partition again brings it back; after that they
are deleted.

## 4. The writer's node

The writer runs with `--readers on`; a `[push]` section in its `project.toml` runs the pusher
beside it:

```toml
[push]
to = "https://relay-eu.kernable.cloud:7400"
copy = "acme.shop.eu"                # <team>.<project>.<partition>: `kernable cloud projects show` names it
key = "pusher.key"
copy-key = "copy.key"
root = ["root.pub"]                  # new readers may ask the pusher for the copy key
```

The connection is TLS, the relay's certificate a public one: nothing to install. The pusher
reconnects by itself after any failure.

## 5. Readers at the copy

A reader needs a key granted `read`, keys of its own for its vault and checkpoints, and the copy
key, or a file that does not exist yet, which the pusher fills, sealed for this reader:

```sh
kernable keys new reader-1 && kernable grant enrol.key reader-1.pub --delegated 'read=*'
kernable start --root root.pub \
  --reader https://relay-eu.kernable.cloud:7400 --reader-copy acme.shop.eu --reader-copy-key copy.key \
  --follow-key reader-1.key --primary-kernel-key kernel.pub \
  --kernel-key reader-1-kernel.key --vault-key reader-1-vault.key
```

The keys: `--follow-key`, the reader's own, granted `read`; `--primary-kernel-key`, the writer's
kernel key (name both of a pair), to check its signed checkpoints; `--kernel-key` and
`--vault-key`, the reader's own for its checkpoints and its vault. Every flag:
[readers](/how-to/readers#_3-start-a-reader).

A reader refuses queries while it is further behind the writer than `--reader-max-lag-ms` (30 s
by default), and reports itself not ready while the writer is away.

To shut a reader out for good, revoke its key and start a new generation of the copy with a new
copy key (`acme.shop.eu.2`); give that key to the pusher and the remaining readers.

## 6. Managed nodes (optional)

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
