# A writer from its copy

Status: decided 2026-11-26, not built.

A partition's copy at a relay is enough to start a writer: a new kernel fetches the copy, opens it
with the partition's copy key, checks it, takes the partition over and pushes on. This brings a
writer back after its machine is lost, and starts a partition at a new site, the way a clone
starts from a repository that exists elsewhere.

## Keys: all made locally, the copy key only from the customer

- **Every key is made where it is used.** An instance (writer, pusher, reader, worker, listener)
  makes its own key pair; the private half never leaves that machine. A relay, and whoever runs
  one, only ever learns public keys.
- **One copy key per partition**, made once with `kernable relay key <file>` on any machine of the
  customer's. The customer distributes it by their own means (a secrets manager, configuration
  management, a file copy) and gives it to every instance that reads or writes that partition
  (`--copy-key`, `--reader-copy-key`).
- **There is no other way to a copy key.** The pusher no longer hands the copy key to new readers
  (`kernable push --root` and the node config's `root =` go away), and partitions no longer get
  each other's copy keys from a pusher (the hand-over of P58): a partition that calls another
  through a relay is given that partition's copy key as a file too.

## Who may push and read: metadata, not keys

The relay lets in the public keys its access list names: per copy, the one key that may push it
and the keys that may read it. With kernable Cloud, the team's members set this list in the
console or the CLI; a self-run relay takes it from its file as today. Nothing in this list opens
data: a key the list lets in without the copy key gets sealed bytes, and cannot make a record any
reader would accept.

## Starting a writer from a copy

1. On the new machine: `kernable keys new` for the kernel and its pusher.
2. The pusher's public key is named as the copy's pusher in the relay's access list. From then on
   the relay takes pushes only from it; the old pusher is refused.
3. `kernable start --from-copy <relay> --copy <name> --copy-key <file> …`: the kernel loads the
   copy (from the relay; a large copy may start from the relay's newest backup and take the rest
   from the relay), opens it, checks every record as a reader does, and then serves as the
   partition's writer with its pusher continuing the same copy.

What the old writer had not pushed yet is not in the copy, so it is lost with that machine:
usually the last seconds.

## Questions still open

- **A running old writer.** The takeover is refused while the old pusher pushed within the last
  60 s, unless forced (proposed, not decided).
- **An old writer that comes back.** Its pusher is refused; it may rejoin only as a reader of the
  new state, with the records it never pushed set aside and listed (proposed, not decided).
- **The kernel's own grants.** The new writer's pusher key needs `read` granted by the root key,
  as every pusher's does. Whether that grant is made once ahead (a key the customer keeps for
  this, from their own sources like the copy key) or another way is to be decided.
- **Rotating a copy key** (an instance retired, a key leaked): a new copy key, given to the
  remaining instances, and the copy starts anew from an image under it; to be specified.
