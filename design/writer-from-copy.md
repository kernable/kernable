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

## Each key's state

Every key in a relay's access list has a state, set by the team's members (metadata, like the list
itself, so it opens no data):

| State | The relay | The instance |
|---|---|---|
| **active** | lets it push or read | works as usual |
| **stopped** | if connected, tells it so, takes what it still has to push, then refuses it | stops taking writes, pushes the rest, stays read-only |
| **revoked** | refuses it at once, telling it so if connected | stays read-only; what it never pushed is set aside |

A partition gets a new pusher only once its current one is stopped or revoked; while it is
active, the console and the CLI refuse and say so plainly ("stop or revoke the current writer of
`plant-a` first"). No waiting time and no override decide it: a person does. A planned move stops
the old writer (it pushes everything, nothing is lost); a lost machine is revoked (what it never
pushed is lost with it). Readers have the same states: a stopped or revoked reader gets nothing
more from the relay, without a new copy key.

The relay cannot stop an instance it cannot reach: a writer cut off from it keeps taking writes
until it connects again, learns that it is revoked, and turns read-only.

## Starting a writer from a copy

1. On the new machine: `kernable keys new` for the kernel and its pusher.
2. The old pusher is stopped or revoked, and the new pusher's public key is named as the copy's
   pusher in the relay's access list. From then on the relay takes pushes only from it.
3. `kernable start --from-copy <relay> --copy <name> --copy-key <file> …`: the kernel loads the
   copy (from the relay; a large copy may start from the relay's newest backup and take the rest
   from the relay), opens it, checks every record as a reader does, and then serves as the
   partition's writer with its pusher continuing the same copy.

What the old writer had not pushed yet is not in the copy, so it is lost with that machine:
usually the last seconds.

## One history per partition

A partition has one copy at a relay, one log; a writer started from it continues that log, it
never starts one of its own. The relay refuses a pusher whose log does not continue the copy
(today the pusher empties the copy and fills it from its own log: with a takeover, a wrongly
started writer would erase the history). Starting a copy anew is an explicit action of the team.

## Readers trust a new writer by the partition key

Readers check every record against the writer's checkpoints, signed with its kernel key, and
accept the kernel keys they are given. Each partition has a **partition key**, made once locally
and kept in the team's own secrets like the copy key; readers are given its public half. It signs
"kernel key B may write `plant-a`", and the new writer brings that signature along: readers accept
B without a change of their configuration, and the Cloud cannot slip a writer in. When the old
writer is stopped (a planned move), it writes one last signed record that hands the partition
over to B, and readers accept B from it.

## A writer that comes back

A writer that was cut off while another took its partition over learns it is revoked when it
connects again. It turns read-only, writes the records it never pushed to a file beside its store,
and says so loudly, with how many and from when to when ("142 records from 09:14 to 11:20 are not
in partition `plant-a`"). Nothing sends them anywhere by itself: their business rules ran against
an older state, so the team decides what to do with them (enter them again at the new writer, or
not).

## The pusher belongs to its writer

The pusher runs beside its writer, on the same machine and in the same node, and the writer
trusts it there without a grant: it no longer reads the writer as a reader with a `read` grant
from the root key. Whether it may push to a relay is decided by the relay's access list alone (its
key and that key's state). So starting a writer from a copy needs no root key.

## A new copy key

Retiring an instance needs no new key: it is stopped or revoked. A new copy key is for a key that
leaked, or when the team wants one: the team makes it and gives it to the writer and the instances
that stay; the copy keeps its name and starts anew from an image of the writer sealed with the new
key; instances with the old key stop following until they get the new one.
