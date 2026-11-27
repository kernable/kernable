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
  each other's copy keys from a pusher (as they did through a relay before): a partition that calls another
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
`plant-a` first"). No waiting time and no override decide it: a person does. **Built in the relay** (part 1c,
2026-11-26): `--access` names keys with role and state, `--allow-push`/`--allow-read` for a relay
of one's own; no root grant counts at a relay any more; the copy key's hand-over by the pusher
(`push --root`, the relay's `CopyKey`) is gone, partitions read each other's copy keys from
`--partition-copy-keys`. The rule "a new pusher only after the old one is stopped or revoked" is
the access list's editor's (kernable Cloud) to keep. A planned move stops
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

**Built** (part 1b): `kernable start --reader <relay> --reader-copy <copy> --become-writer
<pusher.key> --readers on`. It catches up as a reader, waits (said once per reason) until the
relay takes that pusher key, then holds the copy's push while it takes the last records and
becomes the writer; its pusher continues the copy. Until readers trust the new writer by the
project key (part 1d), they are given its kernel key beside the old one. **Built** (part 1d): `kernable approve`, `--approval` for the writer, `--project-key` for
readers and standbys; a takeover record (`TakeoverRecord`, its own table in the store, in images
too) is the new writer's first record, and readers take the new key from it and refuse the old.

## One history per partition

A partition has one copy at a relay, one log; a writer started from it continues that log, it
never starts one of its own. A pusher whose writer's log does not continue the copy pushes
nothing and says so as an error, with the copy's position (before, it emptied the copy and filled
it from its own log: with a takeover, a wrongly started writer would have erased the history).
Starting a copy anew is an explicit action: `kernable push --start-anew <position>` (`start-anew`
under `[push]` on a node), only while the copy stands at that position. **Built** (part 1a); the
relay's own check, by the pusher key's state, comes with the states (1c).

## Readers trust a new writer by the project key

> Changed 2026-11-27 ([instance keys](instance-keys.md)): the project root key signs approvals; there is no separate project key and no `--project-key`.

Readers check every record against the writer's checkpoints, signed with its kernel key, and
accept the kernel keys they are given. Each project has a **project key**, made once locally and
kept in the team's own secrets; readers are given its public half (`--project-key`). It signs,
scoped to one partition, "kernel key B may write `plant-a`": an **approval**, made with
`kernable cloud approve` from the CLI (the console does not hold the key). The project key
approves writers only; it hands out no copy key: copy keys reach instances by hand alone.

**The new writer says it takes over** (the old one may be lost, down or cut off, so it cannot be
asked to hand over): the first record a writer started with `--become-writer` writes is a
**takeover record**, "kernel key B writes this partition from position N", with the approval, and
signed by B. A reader that reaches it checks the approval against the project key and, if it
holds, accepts B's kernel key from N on, without a change of its configuration; the Cloud cannot
slip a writer in, as it holds no project key. From N on it also refuses whatever the previous
writer's key signs: an old writer that comes back cannot add to the partition's history, even
past the relay's access list. A takeover record without a valid approval stops the reader with an
error. A planned move still stops the old writer first (it pushes what it has); the takeover
record is the same either way.

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
