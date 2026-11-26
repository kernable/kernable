# Readers outside the writer's network: relays

A [reader](./readers) normally follows the writer directly. When readers run where they cannot
reach the writer (another site, another cloud, behind a firewall that takes no connection
inward), a **relay** holds a copy of the writer's records for them:

- a **pusher** (`kernable push`) runs beside the writer, follows it as a reader does and sends
  every record to the relay over a connection it opens itself, so the writer accepts no
  connection from outside its network;
- the **relay** (`kernable relay`) writes the records to the copy's log and serves them to
  readers as a writer would;
- **readers** follow the copy at the relay (`--reader <relay> --reader-copy <copy>`), and send
  their writes back through it.

What reaches the relay is **sealed** with a copy key that the pusher and the copy's readers hold
and the relay does not: every record, every vault value and file, every part of an image. The
relay sees the records' positions, hashes and sizes, nothing else, so it can run anywhere,
operated by anyone. Each sealed value is bound to its place: a relay cannot move one to another
position or leave a part of an image out unnoticed.

The relay applies nothing and checks no record. Every reader checks each record against the
writer's checkpoints, as it does from the writer: a relay can hold records back, never change
one. One relay holds the copies of several writers, one directory each.

## How it works

The copy starts where the writer's record log starts. If the writer no longer keeps the records
a copy needs (it pruned them), it sends an image and the copy begins anew from it. If the
writer's log no longer continues the copy (the writer was restored from an older backup, or
another writer was started by mistake), the copy keeps its history: the pusher pushes nothing
and says so as an error, with the copy's position, every minute. Only when you start the pusher
with `--start-anew <position>` (the position it named; `start-anew` under `[push]` in a node's
`project.toml`) does it empty the copy, which then fills again from the writer's first record. A
reader whose copy no longer continues the relay's starts anew, as from a writer.

The writer keeps its records until the copy holds them, as for a slow reader, a million records
at most. A reader of a relay knows how far the writer is (the pusher passes it on), so its lag
is its lag behind the writer, not behind the copy. When the pusher has been silent for 3 s, the
relay stops telling its readers anything, and after their usual 5 s they count the writer as
away (`/ready` answers 503) until the pusher is back. A reader that holds more than the copy (it followed the writer directly before) waits
until the copy catches up.

Commands and every other write sent to a reader of a relay go the same way back, sealed: the
reader sends them to the relay, the relay passes them to the copy's pusher over a second
connection the pusher opened, and the pusher makes the call on the writer. The answer returns
the same way, and a command's answer comes once the reader's own copy holds its events, as from
a writer. While no pusher is connected, the reader answers `writer_away` and the SDKs try again.
A request or answer larger than 64 MiB does not pass a relay.

Each write through a relay is sealed with the time it was sent; the pusher makes it only within a
minute and only once, so a relay cannot send one again later.

A reader of a relay holds what any reader holds, vault values included: keep its keys as a
reader's. The relay lets in only the keys it names, each with a state: stop or revoke a reader's
key there and it gets nothing more from the relay within seconds. It still holds the copy key and
what it read; if the copy key itself leaked, make a new one and give it to the writer's pusher and
the remaining readers (the copy begins anew under it).

## 1. Keys

The pusher needs a key the writer grants `read` (the writer runs with `--readers on`). Readers
need keys granted `read`, as for any reader. The relay knows each of them by its key alone: it
takes pushes and serves readers only for the keys it names (below), whatever a root granted them.

```sh
kernable keys new pusher && kernable grant root.key pusher.pub 'read=*'
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*'
kernable relay key copy.key
```

`copy.key` goes to the pusher and to every reader of the copy, never to the relay, and only by
your own means (a secrets manager, configuration management, a file copy): nothing hands it out.
Whoever holds it reads everything the writer tells a reader: keep it as carefully as a reader's
key.

## 2. The relay

```sh
kernable relay --dir /srv/relay --allow-push pusher.pub --allow-read reader-1.pub,reader-2.pub \
  --listen 0.0.0.0:7400 --tls-cert relay.crt --tls-key relay.key
```

It listens on `127.0.0.1:7400` by default, elsewhere only with TLS. Give it a certificate from a
public CA (Let's Encrypt, for its name): pushers and readers then reach it as
`https://relay.example.net:7400` and need no certificate of their own, because the relay knows
them by their keys. A pusher answers a challenge with its key, which `--allow-push` must name; a
reader answers one with its key, which `--allow-read` must name, and signs each write it sends.
It refuses a push that is not sealed. To admit only clients with a certificate from your
own CA as well, add `--tls-client-ca ca.crt` (mutual TLS); its clients then set
`KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`.

The relay reads its certificate when it starts: restart it when the certificate is renewed.

### A relay several customers share

`--allow-push` and `--allow-read` count for every copy: fine for your own relay. A relay that
holds copies of several customers takes `--access <file>` instead, naming for each copy every key
it takes, with its role (`push` or `read`) and its state; a copy it does not name is refused, and
the file is read at every check, so a change applies at once:

```toml
[copies."shop.eu".keys]
"<pusher's key ID>" = { role = "push", state = "active" }
"<reader-1's key ID>" = { role = "read", state = "active" }
"<reader-2's key ID>" = { role = "read", state = "stopped" }
```

Only an `active` key is let in. A `stopped` or `revoked` one is refused, and so are the streams it
holds: they are checked again every 5 seconds, so a key whose state changes, a key taken off the
file, or a copy taken out of it ends its streams within that. Stopped and revoked differ in what
the instance behind the key does (a writer that is stopped hands over first; see
[design/writer-from-copy.md](https://github.com/kernable/kernable/blob/main/design/writer-from-copy.md)).

### Limits and what a copy served

Each copy lives in its own directory under `--dir`, the log in `log`. To cap a copy, write the
most it may hold, in bytes, to `max-bytes` there: a push that would grow the log beyond it is
refused (`RESOURCE_EXHAUSTED`), the copy stops following and its readers go unready, until the
limit rises. The relay writes the bytes it sent to the copy's readers to `served` every ten
seconds, and beside it `status.json`: the copy's head, its pusher (key, since when) and each
reader's stream (key, the newest record sent to it, since when). Nothing of a record's content. A copy's log is never compacted: it grows with what the writer writes.

The log only ever grows at its end, sealed, so it can be backed up by copying its new bytes as
they come, without the copy key and without a stop.

## 3. The pusher, beside the writer

```sh
kernable push --follow 10.0.0.1:7233,10.0.0.2:7233 --key pusher.key \
  --to https://relay.example.net:7400 --copy shop --copy-key copy.key
```

`https://` reaches the relay over TLS, its certificate checked against the public CAs (and
`KERNABLE_TLS_CA` if set); `KERNABLE_TLS_*` are needed only for mutual TLS, to the relay or to a
writer that listens on TCP with it. The writer's Unix socket (`--follow unix:/srv/shop/kernel.sock`)
is reached without. With a pair,
name both kernels: it follows whichever is the writer. It reads `pusher.grant`
again on every connection and tries again after every failure. A [node](./node) runs it for you
with a `[push]` section (`to`, `copy`, `key`, `copy-key`).

## Partitions at several sites

[Partitions](./partitions#partitions-at-several-sites) whose writers sit in networks that do not
reach each other ask each other through the relay too: global keys, moves, the trust records. The
map names such a partition as `relay:<relay>/<its copy>`; its pusher makes those calls on it. The
relay must let the other partitions' node keys read that copy, and each of them needs its copy
key, as `<copy>.key` in its `--partition-copy-keys` directory.

## 4. Readers at the relay

```sh
kernable start --root root.pub \
  --reader https://relay.example.net:7400 --reader-copy shop --reader-copy-key copy.key \
  --follow-key reader-1.key \
  --primary-kernel-key kernel-a.pub --primary-kernel-key kernel-b.pub \
  --kernel-key reader-1-kernel.key --vault-key reader-1-vault.key --listen 10.1.0.5:7233
```

Everything else is as for [readers](./readers).

## 5. A writer from its copy

The copy is enough to start a writer again, after its machine is lost or at a new site: a new
kernel catches up from the copy as a reader does, then becomes the writer on the same history,
and its pusher continues the copy where it stands. Its own keys are made on its machine; it needs
the copy key and the root's grants as any writer.

```sh
kernable keys new kernel-c && kernable keys new reader-c && kernable keys new pusher-c
kernable grant root.key reader-c.pub 'read=*' && kernable grant root.key pusher-c.pub 'read=*'
kernable start --root root.pub --readers on \
  --reader https://relay.example.net:7400 --reader-copy shop --reader-copy-key copy.key \
  --follow-key reader-c.key --primary-kernel-key kernel-a.pub \
  --become-writer pusher-c.key \
  --kernel-key kernel-c.key --vault-key vault-c.key --listen 10.2.0.5:7233
```

The relay must let `reader-c` read the copy. The kernel catches up, then waits, and says why,
until the relay takes `pusher-c` as the copy's pusher: named there, active, and no other pusher
connected. So first stop or revoke the old writer's pusher key in the access list (a stopped one
has pushed what it had; a revoked one, on a machine that is lost, may have held the last records
it never pushed), then name `pusher-c`'s. The kernel then holds the copy's push while it takes
the last records, so the old pusher cannot come back meanwhile, and serves as the writer. Start
its pusher with `pusher-c.key`: it continues the copy, and the copy's readers follow on.

Readers check every record against the writer's kernel key: until they know the new writer's
(`--primary-kernel-key kernel-c.pub` beside the old one), they take nothing it wrote.
