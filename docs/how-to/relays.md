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
a copy needs (it pruned them), it sends an image and the copy begins anew from it; if the
writer's log no longer continues the copy (the writer was restored from an older backup, say),
the pusher empties the copy and it fills again from the writer's first record. A reader whose
copy no longer continues the relay's starts anew, as from a writer.

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
reader's. The relay knows no revocations, and a reader whose key you revoked still holds the copy
key: to shut one out for good, start a new copy (another name) with a new copy key and give it to
the pusher and the remaining readers.

## 1. Keys

The pusher needs a key the writer grants `read` (the writer runs with `--readers on`); the relay
takes pushes only from the keys it names. Readers need keys granted `read`, as for any reader:

```sh
kernable keys new pusher && kernable grant root.key pusher.pub 'read=*'
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*'
kernable relay key copy.key
```

`copy.key` goes to the pusher and to every reader of the copy, never to the relay. Whoever holds
it reads everything the writer tells a reader: keep it as carefully as a reader's key.

## 2. The relay

```sh
kernable relay --dir /srv/relay --root root.pub --allow-push pusher.pub \
  --listen 0.0.0.0:7400 --tls-cert relay.crt --tls-key relay.key
```

It listens on `127.0.0.1:7400` by default, elsewhere only with TLS. Give it a certificate from a
public CA (Let's Encrypt, for its name): pushers and readers then reach it as
`https://relay.example.net:7400` and need no certificate of their own, because the relay knows
them by their keys. A pusher answers a challenge with its key, which `--allow-push` must name; a
reader presents its grant, which must come from a `--root` and allow `read`, and signs each write
it sends. It refuses a push that is not sealed. To admit only clients with a certificate from your
own CA as well, add `--tls-client-ca ca.crt` (mutual TLS); its clients then set
`KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`.

The relay reads its certificate when it starts: restart it when the certificate is renewed.

### A relay several customers share

`--root` and `--allow-push` count for every copy: fine for your own relay. A relay that holds
copies of several customers takes `--access <file>` instead, naming for each copy the pushers that
may push to it and the roots whose grants let readers follow it; a copy it does not name is
refused, and the file is read at every check, so a change applies at once:

```toml
[copies."shop.eu"]
pushers = ["<pusher's key ID>"]
roots = ["<the customer's root key ID>"]
```

### Limits and what a copy served

Each copy lives in its own directory under `--dir`, the log in `log`. To cap a copy, write the
most it may hold, in bytes, to `max-bytes` there: a push that would grow the log beyond it is
refused (`RESOURCE_EXHAUSTED`), the copy stops following and its readers go unready, until the
limit rises. The relay writes the bytes it sent to the copy's readers to `served` every ten
seconds. A copy's log is never compacted: it grows with what the writer writes.

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

## New readers without copying the key

Copying `copy.key` by hand to every reader works, but the key is better handed over by the pusher:
give it `--root root.pub` (`root` in a node's `[push]`), and a reader started with a
`--reader-copy-key` file that does not exist yet asks the relay for it. The pusher seals the key for
the reader's own key, so the relay never sees it, and writes it to that file (mode 600). It does so
only for a reader whose grant comes from a root it trusts and allows `read`, and only while neither
the reader's key nor whoever granted it is revoked (the pusher notes every revocation it forwards,
in `copy.revoked` beside the copy key).

So that new readers need no ceremony with the root key, let a narrow **enrol key** grant them:
the root allows it `grant=read` and nothing else, and it can live online, in your CI or vault:

```sh
kernable keys new enrol && kernable grant root.key enrol.pub 'grant=read'
kernable keys new reader-2 && kernable grant enrol.key reader-2.pub --delegated 'read=*'
```

Whoever may grant `read` can let an instance of their own read the copy: keep the enrol key as
yours, never with the relay's operator. If it leaks, revoke it with the root key; readers it
granted get no copy key from then on (the ones that have one keep it, so start a new copy with a
new copy key to shut them out).

## 4. Readers at the relay

```sh
kernable start --root root.pub \
  --reader https://relay.example.net:7400 --reader-copy shop --reader-copy-key copy.key \
  --follow-key reader-1.key \
  --primary-kernel-key kernel-a.pub --primary-kernel-key kernel-b.pub \
  --kernel-key reader-1-kernel.key --vault-key reader-1-vault.key --listen 10.1.0.5:7233
```

Everything else is as for [readers](./readers).
