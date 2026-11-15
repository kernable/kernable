# Readers outside the writer's network: relays

A [reader](./readers) normally follows the writer directly. When readers run where they cannot
reach the writer (another site, another cloud, behind a firewall that takes no connection
inward), a **relay** holds a copy of the writer's records for them:

- a **pusher** (`kernable push`) runs beside the writer, follows it as a reader does and sends
  every record to the relay over a connection it opens itself, so the writer accepts no
  connection from outside its network;
- the **relay** (`kernable relay`) writes the records to the copy's log and serves them to
  readers as a writer would;
- **readers** follow the copy at the relay (`--reader <relay> --reader-copy <copy>`).

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
at most. A reader that holds more than the copy (it followed the writer directly before) waits
until the copy catches up.

For now the copy holds what a reader gets, vault values in plaintext included: run the relay
where you would run a reader. Commands and other writes sent to a reader of a relay are refused
(`unavailable`): send them to the writer.

## 1. Keys

The pusher needs a key the writer grants `read` (the writer runs with `--readers on`); the relay
takes pushes only from the keys it names. Readers need keys granted `read`, as for any reader:

```sh
kernable keys new pusher && kernable grant root.key pusher.pub 'read=*'
kernable keys new reader-1 && kernable grant root.key reader-1.pub 'read=*'
```

## 2. The relay

```sh
kernable relay --dir /srv/relay --root root.pub --allow-push pusher.pub \
  --listen 0.0.0.0:7400 --tls-cert relay.tls.crt --tls-key relay.tls.key --tls-client-ca ca.tls.crt
```

It listens on `127.0.0.1:7400` by default, elsewhere only with mutual TLS. A reader presents its
grant, which must come from a `--root` and allow `read`.

## 3. The pusher, beside the writer

```sh
KERNABLE_TLS_CA=ca.tls.crt KERNABLE_TLS_CERT=pusher.tls.crt KERNABLE_TLS_KEY=pusher.tls.key \
kernable push --follow 10.0.0.1:7233,10.0.0.2:7233 --key pusher.key \
  --to relay.example.net:7400 --copy shop
```

With a pair, name both kernels: it follows whichever is the writer. It reads `pusher.grant`
again on every connection and tries again after every failure.

## 4. Readers at the relay

```sh
kernable start --root root.pub \
  --reader relay.example.net:7400 --reader-copy shop --follow-key reader-1.key \
  --primary-kernel-key kernel-a.pub --primary-kernel-key kernel-b.pub \
  --kernel-key reader-1-kernel.key --vault-key reader-1-vault.key --listen 10.1.0.5:7233
```

Everything else is as for [readers](./readers).
