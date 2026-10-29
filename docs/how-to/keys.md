# Keys per service, and changing a key

## One key per service

The tutorial uses one key for everything. In production, give every service its own key and grant it only what it needs. A root key signs the grants; the kernel trusts the root key only (`kernable start --root keys/root.pub`).

```sh
for k in root api confirmations shop-worker read-models deploy; do kernable keys new keys/$k; done
# keep keys/root.key offline
kernable grant keys/root.key keys/api.pub \
  submit=shop.Register,shop.Place,shop.AddItem,shop.Submit 'query=shop.*'
kernable grant keys/root.key keys/confirmations.pub \
  submit=shop.Confirm listen=shop.OrderSubmitted query=shop.GetOrder,shop.GetCustomer
kernable grant keys/root.key keys/shop-worker.pub 'run=shop.*'
kernable grant keys/root.key keys/read-models.pub 'listen=shop.*' 'answer=shop.*' 'vault_read=shop.*'
kernable grant keys/root.key keys/deploy.pub manage=shop   # applies the shop's membership
```

Each call writes `<name>.grant` next to the `.pub` file. A permission is `<action>=<type>,<type>`, or a bare type for every action. The actions are in [Trust](../concepts/trust#keys-and-grants). Give each service its `.key` and `.grant` file; the SDKs read the `.grant` file next to the key file, or the file in the `grant` option or `KERNABLE_GRANT`, and send it with every call.

A key without a grant, or whose grant does not cover a type, gets `PERMISSION_DENIED`.

## Changing what a key may do

Issue a new grant for the key and restart the service with it. The kernel keeps the newest grant per key: once it has seen the new one, it refuses the old one, and workers, listeners and subscriptions whose key may no longer do what it does are disconnected. A new service needs only its grant; the kernel does not change.

A grant cannot be withdrawn. After a kernel restart, the kernel accepts an older grant again until the newer one is presented. To take rights away for certain, change the key and revoke the old one.

## Changing a key

Signatures in the log stay verifiable after a change: a key ID is the public key itself.

**A client or worker key:** create the new key, grant it what the old one had, and restart the process with the new key and grant. Revoke the old key if it may have leaked.

**A listener's key** is part of its registration. Grant the new key with `--succeeds <old key>`, and the listener keeps its position, order and middleware registration:

```sh
kernable keys new keys/read-models-2
kernable grant keys/root.key keys/read-models-2.pub --succeeds keys/read-models.pub \
  'listen=shop.*' 'answer=shop.*' 'vault_read=shop.*'
```

Then restart the listener with the new key and grant. Without `succeeds`, another key is refused, so no service can take over another's listener.

## Revoking a stolen key

When a key leaks, revoke it from the moment it may have been used. The root key signs the revocation, and the running kernel takes it at once:

```sh
kernable revoke http://127.0.0.1:7233 keys/root.key keys/api.pub 2026-10-27T09:00:00Z   # UTC
```

`kernable revoke` connects over mutual TLS when `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY` are set, like the SDKs. The revoke request is valid for 60 s around its creation. The kernel then:

- stores the revocation, signed with its own key; it is never undone and holds after a restart;
- refuses the key everywhere, also with a grant, and disconnects its sessions;
- rejects commands of the key that still wait in the queue and were accepted after that time (`key_revoked`).

`kernable verify` then lists everything the key signed from that time on:

```text
$ kernable verify sqlite:data/shop.db keys/kernel.pub
2311 events, 1903 commands, 41 checkpoints, 0 events after the last checkpoint
SUSPECT command c-8f2a: signed by revoked key 1f0a…
OK, 1 suspect
```

It exits with 2 when there are suspects, 1 when the log was tampered with. Review what the suspects did, and undo it with new commands where needed: the log itself is never rewritten.

## The kernel's key

Write the new key to the file `--kernel-key` names and send the kernel SIGHUP (`kill -HUP <pid>`): it reads the file again, and checkpoints and revocations are signed with the new key from then on. SIGHUP changes nothing else; other settings need a restart. Keep the old public key: `kernable verify` needs every key the kernel ever signed with, `kernable verify <store> old-kernel.pub kernel.pub`.

## The vault's key

Stop the kernel, encrypt the vault again, and start it with `--vault-key` pointing to the new file:

```sh
kernable keys new keys/vault-2 >/dev/null
kernable vault rekey sqlite:data/vault.db keys/vault.key keys/vault-2.key
```

The old key cannot read the vault afterwards. Backups made before still need the old key.

## The root key

The root key signs only grants and revocations, so it can stay offline: issue grants on a separate machine and copy the `.grant` files. To replace it, start the kernel with both roots (`--root keys/root.pub --root keys/root-2.pub`), issue new grants with the new root and restart the services with them. Then restart the kernel without the old `--root`: grants signed by it are refused from then on. If the old root leaked, revoke it first: the kernel drops the grants it signed at once and refuses them from then on.
