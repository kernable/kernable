# Keys per service, and changing a key

Use this page when you move from the tutorial's single key to production, give a new service its rights, or replace a key: routinely, or because it leaked. How keys and grants work is in [Trust](../concepts/trust#keys-and-grants).

| Situation | What to do | Kernel restart? |
|---|---|---|
| a new service, or more rights | [issue a grant](#one-key-per-service) | no |
| fewer rights | [change the key](#changing-a-key), revoke the old one | no |
| a key leaked | [revoke it](#revoking-a-stolen-key) | no |
| the instance key (the kernel's own, and its vault's) | [a new instance key, the vault rekeyed](#replacing-it) | yes |
| the root key | [start with both roots](#the-root-key) | yes, twice |

## One key per service

The tutorial uses one key for everything (`--dev` writes `app.key` with a grant for everything). In production, give every service its own key and grant it only what it needs. A root key signs the grants; the kernel trusts root keys only.

1. **Create the keys.** Each call writes `<name>.key` (secret) and `<name>.pub` (the key ID):

   ```sh
   for k in root api confirmations shop-worker read-models deploy; do kernable keys new keys/$k; done
   ```

   Keep `keys/root.key` offline; the kernel needs only `keys/root.pub`.

2. **Grant each key what it needs:**

   ```sh
   kernable grant keys/root.key keys/api.pub \
     submit=shop.Register,shop.Place,shop.AddItem,shop.Submit 'query=shop.*'
   kernable grant keys/root.key keys/confirmations.pub \
     submit=shop.Confirm listen=shop.OrderSubmitted query=shop.GetOrder,shop.GetCustomer
   kernable grant keys/root.key keys/shop-worker.pub 'run=shop.*'
   kernable grant keys/root.key keys/read-models.pub 'listen=shop.*' 'answer=shop.*' 'vault_read=shop.*'
   kernable grant keys/root.key keys/deploy.pub manage=shop   # applies the shop's membership
   ```

   Each call prints the permissions and `wrote keys/<name>.grant`. A permission is `<action>=<type>,<type>`, or a bare type for every action; a type is `shop.Place`, `shop.*` or `*`. The actions are in [Trust](../concepts/trust#keys-and-grants); `manage` takes service names. Kernels that follow another get keys of their own: a [standby](./standby) `replicate=*`, a [reader](./readers) `read=*`, a [backup follower](./backup#continuous-backup) `backup=*`. A reader's key never follows as a standby, and a standby's never as a reader.

3. **Start the kernel with the root's public key:** `kernable start --root keys/root.pub ...`.

4. **Give each service its `.key` and `.grant` file.** The SDKs read the `.grant` file next to the key file, or the file in the `grant` option or `KERNABLE_GRANT`, and send it with every call. The key file comes from the `key` option or `KERNABLE_KEY` (default `app.key`).

A process that runs an app (`kernable.app(...).run()`) applies its service's membership on every start, so its key also needs `manage=<service>` ([Deploy and upgrade](./deploy)). Without that, a single deploy key applies it.

A key without a grant, or whose grant does not cover a type, gets `PERMISSION_DENIED`. A grant takes effect when a process first presents it: the kernel's settings hold no keys of services, so a new service needs no change to the kernel.

## A key that grants a few actions

To keep the root key offline while a tool adds standbys or readers, let a key of the tool grant just those actions (see [Delegated grants](../concepts/trust#delegated-grants)):

```sh
kernable keys new keys/mover
kernable grant keys/root.key keys/mover.pub grant=replicate,read     # writes keys/mover.grant
kernable grant keys/mover.key keys/reader-1.pub --delegated 'read=*'   # signed by the mover
```

`--delegated` puts `keys/mover.grant` (beside the signing key) into the new grant; the kernel takes it for `replicate` and `read` only and refuses anything else the mover signs. A key the mover granted cannot grant on. To end it, revoke the mover: every grant it signed goes with it. To narrow it, issue the mover a newer grant with fewer actions and push it to every running kernel; it takes effect at once:

```sh
kernable grant keys/root.key keys/mover.pub grant=replicate          # read taken away
kernable push-grant http://127.0.0.1:7233 keys/mover.grant
```

The kernel stores the newer grant, passes it to its standby and readers, drops the grants the mover signed for `read` and ends the sessions that relied on them. A kernel that is not running gets it when it next starts only if something presents it, so push it again then, or revoke the mover instead.

## Changing what a key may do

Issue a new grant for the key and restart the service with it. The kernel keeps the newest grant per key: once it has seen the new one, it refuses the old one, and workers, listeners and subscriptions whose key may no longer do what it does are disconnected. A listener or subscription that asked for every event type stays connected and gets only the types the new grant allows. A new service needs only its grant; the kernel does not change.

A grant cannot be withdrawn, only replaced by a newer one, which the kernel keeps across restarts. To take rights away from a key that may be stolen, change the key and revoke the old one.

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

It answers `revoked <key ID>`. `kernable revoke` connects over mutual TLS when `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY` are set, like the SDKs. The revoke request is valid for 60 s around its creation. The kernel then:

- stores the revocation, signed with its own key; it is never undone and holds after a restart;
- refuses the key everywhere, also with a grant, and disconnects its sessions;
- rejects commands of the key that still wait in the queue and were accepted after that time (`key_revoked`).

`kernable verify` then lists everything the key signed from that time on:

```text
$ kernable verify sqlite:data/shop.db keys/instance.pub
2311 events, 1903 commands, 41 checkpoints, 0 events after the last checkpoint
SUSPECT command c-8f2a: signed by revoked key 1f0a…
OK, 1 suspect
```

It exits with 2 when there are suspects, 1 when the log was tampered with. Review what the suspects did, and undo it with new commands where needed: the log itself is never rewritten.

## The instance key

Each kernel has one key of its own, the file `--instance-key` names (written by `kernable keys new`). Its ID, the `.pub` beside it, names the kernel everywhere:

- it signs the kernel's checkpoints and stored revocations;
- a standby, a kernel of a pair, a partition or a reader follows with it: its grant, `<name>.grant` beside it, allows `replicate` or `read`;
- the writer's pusher pushes its copy to a relay with it, and a relay lets it in by that ID;
- the vault's key is derived from it, so the vault belongs to it.

Keep it as carefully as the vault: whoever has it reads the vault. To hand the vault key alone to a restore or an export, without the instance key, derive it: `kernable keys vault keys/instance.key vault.key`.

### Replacing it

Replace an instance key when it may have leaked, when the machine changes hands, or when its vault key has encrypted many values (each value gets a random nonce, and one key is safe for 2^32 of them; the kernel warns at startup past 2^31, the UI's verdict warns too, **Explore → Data → Vault** shows the count, and it refuses new values at 2^32). A new key is a new instance: give it what the old one had, move the vault to it, and start the kernel with it.

```sh
kernable keys new keys/instance-2
kernable grant keys/root.key keys/instance-2.pub 'replicate=*'      # what the old key had
# with the kernel stopped:
kernable keys vault keys/instance.key old-vault.key
kernable keys vault keys/instance-2.key new-vault.key
kernable vault rekey sqlite:data/vault.db old-vault.key new-vault.key && shred -u new-vault.key
kernable start --instance-key keys/instance-2.key …
```

Its followers check its checkpoints with `--primary-kernel-key`: give them the new key ID beside the old one. If the old key leaked, revoke it. Keep every old public key, as `kernable verify` needs every key the kernel ever signed with (`kernable verify <store> keys/instance-old.pub keys/instance-2.pub`), and the old vault key (`old-vault.key`) as long as backups made before are kept: they are sealed with it. A [continuous backup](./backup#restore-from-a-continuous-backup) begins a new generation with the new vault key, and `kernable backup list` names each generation's key.

SIGHUP (Unix) reads the file again, which unwraps a [wrapped key](#keys-that-are-not-on-the-disk-in-clear) anew; a different key in the file is refused and logged, and the kernel keeps its own.

## Keys that are not on the disk in clear

With `--key-command`, the instance key file holds a **wrapped** key, and a program you name unwraps it when the kernel starts (and on SIGHUP). The kernel runs it with the key file's path as its last argument and reads the key from its output: 64 hex characters, as a key file holds them. Nothing in the kernel depends on a provider; any tool that decrypts a file works.

With [age](https://age-encryption.org) and an identity on a hardware key or in a protected place:

```sh
age -e -r age1… -o instance.key.age instance.key && shred -u instance.key
kernable start --instance-key instance.key.age --key-command "age -d -i /etc/kernable/age.identity" …
```

With systemd-creds, bound to the machine's TPM:

```sh
systemd-creds encrypt --with-key=tpm2 instance.key instance.key.cred && shred -u instance.key
kernable start --instance-key instance.key.cred --key-command "systemd-creds decrypt" …
```

With a cloud KMS, a small script that takes the path, for example `/usr/local/bin/kms-unwrap`:

```sh
#!/bin/sh
aws kms decrypt --ciphertext-blob "fileb://$1" --query Plaintext --output text | base64 -d
```

The command runs once per key file and has 30 s for it; the kernel keeps the unwrapped key in memory until SIGHUP, when it runs the command again. A key the command cannot unwrap stops the kernel at start, with the command's message; on SIGHUP the previous key stays. A key bound to one machine (the TPM) needs wrapping again for the standby on another one. TLS keys (`--tls-key`) are read as files.

## The root key

The root key signs only grants and revocations, so it can stay offline: issue grants on a separate machine and copy the `.grant` files. To replace it, start the kernel with both roots (`--root keys/root.pub --root keys/root-2.pub`), issue new grants with the new root and restart the services with them. Then restart the kernel without the old `--root`: grants signed by it are refused from then on. If the old root leaked, revoke it first, signed with the new root (`kernable revoke <addr> keys/root-2.key keys/root.pub <since>`): the kernel drops the grants the old root signed at once and refuses them from then on.
