# Keys per service, and changing a key

## One key per service

The tutorial uses one key for everything. In production, give every service its own key and allow it only what it needs:

```toml
[clients]
"<api>" = ["shop.Register", "shop.Place", "shop.AddItem", "shop.Submit"]
"<confirmations>" = ["shop.Confirm"]

[workers]
"<shop-worker>" = ["shop.*"]

[listeners]
"<read-models>" = ["shop.*"]
"<confirmations>" = ["shop.OrderSubmitted"]

[queriers]
"<api>" = ["shop.*"]
"<confirmations>" = ["shop.GetOrder", "shop.GetCustomer"]

[query_handlers]
"<read-models>" = ["shop.*"]

[vault.readers]
"<read-models>" = ["shop.*"]
```

A key that is not listed for a type gets `PERMISSION_DENIED`.

## Changing the configuration

Edit `kernable.toml` and send the kernel **SIGHUP** (`kill -HUP <pid>`). New allowlists, successors and revocations take effect at once; workers, listeners and subscriptions whose key may no longer do what it does are disconnected. Settings that need a restart (store, listen address, TLS, snapshots, vault, metrics address) keep their old values, and the log says so.

## Changing a key

Signatures in the log stay verifiable after a change: a key ID is the public key itself.

**A client or worker key:**

1. `kernable keys new <name>`, add the new key ID next to the old one, SIGHUP.
2. Restart the process with the new key.
3. Remove the old key ID, SIGHUP.

**A listener's key** is part of its registration. Declare the new key as the old one's successor, and the listener keeps its position, order and middleware registration:

```toml
[successors]
"<old key ID>" = "<new key ID>"

[listeners]
"<new key ID>" = ["shop.*"]
```

After SIGHUP, restart the listener with the new key. Without a successor entry, another key is refused, so no service can take over another's listener.

## Revoking a stolen key

When a key leaks, revoke it from the moment it may have been used:

```toml
[revoked]
"<key ID>" = "2026-10-27T09:00:00Z"   # UTC
```

After SIGHUP (or at the next start) the kernel:

- stores the revocation, signed with its own key; it is never undone, also when you remove it from the configuration;
- refuses the key everywhere, also where an allowlist still names it, and disconnects its sessions;
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

Point `kernel_key` to a new key and SIGHUP: checkpoints and revocations are signed with it from then on. Keep the old public key: `kernable verify` needs every key the kernel ever signed with, `kernable verify <store> old-kernel.pub kernel.pub`.

## The vault's key

Stop the kernel, encrypt the vault again, and point `[vault] key` to the new file:

```sh
kernable keys new keys/vault-2 >/dev/null
kernable vault rekey sqlite:data/vault.db keys/vault.key keys/vault-2.key
```

The old key cannot read the vault afterwards. Backups made before still need the old key.
