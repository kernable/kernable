# Grants instead of allowlists

Status: built, including the actions added later (`data_read`, `data_write`, `cache`, `manage`), grants stored by the kernel, and narrowed grants on open streams.

## Problem

A signature proves who sent a message, not that the sender may do it: anyone can create a key. So the kernel needs a trust anchor. An earlier design kept that anchor in allowlists in a kernel configuration file (one list of keys per role, plus vault readers and erasers, successors and revoked keys): every new service meant editing the kernel's configuration, and the getting-started guide began with 15 lines of TOML. That file no longer exists.

## Decision

The kernel trusts **one or more root keys** and nothing else. A root key signs **grants**: "key K may do these actions on these types". A process presents its grant with its requests. The kernel checks the chain root → grant → request signature. The kernel's configuration holds no keys of services and no permissions, so it fits in command line flags.

```sh
kernable keys new root                      # keep root.key offline in production
kernable keys new kernel
kernable keys new app
kernable grant root.key app.pub 'shop.*'    # writes app.grant
kernable start --root root.pub
```

## Grants

A grant names the grantee's key ID, a list of permissions, optionally the key it succeeds, and when it was issued:

```proto
message Grant {
  string key_id = 1;                     // the grantee
  repeated Permission permissions = 2;
  string succeeds = 3;                   // a listener registered with this key may move to key_id
  int64 issued_at = 4;                   // ms since epoch
  Signature root_sig = 5;                // by a root key
}
message Permission {
  string action = 1;                     // see below, or "*" for every action
  repeated string types = 2;             // "*", "Domain.*" or "Domain.Type"
}
```

| Action | Covers |
|---|---|
| `submit` | commands it may submit |
| `run` | commands it may run as a worker (it writes events of their domains only) |
| `subscribe` | events it may read with a subscription |
| `listen` | events it may read as a listener |
| `query` | queries it may ask |
| `answer` | queries it may answer |
| `check` | commands and queries it may check as a middleware |
| `privileged` | request context switches (`bypass_authorization`) |
| `vault_read` | events and commands whose vault values it gets as a listener or middleware |
| `vault_erase` | erasing events it may write as a worker |
| `data_read`, `data_write` | files of the data service under a domain |
| `cache` | cache values under a domain |
| `manage` | memberships of a service (its domain), and retrying or dismissing its listeners' parked events |
| `*` | every action above |

Signed layout (`proto/SIGNING.md`), tag `kernable.v1.grant`: `key_id`, `succeeds`, `issued_at: i64`, `permission_count: u64`, then per permission `action`, `type_count: u64`, every type.

**File.** `kernable grant <root.key> <name>.pub <permission>...` writes `<name>.grant`: the encoded `Grant`, hex. A permission is `action=type,type` or a bare type, which means every action on it.

**Presenting.** Every request carries the grant in the gRPC metadata entry `kernable-grant` (the file's content): submit, query, subscribe, and the opening of worker and listener streams. The SDKs read `<key>.grant` next to the key file (or the file in `grant` / `KERNABLE_GRANT`) and attach it to every call. Signed messages do not change.

**Checking.** The kernel verifies the root signature, that the grant names the key that signed the request, and that neither the grantee nor the root key is revoked. It keeps the newest verified grant per key (by `issued_at`) in memory and decides every request by it. A request without a grant is decided by the grant the kernel already holds for its key, so a grant presented once is enough. A newer grant replaces an older one; connected sessions are then checked again and ended if they lost their rights, and open listeners and subscriptions get only the types the newer grant still covers, also those that asked for every type. The kernel stores the newest grant per key and loads it, checked again, at startup.

## Revocation

A root key signs a revocation and sends it to the running kernel:

```sh
kernable revoke <addr> root.key <key-id> 2026-10-27T09:00:00Z
```

New RPC `TrustService.Revoke(RevokeRequest)`, signed with tag `kernable.v1.revoke`: `key_id`, `since: i64`, `issued_at: i64`, valid 60 s around `issued_at`. The kernel stores the revocation signed with its own key (`kernable.v1.revocation`), refuses the key from then on, ends its sessions, and `kernable verify` flags what it signed since. Never undone.

## Successors

A listener moves to a new key when the new key's grant says `succeeds = <old key ID>` (`kernable grant root.key new.pub --succeeds old.pub …`).

## Kernel settings

Every setting is a flag of `kernable start`, each with an environment variable ([the full list](../docs/reference/configuration.md)); the main ones:

| Flag | Variable | Default |
|---|---|---|
| `--root <key-id or .pub>` (repeatable) | `KERNABLE_ROOT` (comma-separated) | required |
| `--store` | `KERNABLE_STORE` | `sqlite:kernable.db` |
| `--listen` | `KERNABLE_LISTEN` | `127.0.0.1:7233` |
| `--instance-key` | `KERNABLE_INSTANCE_KEY` | `instance.key`: the kernel's one key, its vault key derived from it ([instance keys](instance-keys.md)) |
| `--snapshots` | `KERNABLE_SNAPSHOTS` | SQLite next to the event store (`kernable-snapshots.db`); `memory:` for a `memory:` store |
| `--vault-store` | `KERNABLE_VAULT_STORE` | `sqlite:vault.db`; `memory:` for a `memory:` store |
| `--tls-cert`, `--tls-key`, `--tls-client-ca` | `KERNABLE_KERNEL_TLS_CERT`, `KERNABLE_KERNEL_TLS_KEY`, `KERNABLE_KERNEL_TLS_CLIENT_CA` | no TLS |
| `--metrics-listen` | `KERNABLE_METRICS_LISTEN` | off |
| `--lease-ttl-ms`, `--checkpoint-interval-ms`, `--submit-timeout-ms`, `--settle-timeout-ms`, `--query-timeout-ms`, `--middleware-timeout-ms`, `--drain-timeout-ms` | `KERNABLE_LEASE_TTL_MS`, … | see the reference |

Relative paths resolve against the working directory. SIGHUP reads the instance key again (a wrapped key unwrapped anew; another key is refused); everything else needs a restart.

## Consequences

- No kernel configuration change, reload or restart when a service is added: issue a grant.
- The root key is the most valuable secret. It is used only to issue grants and revocations, and can stay offline. Several roots allow rotating it.
- A grant cannot be withdrawn, only superseded by a newer one or ended with a revocation of the key. Narrowing a service's rights means issuing a newer grant; the old one is refused once the newer one was seen, also after a kernel restart, because the kernel stores the newest grant per key. A grant the kernel never saw is not known to be outdated; to be sure, rotate the key and revoke the old one.
- `kernable verify` is unchanged: it checks signatures and the chain, not permissions.
- `kernable start --dev` is for trying kernable out: it creates whatever is missing of `root.key`, `kernel.key`, `app.key` and an `app.grant` that allows everything, and trusts `root.pub`. Without `--dev`, `--root` is required, so a production kernel never starts open by accident.
- Not built: grant expiry.
