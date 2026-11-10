# Identities

An identity is someone *inside one tenant*: a person (through their [account](./account)) or a
service, such as another program that calls your API. Permissions, profiles and group memberships
belong to identities, so the same person can own one tenant and be a guest in another, with a
different display name in each.

One aggregate, `identity.identity`, with a random ID, under its tenant.

| Field | What it is |
|---|---|
| `kind` | `person`, `service`, or `process` (a business process run by the kernel) |
| `accountId` | the account, for persons; at most one identity per account and tenant |
| `profile` | `displayName`, `title`, `email`: personal data, in the vault under `identity:<id>` |
| `memberships` | the groups it belongs to: each with an ID, the group, a scope and an optional expiry, `active` or `suspended` |
| `apiKeys` | services and processes only: the hashes of their keys, with label, expiry and whether revoked |
| `status` | `active`, `suspended` or `deleted` |

## How identities come to be

You rarely create a person's identity yourself; the kit's reactors do it:

- **Invitation accepted**: the person gets an identity in the tenant (or keeps theirs) with the
  invitation's groups, and becomes an owner for an owner invitation.
- **Self-service tenant**: its creator gets an identity in it and becomes its owner.

Services are created with `CreateServiceIdentity` by anyone whose group allows it.

## Memberships

`GrantMembership` puts an identity in a group:

| Scope | Means |
|---|---|
| none | the group's permissions apply in the whole tenant |
| `{ type: "shop.order", id: "o-17" }` | they apply only to that one aggregate, like a relation set by hand |

A membership can **expire** (`expiresAt`): it stops counting at that moment, with no command
needed. It can be suspended and resumed without losing it, or revoked. When a group is deleted its
memberships stop counting at once, and a reactor revokes them afterwards.

Nobody grants a group whose permissions they do not hold themselves (see
[no escalation](./group#no-escalation)).

## API keys

A service identity signs its requests with an API key instead of a session:

1. `KitApi.createApiKey(...)` generates the key, sends `CreateApiKey` with it, and returns it
   **once**: `key_<keyId>_<secret>`. Only its hash is stored.
2. A request carries the key; your API calls `KitApi.checkApiKey(key)`, which compares the hash and
   returns the request context to submit with (the key's identity and tenant). The answer is
   cached for a minute.
3. `RevokeApiKey` ends it. The middleware learns of it from the event and refuses the key at once,
   cache or not.

Every command sent with a key carries the key's ID in `senderSession`, so the log shows each use;
there is no separate event per use.

## Processes

A [business process](../concepts/processes) the kernel runs sends commands like a service
does, so it needs an identity too. The kit creates one for each process on its first
registration: in the platform tenant, kind `process`, named after the process
(`shop.fulfilment`), with an API key, and tells the kernel to act as that identity.

The new identity holds **no permissions**. A platform administrator grants it a group of the
platform tenant with exactly what the process sends (`shop.Confirm`, `shop.Ship`, `shop.Cancel`);
platform groups hold in every tenant. Until then its commands are refused and its cases stop as
incidents, which `kernable processes retry` resumes once the group is granted. Revoking the
process's API key stops the process at once, without a deploy. A later deploy does not give it a new key:
resuming it is a deliberate step (a new key, then `processAct` with it).

## Leaving and deleting

- An identity may **leave** its tenant itself (`DeleteIdentity` on its own identity).
- Others with the permission suspend, reactivate and delete identities.
- An identity that **owns** its tenant cannot be deleted until it is removed as owner; the kernel
  checks this atomically.
- Deleting erases the profile from the vault. When a tenant or an account is deleted, the kit's
  reactors delete the identities concerned.

## Reference

<!--@include: ./generated/identity.md-->
