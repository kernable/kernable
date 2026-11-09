# Identities

An identity is someone *inside one tenant*: a person (through their [account](./account)) or a
service, such as another program that calls your API. Permissions, profiles and group memberships
belong to identities, so the same person can own one tenant and be a guest in another, with a
different display name in each.

One aggregate, `identity.identity`, with a random ID, under its tenant.

| Field | What it is |
|---|---|
| `kind` | `person` or `service` |
| `accountId` | the account, for persons; at most one identity per account and tenant |
| `profile` | `displayName`, `title`, `email`: personal data, in the vault under `identity:<id>` |
| `memberships` | the groups it belongs to: each with an ID, the group, a scope and an optional expiry, `active` or `suspended` |
| `apiKeys` | services only: the hashes of their keys, with label, expiry and whether revoked |
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
[no escalation](./authorization#no-escalation)).

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

## Leaving and deleting

- An identity may **leave** its tenant itself (`DeleteIdentity` on its own identity).
- Others with the permission suspend, reactivate and delete identities.
- An identity that **owns** its tenant cannot be deleted until it is removed as owner; the kernel
  checks this atomically.
- Deleting erases the profile from the vault. When a tenant or an account is deleted, the kit's
  reactors delete the identities concerned.

## Reference

<!--@include: ./generated/identity.md-->
