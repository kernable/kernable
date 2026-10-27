# Signing and hash chain (v1)

Every SDK must produce these byte layouts exactly. `test-vectors/signing.json` holds the reference values; an SDK's test suite must reproduce them.

## Primitives

- Signatures: **Ed25519** (RFC 8032, pure). Verification is strict.
- Hashes: **SHA-256**.
- **Key ID** = lowercase hex of the 32-byte public key. A key file `.key` holds the hex-encoded 32-byte seed; `.pub` holds the key ID.
- A `Signature` message carries `key_id` and the 64-byte `sig`.

## Encoding

A message is a concatenation of fields:

| Type | Encoding |
|---|---|
| `bytes` / `str` | `u32` big-endian length, then the raw bytes (strings as UTF-8) |
| `u64` / `i64` | 8 bytes big-endian (two's complement for `i64`) |
| `context` | a `RequestContext` (an absent one encodes like an empty one): `sender_tenant`, `sender_identity`, `sender_account`, `sender_session`, `on_behalf_of`, `workspace`, then `u64` 1 and `expected_version: u64` if set or `u64` 0 if not, then `correlation_id`, `causation_id`, `trace_id`, `bypass_authorization` as `u64` 0 or 1, `attribute_count: u64`, then per attribute `key`, `value` (in the given order) |

Each message starts with a `str` **tag** for domain separation. Payloads are signed as the exact bytes sent; no JSON canonicalization.

## Messages

| Signed by | Tag | Fields after the tag |
|---|---|---|
| Client | `kernable.v1.command` | `command_id`, `tenant_id`, `domain`, `aggregate_type`, `aggregate_id`, `command_type`, `payload`, `context`, then the vault fields (see below) |
| Client | `kernable.v1.query` | `query_id`, `tenant_id`, `domain`, `query_type`, `payload`, `context`, `min_position: u64`, `issued_at: i64` |
| Worker | `kernable.v1.result` | `command_id`, `tenant_id`, `domain`, `aggregate_type`, `aggregate_id`, `expected_version: u64`, `event_count: u64`, then per event `event_type`, `schema: u64`, `payload`, then the vault fields (see below) |
| Worker | `kernable.v1.rejection` | `command_id`, `code`, `message` |
| Worker | `kernable.v1.snapshot` | `tenant_id`, `domain`, `aggregate_type`, `aggregate_id`, `version: u64`, `schema`, `state` |
| Worker, listener | `kernable.v1.challenge` | `nonce` |
| Subscriber | `kernable.v1.subscribe` | `after_position: u64`, `issued_at: i64`, `type_count: u64`, then per type `event_type` |
| Kernel | `kernable.v1.checkpoint` | `global_position: u64`, `chain_hash`, `created_at: i64` |
| Kernel | `kernable.v1.revocation` | `key_id`, `since: i64`, `created_at: i64` |

The result signature does not cover the lease ID. A replayed result can only reproduce the same events for the same command at the same version, so it is harmless. The commit is idempotent.

A query is valid for 60 s around `issued_at`, like a subscribe request. Answers are not signed: they travel on the read model's authenticated stream and are not stored. The same holds for middleware verdicts. A middleware gets the command or query with the client's signature, so it can check the signature itself.

Listener acknowledgements are not signed. They travel on a stream whose key was proven by the challenge, and they only affect waiting and ordering, never stored data.

A subscribe request is valid for 60 s around `issued_at`. Within that window a captured request could be replayed, but it only grants the read access the key already has. Transport encryption (MVP) closes this gap.

## Derived IDs

`ctx.id()` in a command handler returns IDs that are the same every time the command runs, in every language. The `n`-th ID (counting from 0) for a command is built like this:

1. Take SHA-256 over the message with tag `kernable.v1.id`, followed by `command_id` and `n: u64`.
2. Use the first 16 bytes and set the UUID version to 8 (`b[6] = b[6] & 0x0f | 0x80`) and the RFC 9562 variant (`b[8] = b[8] & 0x3f | 0x80`).
3. Format the result as a lowercase hyphenated UUID.

## Vault references

A command or event keeps personal and secret values in the vault instead of its payload. The payload holds `{"$vault": ref}` in their place. The `ref` is the lowercase hex SHA-256 of the message with tag `kernable.v1.vault`, followed by these fields:

1. `salt` (16 random bytes)
2. `subject` (empty for secrets)
3. `secret: u64` (1 or 0)
4. `value` (the JSON encoding of the field)

The salt makes a reference reveal nothing about its value, also once the value is erased.

**Appended only when set.** The vault fields came with data format 2. They are appended to signed messages and to the event hash only when they are set, so signatures and hashes made before stay valid:

- **Command:** if `vault_refs` is not empty, `count: u64`, then every ref.
- **Result:** if any event has `vault_refs` or `erases`, then per event `count: u64`, every ref, and `erases`.
- **Event hash:** if `vault_refs` or `erases` is set, `count: u64`, every ref, and `erases`, after `context`.

## Event hash chain

Each stored event has `hash = SHA-256(message)`, where `message` has the tag `kernable.v1.event` followed by these fields:

1. `prev_hash`
2. `global_position: u64`
3. `tenant_id`
4. `domain`
5. `aggregate_type`
6. `aggregate_id`
7. `version: u64`
8. `event_type`
9. `schema: u64` (the version of the payload format)
10. `payload`
11. `command_id`
12. `created_at: i64`
13. `producer_key_id`
14. `producer_sig`
15. `context` (of the command that caused the event)
16. the vault fields, only when set (see above)

`prev_hash` is the hash of the event at the previous global position. For the first event it is 32 zero bytes. Every event of a command stores that command's worker result signature as `producer_sig`.

The kernel periodically signs a **checkpoint**, which holds the position and the chain hash at that position.

`kernable verify` recomputes the chain and checks every signature. It detects:

- a modified event: hash mismatch
- a deleted or reordered event: broken link
- a consistently rewritten chain or a truncated log: checkpoint mismatch

Events after the last checkpoint are only protected by the link to their predecessor, so truncating that tail cannot be detected.
