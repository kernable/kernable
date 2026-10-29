# Signing and hash chain (v1)

Every SDK must produce these byte layouts exactly. The source of truth is `crates/kernable-core/src/signing.rs`; `test-vectors/signing.json` holds reference values that an SDK's test suite must reproduce (see [Test vectors](#test-vectors)).

## Primitives

- Signatures: **Ed25519** (RFC 8032, pure). Verification is strict.
- Hashes: **SHA-256**.
- **Key ID** = lowercase hex of the 32-byte public key. A key file `.key` holds the hex-encoded 32-byte seed; `.pub` holds the key ID.
- A `Signature` message carries `key_id` and the 64-byte `sig`.

## Encoding

A message is a concatenation of fields. The protobuf encoding is never signed.

| Type | Encoding |
|---|---|
| `bytes` / `str` | `u32` big-endian length, then the raw bytes (strings as UTF-8) |
| `u64` / `i64` | 8 bytes big-endian (two's complement for `i64`) |
| `bool` | a `u64`, 0 or 1 |
| `u32` fields (`schema`) | widened to a `u64` |
| `context` | a `RequestContext` (an absent one encodes like an empty one): `sender_tenant`, `sender_identity`, `sender_account`, `sender_session`, `on_behalf_of`, then `u64` 1 and `expected_version: u64` if set or `u64` 0 if not, then `correlation_id`, `causation_id`, `trace_id`, `bypass_authorization` as `u64` 0 or 1, `attribute_count: u64`, then per attribute `key`, `value` (in the given order) |

Fields without a type below are `str`, except `payload`, `state`, `nonce`, `chain_hash`, `salt`, `value` and `prev_hash`, which are `bytes`. Every list is a `count: u64` followed by its entries, in the order they are sent. Names and types are encoded as sent: commands, queries, events and snapshots carry unqualified types (`PlaceOrder`) next to a `domain`; subscriptions and memberships carry qualified ones.

Each message starts with a `str` **tag** for domain separation. Payloads are signed as the exact bytes sent; no JSON canonicalization.

## Messages

| Signed by | Tag | Fields after the tag |
|---|---|---|
| Client | `kernable.v1.command` | `command_id`, `tenant_id`, `domain`, `aggregate_type`, `aggregate_id`, `command_type`, `payload`, `context`, then the vault fields (see [Vault references](#vault-references)) |
| Client | `kernable.v1.query` | `query_id`, `tenant_id`, `domain`, `query_type`, `payload`, `context`, `min_position: u64`, `issued_at: i64` |
| Worker | `kernable.v1.result` | `command_id`, `tenant_id`, `domain`, `aggregate_type`, `aggregate_id` (all from the command), `expected_version: u64` (from `Execute`), `event_count: u64`, then per event `event_type`, `schema: u64`, `payload`, then the vault fields |
| Worker | `kernable.v1.rejection` | `command_id`, `code`, `message` |
| Worker | `kernable.v1.snapshot` | `tenant_id`, `domain`, `aggregate_type`, `aggregate_id` (from the command), `version: u64`, `schema` (a `str`), `state` |
| Worker, listener | `kernable.v1.challenge` | `nonce` (the 32 bytes of `Challenge.nonce`) |
| Subscriber | `kernable.v1.subscribe` | `after_position: u64`, `issued_at: i64`, `type_count: u64`, then every event type |
| Data client | `kernable.v1.data.<op>` (`put`, `get`, `delete`) | `tenant_id`, `domain`, `key`, `content_type`, `sha256(data)` (the 32-byte digest as a `bytes` field), `issued_at: i64`; get and delete sign an empty `content_type` and the hash of empty data |
| Cache client | `kernable.v1.cache.<op>` (`set`, `get`, `delete`) | `tenant_id`, `domain`, `key`, `sha256(value)` (as a `bytes` field), `ttl_ms: u64`, `issued_at: i64`; get and delete sign the hash of an empty value and `ttl_ms` 0 |
| Deploy tooling | `kernable.v1.membership` | `service`, then for `workers`, `listeners` and `middlewares` each `count: u64` and every entry, then `metadata_count: u64` and per attribute `key`, `value`, then `issued_at: i64` |
| Kernel | `kernable.v1.checkpoint` | `global_position: u64`, `chain_hash`, `created_at: i64` |
| Kernel | `kernable.v1.revocation` | `key_id`, `since: i64`, `created_at: i64` |
| Root key | `kernable.v1.grant` | `key_id`, `succeeds`, `issued_at: i64`, `permission_count: u64`, then per permission `action`, `type_count: u64`, every type |
| Root key | `kernable.v1.revoke` | `key_id`, `since: i64`, `issued_at: i64` |

Three more tagged messages are hashed, not signed: `kernable.v1.vault` ([Vault references](#vault-references)), `kernable.v1.id` ([Derived IDs](#derived-ids)) and `kernable.v1.event` ([Event hash chain](#event-hash-chain)).

The kernel checks that a result, rejection or snapshot is signed by the key the worker registered with (`Register.key_id`), and a challenge response by the key in `Register` or `Hello`.

## What is not signed

- `protocol` in every request, and `listener` in `SubmitRequest` and `QueryRequest`.
- The lease ID. A replayed result can only reproduce the same events for the same command at the same version, so it is harmless. The commit is idempotent.
- Query answers and middleware verdicts. They travel on the read model's authenticated stream and are not stored. A middleware gets the command or query with the client's signature, so it can check the signature itself.
- Listener acknowledgements. They travel on a stream whose key was proven by the challenge, and they only affect waiting and ordering, never stored data.
- The grant sent with a call (see [Grants and revocation](#grants-and-revocation)).

## Time windows

Queries, subscribe requests, data and cache requests, memberships and revoke requests are valid for 60 s around `issued_at` (ms since epoch), measured on the kernel's clock. Commands have no time window: a command ID runs once, so a replayed command only returns its stored result.

Within the window a captured subscribe or query request could be replayed, but it only grants the read access the key already has. Transport encryption (mutual TLS over TCP) closes this gap. For data and cache requests the operation is part of the tag, so a signed get cannot be replayed as a delete.

## Grants and revocation

A grant says what a key may do. A root key signs it; the process of the grantee sends it, hex-encoded, in the gRPC metadata entry `kernable-grant` with every call. The grant is not part of any other signed message: the kernel checks that the grant names the key that signed the request. A grant has no time window; a newer one (by `issued_at`) replaces an older one for the same key.

A revoke request is signed by a root key; the kernel then stores the revocation signed with its own key (`kernable.v1.revocation`, with `created_at` the time it stored it). Only root-key tooling produces grants and revoke requests.

A membership is signed by a key granted `manage` for its service. Metadata is signed in the order it is sent.

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

**Appended only when set.** The vault fields are appended to signed messages and to the event hash only when they are set, so a message without vault data does not depend on them:

- **Command:** if `vault_refs` is not empty, `count: u64`, then every ref.
- **Result:** if any event has `vault_refs` or `erases`, then for every event (also those without) `count: u64`, every ref, and `erases`.
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
13. `producer_key_id` (`producer_sig.key_id`, a `str`)
14. `producer_sig` (`producer_sig.sig`, `bytes`)
15. `context` (of the command that caused the event)
16. the vault fields, only when set (see above)

`prev_hash` is the hash of the event at the previous global position. For the first event it is 32 zero bytes. Every event of a command stores that command's worker result signature as `producer_sig`.

The kernel periodically signs a **checkpoint**, which holds the position and the chain hash at that position.

`kernable verify` recomputes the chain and checks every signature. It detects:

- a modified event: hash mismatch
- a deleted or reordered event: broken link
- a consistently rewritten chain or a truncated log: checkpoint mismatch

Events after the last checkpoint are only protected by the link to their predecessor, so truncating that tail cannot be detected.

## Test vectors

`test-vectors/signing.json` is generated by `crates/kernable-core/tests/vectors.rs`. Every signature in it is made with the key `key.seed` (its key ID is `key.key_id`); Ed25519 is deterministic, so an SDK must get the same bytes. For a signed message, an entry holds `input`, the full `message` in hex (tag included) and its `signature` in hex.

An SDK must reproduce these entries byte for byte:

| Entry | What to compute from `input` |
|---|---|
| `key` | the key ID from the seed |
| `command`, `query`, `result`, `rejection`, `snapshot`, `challenge`, `subscribe`, `membership`, `checkpoint`, `revocation` | `message` and `signature` |
| `data_put`, `cache_set` | `message` and `signature` for the `op` in `input` |
| `command_vault` | as `command`, with the `vault_refs` in `input` |
| `result_vault` | as `result`, with the input of `result` plus `vault_refs[i]` and `erases[i]` for event `i` |
| `vault_ref` | `refs[i]` from `input[i]` |
| `event_hash` | `hash` from the input, with `prev_hex` as `prev_hash` |
| `event_hash_vault` | `hash` from the input of `event_hash` plus `vault_refs` and `erases` |
| `derived_id` | `ids[i]` for `command_id` and `n[i]` |

`grant` and `revoke` are only produced by root-key tooling, so an SDK need not reproduce them. The context in the vectors sets every field, including `expected_version` and two attributes; the empty and absent cases follow from the [encoding](#encoding).
