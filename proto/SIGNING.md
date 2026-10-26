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

Each message starts with a `str` **tag** for domain separation. Payloads are signed as the exact bytes sent; no JSON canonicalization.

## Messages

| Signed by | Tag | Fields after the tag |
|---|---|---|
| Client | `kernable.v1.command` | `command_id`, `tenant_id`, `aggregate_type`, `aggregate_id`, `command_type`, `payload` |
| Worker | `kernable.v1.result` | `command_id`, `tenant_id`, `aggregate_type`, `aggregate_id`, `expected_version: u64`, `event_count: u64`, then per event `event_type`, `payload` |
| Worker | `kernable.v1.rejection` | `command_id`, `code`, `message` |
| Worker | `kernable.v1.snapshot` | `tenant_id`, `aggregate_type`, `aggregate_id`, `version: u64`, `schema`, `state` |
| Worker, listener | `kernable.v1.challenge` | `nonce` |
| Subscriber | `kernable.v1.subscribe` | `after_position: u64`, `issued_at: i64`, `type_count: u64`, then per type `event_type` |
| Kernel | `kernable.v1.checkpoint` | `global_position: u64`, `chain_hash`, `created_at: i64` |

The result signature does not cover the lease ID. A replayed result can only reproduce the same events for the same command at the same version, so it is harmless. The commit is idempotent.

Listener acknowledgements are not signed. They travel on a stream whose key was proven by the challenge, and they only affect waiting and ordering, never stored data.

A subscribe request is valid for 60 s around `issued_at`. Within that window a captured request could be replayed, but it only grants the read access the key already has. Transport encryption (MVP) closes this gap.

## Event hash chain

Each stored event has `hash = SHA-256(message)`, where `message` has the tag `kernable.v1.event` followed by these fields:

1. `prev_hash`
2. `global_position: u64`
3. `tenant_id`
4. `aggregate_type`
5. `aggregate_id`
6. `version: u64`
7. `event_type`
8. `payload`
9. `command_id`
10. `created_at: i64`
11. `producer_key_id`
12. `producer_sig`

`prev_hash` is the hash of the event at the previous global position. For the first event it is 32 zero bytes. Every event of a command stores that command's worker result signature as `producer_sig`.

The kernel periodically signs a **checkpoint**, which holds the position and the chain hash at that position.

`kernable verify` recomputes the chain and checks every signature. It detects:

- a modified event: hash mismatch
- a deleted or reordered event: broken link
- a consistently rewritten chain or a truncated log: checkpoint mismatch

Events after the last checkpoint are only protected by the link to their predecessor, so truncating that tail cannot be detected.
