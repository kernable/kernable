# Write an SDK for another language

Any language with gRPC and Ed25519 can take part. The TypeScript SDK is the reference implementation: `sdks/typescript/src/rpc.ts` (connection), `signing.ts` (byte layouts) and `index.ts` (everything else). Work through the steps in order; each one builds on the one before.

The details live in the reference: [Protocol](../reference/protocol), [Signatures](../reference/signing), [Errors](../reference/errors), and the [TypeScript](../reference/sdk-typescript), [Python](../reference/sdk-python) and [Rust](../reference/sdk-rust) SDKs for names and options.

## 1. Signatures first

Implement the byte layouts in [Signatures](../reference/signing) before anything else; nothing else works without them.

- Keys: a `.key` file holds a 32-byte Ed25519 seed in hex. The **key ID** is the lowercase hex public key.
- Messages: a `str` tag, then length-prefixed strings and bytes and big-endian integers. Payloads are signed as the exact bytes sent, with no JSON canonicalization.
- Also implement **derived IDs** (`kernable.v1.id`, for `ctx.id()`) and **vault references** (`kernable.v1.vault`).

Test against `proto/test-vectors/signing.json` (see `sdks/typescript/test/signing.test.ts`). Every message and signature must come out byte for byte: `command`, `command_vault`, `query`, `result`, `result_vault`, `rejection`, `snapshot`, `challenge`, `subscribe`, `data_put`, `cache_set`, `membership`, `checkpoint`, `revocation`, `event_hash`, `event_hash_vault`, `derived_id` and `vault_ref`. Skip `grant` and `revoke`: only root-key tooling makes them.

## 2. Connect

Generate gRPC stubs from `proto/kernable/v1/kernable.proto`, package `kernable.v1`.

| Setting | Option | Else | Else |
|---|---|---|---|
| Address | `url` | `KERNABLE_URL` | `http://127.0.0.1:7233` |
| Key file | `key` | `KERNABLE_KEY` | `app.key` in the current directory |
| Grant file | `grant` | `KERNABLE_GRANT` | the `.grant` file next to the key (`app.key` → `app.grant`), if it exists |
| Mutual TLS | `tls` | `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT`, `KERNABLE_TLS_KEY` (all three or none) | plain connection |
| Service | `service` | `KERNABLE_SERVICE` | none |

- Accept `http://host:port`, `host:port` and, on Unix, `unix:/path`.
- **Grant:** send the grant file's content (trimmed) as the gRPC metadata entry `kernable-grant` on every call and every stream opening. It is a hex-encoded `Grant`; send it as it is. Without a grant file, send no entry: the kernel then uses the grant it already holds for the key, if any.
- **Protocol version:** set `protocol: 1` on every request and stream opening (`SubmitRequest`, `QueryRequest`, `SubscribeRequest`, `Register`, `Hello`, and the data, cache and apply requests). Export it as a constant (`PROTOCOL`). The kernel refuses other versions with `FAILED_PRECONDITION`.
- Raise the message size limit for `DataService`: a file can be 16 MiB.

## 3. Clients

All of these are unary calls, except `Subscribe`.

- **`Submit`** (`CommandService`). Build the `Command` with a new UUID as `command_id` unless the caller gives one (it is the idempotency key), default the context's `correlation_id` to the command ID, seal the vault fields (step 7), sign `kernable.v1.command`, and send it with a wait mode:
  - `WAIT_SETTLED` (the default): return the events; if `settled` is false, raise a **NotSettled** error that carries them.
  - `WAIT_COMMITTED`: return the events.
  - `WAIT_ACCEPTED` (`enqueue`): return the command ID.
  - A `rejected` outcome is a normal result (`committed: false`, `code`, `message`), not an error.
- **`Query`** (`QueryService`). Set `issued_at` to now, sign `kernable.v1.query`, send. Return the decoded JSON `response`; raise **Rejected** (with `code` and `message`) on a rejection.
- **`Subscribe`** (`EventService`). Sign `kernable.v1.subscribe` with `after_position`, the event types and `issued_at`. Hand events to the handler one at a time. On any stream end or error, reconnect with `after_position` set to the last handled event, signed again with a new `issued_at`.
- **Data and cache** (`DataService`, `CacheService`). Sign `kernable.v1.data.<op>` or `kernable.v1.cache.<op>` per call; get and delete sign the hash of empty data.

Payloads are JSON, encoded as UTF-8 bytes; an empty payload reads as `{}`.

## 4. Workers

Open `WorkerGateway.Session` and send `Register`:

- `aggregates`: each aggregate as a `WorkerAggregate`, with its qualified type (`shop.Cart`) and qualified command types (`shop.AddItem`).
- `event_schemas`: the payload version this worker writes for every event type its aggregates know (step 8).
- `service`, `key_id`, `worker_id` (for logs), `max_concurrency` (TypeScript: 16), `protocol`.

Answer the `Challenge` with a signature over `kernable.v1.challenge` and the nonce; `Registered` means the worker is connected. Then, for every `Execute`, in parallel up to `max_concurrency`:

1. Find the aggregate by command type; else reject with `unknown_command`.
2. If there is a `snapshot` whose `schema` is not the aggregate's, reject with `snapshot_stale`: the kernel runs the command again with every event.
3. Fold the snapshot state (or the initial state) and the `history`, upcast and unsealed with the `vault` values.
4. Run the handler with the unsealed command. `now()` is `received_at`; `id()` is the n-th derived ID of the command.
5. Seal the new events' vault fields, sign `kernable.v1.result` and send a `Result` with the events, their vault values and, optionally, a signed snapshot.
6. If the handler fails, sign `kernable.v1.rejection` and send a `Rejection`: the handler's own `Rejected` code, else `handler_error` with the error text.

If the worker meets an event with a newer schema than it knows, it must not reject the command: log an error and end the session. The worker needs an update.

## 5. Listeners

Open `ListenerService.Listen` and send `Hello`: `name`, `key_id`, `order`, qualified `event_types` and `query_types`, `middlewares` (at most one per `when`: `order`, types, `WHEN_BEFORE` or `WHEN_AFTER`), `service`, `live`, `protocol`, and `from_position` only when you want to start somewhere else than the stored position (on a reconnect, or for a read model with its own store). Answer the challenge, then wait for `Ready`: events up to `head_position` are a replay.

Handle the deliveries **one at a time, in order**:

| Delivery | What to do |
|---|---|
| `Batch` | Run the handlers for its events (upcast, unsealed with the batch's `vault`), then send `Ack` with `up_to`. With a store, commit the data and `up_to` together before the ack. |
| `QueryCall` | Run the query handler and send a `QueryAnswer` with the JSON response, or a rejection: the handler's code, `unknown_query` if there is no handler, `handler_error` if it fails. |
| `MiddlewareCall` | Run the middleware for its `when` and send a `Verdict`: empty to pass, else a rejection (`not_a_middleware` if there is none for that `when`, `middleware_error` if it fails). After calls carry the outcome. |

**Delivery is at least once.** If a handler fails, end the stream and reconnect with `from_position` set to the last handled position: the kernel delivers the rest again. Handlers must be safe to run twice.

Submits and queries sent from inside a handler must set `listener` to the listener's name, so the kernel never makes them wait for the listener that is busy sending them. A context for a follow-up command keeps the event's correlation ID and sets `causation_id` to the event's command ID.

## 6. Reconnect

Workers, listeners and subscriptions run until stopped. When a stream ends or fails for any reason, log it and reconnect after a delay (TypeScript: 1 s, option `reconnectMs`). A listener resumes from its last handled position, a subscription from its last handled event. Commands in flight when a worker drops go to another worker once their lease expires.

## 7. Vault fields

A domain declares **personal** fields (with a subject, from the payload and the aggregate ID) and **secret** fields per command and event type.

- **Seal** before signing: replace each set field with `{"$vault": ref}`, where `ref` is the vault reference over 16 random salt bytes, the subject (empty for secrets), `secret`, and the field's JSON. Send the values next to the command (`SubmitRequest.vault`) or result (`Result.vault`) and list the refs in `vault_refs`, in payload order.
- **Unseal** on reading (`Execute`, `Batch`, `MiddlewareCall`): put the values back; a reference without a value reads as `null`.
- An event type that erases a subject sets `erases` on its `NewEvent`.

## 8. Event versions

A domain declares per event type its current `schema` and an upcast from each older version. Write events with their schema; before handing an event to a fold or a handler, upcast it step by step. An event whose schema is newer than known, or with a missing step, is an error, not something to skip. See [Change an event's format](./event-versions).

## 9. Memberships and apps

- **`apply(manifest)`** sends a `Membership` to `MembershipService.Apply`: the service, the qualified aggregate types of its workers, the names of its listeners and of its middlewares, metadata in the given order, and `issued_at` now, signed over `kernable.v1.membership`. The key needs `manage` for the service.
- **`app({ service, version, description, domains })`** runs whole domains as one service. Its `run()` applies the membership (all aggregates as workers; durable listeners without middlewares as listeners, those with one as middlewares; live listeners not listed; `version` and `description` in the metadata), then runs one worker and every listener.

See [Remove a listener, middleware or aggregate](./remove-listener).

## 10. Errors

Pass the kernel's gRPC status to the caller with its code, so callers can act on it ([Errors](../reference/errors)):

| What | Where | SDK |
|---|---|---|
| `NOT_FOUND` | submit | No worker registered the aggregate or command type, and no membership lists it. Raise it. |
| `UNAVAILABLE` | submit, query | A middleware is away, no read model answers, or the kernel is shutting down. Raise it; the caller retries. On a stream: reconnect. |
| `PERMISSION_DENIED` | any call or stream | The grant does not allow it, or the key is revoked. Raise it; on a stream, log and reconnect. |
| `FAILED_PRECONDITION` | any | Unsupported protocol version, an outdated worker, or a handler request that would wait for its own listener. Raise it. |
| `version_mismatch` and other rejections | submit outcome | Not an error: return `committed: false` with the code. |
| `handler_error` | worker, read model | What the SDK sends when a handler fails with something other than `Rejected`. |

## 11. Logs

Print connection and activity lines to stderr as `HH:MM:SS  what  line`, for example `12:04:31  listener carts  connected to …`, with the level from `KERNABLE_LOG` (`off`, `error`, `warn`, `info` by default, `debug`) and colors only on a terminal without `NO_COLOR`. An app prints one overview instead, once everything is connected; match the format in [Getting started](../tutorial/getting-started).

## 12. Prove it works

1. The signing test vectors pass (step 1).
2. Port the tutorial's shop (`docs/examples/shop/ts`) to your language, with the same output as the `.out` files.
3. Run it in [part 5's](../tutorial/shop/5-languages) mix: your worker with the TypeScript read models and the Rust reactor, and every other combination (see `docs/examples/shop.test.ts`). If it works next to the TypeScript, Python and Rust processes, it speaks kernable.
