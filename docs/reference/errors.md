# Errors

A request can fail in two ways:

- **Refused by the kernel:** a gRPC status, before or instead of an answer. The request was not accepted or not completed; the SDKs throw (TypeScript) or raise `grpc.RpcError` (Python).
- **Rejected:** an answer with a code and a message. A command handler, a middleware or a query handler decided against it, or the kernel rejected a worker's result. A rejected command is completed and stored as rejected; resubmitting it with the same ID returns the same rejection. The SDKs return `{ committed: false, code, message }` from `submit` and throw `Rejected` from `query`.

Handlers choose their own codes with `Rejected(message, code)`; the default code is `rejected`. The codes below come from kernable itself. `make check` fails if a code in the sources is missing from this page.

## Rejection codes

| Code | From | Meaning |
|---|---|---|
| `handler_error` | SDK, worker | A command handler threw something other than `Rejected`. The message is the error. |
| `unknown_command` | SDK, worker | The worker has no handler for the command type on this aggregate type. |
| `snapshot_stale` | SDK, worker | The worker cannot read the snapshot it got (another `schema`). The kernel drops the snapshot and runs the command again with every event; this rejection is never final. |
| `version_mismatch` | kernel | The command's `expected_version` (request context) is not the aggregate's version. |
| `concurrency_conflict` | kernel | The aggregate kept changing while the command ran, also after retries. Retry the command. |
| `invalid_event_type` | kernel | A worker returned an event type that is empty or contains a dot. |
| `invalid_event_schema` | kernel | A worker wrote an event at another payload version than it declared. |
| `no_vault` | kernel | A worker's events refer to the vault, but the kernel has no `[vault]`. |
| `invalid_vault_values` | kernel | A worker's vault values do not match the references in its signed events. |
| `vault_error` | kernel | The vault could not erase a subject. |
| `key_revoked` | kernel | The command waited in the queue and was signed by a key revoked since a time before it was accepted. |
| `erase_not_allowed` | kernel | A worker key not listed in `[vault.erasers]` for that event type returned an erasing event. |
| `middleware_error` | SDK, middleware | A middleware threw something other than `Rejected`. The request is refused, as by any rejection. |
| `not_a_middleware` | SDK, listener | A check reached a listener that registered no middleware handler. |
| `unknown_query` | SDK, read model | The read model has no handler for the query type. |

## gRPC statuses

| Status | When |
|---|---|
| `UNAUTHENTICATED` | A missing or invalid signature, a failed key challenge, an expired request (queries and subscriptions are valid 60 s around `issued_at`), or a client certificate the CA did not sign. |
| `PERMISSION_DENIED` | The key is not allowed for this type in the allowlist (`[clients]`, `[workers]`, `[listeners]`, `[queriers]`, `[query_handlers]`, `[middlewares]`, `[privileged]`), or it is revoked. Also ends a worker's, listener's or subscriber's stream when a reload takes its rights away, and refuses a listener registered with another key unless the new key is that key's successor. |
| `INVALID_ARGUMENT` | A malformed request: names that are not qualified, a missing command, vault values that do not match their references, an unknown listener name, a negative middleware order. |
| `FAILED_PRECONDITION` | A protocol version the kernel does not speak (the message names which side to update); an outdated worker (it would write an older event version than the store holds); a request from a listener's handler that would wait for that listener; vault values without a `[vault]`. |
| `UNAVAILABLE` | A middleware that checks the request is not connected; no read model answers the query; the kernel is shutting down. Retry later. |
| `DEADLINE_EXCEEDED` | A submit did not complete within `submit_timeout_ms` (the command is stored and still runs: resubmit it with the same ID to get the result); a query was not answered within `query_timeout_ms`; a middleware did not answer within `middleware_timeout_ms`. |
| `NotSettled` (SDK) | Not a status: the command was committed, but not every listener processed its events within `settle_timeout_ms`. The outcome is attached. |
