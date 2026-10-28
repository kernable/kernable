# Write an SDK for another language

Any language with gRPC can take part. An SDK is a few hundred lines; the TypeScript SDK (`sdks/typescript/src`) is the model to follow.

## 1. The protocol

Generate gRPC stubs from `proto/kernable/v1/kernable.proto` ([reference](../reference/protocol)). Every request and stream opening carries `protocol: 1`.

Every call sends the key's grant in the gRPC metadata entry `kernable-grant`: submit, query, subscribe, and the opening of worker and listener streams. The value is the content of the `.grant` file that `kernable grant` writes, a hex-encoded `Grant`; send it as it is, without decoding it. Take the file from a `grant` option, else from `KERNABLE_GRANT`, else the `.grant` file next to the key file (`app.key` → `app.grant`), and send nothing if there is none. Take the key file from a `key` option, else from `KERNABLE_KEY`, else `app.key` in the current directory; the address from a `url` option, else `KERNABLE_URL`, else `http://127.0.0.1:7233`; and mutual TLS from a `tls` option, else from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY` (all three or none). A request without a grant is decided by the grant the kernel already holds for the key, if any.

## 2. Signatures

Implement the byte layouts in [Signatures](../reference/signing): commands, results, queries, subscriptions, snapshots, rejections, challenges, derived IDs and vault references. Ed25519 keys are 32-byte seeds in hex; the key ID is the hex public key.

Test your implementation against `proto/test-vectors/signing.json`: every message and signature there must come out byte for byte, except `grant` and `revoke`, which only root-key tooling produces.

## 3. Clients

- `Submit`: build the command, sign it, send it with a wait mode; map the outcome to committed or rejected, and `settled: false` to a "not settled" error.
- `Query`: set `issued_at` to now, sign, send; an answer is JSON or a rejection.

## 4. Workers

Open `WorkerGateway.Session`: send `Register` with the command types and the event schemas you write, answer the `Challenge` with a signature over its nonce, then loop: for each `Execute`, fold the snapshot and history (after upcasts) into state, run the handler, and send a signed `Result` or `Rejection`. Use `received_at` for `now()` and derived IDs for `id()`.

## 5. Listeners

Open `ListenerService.Listen`: send `Hello` (name, order, event types, query types, middleware), answer the challenge, wait for `Ready`, then handle messages **one at a time**: run handlers for a `Batch` and acknowledge its `up_to`; answer a `QueryCall`; give a `Verdict` on a `MiddlewareCall`. Reconnect after errors with `from_position` set to your last handled position.

Name the listener in submits and queries sent from its handlers (`listener`), so the kernel never makes them wait for it.

## 6. The rest of the API

To match the other SDKs: domains with event schemas and upcasts, vault fields (seal before signing, unseal on reading), read model stores, and the same names. Then run the tutorial's shop with your SDK in part 5's mix: if it works next to the TypeScript, Python and Rust processes, it speaks kernable.
