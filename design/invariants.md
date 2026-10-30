# Invariants

Status: H0 of the hardening plan; O5–O7 and X1–X8 made executable in H4.

What must hold whatever happens: any input, any crash, any fault, any interleaving. Every robustness test checks these invariants rather than its own ad-hoc assertions, so a failure names the guarantee that broke. The threat model is in [threat-model.md](threat-model.md).

The **S** and **O** invariants are executable today, in `kernable_store::invariants`:

- `Log::read(store, kernel_keys)` then `Log::check()` for S1–S8;
- a harness records what it was told in an `Observed`, and `Observed::check(&log)` checks O1–O4.

The crash test (`crates/kernable-kernel/tests/crashes.rs`), the storage-fault test (`storage.rs`) and the model-based test (`model.rs`) run all of them. The model-based test also checks O5–O7, X1, X7 and X8 while a seeded random sequence of legitimate and hostile requests runs (`KERNABLE_MODEL_SEEDS`, `KERNABLE_MODEL_STEPS`).

## S: the store

Checked after the fact, on any store, for every backend.

| # | Invariant | Guarantee | Checked by |
|---|---|---|---|
| S1 | **Integrity.** `verify` finds no problem: the hash chain links, every hash matches, checkpoints and revocations carry a kernel signature, every command a client signature, every event group its worker's | tamper evidence | `Log::check` |
| S2 | **Global positions** are exactly 1..=head, in order, and the head is the last event | nothing lost or duplicated | `Log::check` |
| S3 | **Stream versions.** Every stream's versions are exactly 1..=n, in log order | optimistic concurrency | `Log::check` |
| S4 | **Atomic commands.** A command's events are one contiguous run in the log | all events or none | `Log::check` |
| S5 | **Events follow their command.** Every event has its command's tenant, domain, aggregate type and ID, and context | domain isolation | `Log::check` |
| S6 | **One outcome.** A command with events is stored, committed with exactly those positions, and not pending; a committed command's events are in the log; no command is both completed and pending | at most once | `Log::check` |
| S7 | **Schemas only grow.** Per event type, the payload version never goes down in log order | schemas evolve | `Log::check` |
| S8 | **Listener positions** are at most the head | | `Log::check` |

## O: what processes were told

Recorded by the test harness while it runs; checked against the store afterwards.

| # | Invariant | Guarantee | Checked by |
|---|---|---|---|
| O1 | **Stable outcomes.** Every committed or rejected answer a client got for a command ID is exactly what the store holds; answers for one ID never differ | idempotent command IDs, at most once | `Observed::check` |
| O2 | **Acknowledged means delivered.** On each connection, a listener got every event of its types up to what it acknowledged; it never acknowledges beyond a batch; a reconnect never resumes beyond what it acknowledged; its stored position was acknowledged | at least once | `Observed::check` |
| O3 | **Delivery in order.** On one connection, positions strictly increase and start after `Ready.after_position` | ordered delivery | `Observed::check` |
| O4 | **Faithful delivery.** Every delivered event is exactly the stored one and of a type the listener asked for | integrity, confidentiality | `Observed::check` |
| O5 | **Settled means processed.** When a submit returns `settled`, every listener that counts has acknowledged its last event | read your writes | `model.rs`, at every settled submit, also for requests from a listener's handler |
| O6 | **Listener order.** Event N reaches order k only after every counting listener of a lower order acknowledged N | listener order | `model.rs`: every delivery to order 1 against order 0's acknowledgements |
| O7 | **One writer.** At most one lease per aggregate at any time | one writer | `model.rs`: two workers record what they execute (`Held`) |

## X: security

Checked against a running kernel, with S and O run afterwards.

| # | Invariant | Checked by |
|---|---|---|
| X1 | Nothing is accepted without a valid signature by a key whose current grant covers the action and type, and that is not revoked at the time | `model.rs` (forged, unauthorized, revoked, narrowed and replayed-grant submits; refused commands never stored), `e2e.rs` `unsigned_forged_and_unauthorized_commands_are_refused`, `replays.rs` |
| X2 | A domain only writes its own events, and a command's events stay on its stream and tenant | S5 in every `Log::check`; `e2e.rs` `unauthorized_and_impostor_workers_are_refused`, `equal_names_in_two_domains_do_not_collide` |
| X3 | No request covered by a registered middleware passes without that middleware's verdict, also while it is away, retired or being replaced | `middleware.rs`: missing, reconnecting and restarted middlewares, spoofed verdicts; `listeners.rs`: retired by a membership |
| X4 | Signed byte layouts are injective: two different messages never sign the same bytes | `kernable-core/tests/layouts.rs`: property test over commands, results, grants, memberships and queries |
| X5 | Vault values reach only `vault_read` keys; an erased value never comes back; an erasure stays within its tenant | `vault.rs` (a listener without `vault_read` gets none; only allowed workers erase), store `an_erasure_deletes_the_subject_of_its_own_tenant_only` |
| X6 | A listener name or aggregate registration moves to another key only as its declared successor | `keys.rs` `a_listener_moves_to_its_successor_key_and_keeps_its_place`, `e2e.rs` impostor workers |
| X7 | A membership changes only by a signed apply that is newer than the stored one (after F1) | `model.rs`, `replays.rs` |
| X8 | Unsigned fields (`protocol`, `listener`, acks, verdicts, the lease ID) never widen what anyone may do | `model.rs` (random `listener` values); `middleware.rs` `naming_a_listener_never_skips_a_middleware`, `only_the_asked_middleware_decides` (spoofed verdicts); `faults.rs` `a_result_under_a_wrong_lease_never_commits`; acks: O2 |

## R: resources and liveness

Checked by overload and soak runs.

| # | Invariant |
|---|---|
| R1 | Every request gets an answer or an error within its timeout, also under overload |
| R2 | Memory stays bounded whatever the input rate, the payload sizes and the number of connections; overload is refused with `RESOURCE_EXHAUSTED`. Each limit is tested in `tests/limits.rs`; bounded memory under sustained load is measured in H7 |
| R3 | A stored command completes once a worker for its aggregate is connected |
| R4 | A failed write (ENOSPC, EIO, fsync) stops the kernel or the request; it never leaves the store inconsistent (fail stop, never fail corrupt). Checked by `tests/storage.rs` with S1–S8 and O1–O4 |
| R5 | After any crash and restart, S1–S8 hold and processing resumes without help. Checked at 13 crash points and by random `kill -9` under load |
