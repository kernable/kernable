# Invariants

Status: O5–O7 and X1–X8 executable; the soak's bank rules B1–B7; replication and failover P1–P5 built and checked ([below](#p-replication-and-failover)). Covers parked events, erasure after the commit, file writes once tried and requirements by key.

What must hold whatever happens: any input, any crash, any fault, any interleaving. Every robustness test checks these invariants rather than its own ad-hoc assertions, so a failure names the guarantee that broke.

The **S** and **O** invariants are executable today, in `kernable_store::invariants`:

- `Log::read(store, kernel_keys)` then `Log::check()` for S1–S9;
- a harness records what it was told in an `Observed`, and `Observed::check(&log)` checks O1–O4.

The crash test (`crates/kernable-kernel/tests/crashes.rs`), the storage-fault test (`storage.rs`), the model-based test (`model.rs`) and every check round of the soak (`crates/kernable-soak`) run all of them. The model-based test also checks O5–O7, X1, X7 and X8 while a seeded random sequence of legitimate and hostile requests runs (`KERNABLE_MODEL_SEEDS`, `KERNABLE_MODEL_STEPS`).

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
| S9 | **Unique keys.** No key ID is held by two streams; every key has either an ID or a vault reference (a personal key); only streams with events hold keys or facts (a command that leaves its aggregate without events holds nothing). Personal keys are unique through the vault, which files each by a hash of its ID keyed with the vault key; the store cannot see their IDs | unique keys | `Log::check` |

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
| O8 | **Parked, never lost.** An event a reactor gives up on is parked with its reason and stays parked, also across a crash, until a retry gets past it or it is dismissed; the listener's position moves on. Only a rejection, or an error repeated `park_after` times, parks; a kernel that is away or busy never does, nor a read model, a live listener, or an erasing event | nothing lost | `listeners.rs` (`a_parked_event_stays_parked_until_a_retry_gets_past_it`, `a_retry_waits_for_the_listener_and_survives_a_restart`), `crashes.rs` (a listener parks at every crash point), three tests per SDK, soak B7 |
| O9 | **One instance acts.** A single group delivers to one instance at a time; an instance never starts behind the group's position, so a reactor's side effect is not made twice by two instances; a replicated group's instances never park | one instance | `tests/listeners.rs` (`one_instance_of_a_single_listener_runs_and_the_next_takes_over`), SDK tests per language |

## X: security

Checked against a running kernel, with S and O run afterwards.

| # | Invariant | Checked by |
|---|---|---|
| X1 | Nothing is accepted without a valid signature by a key whose current grant covers the action and type, and that is not revoked at the time. A signed write the kernel tried is never accepted again, also when it failed | `model.rs` (forged, unauthorized, revoked, narrowed and replayed-grant submits; refused commands never stored), `e2e.rs` `unsigned_forged_and_unauthorized_commands_are_refused`, `replays.rs` (`a_failed_data_write_cannot_be_sent_again_under_the_same_signature`), `listeners.rs` `parked_events_are_checked` (retries need `manage`) |
| X2 | A domain only writes its own events, and a command's events stay on its stream and tenant | S5 in every `Log::check`; `e2e.rs` `unauthorized_and_impostor_workers_are_refused`, `equal_names_in_two_domains_do_not_collide` |
| X3 | No request covered by a registered middleware passes without that middleware's verdict, also while it is away, retired or being replaced | `middleware.rs`: missing, reconnecting and restarted middlewares, spoofed verdicts; `listeners.rs`: retired by a membership |
| X4 | Signed byte layouts are injective: two different messages never sign the same bytes | `kernable-core/tests/layouts.rs`: property test over commands, results, indexes (with requirements by key and absent ones), grants, memberships and queries |
| X5 | Vault values reach only `vault_read` keys; an erased value never comes back, neither from the kernel nor from an SDK's listener store; an erasure stays within its tenant; values are erased only after their erasing event is committed, and every committed erasure is carried out, also after a crash | `vault.rs` (a listener without `vault_read` gets none; only allowed workers erase), store `an_erasure_deletes_the_subject_of_its_own_tenant_only`, `crashes.rs` (`erasure.intended`, `erasure.pending`, `erasure.done`), SDK store tests (the WAL emptied on open) |
| X6 | A listener name or aggregate registration moves to another key only as its declared successor | `keys.rs` `a_listener_moves_to_its_successor_key_and_keeps_its_place`, `e2e.rs` impostor workers |
| X7 | A membership changes only by a signed apply that is newer than the stored one | `model.rs`, `replays.rs` |
| X8 | Unsigned fields (`protocol`, `listener`, acks, verdicts, the lease ID, parked events in an ack) never widen what anyone may do | `model.rs` (random `listener` values); `middleware.rs` `naming_a_listener_never_skips_a_middleware`, `only_the_asked_middleware_decides` (spoofed verdicts); `faults.rs` `a_result_under_a_wrong_lease_never_commits`, `a_late_result_that_arrives_before_the_new_holders_never_commits`; acks: O2 |
| X9 | A grant that narrows narrows open streams at once: a listener or subscription gets only the types the newest grant covers, also one that asked for every type | `subscribe.rs` `a_narrowed_grant_narrows_an_open_subscription_to_every_type`, `listeners.rs` `a_narrowed_grant_narrows_a_listener_of_every_type` |
| X10 | Keys, facts and requirements stay within the command's tenant: a requirement by ID or by key sees only aggregates of that tenant (a global key held in another tenant names none); a rejection names the key or fact, never its value or hash | `uniqueness.rs` (`a_requirement_names_its_aggregate_by_a_unique_key`, `only_the_kernel_names_a_key_or_requirement_in_a_rejection`) |

## R: resources and liveness

Checked by overload and soak runs.

| # | Invariant |
|---|---|
| R1 | Every request gets an answer or an error within its timeout, also under overload |
| R2 | Memory stays bounded whatever the input rate, the payload sizes and the number of connections; overload is refused with `RESOURCE_EXHAUSTED`. Each limit is tested in `tests/limits.rs`; bounded memory under sustained load is measured by the soak. A listener holds at most 10,000 parked events; beyond that its ack is refused and it stops instead of parking everything |
| R3 | A stored command completes once a worker for its aggregate is connected, also when the aggregate's history is longer than one message (it is sent in parts) |
| R4 | A failed write (ENOSPC, EIO, fsync) stops the kernel or the request; it never leaves the store inconsistent (fail stop, never fail corrupt). Checked by `tests/storage.rs` with S1–S9 and O1–O4 |
| R5 | After any crash and restart, S1–S9 hold and processing resumes without help. Checked at 15 crash points and by random `kill -9` under load (including the erasure points) and by the soak's kernel kills |

## P: replication and failover

Checked by `crates/kernable-kernel/tests/replication.rs` (P1 after every answer, P3 after a promotion, P4 with a standby that holds a tail of its own) and `crates/kernable-store/tests/replication.rs` (records that do not continue a log are refused). With the witness, `tests/pair.rs` checks P1 across a failover and P2 in the form it takes there: a primary alone tells nothing once its lease ran out; the witness unit tests check that a deposed primary is refused its lease and the right to go alone. `tests/replication_kills.rs` checks P1 and P3 under random `kill -9` of primary and standby, and of a pair's primary across failovers, with every S invariant at the end; `tests/pair.rs` checks that a deposed primary stops. `tests/replication_crashes.rs` aborts the primary and the standby at each replication crash point. The soak with `--pair yes` (2026-11-04) checks P2 in every check round across failovers: 60 minutes on SQLite (153 faults: kernel kills, kernels away longer than the lease, network down, app kills; 18 failovers; 448,750 commands) and 60 minutes on the file log (173 faults, 23 failovers, 249,668 commands), 6 rounds each without a violation. The model-based test (2026-11-04, `a_pair_under_seeded_kills_and_pauses_keeps_p1_to_p4` in `tests/replication_kills.rs`) runs a seeded sequence of faults on a pair: kills of either kernel and of the witness, and pauses (`SIGSTOP`) of either kernel shorter or longer than the lease, so a paused primary wakes up deposed. Clients submit and a listener follows the pair throughout. Afterwards O1–O4 hold against the primary's log for everything a client or the listener was told (P1, P3), the standby holds the same events (P2, P4), and S1–S9 hold on both. Seeds 1 to 3 run in `make check` on both backends; 30 seed runs on SQLite and 6 on the file log passed. A primary that answers before its standby stored the record (a mutant) fails it at seed 3.

| # | Invariant | Guarantee |
|---|---|---|
| P1 | **Replicated before seen.** No result reaches a client and no event reaches a listener before the in-sync set has stored it | nothing anyone saw is lost on failover |
| P2 | **One primary per term.** At most one kernel commits per term; a replication record of an older term is refused | no split brain |
| P3 | **Promoted means complete.** A promoted standby holds every acknowledged record | RPO 0 |
| P4 | **The unacknowledged tail is cut.** A former primary that rejoins cuts the records after the point where the terms diverge, keeps them aside (like `<path>.torn-<offset>`) and catches up from the new primary; by P1, nobody saw them | the log converges |
| P5 | **A handover tells nothing twice and nothing lost.** A switchover hands over only once the standby in sync holds every record the primary wrote; after HANDOVER the witness renews no lease of the old primary, which tells nothing more, and only the standby in sync may claim | planned updates without downtime; checked by `tests/pair.rs` (`a_switchover_hands_the_primary_role_over_without_waiting_for_the_lease`), witness unit tests, the soak's `switchover` fault |

## B: the soak's bank rules

Checked by every check round of the soak (`crates/kernable-soak/src/check.rs`), with S1–S9 and O1–O4, after load and faults pause and every listener has reached the head. Each bank app (TypeScript, Python, Rust) keeps accounts, a read model with a store, and a reactor that flags big withdrawals.

| # | Rule | Guards |
|---|---|---|
| B1 | **Never below zero.** No account's balance goes below zero in the log | one writer, optimistic concurrency |
| B2 | **Flagged once.** Every withdrawal of 50 or more is flagged exactly once, and every flag belongs to one; a missing flag names what became of its command | at-least-once delivery, idempotent command IDs |
| B3 | **Read models equal the log.** Each app's read model answers what the log says | listener stores, redelivery |
| B4 | **One holder.** An account number is held by one account at a time, and the kernel's index equals what the log derives | S9, unique keys |
| B5 | **Requirements honoured.** No transfer reached an account that was not open at its commit | atomic requirements |
| B6 | **Justified refusals.** Every rejection had its reason in the log at the time | atomic checks |
| B7 | **Justified parking, nothing lost silently.** Only rejected flags are parked, and a big withdrawal's flag is either committed or parked | O8, parking |
