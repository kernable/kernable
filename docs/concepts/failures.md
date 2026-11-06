# Failures and recovery

kernable is built so that a crash anywhere loses nothing and does nothing twice. kernable's tests abort the real kernel at each point where it has stored something but not yet told anyone, restart it, and check exactly that.

| When this fails | what happens | what you do |
|---|---|---|
| A **worker** crashes during a command | The kernel hands the command to another worker at once (or after `--lease-ttl-ms` if the connection hangs). It runs again from the same history, so it decides the same way. | nothing; keep handlers deterministic |
| A **listener** crashes | It reconnects and gets the events after its last acknowledgement again. Submits that settle wait for it (up to `--settle-timeout-ms`). | make handlers idempotent, or use a store |
| A **command handler throws** (a bug, not `Rejected`) | The command is rejected with the code `handler_error`. The rejection is kept for good and goes back to the sender, so it names only the error's type (`TypeError`; Rust: `handler failed`): error texts can hold personal data or internals. The worker logs the whole error. It is not retried: a deterministic handler would throw again. No event is stored. | fix the bug; the sender sees the code and can submit again, with a new command ID (the same ID returns the stored rejection) |
| A **read model's handler throws** on one event | The read model reconnects and gets that event again, until the handler gets past it: skipping it would leave its state wrong. It and every listener of a higher order wait; settling submits end in `NotSettled`. | fix the handler and deploy it; the [UI](../how-to/monitor#the-ui) shows the listener as *behind* |
| A **reactor's follow-up is rejected** (`ctx.submit`), or its handler throws `Rejected` | The reactor **parks** the event at once and moves on: the kernel keeps its position, type, code and message for the listener. Retrying the same command ID returns the same rejection, so trying again would never get further. | `kernable listeners parked` lists it, the UI and `kernable_listener_parked` show it; fix the cause (or the handler), then `kernable listeners retry` it, or `dismiss` it |
| A **reactor's handler throws** something else (a bug, a mail server that is down) | It gets the event again, backing off like a reconnect; after 5 attempts (`parkAfter`, `park_after`) it parks the event as `handler_error`, with only the error's type, and moves on. The log has the whole error. | as above; set `parkAfter: 0` to keep waiting instead. An event that **erases** a subject is never parked: the reactor tries until its handler gets past it, so no personal data stays behind |
| A reactor's handler fails because the **kernel is away or busy** (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `NotSettled`) or its store fails | It waits and tries again for as long as it takes; this never parks. | bring the kernel back |
| A listener's **store** cannot be opened (a full disk) | The listener logs an error and tries again, backing off like a reconnect (up to 30 s), until the store opens; the rest of the app keeps running. Until then it and every listener of a higher order wait. | free space or fix the path |
| A submit does not **settle** in time | `NotSettled` after `--settle-timeout-ms`. The command is committed; only the guarantee that every listener has it is missing. | find the listener that is behind; submitting the same ID again returns the same outcome |
| No worker **registered** an aggregate | Its commands are refused with `NOT_FOUND` at once, also a typo in a type. Once registered, its commands wait in the queue while its workers are away, and `/ready` answers 503. | start the worker before sending its commands |
| A **middleware** is away | Everything it checks is refused with `UNAVAILABLE` until it is back. | run it next to the kernel; deploy scripts wait for `/ready` |
| The **kernel** crashes | Every stored command is still queued and runs right after the restart; listeners resume after their last acknowledgement. Clients get a connection error. | resubmit with the same command ID: you get the original outcome |
| The **network** between a process and the kernel drops | A worker's commands go to another worker, at once or when its lease expires; a listener resumes after its last acknowledgement. A client's request waits for the connection up to its timeout (60 s by default), then fails. The SDKs reconnect by themselves, waiting longer after each failed attempt (up to 30 s); keepalive pings notice a connection that died silently within 40 s. | resubmit with the same command ID |
| A process's **clock** is off by more than 60 s | Its queries, subscriptions, file and cache requests and memberships are refused (`expired; check the clock`). Commands carry no time window and are not affected. | keep clocks in sync (NTP) |
| A **key is revoked** | The kernel refuses it from then on, ends its connections, and rejects its commands still in the queue. | give the service a new key and grant |
| The kernel gets **SIGTERM** | It refuses new requests, lets running ones finish for `--drain-timeout-ms`, stores its state and exits cleanly. The primary of a pair hands its role to the standby in sync first, so the standby serves at once. | stop the kernel this way for deploys |
| The **file log** is torn by a crash mid-write | The torn last record is moved aside and cut off on the next start. | nothing |
| The **machine** loses power or its OS crashes | With `--durability full` (the default) every committed command was on the disk before anyone was told: nothing acknowledged is lost. With `normal` (the default of `--dev`) the last moments of commands may be lost. | keep `full` in production |
| The **disk** is full or fails a write | The request fails with `INTERNAL` and nothing half-written stays behind; a write that failed after it took effect is found and answered with its stored result. A command whose events cannot be stored waits in the queue and runs again after a pause that doubles each time, up to 5 s, until the disk takes it. If the file log cannot sync, it refuses every further write until a restart. | free space or fix the disk; resubmit with the same command ID. A file write that failed is signed anew (the same signature counts as tried: it may have taken effect) |
| A **second kernel** opens the same store | It is refused: the file log and SQLite (`<store>.lock`) admit one writer. `kernable verify` and the inspection commands still read a running kernel's store. | run one kernel per store |
| The **primary's machine** is lost (with a [standby](../how-to/standby)) | The standby holds every write anyone was told of: commits, listener acknowledgements, files. Writes the primary had not confirmed yet are gone, and nobody heard of them. With a witness, the standby takes over by itself within about 4 s. | point your processes at the new primary; without a witness, `kernable promote` it first |
| The **standby** is away (`--replication sync`) | With a witness, the primary commits alone after `--standby-timeout-ms`. Without one, its writes wait: commands stay queued, submits time out, `/ready` answers 503. Back, the standby catches up. | bring it back; without a witness, or restart the primary without `--replication sync` (see [Run a standby](../how-to/standby)) |
| A file is **damaged or changed** | Damage in the middle refuses to open; a consistent rewrite shows in `kernable verify`. | restore from a backup |
| A process speaks an **old protocol** or meets a **newer event version** | It is refused with a message, or stops at that event. | update it |

## What kernable guarantees

- A command is stored before it runs and completes **at most once**: committed or rejected, with all its events or none.
- With `--durability full`, what a client, worker or listener was told is on the disk: a power loss does not take it back.
- Events are never lost, reordered or duplicated in the log; `kernable verify` proves it.
- Listeners see every event, in order, **at least once**; with a store, exactly once.
- A settled submit has been processed by every listener that [counts](./ordering#who-counts).

The tests prove it, with both stores, in three ways, and check the same invariants every time (no event lost, reordered or duplicated, one outcome per command, every listener given every event it acknowledged, an intact hash chain):

- they abort the kernel at each of the points below, restart it, and check that nothing was lost or done twice;
- they `kill -9` it at random moments while clients keep placing orders, and restart it, hundreds of times;
- they run it on a store that fails a share of its reads and writes, before or after the write took effect.

| Crash point | Stored | Not yet |
|---|---|---|
| `submit.enqueued` | the command | the client's answer |
| `execute.sent` | nothing for it yet | a worker has the command |
| `commit.stored` | the events | anyone was told |
| `snapshot.stored` | the snapshot, after the events | anyone was told |
| `reject.stored` | the rejection | anyone was told |
| `listeners.acked` | a listener's acknowledgement, in memory | its position on disk |
| `listeners.flushed` | listener positions | — |
| `settle.done` | every listener processed the events | the client's answer |
| `checkpoint.stored` | a signed checkpoint | — |
| `vault.stored` | a client's vault values | the command in the queue |
| `erasure.intended` | the subject's erasure, noted | the erasing event |
| `erasure.pending` | the erasing event | the subject's values erased (done at the next start, before anything is served) |
| `erasure.done` | the erasing event, the subject's values erased | anyone was told |
| `membership.stored` | a service's membership | the membership in effect |
| `revocation.stored` | a revocation | the key refused |


## What it does not

- **One standby.** A pair has one primary and one standby; a second standby comes later.
- Side effects of reactors (emails, calls to other systems) happen at least once. Make them idempotent, or record them as commands first.

## Finding the cause

| What you see | Where to look |
|---|---|
| submits end in `NotSettled` | the UI's map or connections: which listener is *behind*, and by how many events; `/ready` names it |
| commands wait and never run | the UI: an aggregate without a worker (red); `kernable_queued_commands` in the metrics |
| requests refused with `UNAVAILABLE` | a middleware is away: the UI shows it as missing |
| commands refused with `NOT_FOUND` | no worker registered the aggregate or command type, and no membership lists it: often a typo |
| commands rejected with `handler_error` | the rejection names the error's type; the worker's log has the whole error, with its stack |
| a listener restarts again and again | its log: `connection lost (…)` with the handler's error, then `connected again at position N`; it is stuck at the event after N |

