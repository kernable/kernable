# Failures and recovery

kernable is built so that a crash anywhere loses nothing and does nothing twice. kernable's tests abort the real kernel at each point where it has stored something but not yet told anyone, restart it, and check exactly that.

| When this fails | what happens | what you do |
|---|---|---|
| A **worker** crashes during a command | The kernel hands the command to another worker at once (or after `--lease-ttl-ms` if the connection hangs). It runs again from the same history, so it decides the same way. | nothing; keep handlers deterministic |
| A **listener** crashes | It reconnects and gets the events after its last acknowledgement again. Submits that settle wait for it (up to `--settle-timeout-ms`). | make handlers idempotent, or use a store |
| A **command handler throws** (a bug, not `Rejected`) | The command is rejected with the code `handler_error` and the error's message. It is not retried: a deterministic handler would throw again. No event is stored. | fix the bug; the sender sees the code and can submit again |
| A **listener handler throws** on one event | The listener reconnects and gets that event again, until the handler gets past it. It and every listener of a higher order wait; settling submits end in `NotSettled`. | fix the handler and deploy it; the [UI](../how-to/monitor#the-ui) shows the listener as *behind* |
| A submit does not **settle** in time | `NotSettled` after `--settle-timeout-ms`. The command is committed; only the guarantee that every listener has it is missing. | find the listener that is behind; submitting the same ID again returns the same outcome |
| No worker **registered** an aggregate | Its commands are refused with `NOT_FOUND` at once, also a typo in a type. Once registered, its commands wait in the queue while its workers are away, and `/ready` answers 503. | start the worker before sending its commands |
| A **middleware** is away | Everything it checks is refused with `UNAVAILABLE` until it is back. | run it next to the kernel; deploy scripts wait for `/ready` |
| The **kernel** crashes | Every stored command is still queued and runs right after the restart; listeners resume after their last acknowledgement. Clients get a connection error. | resubmit with the same command ID: you get the original outcome |
| The **network** between a process and the kernel drops | A worker's commands go to another worker, at once or when its lease expires; a listener resumes after its last acknowledgement; a client gets an error. The SDKs reconnect by themselves. | resubmit with the same command ID |
| A process's **clock** is off by more than 60 s | Its queries, subscriptions, file and cache requests and memberships are refused (`expired; check the clock`). Commands carry no time window and are not affected. | keep clocks in sync (NTP) |
| A **key is revoked** | The kernel refuses it from then on, ends its connections, and rejects its commands still in the queue. | give the service a new key and grant |
| The kernel gets **SIGTERM** | It refuses new requests, lets running ones finish for `--drain-timeout-ms`, stores its state and exits cleanly. | stop the kernel this way for deploys |
| The **file log** is torn by a crash mid-write | The torn last record is moved aside and cut off on the next start. | nothing |
| A file is **damaged or changed** | Damage in the middle refuses to open; a consistent rewrite shows in `kernable verify`. | restore from a backup |
| A process speaks an **old protocol** or meets a **newer event version** | It is refused with a message, or stops at that event. | update it |

## What kernable guarantees

- A command is stored before it runs and completes **at most once**: committed or rejected, with all its events or none.
- Events are never lost, reordered or duplicated in the log; `kernable verify` proves it.
- Listeners see every event, in order, **at least once**; with a store, exactly once.
- A settled submit has been processed by every listener that [counts](./ordering#who-counts).

The tests prove it, with both stores: they abort the kernel at each of these points, restart it, and check that nothing was lost or done twice.

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


## What it does not

- **One kernel per store.** There is no failover to a second kernel yet; a restarted kernel picks up where it stopped.
- Side effects of reactors (emails, calls to other systems) happen at least once. Make them idempotent, or record them as commands first.

## Finding the cause

| What you see | Where to look |
|---|---|
| submits end in `NotSettled` | the UI's map or connections: which listener is *behind*, and by how many events; `/ready` names it |
| commands wait and never run | the UI: an aggregate without a worker (red); `kernable_queued_commands` in the metrics |
| requests refused with `UNAVAILABLE` | a middleware is away: the UI shows it as missing |
| commands refused with `NOT_FOUND` | no worker registered the aggregate or command type, and no membership lists it: often a typo |
| commands rejected with `handler_error` | the rejection's message names the error; with `KERNABLE_LOG=debug`, the worker logs each rejection too |
| a listener restarts again and again | its log: `connection lost (…)` with the handler's error, then `connected again at position N`; it is stuck at the event after N |

