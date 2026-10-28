# Failures and recovery

kernable is built so that a crash anywhere loses nothing and does nothing twice. kernable's tests abort the real kernel at each point where it has stored something but not yet told anyone, restart it, and check exactly that.

| When this fails | what happens | what you do |
|---|---|---|
| A **worker** crashes during a command | The kernel hands the command to another worker at once (or after `--lease-ttl-ms` if the connection hangs). It runs again from the same history, so it decides the same way. | nothing; keep handlers deterministic |
| A **listener** crashes | It reconnects and gets the events after its last acknowledgement again. Submits that settle wait for it (up to `--settle-timeout-ms`). | make handlers idempotent, or use a store |
| A **middleware** is away | Everything it checks is refused with `UNAVAILABLE` until it is back. | run it next to the kernel; deploy scripts wait for `/ready` |
| The **kernel** crashes | Every stored command is still queued and runs right after the restart; listeners resume after their last acknowledgement. Clients get a connection error. | resubmit with the same command ID: you get the original outcome |
| The kernel gets **SIGTERM** | It refuses new requests, lets running ones finish for `--drain-timeout-ms`, stores its state and exits cleanly. | stop the kernel this way for deploys |
| The **file log** is torn by a crash mid-write | The torn last record is moved aside and cut off on the next start. | nothing |
| A file is **damaged or changed** | Damage in the middle refuses to open; a consistent rewrite shows in `kernable verify`. | restore from a backup |
| A process speaks an **old protocol** or meets a **newer event version** | It is refused with a message, or stops at that event. | update it |

## What kernable guarantees

- A command is stored before it runs and completes **at most once**: committed or rejected, with all its events or none.
- Events are never lost, reordered or duplicated in the log; `kernable verify` proves it.
- Listeners see every event, in order, **at least once**; with a store, exactly once.
- A settled submit has been processed by every listener that was registered.

## What it does not

- **One kernel per store.** There is no failover to a second kernel yet; a restarted kernel picks up where it stopped.
- Side effects of reactors (emails, calls to other systems) happen at least once. Make them idempotent, or record them as commands first.
