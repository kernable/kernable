# Data over time

The log is kept forever, so everything about its data is designed to change without rewriting it.

## Where kernable keeps what

| Data | Kept | Deletable | Verifiable | Who reads it |
|---|---|---|---|---|
| **events** (the log) | forever | never | hash chain, worker signatures | listeners and subscriptions, per grant |
| **commands** | forever, with their outcome | never | client signatures | the kernel; `kernable verify` |
| **vault values** | until the person is erased; secrets until their command completes | by an erasing event | references in the log | keys granted `vault_read` |
| **snapshots** | until replaced, erased, or of an old `schema` | yes: a cache | worker signature | workers |
| **files** | until deleted | yes, but **not** by an erasure | signed requests | keys granted `data_read` |
| **cache** values | until they expire or the kernel restarts | yes | signed requests | keys granted `cache` |
| **read model stores** | in your process | yours to manage | no | the read model |

The log only grows: there is no compaction, on purpose, because a verifiable history cannot lose events. Events are small JSON, so a store of millions of events is usually a few gigabytes. Keep data that should expire in the cache, large binaries in files, and back up the store and the vault ([Back up and restore](../how-to/backup)).

## Event versions

Every event carries its payload's **schema** version (from 1), signed and part of the hash. To change a payload, raise the version and add an **upcast** from the old one:

<<< @/examples/concepts/upcast.ts#upcast

Aggregates and listeners get every event upcast to the version they know; stored events stay as they were:

| Position | Stored as | Schema | A reader on version 2 gets |
|---|---|---|---|
| 41 | `{"sku":"apple","qty":2}` | 1 | `{"sku":"apple","count":2}`, upcast |
| 57 | `{"sku":"pear","count":3}` | 2 | `{"sku":"pear","count":3}` |

**When to raise the version:** whenever a reader on the old version would misread the new payload, by renaming, removing, or changing the meaning or type of a field. A new optional field needs no new version if every reader can ignore it; it does if old readers must not miss it. Upcasts chain: a version-1 payload reaches version 3 through `upcast(1, …)` and `upcast(2, …)`.

Rules:

- **A process that meets a newer version stops** instead of misreading it: it logs, retries, and keeps its position until it is updated. Listeners after it wait too, because of the order. So **deploy readers before writers**.
- **A worker cannot write an older version** than the store holds: the kernel refuses it. Old and new workers never write side by side.
- A handler marked `raw` gets events as stored, for archives and exports.

## Formats and protocol

- **Protocol versions.** Every request carries the protocol version of its SDK; the kernel accepts a range and names the side to update otherwise.
- **Store formats.** A store records its data format. A kernel opens only its own format and refuses others at startup; `kernable migrate` upgrades an older store, explicitly and after a backup. Stores of every past format are kept in kernable's tests and must stay readable.

## Personal data: the vault

Events cannot be deleted, but personal data must be deletable. Fields declared `personal` (with the person they belong to) or `secret` go to the **vault** instead of the log; the log keeps a salted reference:

```json
{ "name": { "$vault": "41a4ffe62b20…" }, "email": { "$vault": "c56522f357b1…" } }
```

<FlowDiagram name="vault" />

- Workers and listeners granted `vault_read` get the values; everyone else gets `null`.
- Secrets are deleted once their command completes.
- An **erasing event** deletes a person's values in every stream and domain of its tenant, and the snapshots that may contain them. Read models must forget the person when they see it; rebuilt, they read `null`.
- The vault keeps each value encrypted with the vault key, together with its subject and whether it is secret. An erasure leaves a **proof** without the person: the SHA-256 of the subject, the erasing command, how many values went, and when.
- **Files and cache values are not erased** with the person: they are not part of the vault. Delete a person's files yourself when you erase them ([Erase a person](../how-to/erasure)).

## Snapshots and read model stores

- **Snapshots** of aggregates live in their own store, not in the log: a cache that workers send every 100 events (`snapshotEvery`) and rebuild whenever it is gone or of another `schema`. The kernel accepts a snapshot only from the worker that ran the command, signed by it, and only at exactly the version after the command's events; it stores it with the hash of that event. A worker that cannot read a snapshot (an unknown `schema`) rejects it, and the kernel drops it and sends all events instead.
- **Read models** can keep their state in a **store** in their own process: data and position are written together per batch, so a restart continues where it left off. A new `schema` rebuilds from the first event.
