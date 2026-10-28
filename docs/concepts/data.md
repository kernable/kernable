# Data over time

The log is kept forever, so everything about its data is designed to change without rewriting it.

## Event versions

Every event carries its payload's **schema** version (from 1), signed and part of the hash. To change a payload, raise the version and add an **upcast** from the old one:

<<< @/examples/concepts/upcast.ts#upcast

Aggregates and listeners get every event upcast to the version they know; stored events stay as they were. Rules:

- **A process that meets a newer version stops** instead of misreading it: it logs, retries, and keeps its position until it is updated. Listeners after it wait too, because of the order. So **deploy readers before writers**.
- **A worker cannot write an older version** than the store holds: the kernel refuses it. Old and new workers never write side by side.
- A handler marked `raw` gets events as stored, for archives and exports.

## Formats and protocol

- **Protocol versions.** Every request carries the protocol version of its SDK; the kernel accepts a range and names the side to update otherwise.
- **Store formats.** A store records its data format. A kernel opens only its own format and refuses others at startup; `kernable migrate` upgrades an older store, explicitly and after a backup. Stores of every past format are kept in kernable's tests and must stay readable.

## Personal data: the vault

Events cannot be deleted, but personal data must be deletable. Fields declared `personal` (with the person they belong to) or `secret` go to the **vault** instead of the log; the log keeps a salted reference.

<FlowDiagram name="vault" />

- Workers and listeners granted `vault_read` get the values; everyone else gets `null`.
- Secrets are deleted once their command completes.
- An **erasing event** deletes a person's values in every stream and domain, and the snapshots that may contain them. Read models must forget the person when they see it; rebuilt, they read `null`.

## Snapshots and read model stores

- **Snapshots** of aggregates live in their own store, not in the log: a cache that workers send every 100 events and rebuild whenever it is gone or of another `schema`.
- **Read models** can keep their state in a **store** in their own process: data and position are written together per batch, so a restart continues where it left off. A new `schema` rebuilds from the first event.
