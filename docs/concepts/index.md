# Concepts

How kernable works and why. Each page is short; the [tutorials](../tutorial/) show the same ideas in code, and the [reference](../reference/) has the details.

| Page | Covers |
|---|---|
| [Architecture](./architecture) | What the kernel does and what your processes do |
| [Event sourcing in kernable](./event-sourcing) | Aggregates, commands, events, and how it differs from CRUD |
| [Order and consistency](./ordering) | One command per aggregate, listener order, settling, at-least-once delivery |
| [Trust](./trust) | Keys, grants, signatures, the hash chain |
| [Queries and middleware](./queries-and-middleware) | Why every read goes through the kernel, and how checks fail closed |
| [Data over time](./data) | Event versions, formats and migration, the vault, snapshots |
| [Failures and recovery](./failures) | What happens when something crashes |
| [Compared with other systems](./comparison) | kernable next to event stores, frameworks and logs |
