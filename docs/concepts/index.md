# Concepts

How kernable works and why. Each page is short; the [tutorials](../tutorial/) show the same ideas in code, and the [reference](../reference/) has the details.

| Page | Covers |
|---|---|
| [Architecture](./architecture) | What the kernel does and what your processes do |
| [Event sourcing in kernable](./event-sourcing) | Aggregates, commands, events, and how it differs from CRUD |
| [Domains](./domains) | What a domain holds, its rules, how it relates to services, a code layout |
| [Order and consistency](./ordering) | One command per aggregate, listener order, settling, at-least-once delivery |
| [Unique keys and conditions](./uniqueness) | Values unique across aggregates, commands that require a fact of another aggregate |
| [Trust](./trust) | Keys, grants, signatures, the hash chain |
| [Compliance](./compliance) | The technical controls kernable covers for ISO 27001, SOC 2, the GDPR and others, and where it stops |
| [Queries and middleware](./queries-and-middleware) | Why every read goes through the kernel, and how checks fail closed |
| [Business processes](./processes) | Flows across commands, events and queries, run by the kernel and shown in its UI |
| [Data over time](./data) | Event versions, formats, the vault, snapshots |
| [Failures and recovery](./failures) | What happens when something crashes |
| [Compared with other systems](./comparison) | kernable next to event stores, frameworks and logs |
