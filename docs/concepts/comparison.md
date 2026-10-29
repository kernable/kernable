# Compared with other systems

kernable is an **application kernel**: storage, ordering, trust and delivery in one process, with business logic in yours. Other tools cover parts of that. This page places kernable among them; it describes their general approach, not every feature of every version.

| | kernable | A database, an outbox and a message bus (built yourself) | Event store databases (e.g. EventStoreDB/KurrentDB) | Event sourcing frameworks (e.g. Axon) | Logs and streams (e.g. Kafka) |
|---|---|---|---|---|---|
| What it is | kernel and runtime for event-sourced applications | the usual setup: tables, plus events published from them | a database for event streams | a library and server for one platform | a distributed log for messages |
| Business logic runs | in your processes, any language, driven by the kernel | in your services | in your application, which reads and appends | in your application, on the framework's platform | in your consumers and producers |
| Languages | any with gRPC; SDKs for TypeScript, Python and Rust share one API | any | clients for several languages | mostly one platform (e.g. the JVM) | clients for many languages |
| One decision per aggregate at a time | yes, the kernel queues commands per aggregate | row locks or optimistic versions, per table, yours | with optimistic concurrency on append; queuing is yours | yes, within the framework | per partition key, if you design for it; concurrency control is yours |
| Read your own writes | built in: a submit settles after every listener | across services, you build it | you build it, e.g. by waiting on subscriptions | depends on the setup | you build it |
| Order across listeners | fixed: lower orders first | per consumer, yours | each subscription on its own | depends on the setup | each consumer group on its own |
| History of every change | the log itself, with command and sender | audit tables, if you add them | the streams | the event store | as long as the topic retains it |
| Signed requests and results, verifiable hash chain | built in | not built in | not the database's concern | not built in | not built in |
| Authorization of every command and query | middleware in the kernel, fails closed | in each service | access control on streams | in your application | access control on topics |
| Personal data that must be erasable | vault with erasing events | delete rows; copies in events and backups are yours | you design it (e.g. crypto-shredding) | you design it | you design it (e.g. compaction, encryption) |
| Changing an event's format | versions with upcasts; old readers stop instead of misreading | migrations, and versioned messages | yours | upcasters | schema registry, if you add one |
| Delivery to your processes | the kernel delivers to every connected process; no separate broker | the message bus, a separate server | subscriptions from the database | an event bus, often a separate server | it is the broker |
| Files and shared cache | included: signed requests, governed by grants | separate systems | separate systems | separate systems | separate systems |
| What a service consists of | memberships: the kernel waits for exactly what is listed (`/ready`) | your deploy tooling | your deploy tooling | your deploy tooling | your deploy tooling |
| Seeing what runs | a built-in UI: map, connections, any aggregate's history; metrics | dashboards you assemble | a database UI | depends on the setup | tools around the cluster |
| Operating it | one binary and a SQLite file (or a log file) | a database and a message bus | a database server or cluster | a server or cluster, plus your application | a cluster |
| Scale | one kernel per store today | as far as you build it | clusters | clusters | clusters, very large |
| License | Apache-2.0 | — | open source and commercial editions | open source and commercial editions | open source, and commercial offerings |

**Choose kernable** when the logic of one application is spread over teams or languages, when you need to prove what happened and who asked for it, and when "write, then read it back" must simply work.

**Choose something else** when you need a multi-node cluster today, or a message bus between many independent systems rather than one application's kernel. kernable can sit next to such systems: reactors can publish to them.

## Where kernable is young

- **One kernel per store.** No failover to a second kernel yet: a kernel restarts in seconds and loses nothing, but while it is down, nothing runs.
- **No compaction.** The log only grows; that is what keeps it verifiable. Expiring data belongs in the cache.
- **SQLite sets the limits of one store**: a single writer on one machine. Commands of different aggregates run in parallel on the workers, but every commit goes through that one writer.
- **Three SDKs.** Other languages speak the gRPC protocol directly ([Write an SDK](../how-to/new-sdk)).
- **A young project.** The data format and the protocol are versioned, with migrations, but the API may still change before 1.0.

## Moving an existing application

You do not have to rewrite anything at once:

1. **Start with one aggregate where decisions matter**: orders, bookings, payments. Its commands go through kernable; everything else stays.
2. **Keep your tables as read models.** A read model, or a reactor, writes the events into the tables your existing code reads, so the rest of the application does not notice.
3. **Tell other systems through reactors.** A reactor publishes events to your message bus, or calls the systems that need them.
4. **Move the next aggregate** when the first one runs. The old and the new parts run side by side as long as needed.

