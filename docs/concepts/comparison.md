# Compared with other systems

kernable is an **application kernel**: storage, ordering, trust and delivery in one process, with business logic in yours. Other tools cover parts of that. This page places kernable among them; it describes their general approach, not every feature of every version.

| | kernable | Event store databases (e.g. EventStoreDB/KurrentDB) | Event sourcing frameworks (e.g. Axon) | Logs and streams (e.g. Kafka) |
|---|---|---|---|---|
| What it is | kernel and runtime for event-sourced applications | a database for event streams | a library and server for one platform | a distributed log for messages |
| Business logic runs | in your processes, any language, driven by the kernel | in your application, which reads and appends | in your application, on the framework's platform | in your consumers and producers |
| One decision per aggregate at a time | yes, the kernel queues commands per aggregate | with optimistic concurrency on append; queuing is yours | yes, within the framework | per partition key, if you design for it; concurrency control is yours |
| Read your own writes | built in: a submit settles after every listener | you build it, e.g. by waiting on subscriptions | depends on the setup | you build it |
| Order across listeners | fixed: lower orders first | each subscription on its own | depends on the setup | each consumer group on its own |
| Signed requests and results, verifiable hash chain | built in | not the database's concern | not built in | not built in |
| Authorization of every command and query | middleware in the kernel, fails closed | access control on streams | in your application | access control on topics |
| Personal data that must be erasable | vault with erasing events | you design it (e.g. crypto-shredding) | you design it | you design it (e.g. compaction, encryption) |
| Scale | one kernel per store today | clusters | clusters | clusters, very large |

**Choose kernable** when the logic of one application is spread over teams or languages, when you need to prove what happened and who asked for it, and when "write, then read it back" must simply work.

**Choose something else** when you need a multi-node cluster today, or a message bus between many independent systems rather than one application's kernel. kernable can sit next to such systems: reactors can publish to them.
