# Architecture

kernable splits an application into a **kernel** and **your processes**.

```mermaid
flowchart LR
  subgraph yours [Your processes, any language]
    C[clients / APIs]
    W[workers<br/>aggregates]
    L[listeners<br/>read models, reactors, middleware]
  end
  K((kernel))
  S[(event store)]
  V[(vault)]
  N[(snapshots)]
  C -- commands, queries --> K
  K -- commands --> W
  W -- events --> K
  K -- events, queries, checks --> L
  K --- S
  K --- V
  K --- N
```

**The kernel** (Rust, one process per store) owns everything that must be right for every application:

- it checks every signature and every key's grant;
- it queues commands, one at a time per aggregate, and hands them to workers;
- it commits events atomically, on the version they were based on, into a hash chain;
- it delivers events to listeners in a fixed order and waits for them when a submit settles;
- it routes queries to the read model that answers them and runs the middleware chain;
- it keeps personal data in the vault and snapshots in their own store.

**Your processes** own the business logic and nothing else:

- **Workers** run aggregates: given the history and a command, they return new events or a rejection.
- **Listeners** receive events in order: **read models** keep views and answer queries, **reactors** cause side effects and follow-up commands, a **middleware** checks requests.
- **Clients** submit commands and ask queries.

Processes talk to the kernel over gRPC: workers and listeners keep one stream open each, clients make single calls. Any language with gRPC can take part; TypeScript, Python and Rust have SDKs.

## A command's way

```mermaid
sequenceDiagram
  participant C as client
  participant K as kernel
  participant M as middleware
  participant W as worker
  participant R as read model
  C->>K: Submit (signed)
  K->>K: check signature, grant
  K->>M: check
  M-->>K: pass
  K->>K: store command, queue it
  K->>W: Execute (history, snapshot)
  W-->>K: Result (events, signed)
  K->>K: commit, extend the hash chain
  K->>R: events, in order
  R-->>K: ack
  K-->>C: committed and settled
```

The kernel never interprets payloads: commands, events and queries are JSON your processes write and read. That is what lets every process be in another language.

**Limits.** One kernel serves one store; there is no clustering yet. A kernel restarts in seconds and loses nothing (see [Failures and recovery](./failures)).
