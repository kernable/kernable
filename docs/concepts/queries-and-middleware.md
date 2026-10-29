# Queries and middleware

## Every read goes through the kernel

A read model's state is private to its process. Every other process reads it with a **query**, sent to the kernel like a command:

- it is signed, and the key's grant must allow it (`query` to ask, `answer` to answer);
- it passes the middleware chain, exactly like a command;
- the kernel routes it to a read model that answers its type, **in order with that read model's events**: the answer reflects every event the read model was sent before the query.

There are no shortcuts, also not inside one process. A shortcut would be the one place where authorization does not apply.

If several read models answer the same type, the kernel spreads queries over them: that is how a read model scales.

## Middleware

A **middleware** is a listener that checks commands and queries before the kernel accepts or routes them (or after, see below). It builds its knowledge from events, like a read model, and answers each check from it.

```mermaid
flowchart LR
  R[request] --> M1["order 0<br/>authorization"] --> M2["order 5<br/>quotas"] --> A{accepted}
  M1 -- rejected --> X[refused]
  M2 -- rejected --> X
```

- **Registered in code.** A listener declares the types it checks and its order; its key needs a grant for `check` on those types. Negative orders are reserved for kernable.
- **A chain.** Middlewares run by order, then by name; the first rejection refuses the request. A refused command is never stored.
- **Fails closed.** The kernel keeps the registration. While a middleware is away, also right after a kernel restart, every request it covers is refused with `UNAVAILABLE`. Only `kernable listeners remove` unregisters it.
- **Never stale.** After a reconnect, a middleware is asked only once it has processed every event committed before; checks travel in order with its events.
- **Checks only.** A middleware decides; it cannot change a request, because the request is signed.

## Middleware after a request

A middleware can also run **after** a request (`when: "after"`); a listener has at most one before and one after. After a request, the handler gets the request and what came of it:

| Request | Outcome | What the middleware can do |
|---|---|---|
| query, answered | the response | refuse it: the client gets the rejection instead. Fails closed, like before. |
| query, rejected | code and message | observe |
| command | committed with its events, or rejected (also by a middleware before) | observe |

Observing is **best effort**: the kernel tells the middleware once the command completes, also for `wait: "accepted"` and for commands that finish after a kernel restart, but nobody waits for it, and what happens while it is away is missed. For anything that must not be missed, use a reactor: events are delivered at least once. Middleware after a request gets no vault values, and like before it changes nothing.

Typical uses: an audit trail of refused requests, which never become events; keeping a query from returning data the asker must not see.

## The request context

Every command and query carries a signed **request context**: who sends it (`senderIdentity`, `senderTenant`, `senderAccount`, `senderSession`), on whose behalf, its correlation and causation, a trace ID and free attributes. The kernel stores it with every event the command causes, and middleware decides on it. `follow()` gives a follow-up command the context of the event that caused it.
