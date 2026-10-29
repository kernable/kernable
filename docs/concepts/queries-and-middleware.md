# Queries and middleware

## Every read goes through the kernel

A read model's state is private to its process. Every other process reads it with a **query**, sent to the kernel like a command:

- it is signed, and the key's grant must allow it (`query` to ask, `answer` to answer);
- it passes the middleware chain, exactly like a command;
- the kernel routes it to a read model that answers its type, **in order with that read model's events**: the answer reflects every event the read model was sent before the query.

There are no shortcuts, also not inside one process. A shortcut would be the one place where authorization does not apply.

If several read models answer the same type, the kernel spreads queries over them: that is how a read model scales.

## Designing queries

- **One query type per question**: `GetCart`, `ListOpenOrders`, `GetRevenue`. A read model may answer several; each type is answered by read models that keep the data for it.
- **Unknown is a rejection.** Throw `Rejected` with a code (`not_found`): the client gets it as a result, not as an error.
- **After a `committed` submit, ask with `minPosition`**, the position of its events: the answer waits until the read model has them. After a settled submit, it is not needed.
- **A listener cannot ask its own read model**: the answer would wait behind the event it is handling. The kernel refuses it at once; ask another read model, or keep what you need in the listener itself.
- **No read model connected?** The query waits up to `--query-timeout-ms` (default 5 s) for one, then fails: run read models next to the clients that need them, or several of them.

## Middleware

A **middleware** is a listener that checks commands and queries before the kernel accepts or routes them (or after, see below). It builds its knowledge from events, like a read model, and answers each check from it.

<FlowDiagram name="chain" loop bare silent />

- **Registered in code.** A listener declares the types it checks and its order; its key needs a grant for `check` on those types. Negative orders are reserved for kernable. If its service has a membership, it must list the listener as a middleware ([membership](../how-to/remove-listener)).
- **A chain.** Middlewares run by order, then by name; the first rejection refuses the request. A refused command is never stored.
- **Fails closed.** The kernel keeps the registration. While a middleware is away, also right after a kernel restart, every request it covers is refused with `UNAVAILABLE`. Only a membership of its service that no longer lists it as a middleware, or `kernable listeners remove`, unregisters it.
- **Never stale.** After a reconnect, a middleware is asked only once it has processed every event committed before; checks travel in order with its events.
- **Checks only.** A middleware decides; it cannot change a request, because the request is signed.

A whole middleware: it learns who is a customer from events, and refuses shop requests of anyone else.

::: code-group
<<< @/examples/concepts/rbac.ts#rbac{ts} [TypeScript]

```python [Python]
identity_domain = domain("identity")
rbac_readmodel = identity_domain.read_model("rbac")
known, blocked = set(), set()

@rbac_readmodel.on("CustomerRegistered")
def _(data, evt):
    known.add(evt.aggregate_id)

@rbac_readmodel.on("CustomerBlocked")
def _(data, evt):
    blocked.add(evt.aggregate_id)

@rbac_readmodel.middleware(["shop.*"], order=0)
def _(check):
    who = check.request.sender_identity
    if not who:
        raise Rejected("who are you?", "unauthenticated")
    if who not in known:
        raise Rejected(f"{who} is no customer", "unknown_customer")
    if who in blocked:
        raise Rejected(f"{who} is blocked", "blocked")
```

<<< @/examples/concepts/rbac.rs#rbac{rust} [Rust]

:::

Run it like any read model; its key needs `listen` for the identity events and `check` for `shop.*`.

## What middleware is for

| Middleware | Runs | Decides on |
|---|---|---|
| authorization (rbac) | before | the sender, and who owns what, from events |
| tenant isolation | before | `senderTenant` against the request's tenant |
| quota, rate limit | before | counts from events, per sender or per aggregate |
| maintenance mode, feature flag | before | a switch, kept as an event |
| privacy filter | after a query | the answer against the asker |
| audit of refusals | after a command | rejections, which never become events |

## What it costs

Every request a middleware covers waits for it: one round trip per middleware in the chain, in order. So keep the decision in memory, from what the middleware learned from events, with no calls to databases or other services. A middleware that does not answer within `--middleware-timeout-ms` (default 5 s) refuses the request; the kernel's metrics (`kernable_middleware_seconds`, passed and rejected counts) show what the chain costs. Cover only the types that need a check: `shop.*` rather than `*`.

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

Every command and query carries a signed **request context**. The kernel stores it with every event the command causes, and middleware decides on it. `follow()` gives a follow-up command the context of the event that caused it.

| Field | Means |
|---|---|
| `senderIdentity` | the person or system the request is for: who middleware checks |
| `senderTenant`, `senderAccount`, `senderSession` | the sender's tenant, account and session, as your application defines them |
| `onBehalfOf` | act as this identity of the target tenant; the sender stays the real principal |
| `correlationId` | shared by everything one original request causes; defaults to the command ID |
| `causationId` | the command that caused this one (`follow()` sets it) |
| `traceId` | your tracing system's ID |
| `expectedVersion` | reject the command (`version_mismatch`) unless the aggregate is still at this version |
| `bypassAuthorization` | privileged: only keys granted `privileged` for it may set it; what it means, middleware decides |
| `attributes` | free key-value pairs for business data, never for security decisions |
