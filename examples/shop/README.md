# Shop: the reference application

Two domains in two languages on one kernel (`MVP_PLAN.md` §3):

| Process | Language | Contains |
|---|---|---|
| `identity/identity.py` | Python | `Account` aggregate and worker, `accounts` read model (order 0) answering `identity.GetAccount`, `rbac` middleware for both domains |
| `orders/service.ts` | TypeScript | `Order` aggregate and worker, `orders` read model (order 0) answering `orders.GetOrder`, `confirmations` reactor (order 10), the public HTTP API |

The reactor reacts to `OrderSubmitted`, looks up the customer's email in the `accounts` read
model of the other domain and records `ConfirmationSent`. Because `accounts` has a lower order,
it has always processed the account before the reactor runs.

Every `POST` answers with the read model's state: the submit returns only once all listeners
have processed the command's events, so no polling is needed.

All reads are queries through the kernel, also the API's reads of the `orders` read model in
its own process. Nothing reads a read model's memory directly, so signatures, allowlists and
the `rbac` middleware apply to every read.

`rbac` is a read model of accounts and order owners that is also a middleware: the kernel asks
it about every `identity.*` and `orders.*` command and query before accepting it. Anyone may
register; an account sees and changes only itself and its own orders; the service identity
`svc-orders` may read. The API takes the caller from the `x-identity` header, a stand-in for
real authentication, and passes it as the request context's `senderIdentity`. While the
identity service is down, the kernel refuses all of these requests: the chain fails closed.

`shop.test.ts` runs the HTTP flow, checks the `rbac` rules, and kills each process (`kill -9`)
once, then checks that no event is lost or duplicated. It runs as part of `make check`.
