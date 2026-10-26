# Shop: the reference application

Two domains in two languages on one kernel (`MVP_PLAN.md` §3):

| Process | Language | Contains |
|---|---|---|
| `identity/identity.py` | Python | `Account` aggregate and worker, `accounts` read model (order 0) answering `identity.GetAccount` |
| `orders/service.ts` | TypeScript | `Order` aggregate and worker, `orders` read model (order 0) answering `orders.GetOrder`, `confirmations` reactor (order 10), the public HTTP API |

The reactor reacts to `OrderSubmitted`, looks up the customer's email in the `accounts` read
model of the other domain and records `ConfirmationSent`. Because `accounts` has a lower order,
it has always processed the account before the reactor runs.

Every `POST` answers with the read model's state: the submit returns only once all listeners
have processed the command's events, so no polling is needed.

All reads are queries through the kernel, also the API's reads of the `orders` read model in
its own process. Nothing reads a read model's memory directly, so signatures, allowlists and,
later, RBAC apply to every read.

`shop.test.ts` runs the HTTP flow and kills each process (`kill -9`) once, then checks that no
event is lost or duplicated. It runs as part of `make check`.
