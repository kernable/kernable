# Shop: the reference application

Two domains in two languages on one kernel, over mutual TLS. The documentation's tutorial builds a smaller shop step by step; this one adds an HTTP API and kill tests.

| Process | Language | Contains |
|---|---|---|
| `identity/identity.py` | Python | `account` aggregate and worker; `accounts` read model in a store (`accounts.db`), answering `identity.GetAccount`; `rbac` middleware for both domains |
| `orders/service.ts` | TypeScript | `order` aggregate and worker; `orders` read model answering `orders.GetOrder`; `confirmations` reactor; the HTTP API |

- **Order and settling.** The reactor reacts to `OrderSubmitted`, looks up the customer's email in the other domain's `accounts` read model, and records `ConfirmationSent`. Read models run before reactors, and every `POST` answers with the read model's state: a submit returns once every listener has processed its events.
- **Every read is a query through the kernel**, also the API's reads of the read model in its own process, so `rbac` checks every read.
- **Authorization.** `rbac` is a read model of accounts and order owners and a middleware. Anyone may register; an account sees and changes only itself and its orders; the service identity `svc-orders` may read. The API takes the caller from the `x-identity` header, a stand-in for real authentication. While the identity service is down, the kernel refuses these requests.
- **Personal data.** Email, name and password hash live in the vault; the password itself is a secret that is gone once `RegisterAccount` or `SignIn` ran. `DELETE /accounts/:id` erases the account's personal data in both domains.

`shop.test.ts` runs the HTTP flow, checks the `rbac` rules and the erasure, and kills each process (`kill -9`) once, then checks that no event is lost or duplicated. It runs as part of `make check`.
