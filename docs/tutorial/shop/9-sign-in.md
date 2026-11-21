# 9. Accounts and sign-in

In part 6 the guard trusted whatever `senderIdentity` a request named. Real customers sign in. In
this part the shop gets an HTTP API with accounts, passwords and sessions, and the hand-written
guard is replaced by [the kit](../../kits/): tenants, accounts, groups, invitation links, and a
middleware that decides every request.

The story: Ann opens her shop on your platform. She creates a group of customers and puts an
invitation link on her website. Bob and Cleo sign up and join through it. Each may work only on
their own orders.

## Copy the kit

The kit is source code you copy into your project, not a dependency:

::: code-group
```sh [TypeScript]
npx @kernable/sdk add kit
npm install @node-rs/argon2 @simplewebauthn/server openid-client
```
```sh [Python]
.venv/bin/kernable-sdk add kit
.venv/bin/pip install -r kit/requirements.txt
```
```sh [Rust]
cargo install kernable-sdk
kernable-sdk add kit     # then add the crate `kit` to your workspace and as a dependency
```
```sh [Go]
go run github.com/kernable/kernable/sdks/go/cmd/kernable-sdk@latest add kit   # the package shop/kit
go mod tidy
```
:::

Stop the guard from part 6 and the confirmations reactor from part 4: from now on the kit's
middleware decides, and a reactor needs an identity of its own under it (see
[Identities](../../kits/identity)).

## Say what a query is about

`GetOrder { id: "o1" }` is about order `o1`, but only the shop's code knows that `id` names an
order. Declare it once, in the domain:

::: code-group
<<< @/examples/shop/ts/domain.ts#target [domain.ts]
<<< @/examples/shop/py/domain.py#target [domain.py]
<<< @/examples/shop/rs/src/domain.rs#target [domain.rs]
<<< @/examples/shop/go/domain.go#target [domain.go]
:::

Now a rule can say "only the order's owner may read it", for queries as for commands.

## The API

One new process, `api`, runs the kit's domains and its example HTTP server. It declares who owns
an order, from the shop's own events:

::: code-group
<<< @/examples/shop/ts/api.ts#relation [api.ts]
<<< @/examples/shop/py/api.py#relation [api.py]
<<< @/examples/shop/rs/src/bin/api.rs#relation [api.rs]
<<< @/examples/shop/go/api/main.go#relation [api/main.go]
:::

The kit sends mail for email verification, password resets and invitations. In development, the
API writes each mail to a file in `mail/` instead:

::: code-group
<<< @/examples/shop/ts/api.ts#mail [api.ts]
<<< @/examples/shop/py/api.py#mail [api.py]
<<< @/examples/shop/rs/src/bin/api.rs#mail [api.rs]
<<< @/examples/shop/go/api/main.go#mail [api/main.go]
:::

Groups say which *types* someone may send. One rule of the shop is about the data instead: a new
order's customer is whoever places it, or a customer could order in someone else's name. The API
runs a small middleware of the shop for it. It comes after the kit's (order 0 against the kit's
-30), so the sender is already checked when it compares:

::: code-group
<<< @/examples/shop/ts/api.ts#rule [api.ts]
<<< @/examples/shop/py/api.py#rule [api.py]
<<< @/examples/shop/rs/src/bin/api.rs#rule [api.rs]
<<< @/examples/shop/go/api/main.go#rule [api/main.go]
:::

Like the kit's middleware, it fails closed: while it is away, every `Place` is refused.

Then it starts the kit, sets up the platform once (`bootstrap`: the platform tenant, open sign-up,
the API's own identity and key) and serves HTTP on port 3000:

::: code-group
<<< @/examples/shop/ts/api.ts#run [api.ts]
<<< @/examples/shop/py/api.py#run [api.py]
<<< @/examples/shop/rs/src/bin/api.rs#run [api.rs]
<<< @/examples/shop/go/api/main.go#run [api/main.go]
:::

Start the worker, the read models and the API (`node api.ts`, `.venv/bin/python api.py`,
`cargo run -q --bin api` or `go run ./api`). The example server's routes:

| Route | Does |
|---|---|
| `POST /register`, `POST /verify`, `POST /sign-in` | sign up, confirm the email with the mailed token, sign in for an access token |
| `POST /tenants` | open a tenant the caller owns |
| `POST /invitation-links`, `POST /invitation-links/join` | create an invitation link; join a tenant through one |
| `GET /me` | the caller's request context in a tenant |
| `POST /commands/<domain>/<aggregateType>/<Type>` | send a command as the caller |
| `POST /queries/<domain>/<Type>` | ask a query as the caller |

A request carries `Authorization: Bearer <access token>` and, inside a tenant,
`X-Tenant: <tenant ID>`. The API turns them into the request context; it decides nothing itself.

## Try it

A small client of the API, for the people of the story:

::: code-group
<<< @/examples/shop/ts/part9.ts#client [part9.ts]
<<< @/examples/shop/py/part9.py#client [part9.py]
<<< @/examples/shop/rs/src/bin/part9.rs#client [part9.rs]
<<< @/examples/shop/go/part9/main.go#client [part9/main.go]
:::

Ann opens her shop. A tenant is one customer of your platform, here Ann's shop; Ann is its owner
and may do everything in it. She creates the group Customers: they may place orders, and add
items to, submit and read only the orders they own. Then she creates an invitation link with
that group:

::: code-group
<<< @/examples/shop/ts/part9.ts#owner [part9.ts]
<<< @/examples/shop/py/part9.py#owner [part9.py]
<<< @/examples/shop/rs/src/bin/part9.rs#owner [part9.rs]
<<< @/examples/shop/go/part9/main.go#owner [part9/main.go]
:::

Bob and Cleo sign up and join through the link. Bob places an order and works on it; Cleo tries
to touch it:

::: code-group
<<< @/examples/shop/ts/part9.ts#customers [part9.ts]
<<< @/examples/shop/py/part9.py#customers [part9.py]
<<< @/examples/shop/rs/src/bin/part9.rs#customers [part9.rs]
<<< @/examples/shop/go/part9/main.go#customers [part9/main.go]
:::

Bob tries to place an order in Cleo's name:

::: code-group
<<< @/examples/shop/ts/part9.ts#rule [part9.ts]
<<< @/examples/shop/py/part9.py#rule [part9.py]
<<< @/examples/shop/rs/src/bin/part9.rs#rule [part9.rs]
<<< @/examples/shop/go/part9/main.go#rule [part9/main.go]
:::

Finally, `Explain` tells Cleo why she may not read the order, and a request without an access
token is refused before anything else:

::: code-group
<<< @/examples/shop/ts/part9.ts#explain [part9.ts]
<<< @/examples/shop/py/part9.py#explain [part9.py]
<<< @/examples/shop/rs/src/bin/part9.rs#explain [part9.rs]
<<< @/examples/shop/go/part9/main.go#explain [part9/main.go]
:::

::: code-group
<<< @/examples/shop/ts/part9.out{text} [TypeScript]
<<< @/examples/shop/py/part9.out{text} [Python]
<<< @/examples/shop/rs/part9.out{text} [Rust]
<<< @/examples/shop/go/part9.out{text} [Go]
:::

## What decided

- **Who is asking** comes from the access token, checked by the API without the kernel. The
  middleware still checks that the session is active: signing out ends it at once.
- **Ann** is the tenant's owner: owners hold every permission in their tenant.
- **Bob** may add to `o1` because he is its owner: the relation `owner` was set from the
  `OrderPlaced` event. The same holds for reading it, because `GetOrder` declares the order it is
  about.
- **Cleo** is a customer too, but not `o1`'s owner: `forbidden`, for the command and the query.
- **Bob's order in Cleo's name** passes the kit (customers may place orders) and is refused by the
  shop's own middleware, which compares the order's customer with the sender.
- Bob and Cleo got the group through the link, without Ann inviting each of them. Ann can limit a
  link (how many may join, until when) and revoke it at any time.

Every sign-in, failed attempt, membership and join is an event in the log. To see the domains in
detail, continue with [the kit's documentation](../../kits/).

Next: [10. A business process](./10-process).
