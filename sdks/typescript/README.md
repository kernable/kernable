# @kernable/sdk

Event sourcing and CQRS with a Rust kernel and business logic in any language. This is the
TypeScript SDK: aggregates, read models, reactors, middleware and business processes in TypeScript, run by
the [kernable](https://github.com/kernable/kernable) kernel. The TypeScript, Python and Rust SDKs
share one API.

```sh
npm install @kernable/sdk       # Node.js 22.18 or newer
```

Start a kernel (macOS and Linux):

```sh
curl -fsSL https://kernable.com/install.sh | sh
kernable start --dev            # a kernel on 127.0.0.1:7233, its UI on :7234, keys in this directory
```

A worker that decides on a cart's commands:

```ts
import { Kernable, Rejected, domain } from "@kernable/sdk";

const cart = domain("shop").aggregate("cart", { items: 0 });
cart.apply("ItemAdded", (state, data: { qty: number }) => { state.items += data.qty; });
cart.command("AddItem", (state, data: { sku: string; qty: number }) => {
  if (data.qty < 1) throw new Rejected("At least one");
  return [{ type: "ItemAdded", data }];
});

await new Kernable({ key: "app.key" }).worker(cart).run();
```

The [getting-started guide](https://kernable.com/docs/tutorial/getting-started) goes on from
here: a read model, a query that reads its own writes, the same in the other languages. Then
[build a shop](https://kernable.com/docs/tutorial/shop/) in ten parts, and the
[TypeScript reference](https://kernable.com/docs/reference/sdk-typescript).

`npx @kernable/sdk add kit` copies the kits (tenancy, accounts, identities, groups, invitations and
authorization) into your project as code you own: see [Kits](https://kernable.com/docs/kits/).

kernable is before 1.0: the protocol and the APIs may still change between 0.x releases. MIT
license.
