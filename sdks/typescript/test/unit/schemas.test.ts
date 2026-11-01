// Event schemas and listener types: checked when defined, and types without a domain are the
// listener's own.

import assert from "node:assert/strict";
import { test } from "node:test";

import { domain } from "../../src/index.ts";

test("event schemas are checked when defined", () => {
  const shop = domain("shop");
  assert.throws(() => shop.event("ItemAdded", 0), /from 1/);
  assert.throws(() => shop.event("shop.ItemAdded", 2), /unqualified/);
  assert.throws(() => shop.event("ItemAdded", 2).upcast(2, (d) => d), /between 1 and 1/);
});

test("listener types without a domain are the listener's own", () => {
  const shop = domain("Shop");
  const identity = domain("Identity");
  const Carts = shop.readModel("carts")
    .use(identity)
    .on("ItemAdded", () => {})
    .on("Identity.AccountRegistered", () => {})
    .on("Billing.*", () => {})
    .query("GetCart", () => null)
    .query("Identity.GetAccount", () => null)
    .middleware({ types: ["*", "AddItem", "Identity.*"] }, () => {});
  assert.deepEqual([...Carts.handlers.keys()], ["Shop.ItemAdded", "Identity.AccountRegistered", "Billing.*"]);
  assert.deepEqual([...Carts.queries.keys()], ["Shop.GetCart", "Identity.GetAccount"]);
  assert.deepEqual(Carts.middlewares.get("before")?.options.types, ["*", "Shop.AddItem", "Identity.*"]);
  // The own domain's event schemas apply without `use`.
  assert.deepEqual([...Carts.domains.keys()], ["Shop", "Identity"]);
  assert.deepEqual([Carts.options.order, Carts.options.start], [0, 0]);
  const mails = shop.reactor("mails");
  assert.deepEqual([mails.options.order, mails.options.start], [10, undefined]);

  assert.throws(() => Carts.on("*", () => {}), /listen to a type/);
  assert.throws(() => Carts.on("a.b.c", () => {}), /listen to a type/);
  assert.throws(() => Carts.query("Identity.*", () => null), /answer a query type/);
  assert.throws(() => Carts.middleware({ types: ["a.b.c"] }, () => {}), /check `\*`/);
});

test("a domain's aggregates and listeners have names of their own", () => {
  const shop = domain("shop");
  shop.aggregate("cart", 0);
  shop.readModel("carts");
  assert.throws(() => shop.aggregate("cart", 1), /shop has an aggregate cart already/);
  assert.throws(() => shop.readModel("carts"), /shop has a listener carts already/);
  assert.throws(() => shop.reactor("carts"), /shop has a listener carts already/);
  // Another domain, or the other kind, may use the name.
  assert.ok(domain("billing").readModel("carts"));
  assert.ok(shop.reactor("cart"));
});
