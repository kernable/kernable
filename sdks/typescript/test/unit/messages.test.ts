// Events, commands and queries as classes (messages.ts): typed, created with `.new`, declared on use, same as by name.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Command, CommandContext, Event, Query, Rejected, domain } from "../../src/index.ts";

class ItemAdded extends Event {
  static version = 2;
  static upcasts = { 1: (d: { qty: number; sku: string }) => ({ sku: d.sku, count: d.qty }) };
  sku!: string;
  count!: number;
}
class CartCleared extends Event {}
class AddItem extends Command {
  static secret = ["coupon"] as const;
  sku!: string;
  count!: number;
  coupon?: string;
}
class CountItems extends Query<number> {
  cart!: string;
}
class Renamed extends Event {
  static type = "CartRenamed";
  name!: string;
}

const shop = domain("shop");
type Cart = { items: number; name?: string };
const cart = shop
  .aggregate<Cart>("cart", { items: 0 })
  .apply(ItemAdded, (s, d) => void (s.items += d.count))
  .apply(CartCleared, (s) => void (s.items = 0))
  .apply(Renamed, (s, d) => void (s.name = d.name))
  .command(AddItem, (s, d) => {
    if (d.count < 1) throw new Rejected("Add at least one", "invalid_count");
    return [ItemAdded.new({ sku: d.sku, count: d.count })];
  });

test("an event class is its type and payload; the class declares version, upcasts and vault fields", async () => {
  const events = await cart.handle(cart.initial, "AddItem", { sku: "apple", count: 2 }, new CommandContext("c-1"));
  assert.deepEqual(events, [{ type: "ItemAdded", data: { sku: "apple", count: 2 } }]);
  assert.equal(shop.events.get("ItemAdded")?.schema, 2);
  assert.deepEqual(shop.commands.get("AddItem")?.vault.secret, ["coupon"]);
  // A version-1 payload is upcast with the class's step before its apply.
  const state = cart.fold([{ type: "ItemAdded", schema: 1, data: { sku: "pear", qty: 3 } }, { type: "CartRenamed", schema: 1, data: { name: "x" } }]);
  assert.deepEqual(state, { items: 3, name: "x" });
});

test("given fields win over field defaults; a default applies when a field is not given", () => {
  class Opened extends Event {
    status?: "open" | "closed" = "open";
    by?: string;
  }
  assert.deepEqual({ ...Opened.new({ status: "closed" }) }, { status: "closed", by: undefined });
  assert.equal(Opened.new({}).status, "open");
  assert.ok(Opened.new({ status: "closed" }) instanceof Event);
  // @ts-expect-error the constructor is protected: create with .new, which checks the fields
  assert.ok(new Opened() instanceof Event);
});

test("a class without fields takes no data, and `static type` overrides the class name", () => {
  assert.deepEqual({ ...CartCleared.new() }, {});
  assert.ok(cart.appliers.has("CartRenamed"));
});

test("a read model answers a query class and listens to event classes, also of another domain", () => {
  const reports = domain("reports");
  const counts = reports.readModel("counts")
    .on(ItemAdded, () => {})
    .query(CountItems, (d) => d.cart.length);
  assert.ok(counts.handlers.has("shop.ItemAdded"));
  assert.ok(counts.queries.has("reports.CountItems"));
  // The event's own domain upcasts it for the other domain's listener.
  assert.equal(counts.domains.get("shop")?.events.get("ItemAdded")?.schema, 2);
});

test("a renamed class (a bundler) and an undeclared one are refused", () => {
  const minified = class e extends Event {};
  assert.throws(() => shop.aggregate("other", {}).apply(minified, () => {}), /static type/);
  class Unused extends Event {}
  assert.throws(() => domain("x").readModel("r").on(Unused, () => {}), /not declared/);
});
