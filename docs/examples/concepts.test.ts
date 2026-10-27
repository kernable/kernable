// The code on the concept pages.

import assert from "node:assert/strict";
import { test } from "node:test";

import { shop } from "./concepts/upcast.ts";

test("an upcast turns version 1 payloads into version 2 before apply", () => {
  const cart = shop.aggregate("Cart", 0).apply("ItemAdded", (n, e: { count: number }) => n + e.count);
  const history = [
    { type: "ItemAdded", schema: 1, data: { sku: "apple", qty: 2 } },
    { type: "ItemAdded", schema: 2, data: { sku: "pear", count: 3 } },
  ];
  assert.equal(cart.fold(history), 5);
});
