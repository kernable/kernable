// The code on the concept pages.

import assert from "node:assert/strict";
import { test } from "node:test";

import { shopDomain } from "./concepts/upcast.ts";

test("an upcast turns version 1 payloads into version 2 before apply", () => {
  const cartAggregate = shopDomain.aggregate("cart", 0).apply("ItemAdded", (n, data: { count: number }) => n + data.count);
  const history = [
    { type: "ItemAdded", schema: 1, data: { sku: "apple", qty: 2 } },
    { type: "ItemAdded", schema: 2, data: { sku: "pear", count: 3 } },
  ];
  assert.equal(cartAggregate.fold(history), 5);
});
