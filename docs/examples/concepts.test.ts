// The code on the concept pages.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Rejected } from "@kernable/sdk";

import { rbacReadmodel } from "./concepts/rbac.ts";
import { shopDomain } from "./concepts/upcast.ts";

test("an upcast turns version 1 payloads into version 2 before apply", () => {
  const cartAggregate = shopDomain.aggregate("cart", 0).apply("ItemAdded", (n, data: { count: number }) => n + data.count);
  const history = [
    { type: "ItemAdded", schema: 1, data: { sku: "apple", qty: 2 } },
    { type: "ItemAdded", schema: 2, data: { sku: "pear", count: 3 } },
  ];
  assert.equal(cartAggregate.fold(history), 5);
});

test("the rbac middleware decides from the events it has seen", async () => {
  const on = (type: string, id: string) => rbacReadmodel.handlers.get(`identity.${type}`)!.handler({}, { aggregateId: id } as never, {} as never);
  const check = rbacReadmodel.middlewares.get("before")!.handler;
  const as = (senderIdentity?: string) => check({ kind: "command", type: "AddItem", domain: "shop", request: { senderIdentity } } as never);
  const code = async (who?: string) => {
    try {
      await as(who);
      return "passed";
    } catch (err) {
      return (err as Rejected).code;
    }
  };
  assert.equal(await code(), "unauthenticated");
  assert.equal(await code("max"), "unknown_customer");
  await on("CustomerRegistered", "max");
  assert.equal(await code("max"), "passed");
  await on("CustomerBlocked", "max");
  assert.equal(await code("max"), "blocked");
});
