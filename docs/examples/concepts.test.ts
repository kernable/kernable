// The code on the concept pages.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Rejected } from "@kernable/sdk";

import { rbacReadmodel } from "./concepts/rbac.ts";
import { shopDomain } from "./concepts/upcast.ts";
import { accountAggregate, fundAggregate, investorAggregate } from "./concepts/uniqueness.ts";

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

test("a fund holds its keys and facts from its state, and frees its name when wound up", () => {
  const launched = fundAggregate.fold([{ type: "FundLaunched", schema: 1, data: { isin: "lu0001 ", name: " Alpha Fund" } }]);
  assert.deepEqual(fundAggregate.index(launched), {
    keys: [
      { name: "isin", value: "LU0001", scope: "global" },
      { name: "name", value: "alpha fund", scope: "tenant" },
    ],
    facts: ["open"],
  });
  const woundUp = fundAggregate.fold([{ type: "FundWoundUp", schema: 1, data: {} }], launched);
  assert.deepEqual(fundAggregate.index(woundUp), { keys: [{ name: "isin", value: "LU0001", scope: "global" }], facts: [] });
});

test("opening an account requires its fund to be open", () => {
  const requires = accountAggregate.requirements.get("OpenAccount")!;
  assert.deepEqual(requires({ fundId: "f1" }, { status: "none" }), [{ domain: "funds", aggregateType: "fund", aggregateId: "f1", fact: "open" }]);
});

test("an investor's email is a personal key of the investor", () => {
  const registered = investorAggregate.fold([{ type: "InvestorRegistered", schema: 1, data: { investorId: "i1", email: "Max@Example.com" } }]);
  assert.deepEqual(investorAggregate.index(registered).keys, [{ name: "email", value: "max@example.com", scope: "tenant", subject: "i1" }]);
});
