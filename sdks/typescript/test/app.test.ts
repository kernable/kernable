// An app made of whole domains: its membership comes from what the domains define, and `run()`
// applies it and runs everything.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

function shopAndBilling() {
  const shop = domain("shop");
  const cart = shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const counts = new Map<string, number>();
  shop.readModel("carts")
    .on("Added", (_data, evt) => void counts.set(evt.aggregateId, (counts.get(evt.aggregateId) ?? 0) + 1))
    .query("Count", (data: { id: string }) => counts.get(data.id) ?? 0);
  shop.reactor("push", { live: true }).on("Added", () => {});
  const billing = domain("billing");
  billing.reactor("invoices").on("shop.Added", () => {});
  billing.readModel("guard").on("shop.Added", () => {}).middleware({ types: ["shop.*"] }, () => {});
  return { shop, billing, cart };
}

test("an app lists its domains' aggregates, listeners and middlewares, but no live ones", () => {
  const { shop, billing } = shopAndBilling();
  const kernable = new Kernable({ url: "127.0.0.1:1", key: new TestKernel(["shop.*"]).appKey });
  try {
    const app = kernable.app({ service: "shop", version: "1.0.0", description: "Carts", domains: [shop, billing] });
    const m = app.manifest;
    assert.deepEqual(m.workers?.map((a) => `${a.domain}.${a.name}`), ["shop.Cart"]);
    assert.deepEqual(m.listeners?.map((l) => l.name), ["carts", "invoices"]);
    assert.deepEqual(m.middlewares?.map((l) => l.name), ["guard"]);
    assert.deepEqual(m.metadata, { version: "1.0.0", description: "Carts" });
    assert.equal(app.listeners.length, 4, "the live one runs too");
    assert.throws(() => kernable.app({ domains: [shop] }), /service/);
  } finally {
    kernable.close();
  }
});

test("an app applies its membership and runs everything", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const { shop, billing } = shopAndBilling();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const app = kernable.app({ service: "shop", version: "1.0.0", domains: [shop, billing] });
  const running = app.run();
  try {
    await app.ready;
    assert.ok((await kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: "c1", type: "Add" })).committed);
    assert.equal(await kernable.query({ domain: "shop", type: "Count", data: { id: "c1" } }), 1);
  } finally {
    app.stop();
    await Promise.allSettled([running]);
    kernable.close();
    await kernel.dispose();
  }
});
