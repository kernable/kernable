// Event schemas: a new payload version is upcast for aggregates and listeners, old events stay
// as they were stored, and a process that does not know a version stops instead of misreading.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const timeout = (ms: number) => new Promise((resolve) => setTimeout(() => resolve("timeout"), ms));

test("a new event schema is upcast for aggregates and listeners", async () => {
  const kernel = new TestKernel(["shop.*"], ["--settle-timeout-ms", "300"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const add = (data: object, id?: string) =>
    kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: "c1", type: "AddItem", data, id, wait: "committed" });

  // Version 1 stores a quantity as `qty`.
  const v1 = domain("shop");
  const cartV1 = v1.aggregate("Cart", 0)
    .apply("ItemAdded", (n, data: { qty: number }) => n + data.qty)
    .command("AddItem", (_n, data: { qty: number }) => [{ type: "ItemAdded", data: { sku: "apple", qty: data.qty } }]);

  // Version 2 renames it to `count` and caps a cart at 5 items.
  const v2 = domain("shop");
  v2.event("ItemAdded", 2).upcast(1, (d: { sku: string; qty: number }) => ({ sku: d.sku, count: d.qty }));
  const cartV2 = v2.aggregate("Cart", 0)
    .apply("ItemAdded", (n, data: { count: number }) => n + data.count)
    .command("AddItem", (n, data: { count: number }) => {
      if (n + data.count > 5) throw new Rejected("at most 5 items", "full");
      return [{ type: "ItemAdded", data: { sku: "pear", count: data.count } }];
    });

  const archived: [number, unknown][] = [];
  const archive = kernable.listener(v2.reactor("archive", { start: 0, order: 0 }).on("shop.*", (data, evt) => void archived.push([evt.schema, data]), { raw: true }));
  // A listener upcasts its own domain's events without `use`, and other domains' with it.
  const counts: unknown[] = [];
  const current = kernable.listener(v2.readModel("current").on("ItemAdded", (data) => void counts.push(data)));
  const foreignCounts: unknown[] = [];
  const foreign = kernable.listener(domain("audit").readModel("foreign").use(v2).on("shop.ItemAdded", (data) => void foreignCounts.push(data)));
  const stale = kernable.listener(v1.readModel("stale").on("ItemAdded", () => {}));

  const old = kernable.worker(cartV1);
  const worker = kernable.worker(cartV2);
  const outdated = kernable.worker(cartV1);
  const running = [old.run({ reconnectMs: 50 }), archive.run(), current.run(), foreign.run(), stale.run()];
  try {
    await Promise.all([old.ready, archive.ready, current.ready, foreign.ready, stale.ready]);
    assert.ok((await add({ qty: 3 })).committed);
    old.stop();

    running.push(worker.run({ reconnectMs: 50 }));
    await worker.ready;
    // The v1 event is upcast before `apply`: 3 + 3 > 5.
    assert.deepEqual(await add({ count: 3 }), { committed: false, code: "full", message: "at most 5 items" });
    const added = await add({ count: 2 });
    assert.ok(added.committed && added.events[0].schema === 2);

    // An outdated worker is refused: it would write version 1 next to version 2.
    running.push(outdated.run({ reconnectMs: 50 }));
    assert.equal(await Promise.race([outdated.ready.then(() => "ready"), timeout(300)]), "timeout");
    outdated.stop();

    await new Promise((r) => setTimeout(r, 200)); // let the listeners catch up
    assert.deepEqual(counts, [{ sku: "apple", count: 3 }, { sku: "pear", count: 2 }]);
    assert.deepEqual(foreignCounts, counts);
    assert.deepEqual(archived, [[1, { sku: "apple", qty: 3 }], [2, { sku: "pear", count: 2 }]]);
    // The listener that knows only version 1 stopped before the version 2 event.
    assert.equal(stale.position, 1);
  } finally {
    for (const s of [old, worker, outdated, archive, current, foreign, stale]) s.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

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
  assert.deepEqual([shop.reactor("mails").options.order, shop.reactor("mails").options.start], [10, undefined]);

  assert.throws(() => Carts.on("*", () => {}), /listen to a type/);
  assert.throws(() => Carts.on("a.b.c", () => {}), /listen to a type/);
  assert.throws(() => Carts.query("Identity.*", () => null), /answer a query type/);
  assert.throws(() => Carts.middleware({ types: ["a.b.c"] }, () => {}), /check `\*`/);
});
