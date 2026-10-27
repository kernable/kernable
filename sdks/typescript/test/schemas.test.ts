// Event schemas: a new payload version is upcast for aggregates and listeners, old events stay
// as they were stored, and a process that does not know a version stops instead of misreading.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain, type StoredEvent } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const timeout = (ms: number) => new Promise((resolve) => setTimeout(() => resolve("timeout"), ms));

test("a new event schema is upcast for aggregates and listeners", async () => {
  const kernel = new TestKernel(["shop.*"], "settle_timeout_ms = 300");
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const add = (data: object, id?: string) =>
    kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: "c1", type: "AddItem", data, id, wait: "committed" });

  // Version 1 stores a quantity as `qty`.
  const v1 = domain("shop");
  const cartV1 = v1.aggregate("Cart", 0)
    .apply("ItemAdded", (n, d: { qty: number }) => n + d.qty)
    .command("AddItem", (_n, d: { qty: number }) => [{ type: "ItemAdded", data: { sku: "apple", qty: d.qty } }]);

  // Version 2 renames it to `count` and caps a cart at 5 items.
  const v2 = domain("shop");
  v2.event("ItemAdded", 2).upcast(1, (d: { sku: string; qty: number }) => ({ sku: d.sku, count: d.qty }));
  const cartV2 = v2.aggregate("Cart", 0)
    .apply("ItemAdded", (n, d: { count: number }) => n + d.count)
    .command("AddItem", (n, d: { count: number }) => {
      if (n + d.count > 5) throw new Rejected("at most 5 items", "full");
      return [{ type: "ItemAdded", data: { sku: "pear", count: d.count } }];
    });

  const archived: StoredEvent[] = [];
  const archive = kernable.reactor("archive", { start: 0, order: 0 }).on("shop.*", (e) => void archived.push(e), { raw: true });
  const counts: unknown[] = [];
  const current = kernable.readModel("current").use(v2).on("shop.ItemAdded", (e) => void counts.push(e.data));
  const stale = kernable.readModel("stale").use(v1).on("shop.ItemAdded", () => {});

  const old = kernable.worker(cartV1);
  const worker = kernable.worker(cartV2);
  const outdated = kernable.worker(cartV1);
  const running = [old.run({ reconnectMs: 50 }), archive.run(), current.run(), stale.run()];
  try {
    await Promise.all([old.ready, archive.ready, current.ready, stale.ready]);
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
    assert.deepEqual(archived.map((e) => [e.schema, e.data]), [[1, { sku: "apple", qty: 3 }], [2, { sku: "pear", count: 2 }]]);
    // The listener that knows only version 1 stopped before the version 2 event.
    assert.equal(stale.position, 1);
  } finally {
    for (const s of [old, worker, outdated, archive, current, stale]) s.stop();
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
