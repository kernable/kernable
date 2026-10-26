// The OrdersOverview read model shows every change as soon as submit returns, a reactor sees
// each event only after the read model has processed it, and both survive a kernel crash.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, NotSettled } from "../../sdks/typescript/src/index.ts";
import { TestKernel } from "../../sdks/typescript/test/kernel.ts";
import { order } from "./order.ts";
import { OVERVIEW_EVENTS, project, type OrdersOverview } from "./overview.ts";

async function until(condition: () => boolean, what: string) {
  for (let i = 0; i < 250 && !condition(); i++) await new Promise((r) => setTimeout(r, 20));
  assert.ok(condition(), `timed out waiting for ${what}`);
}

test("read model shows its own writes and runs before the reactor, also across a kernel crash", async () => {
  const kernel = new TestKernel(["orders.*"], "settle_timeout_ms = 1000");
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey, tenant: "t1" });
  const worker = kernable.worker(order);

  const view: OrdersOverview = {};
  const overview = kernable.readModel("overview", { reconnectMs: 100 });
  for (const type of OVERVIEW_EVENTS) overview.on(type, (e) => void project(view, e));

  // The reactor queries the read model; it must always find the order there.
  const confirmed = new Set<string>();
  const confirmations = kernable.reactor("confirmations", { reconnectMs: 100 }).on("orders.OrderPlaced", (e) => {
    assert.ok(view[e.aggregateId], `${e.aggregateId} is not in the read model yet`);
    confirmed.add(e.aggregateId);
  });

  const running = [worker.run({ reconnectMs: 100 }), overview.run(), confirmations.run()];
  const submit = async (id: string, type: string, data?: unknown) => {
    const outcome = await kernable.submit({ domain: "orders", aggregateType: "Order", aggregateId: id, type, data });
    assert.ok(outcome.committed, `${type} on ${id}`);
    return outcome.events.at(-1)!.position;
  };

  try {
    await Promise.all([worker.ready, overview.ready, confirmations.ready]);

    // No polling: when submit returns, every listener has processed the events.
    await submit("o1", "PlaceOrder", { customerId: "k-1" });
    assert.deepEqual(view.o1, { customerId: "k-1", status: "placed", items: 0 });
    assert.ok(confirmed.has("o1"));
    await submit("o1", "AddItem", { sku: "a", qty: 2 });
    assert.equal(view.o1.items, 2);

    // The kernel dies; worker and listeners reconnect and resume where they left off.
    await kernel.kill("SIGKILL");
    await kernel.start();
    await submit("o2", "PlaceOrder", { customerId: "k-2" });
    await submit("o2", "AddItem", { sku: "b", qty: 5 });
    const last = await submit("o2", "CancelOrder");
    assert.deepEqual(view.o2, { customerId: "k-2", status: "cancelled", items: 5 });
    assert.deepEqual([...confirmed].sort(), ["o1", "o2"]);

    // A read model rebuilt from scratch must match the one that lived through the crash.
    const rebuilt: OrdersOverview = {};
    const replay = kernable.subscribe({ types: OVERVIEW_EVENTS }, (e) => void project(rebuilt, e));
    await until(() => replay.position === last, "rebuild");
    replay.stop();
    assert.deepEqual(view, rebuilt);

    // A stopped listener still counts: the next submit cannot settle and says so.
    confirmations.stop();
    await assert.rejects(submit("o3", "PlaceOrder", { customerId: "k-3" }), NotSettled);
    const committed = await kernable.submit({ domain: "orders", aggregateType: "Order", aggregateId: "o4", type: "PlaceOrder", data: { customerId: "k-4" }, wait: "committed" });
    assert.ok(committed.committed);
  } finally {
    worker.stop();
    overview.stop();
    confirmations.stop();
    await Promise.all(running);
    kernable.close();
    await kernel.dispose();
  }
});
