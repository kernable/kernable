// The OrdersOverview projection follows backlog and live events, and survives a kernel crash
// without missing or duplicating anything.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable } from "../../sdks/typescript/src/index.ts";
import { TestKernel } from "../../sdks/typescript/test/kernel.ts";
import { order } from "./order.ts";
import { OVERVIEW_EVENTS, project, type OrdersOverview } from "./overview.ts";

async function until(condition: () => boolean, what: string) {
  for (let i = 0; i < 250 && !condition(); i++) await new Promise((r) => setTimeout(r, 20));
  assert.ok(condition(), `timed out waiting for ${what}`);
}

test("projection follows backlog and live events and survives a kernel crash", async () => {
  const kernel = new TestKernel(["PlaceOrder", "AddItem", "CancelOrder", "ShipOrder"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey, tenant: "t1" });
  const worker = kernable.worker(order);
  const running = worker.run({ reconnectMs: 100 });
  const submit = async (id: string, type: string, data?: unknown) => {
    const outcome = await kernable.submit({ aggregateType: "Order", aggregateId: id, type, data });
    assert.ok(outcome.committed, `${type} on ${id}`);
    return outcome.events.at(-1)!.position;
  };

  const view: OrdersOverview = {};
  let live: ReturnType<Kernable["subscribe"]> | undefined;
  try {
    await worker.ready;
    await submit("o1", "PlaceOrder", { customerId: "k-1" });
    await submit("o1", "AddItem", { sku: "a", qty: 2 });

    // Backlog, then live.
    live = kernable.subscribe({ types: OVERVIEW_EVENTS, reconnectMs: 100 }, (e) => void project(view, e));
    await until(() => live!.position === 2, "backlog");
    assert.deepEqual(view, { o1: { customerId: "k-1", status: "placed", items: 2 } });
    let last = await submit("o1", "ShipOrder");
    await until(() => live!.position === last, "live event");
    assert.equal(view.o1.status, "shipped");

    // The kernel dies; worker and subscription reconnect and resume where they left off.
    await kernel.kill("SIGKILL");
    await kernel.start();
    await submit("o2", "PlaceOrder", { customerId: "k-2" });
    await submit("o2", "AddItem", { sku: "b", qty: 5 });
    last = await submit("o2", "CancelOrder");
    await until(() => live!.position === last, "events after restart");

    // A projection rebuilt from scratch must match the one that lived through the crash.
    const rebuilt: OrdersOverview = {};
    const replay = kernable.subscribe({ types: OVERVIEW_EVENTS }, (e) => void project(rebuilt, e));
    await until(() => replay.position === last, "rebuild");
    replay.stop();
    assert.deepEqual(view, rebuilt);
    assert.deepEqual(view.o2, { customerId: "k-2", status: "cancelled", items: 5 });
  } finally {
    live?.stop();
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});
