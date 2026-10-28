// Requests from listener handlers: a listener handles one event at a time, so the kernel never
// makes such a request wait for the listener itself.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a reactor's settling submit waits for the read models before it, not for itself", async () => {
  const kernel = new TestKernel(["shop.*"], ["--settle-timeout-ms", "2000"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });

  const shop = domain("shop");
  const order = shop.aggregate("Order", { confirmed: false })
    .apply("Confirmed", () => ({ confirmed: true }))
    .command("Place", () => [{ type: "Placed" }])
    .command("Confirm", () => [{ type: "Confirmed" }]);
  const confirmed = new Set<string>();
  const orders = shop.readModel("orders")
    .on("Confirmed", (_data, evt) => void confirmed.add(evt.aggregateId))
    .query("IsConfirmed", (data: { id: string }) => {
      if (data.id === "self") return kernable.query({ domain: "shop", type: "IsConfirmed", data: { id: "x" } });
      return confirmed.has(data.id);
    });
  const seen: unknown[] = [];
  const confirmations = shop.reactor("confirmations").on("Placed", async (_data, evt) => {
    const started = Date.now();
    // Settles on the read model (order 0), not on this reactor (order 10).
    await kernable.submit({ domain: "shop", aggregateType: "Order", aggregateId: evt.aggregateId, type: "Confirm", id: `confirm-${evt.aggregateId}` });
    seen.push(await kernable.query({ domain: "shop", type: "IsConfirmed", data: { id: evt.aggregateId } }));
    seen.push(Date.now() - started < 1000);
  });

  const worker = kernable.worker(order);
  const listeners = [kernable.listener(orders), kernable.listener(confirmations)];
  const running = [worker.run(), ...listeners.map((l) => l.run())];
  try {
    await Promise.all([worker.ready, ...listeners.map((l) => l.ready)]);
    await kernable.submit({ domain: "shop", aggregateType: "Order", aggregateId: "o1", type: "Place" });
    assert.deepEqual(seen, [true, true]);

    // A query handler asking its own read model is refused at once.
    await assert.rejects(
      kernable.query({ domain: "shop", type: "IsConfirmed", data: { id: "self" } }),
      // The inner query fails at once, so the handler fails and the outer query is refused.
      (err: Error) => err instanceof Rejected && /answered only by orders itself/.test(err.message),
    );
  } finally {
    for (const s of [worker, ...listeners]) s.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
