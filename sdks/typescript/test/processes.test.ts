// Processes against a real kernel: an app registers its domains' processes once its worker is
// connected; the same model again registers nothing new, a changed one is the next version.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain, type ProcessVersion } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

/** The shop with a worker for the process's commands; `changed` adds a step to the process. */
function shop(changed = false) {
  const shop = domain("shop");
  shop.aggregate("order", { confirmed: false })
    .apply("OrderSubmitted", () => {})
    .apply("OrderConfirmed", (s) => void (s.confirmed = true))
    .command("Submit", () => [{ type: "OrderSubmitted", data: {} }])
    .command("Confirm", () => [{ type: "OrderConfirmed", data: {} }])
    .command("Notify", () => [{ type: "Notified", data: {} }]);
  const process = shop.process("fulfilment")
    .start("OrderSubmitted", { key: "aggregateId", data: { orderId: "aggregateId" } })
    .command("confirm", "order.Confirm", { aggregate: "case.orderId" });
  if (changed) process.command("notify", "order.Notify", { aggregate: "case.orderId" });
  process.end("done");
  return shop;
}

/** Runs an app of `domains` until its processes are registered; what the kernel answered. */
async function deploy(kernel: TestKernel, changed: boolean): Promise<ProcessVersion[]> {
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const app = kernable.app({ service: "shop", domains: [shop(changed)] });
  const running = app.run();
  try {
    await Promise.race([app.ready, running]);
    assert.deepEqual(app.manifest.processes?.map((p) => (typeof p === "string" ? p : p.qualified)), ["shop.fulfilment"]);
    return [...app.registered];
  } finally {
    app.stop();
    await running.catch(() => {});
    kernable.close();
  }
}

test("an app registers its processes: version 1, unchanged the same, changed version 2", async () => {
  const kernel = new TestKernel(["shop.*"], [], { processes: true });
  await kernel.start();
  try {
    assert.deepEqual(await deploy(kernel, false), [{ name: "shop.fulfilment", version: 1, new: true }]);
    assert.deepEqual(await deploy(kernel, false), [{ name: "shop.fulfilment", version: 1, new: false }]);
    assert.deepEqual(await deploy(kernel, true), [{ name: "shop.fulfilment", version: 2, new: true }]);

    // Alone, and refused by the kernel for a model it cannot run: the step is named.
    const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
    try {
      assert.deepEqual(await kernable.registerProcess(shop(true).processes[0]), { name: "shop.fulfilment", version: 2, new: false });
      const bad = domain("shop").process("broken")
        .start("OrderSubmitted", { key: "aggregateId" })
        .decide("large", { when: "case.total >", then: "done", otherwise: "done" })
        .end("done");
      await assert.rejects(kernable.registerProcess(bad), /large/);
    } finally {
      kernable.close();
    }
  } finally {
    await kernel.dispose();
  }
});
