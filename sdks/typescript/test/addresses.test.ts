// A client given several kernels (a pair, see Run a standby) uses the one that answers.

import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

test("a kernel that cannot be reached hands over to the next address", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const gone = `unix:${join(kernel.dir, "gone.sock")}`;
  const kernable = new Kernable({ url: `${gone},${kernel.url}`, key: kernel.appKey, timeoutMs: 10_000 });
  const worker = kernable.worker(counter);
  const running = worker.run({ reconnectMs: 50 });
  try {
    await worker.ready;
    const outcome = await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 2 }, wait: "committed" });
    assert.ok(outcome.committed);
    assert.equal(outcome.events.length, 1);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});
