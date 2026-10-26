// The kernel is killed (SIGKILL) while a worker executes a command, then restarted.
// The worker must reconnect on its own, and the command must be committed exactly once.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("kernel crash mid-command: worker reconnects, command commits exactly once", async () => {
  let entered!: () => void;
  const enteredOnce = new Promise<void>((r) => (entered = r));
  let open!: () => void;
  const gate = new Promise<void>((r) => (open = r));
  let first = true;

  const counter = domain("counters").aggregate("Counter", 0)
    .apply("Incremented", (n) => n + 1)
    .command("Increment", async () => {
      if (first) {
        first = false;
        entered();
        await gate; // still running when the kernel dies
      }
      return [{ type: "Incremented" }];
    });

  const kernel = new TestKernel(["counters.*"], "lease_ttl_ms = 500");
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey, tenant: "t1" });
  const worker = kernable.worker(counter);
  const running = worker.run({ reconnectMs: 100 });
  const increment = (id: string) => kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", id });

  try {
    await worker.ready;
    const lost = assert.rejects(increment("inc-1")); // the client sees the crash ...
    await enteredOnce;
    await kernel.kill("SIGKILL");
    await lost;

    await kernel.start();
    open(); // the stale execution finishes into a dead session
    const retried = await increment("inc-1"); // ... and resubmits with the same ID
    assert.ok(retried.committed);
    assert.deepEqual(retried.events.map((e) => e.version), [1]);

    const next = await increment("inc-2");
    assert.ok(next.committed);
    assert.deepEqual(next.events.map((e) => e.version), [2]);
    assert.match(kernel.verify(), /OK$/m);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});
