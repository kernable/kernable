// Settling: a submit waits for every registered listener, also one that is stopped, and says so
// when it cannot; `wait: "committed"` does not wait.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, NotSettled, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a stopped listener still counts: a settling submit reports NotSettled", async () => {
  const kernel = new TestKernel(["shop.*"], "settle_timeout_ms = 300");
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const cart = domain("shop").aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const seen: string[] = [];
  const audit = kernable.reactor("audit").on("shop.Added", (e) => void seen.push(e.aggregateId));
  const worker = kernable.worker(cart);
  const running = [worker.run(), audit.run()];
  const add = (id: string, wait?: "committed") =>
    kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add", wait });
  try {
    await Promise.all([worker.ready, audit.ready]);
    assert.ok((await add("c1")).committed);
    assert.deepEqual(seen, ["c1"]);

    audit.stop();
    await running[1];
    const refused = await add("c2").catch((err: unknown) => err);
    assert.ok(refused instanceof NotSettled, `${refused}`);
    assert.ok(refused.outcome.committed, "the command itself was committed");
    assert.ok((await add("c3", "committed")).committed);
  } finally {
    worker.stop();
    audit.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
