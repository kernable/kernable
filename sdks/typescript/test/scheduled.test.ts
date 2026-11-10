// Scheduled commands: enqueued with `at`, run at their time while the aggregate's other commands
// pass; cancelled before they are due; a middleware sees the cancel as the command's type.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a scheduled command runs at its time; others of its aggregate pass meanwhile; a cancel stops one", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const shop = domain("shop");
  const ran: [string, number][] = [];
  const order = shop.aggregate("order", 0)
    .command("Remind", (_s, _d, cmd) => {
      ran.push([cmd.commandId, Date.now()]);
      return [{ type: "Reminded" }];
    })
    .command("Note", () => [{ type: "Noted" }]);
  const cancels: string[] = [];
  const guard = kernable.listener(shop.readModel("guard").middleware({ types: ["shop.*"] }, (check) => {
    if (check.kind !== "cancel") return;
    cancels.push(`${check.type} ${check.aggregateType} ${check.aggregateId}`);
    if (check.request.senderIdentity === "mallory") throw new Rejected("Not yours", "forbidden");
  }));
  const worker = kernable.worker(order);
  const running = [worker.run(), guard.run()];
  try {
    await Promise.all([worker.ready, guard.ready]);
    const at = Date.now() + 2_000;
    const id = await kernable.enqueue({ domain: "shop", aggregateType: "order", aggregateId: "o1", type: "Remind", at });
    // The aggregate is not blocked by it.
    assert.ok((await kernable.submit({ domain: "shop", aggregateType: "order", aggregateId: "o1", type: "Note" })).committed);
    assert.deepEqual(ran, []);
    await assert.rejects(kernable.submit({ domain: "shop", aggregateType: "order", aggregateId: "o1", type: "Remind", at }), /enqueue/);
    // A deadline without a time to run is refused before it is sent.
    await assert.rejects(kernable.enqueue({ domain: "shop", aggregateType: "order", aggregateId: "o1", type: "Remind", until: at }), /`until` needs `at`/);

    const second = await kernable.enqueue({ domain: "shop", aggregateType: "order", aggregateId: "o2", type: "Remind", at: new Date(Date.now() + 60_000) });
    assert.deepEqual(await kernable.cancel(second, { context: { senderIdentity: "mallory" } }), { cancelled: false, code: "forbidden", message: "Not yours" });
    assert.deepEqual(await kernable.cancel(second), { cancelled: true });
    assert.deepEqual(await kernable.cancel(second), { cancelled: true }, "cancelling twice answers the same");
    assert.deepEqual(cancels, ["Remind order o2", "Remind order o2"]);
    const outcome = await kernable.submit({ domain: "shop", aggregateType: "order", aggregateId: "o2", type: "Remind", id: second });
    assert.equal(!outcome.committed && outcome.code, "cancelled");
    assert.equal((await kernable.cancel("nope")).cancelled, false);

    // The first one runs at its time, not before. Asking for its result by its ID waits for it.
    const result = await kernable.submit({ domain: "shop", aggregateType: "order", aggregateId: "o1", type: "Remind", id });
    assert.ok(result.committed);
    assert.equal(ran.length, 1);
    assert.equal(ran[0][0], id);
    assert.ok(ran[0][1] >= at, "not before its time");
    assert.equal((await kernable.cancel(id)).cancelled, false, "too late once it ran");
  } finally {
    worker.stop();
    guard.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
