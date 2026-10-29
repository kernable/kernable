// Settling: a submit waits for every listener that counts, also one that is stopped, and says so
// when it cannot; `wait: "committed"` does not wait.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, NotSettled, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a stopped listener still counts: a settling submit reports NotSettled", async () => {
  const kernel = new TestKernel(["shop.*"], ["--settle-timeout-ms", "300"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const shop = domain("shop");
  const cart = shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const seen: string[] = [];
  const audit = kernable.listener(shop.reactor("audit").on("Added", (_data, evt) => void seen.push(evt.aggregateId)));
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

test("a membership decides which listeners count; live ones never count", async () => {
  const kernel = new TestKernel(["shop.*"], ["--settle-timeout-ms", "300"]);
  await kernel.start();
  const shop = domain("shop");
  const cart = shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const v1 = new Kernable({ url: kernel.url, key: kernel.appKey, service: "shop" });
  const worker = v1.worker(cart);
  const seen: string[] = [];
  const mails = v1.listener(shop.reactor("mails").on("Added", (_data, evt) => void seen.push(evt.aggregateId)));
  const auditor = shop.reactor("audit").on("Added", () => {});
  const audit = v1.listener(auditor);
  await v1.apply({ workers: [cart], listeners: [mails.listener, auditor], metadata: { version: "1" } });
  const running = [worker.run(), mails.run(), audit.run()];
  const add = (id: string) => v1.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add" });
  try {
    await Promise.all([worker.ready, mails.ready, audit.ready]);
    assert.ok((await add("c1")).committed);
    mails.stop();
    audit.stop();
    await Promise.all(running.slice(1));

    // The next version of the service has no audit any more: nobody waits for it.
    const v2 = new Kernable({ url: kernel.url, key: kernel.appKey, service: "shop" });
    const mails2 = v2.listener(shop.reactor("mails").on("Added", (_data, evt) => void seen.push(evt.aggregateId)));
    await v2.apply({ workers: [cart], listeners: [mails2.listener], metadata: { version: "2" } });
    // A live listener counts for nobody, even while it does not get through its events.
    let release = () => {};
    const stuck = new Promise<void>((resolve) => (release = resolve));
    const push = v2.listener(shop.reactor("push", { live: true }).on("Added", () => stuck));
    const again = [mails2.run(), push.run()];
    await Promise.all([mails2.ready, push.ready]);
    assert.ok((await add("c2")).committed, "settled without audit and push");
    // mails resumed where it left off.
    assert.deepEqual(seen, ["c1", "c2"]);
    mails2.stop();
    push.stop();
    release();
    await Promise.allSettled(again);
    v2.close();

    assert.throws(() => shop.reactor("x", { live: true }).middleware({ types: ["*"] }, () => {}), /live/);
  } finally {
    worker.stop();
    mails.stop();
    audit.stop();
    await Promise.allSettled(running);
    v1.close();
    await kernel.dispose();
  }
});
