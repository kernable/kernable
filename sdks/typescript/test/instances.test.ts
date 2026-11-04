// Instances: one listener run by several processes under one name. A single reactor runs on
// one instance and the next takes over; a replicated read model runs on every instance.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

/** The shop as a process on machine `node` defines it: its own domain object, the same names. */
function shopOn(node: string, seen: string[]) {
  const shop = domain("shop");
  const cart = shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const mailer = shop.reactor("mailer", { instance: node }).on("Added", (_data, evt) => void seen.push(`${node}:${evt.aggregateId}`));
  const carts = shop.readModel("carts", { instance: node }).on("Added", (_data, evt) => void seen.push(`${node}:carts:${evt.aggregateId}`));
  return { cart, mailer, carts };
}

test("a single reactor runs on one instance, and the next takes over where the group left off", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const seen: string[] = [];
  const a = shopOn("node-a", seen);
  const b = shopOn("node-b", seen);
  const worker = kernable.worker(a.cart);
  const mailerA = kernable.listener(a.mailer);
  const mailerB = kernable.listener(b.mailer);
  const running = [worker.run(), mailerA.run()];
  const add = (id: string) => kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add" });
  try {
    await Promise.all([worker.ready, mailerA.ready]);
    running.push(mailerB.run());
    // node-b waits, and counts as started: it does not hold up its process.
    await mailerB.ready;
    assert.ok((await add("c1")).committed);
    assert.deepEqual(seen, ["node-a:c1"]);

    mailerA.stop();
    await running[1];
    assert.ok((await add("c2")).committed, "settled by node-b once it took over");
    assert.deepEqual(seen, ["node-a:c1", "node-b:c2"], "c1 is not mailed again");
  } finally {
    worker.stop();
    mailerA.stop();
    mailerB.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("every instance of a read model gets every event, and one that is gone holds nothing back", async () => {
  const kernel = new TestKernel(["shop.*"], ["--settle-timeout-ms", "500"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const seen: string[] = [];
  const a = shopOn("node-a", seen);
  const b = shopOn("node-b", seen);
  const worker = kernable.worker(a.cart);
  const cartsA = kernable.listener(a.carts);
  const cartsB = kernable.listener(b.carts);
  const running = [worker.run(), cartsA.run(), cartsB.run()];
  const add = (id: string) => kernable.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add" });
  try {
    await Promise.all([worker.ready, cartsA.ready, cartsB.ready]);
    assert.ok((await add("c1")).committed);
    // Settled once one instance had it; give the other a moment.
    for (let i = 0; i < 50 && seen.length < 2; i++) await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual([...seen].sort(), ["node-a:carts:c1", "node-b:carts:c1"]);

    cartsA.stop();
    await running[1];
    assert.ok((await add("c2")).committed, "node-b settles alone");
    assert.ok(seen.includes("node-b:carts:c2"));
  } finally {
    worker.stop();
    cartsA.stop();
    cartsB.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
