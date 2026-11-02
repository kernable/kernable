// Reactors park events their handlers give up on, and get them again when an operator asks.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

/** The lines of `kernable listeners parked`, once there are `n`. */
async function parked(kernel: TestKernel, n: number): Promise<string[]> {
  let lines: string[] = [];
  for (let i = 0; i < 100; i++) {
    const { ok, out } = kernel.cli("listeners", "parked", kernel.store);
    assert.ok(ok);
    lines = out.split("\n").filter(Boolean);
    if (lines.length === n) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return lines;
}

async function eventually(done: () => boolean): Promise<void> {
  for (let i = 0; i < 100 && !done(); i++) await new Promise((resolve) => setTimeout(resolve, 20));
}

/** A shop whose `Ship` is rejected for an order that was not placed. */
function shopDomain() {
  const shop = domain("shop");
  const order = shop
    .aggregate("Order", false)
    .apply("Placed", () => true)
    .command("Place", () => [{ type: "Placed" }])
    .command("Request", () => [{ type: "Requested" }])
    .command("Ship", (placed) => {
      if (!placed) throw new Rejected("not placed", "not_placed");
      return [{ type: "Shipped" }];
    });
  return { shop, order };
}

test("a rejected follow-up parks the event and the reactor moves on", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const { shop, order } = shopDomain();
  const shipped: string[] = [];
  const shipper = shop.reactor("shipper").on("Requested", async (_data, evt, ctx) => {
    await ctx.submit({ aggregateType: "Order", aggregateId: evt.aggregateId, type: "Ship", id: `ship-${evt.aggregateId}` });
    shipped.push(evt.aggregateId);
  });
  let refused: unknown;
  const noId = shop.reactor("no-id").on("Placed", async (_data, evt, ctx) => {
    refused = await ctx.submit({ aggregateType: "Order", aggregateId: evt.aggregateId, type: "Ship", id: "" }).catch((err: unknown) => err);
  });
  const worker = kernable.worker(order);
  const handles = [kernable.listener(shipper), kernable.listener(noId)];
  const running = [worker.run(), ...handles.map((h) => h.run())];
  const submit = (id: string, type: string) => kernable.submit({ domain: "shop", aggregateType: "Order", aggregateId: id, type });
  try {
    await Promise.all([worker.ready, ...handles.map((h) => h.ready)]);
    await submit("o1", "Request");
    await submit("o2", "Place");
    await submit("o2", "Request");
    // o1 was never placed: its shipping is rejected, parked at once; o2 ships.
    assert.deepEqual(shipped, ["o2"]);
    const lines = await parked(kernel, 1);
    assert.match(lines[0], /^shipper position 1 shop\.Requested not_placed attempts 1 parked .*: not placed$/);
    // A follow-up without a fixed id is refused before it is sent.
    assert.match(String(refused), /needs a fixed id/);
  } finally {
    worker.stop();
    for (const h of handles) h.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a failing handler parks after its attempts, keeps the rest of its batch, and a retry handles it", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const { shop, order } = shopDomain();
  let down = true;
  const mailed: string[] = [];
  // With a store: a parked event's writes go, the rest of the batch stays.
  const mailer = shop.reactor("mailer", { parkAfter: 2, reconnectMs: 20, store: "memory:" }).on("Placed", (_data, evt, ctx) => {
    ctx.store!.set("tried", evt.aggregateId, true);
    if (evt.aggregateId === "o1" && down) throw new TypeError("the mail server is down");
    mailed.push(evt.aggregateId);
  });
  const worker = kernable.worker(order);
  const handle = kernable.listener(mailer);
  const running = [worker.run(), handle.run()];
  const place = (id: string) => kernable.submit({ domain: "shop", aggregateType: "Order", aggregateId: id, type: "Place" });
  try {
    await Promise.all([worker.ready, handle.ready]);
    await place("o1");
    await place("o2");
    await eventually(() => mailed.length === 1);
    assert.deepEqual(mailed, ["o2"]);
    const lines = await parked(kernel, 1);
    assert.match(lines[0], /^mailer position 1 shop\.Placed handler_error attempts 2 parked .*: TypeError$/);
    assert.equal(handle.store.get("tried", "o1"), undefined);
    assert.equal(handle.store.get("tried", "o2"), true);

    // Fixed: the operator retries it, and it is handled.
    down = false;
    const retried = kernel.cli("listeners", "retry", kernel.url, "app.key", "mailer", "--all");
    assert.ok(retried.ok, retried.out);
    assert.equal(retried.out.trim(), "retrying 1 parked events of mailer: 1");
    await eventually(() => mailed.length === 2);
    assert.deepEqual(mailed, ["o2", "o1"]);
    assert.deepEqual(await parked(kernel, 0), []);
    assert.equal(handle.store.get("tried", "o1"), true);
  } finally {
    worker.stop();
    handle.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a read model never parks by default", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const { shop, order } = shopDomain();
  const seen: string[] = [];
  const orders = shop.readModel("orders", { reconnectMs: 20 }).on("Placed", (_data, evt) => {
    seen.push(evt.aggregateId);
    if (evt.aggregateId === "o1") throw new Rejected("cannot show it");
  });
  const worker = kernable.worker(order);
  const handle = kernable.listener(orders);
  const running = [worker.run(), handle.run()];
  const place = (id: string) => kernable.submit({ domain: "shop", aggregateType: "Order", aggregateId: id, type: "Place", wait: "committed" });
  try {
    await Promise.all([worker.ready, handle.ready]);
    await place("o1");
    await place("o2");
    // It keeps getting o1; o2 waits behind it.
    await eventually(() => seen.length >= 5);
    assert.ok(seen.length >= 5 && seen.every((id) => id === "o1"), `${seen}`);
    assert.deepEqual(await parked(kernel, 0), []);
  } finally {
    worker.stop();
    handle.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
