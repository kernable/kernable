// Partitions: two kernels share the tenants by a map. One client, given only the first,
// sends each tenant's commands to its partition, runs its worker on both, and a read model
// with a store sees the events of both, each partition's position kept apart.

import assert from "node:assert/strict";
import { copyFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(what: string, done: () => boolean): Promise<void> {
  for (let i = 0; i < 300 && !done(); i++) await sleep(20);
  assert.ok(done(), what);
}

test("one client serves the tenants of two partitions", async () => {
  // p2 trusts p1's root and takes the map and the trust records from p1, with a node key.
  const p1 = new TestKernel(["counters.*"], ["--partition", "p1"]);
  p1.cli("keys", "new", "node");
  p1.cli("grant", "root.key", "node.pub", "replicate=*");
  const p2 = new TestKernel(["counters.*"], ["--partition", "p2", "--control", p1.url, "--node-key", "node.key"]);
  for (const file of ["root.pub", "app.key", "app.pub", "app.grant", "node.key", "node.grant"]) {
    copyFileSync(join(p1.dir, file), join(p2.dir, file));
  }
  await p1.start();
  await p2.start();
  const map = { epoch: 1, partitions: { p1: [p1.url], p2: [p2.url] }, default: "p1", control: "p1", tenants: { acme: "p2" } };
  writeFileSync(join(p1.dir, "map.json"), JSON.stringify(map));
  assert.ok(p1.cli("partitions", "apply", p1.url, "app.key", "map.json").ok, "map applied");

  const kernable = new Kernable({ url: p1.url, key: p1.appKey, tenant: "t1" });
  const totals = counters.readModel("totals", { store: `sqlite:${join(p1.dir, "totals.db")}`, reconnectMs: 50 }).on("Incremented", (data: { by: number }, evt, ctx) => {
    ctx.store!.set("totals", evt.tenant, (ctx.store!.get<number>("totals", evt.tenant) ?? 0) + data.by);
  });
  const worker = kernable.worker(counter);
  const listener = kernable.listener(totals);
  const seen: string[] = [];
  const running = [worker.run({ reconnectMs: 50 }), listener.run()];
  const subscription = kernable.subscribe({ types: ["counters.Incremented"], reconnectMs: 50 }, (e) => {
    seen.push(e.tenant);
  });
  try {
    await worker.ready;
    await listener.ready;
    // p2 takes the map from p1 within a second or two; until then it refuses acme.
    let acme = false;
    for (let i = 0; i < 100 && !acme; i++) {
      try {
        await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 5 }, tenant: "acme", id: "acme-1" });
        acme = true;
      } catch {
        await sleep(50);
      }
    }
    assert.ok(acme, "acme's command reached partition p2");
    await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 2 }, id: "t1-1" });
    await until("the read model saw both partitions", () => listener.store.get("totals", "acme") === 5 && listener.store.get("totals", "t1") === 2);
    await until("the subscription saw both partitions", () => seen.includes("acme") && seen.includes("t1"));
    assert.ok(listener.store.committedFor("p1") > 0 && listener.store.committedFor("p2") > 0, "a position per partition");

    // t1 moves to p2 with its events; the client finds it there.
    await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 3 }, id: "t1-2" });
    const moved = p1.cli("partitions", "move", p1.url, "app.key", "t1", "p2");
    assert.ok(moved.ok, `moved: ${moved.out}`);
    await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 4 }, id: "t1-3" });
    await until("the running read model saw t1's event on p2", () => listener.store.get("totals", "t1") === 9);

    // A new read model replays both partitions: t1's events once each, in their order, though
    // p2's stream is the shorter one.
    const order: number[] = [];
    const replay = counters.readModel("replay", { reconnectMs: 50 }).on("Incremented", (_data: unknown, evt) => {
      if (evt.tenant === "t1") order.push(evt.version);
    });
    const replaying = kernable.listener(replay);
    const replayed = replaying.run();
    try {
      await until("the new read model saw t1's events", () => order.length >= 3);
      await sleep(200);
      assert.deepEqual(order, [1, 2, 3]);
    } finally {
      replaying.stop();
      await replayed;
    }

    // A read model across tenants, slow on acme's events: asked in t9's partition (p1), a query with
    // `allPartitions` waits until it has processed p2 as well.
    let sum = 0;
    const slow = counters.readModel("sum", { reconnectMs: 50 })
      .on("Incremented", async (data: { by: number }, evt) => {
        if (evt.tenant === "acme") await sleep(1000);
        sum += data.by;
      })
      .query("Sum", () => sum);
    const summing = kernable.listener(slow);
    const summed = summing.run();
    try {
      await summing.ready;
      const ask = (allPartitions: boolean) => kernable.query<number>({ domain: "counters", type: "Sum", tenant: "t9", allPartitions });
      assert.equal(await ask(true), 14, "caught up on both partitions");
      await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 10 }, tenant: "acme", id: "acme-2", wait: "committed" });
      assert.equal(await ask(false), 14, "a plain query is current only on p1");
      assert.equal(await ask(true), 24, "allPartitions waits for acme's event on p2");
    } finally {
      summing.stop();
      await summed;
    }
  } finally {
    worker.stop();
    listener.stop();
    subscription.stop();
    await Promise.all([...running, subscription.done]);
    kernable.close();
    await p2.dispose();
    await p1.dispose();
  }
});
