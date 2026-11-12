// Deployments: code versions side by side. Read models of each deployment are kept apart and
// answer their own deployment's queries; the membership's deployment runs the commands, so a
// rollback is applying a membership that names the earlier one; events carry who wrote them.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain, waitsForSwitch, type Outcome } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("read models of two deployments run side by side, each answering its own deployment", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const shop = domain("shop");
  const cart = shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
  const app = new Kernable({ url: kernel.url, key: kernel.appKey });
  const v1 = new Kernable({ url: kernel.url, key: kernel.appKey, deployment: "v1" });
  const v2 = new Kernable({ url: kernel.url, key: kernel.appKey, deployment: "v2" });
  // The same read model in both deployments' code; each answers with its deployment.
  const carts = (deployment: string) => {
    const seen: string[] = [];
    const definition = domain("shop").readModel("carts")
      .on("Added", (_data, evt) => void seen.push(evt.aggregateId))
      .query("Count", () => `${deployment}:${seen.length}`);
    return { seen, definition };
  };
  const one = carts("v1");
  const two = carts("v2");
  const worker = app.worker(cart);
  const first = v1.listener(one.definition);
  const second = v2.listener(two.definition);
  const running = [worker.run(), first.run()];
  const add = (id: string) => app.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add" });
  const count = (client: Kernable) => client.query({ domain: "shop", type: "Count" });
  try {
    await Promise.all([worker.ready, first.ready]);
    const added = await add("c1");
    assert.ok(added.committed);
    assert.equal(added.events[0].deployment, "", "a worker without a deployment writes none");
    assert.ok((await add("c2")).committed);

    // v2 connects while v1 runs: the name has a position under v1, v2 builds its own from the first event.
    running.push(second.run());
    await second.ready;
    assert.ok((await add("c3")).committed);
    assert.equal(await count(v2), "v2:3");
    assert.equal(await count(v1), "v1:3");
    assert.deepEqual(two.seen, ["c1", "c2", "c3"]);
    assert.deepEqual(one.seen, two.seen);
    assert.equal(first.skipped + second.skipped, 0);
  } finally {
    for (const s of [worker, first, second]) s.stop();
    await Promise.allSettled(running);
    for (const c of [app, v1, v2]) c.close();
    await kernel.dispose();
  }
});

test("a rollback to an older deployment: its worker runs again, history it cannot read is refused, its read model skips what it does not know", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  // v1 knows ItemAdded at schema 1; v2 renamed `qty` to `count` (schema 2).
  const shopV1 = domain("shop");
  const cartV1 = shopV1.aggregate("Cart", 0)
    .apply("ItemAdded", (n, data: { qty: number }) => n + data.qty)
    .command("Add", (_n, data: { n: number }) => [{ type: "ItemAdded", data: { qty: data.n } }]);
  const shopV2 = domain("shop");
  shopV2.event("ItemAdded", 2).upcast(1, (d: { qty: number }) => ({ count: d.qty }));
  let v2Commands = 0;
  const cartV2 = shopV2.aggregate("Cart", 0)
    .apply("ItemAdded", (n, data: { count: number }) => n + data.count)
    .command("Add", (_n, data: { n: number }) => (v2Commands++, [{ type: "ItemAdded", data: { count: data.n } }]));
  const seen: [string, number, string][] = [];
  const itemsV1 = shopV1.readModel("items")
    .on("ItemAdded", (data: { qty: number }, evt) => void seen.push([evt.aggregateId, data.qty, evt.deployment]))
    .query("Seen", () => seen.length);

  const v1 = new Kernable({ url: kernel.url, key: kernel.appKey, service: "shop", deployment: "v1" });
  const v2 = new Kernable({ url: kernel.url, key: kernel.appKey, service: "shop", deployment: "v2" });
  const client = new Kernable({ url: kernel.url, key: kernel.appKey });
  const add = (id: string, n: number): Promise<Outcome> =>
    client.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add", data: { n }, wait: "committed" });

  await v2.apply({ workers: [cartV2] }); // deployment v2, the client's
  const workerV2 = v2.worker(cartV2);
  const workerV1 = v1.worker(cartV1); // older schema than the store will hold: no refusal
  const items = v1.listener(itemsV1);
  const running = [workerV2.run(), workerV1.run(), items.run()];
  try {
    await Promise.all([workerV2.ready, workerV1.ready, items.ready]);
    const a = await add("A", 2);
    assert.ok(a.committed);
    assert.deepEqual([a.events[0].schema, a.events[0].deployment], [2, "v2"]);
    assert.equal(v2Commands, 1, "the membership's deployment runs the commands");

    // Roll back by force: a later membership names v1. v1 never served and does not read the
    // history's schema 2, so it would not take over by itself (unknown_history in its warm-up).
    await v1.apply({ workers: [cartV1], switch: "now" });
    const refused = await add("A", 1);
    assert.equal(refused.committed ? "committed" : refused.code, "unknown_history");
    const b = await add("B", 3);
    assert.ok(b.committed, b.committed ? "" : `${b.code}: ${b.message}`);
    assert.deepEqual([b.events[0].schema, b.events[0].deployment], [1, "v1"]);
    assert.equal(v2Commands, 1, "v2's worker gets no commands after the rollback");

    // v1's read model skipped A's schema 2 event and handled B's.
    assert.equal(await v1.query({ domain: "shop", type: "Seen", minPosition: b.events[0].position }), 1);
    assert.deepEqual(seen, [["B", 3, "v1"]]);
    assert.equal(items.skipped, 1);
    assert.equal(items.position, b.events[0].position);
  } finally {
    for (const s of [workerV2, workerV1, items]) s.stop();
    await Promise.allSettled(running);
    for (const c of [v1, v2, client]) c.close();
    await kernel.dispose();
  }
});

test("a new deployment warms up beside the serving one before it takes over", async () => {
  const kernel = new TestKernel(["shop.*"], ["--deployment-switch-lag", "0"]);
  await kernel.start();
  const client = new Kernable({ url: kernel.url, key: kernel.appKey });
  // Each deployment's code, as its own process defines it.
  const shopApp = (deployment: string, sw: "caught-up" | "never") => {
    const shop = domain("shop");
    shop.aggregate("Cart", 0).command("Add", () => [{ type: "Added" }]);
    let seen = 0;
    shop.readModel("carts").on("Added", () => void seen++).query("Count", () => seen);
    const k = new Kernable({ url: kernel.url, key: kernel.appKey, service: "shop", deployment });
    return { k, app: k.app({ domains: [shop], switch: sw }), seen: () => seen };
  };
  const by = async (id: string) => {
    const outcome = await client.submit({ domain: "shop", aggregateType: "Cart", aggregateId: id, type: "Add", wait: "committed" });
    assert.ok(outcome.committed);
    return outcome.events[0].deployment;
  };
  const eventually = async (done: () => boolean) => {
    for (let i = 0; i < 100 && !done(); i++) await new Promise((r) => setTimeout(r, 20));
  };
  const within = <T>(p: Promise<T>, what: string) =>
    Promise.race([p, new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`${what} within 10 s`)), 10_000))]);
  const apps: ReturnType<typeof shopApp>[] = [];
  const running: Promise<void>[] = [];
  const start = (deployment: string, sw: "caught-up" | "never") => {
    const a = shopApp(deployment, sw);
    apps.push(a);
    running.push(a.app.run());
    return a;
  };
  try {
    const v1 = start("v1", "caught-up");
    await within(v1.app.ready, "v1 serves");
    assert.equal(await by("c1"), "v1");
    assert.equal(await by("c2"), "v1");

    // v2 leaves the switch to an operator: it connects and builds its read model, v1 serves.
    const v2 = start("v2", "never");
    await within(v2.app.connected, "v2 connects");
    await eventually(() => v2.seen() >= 2);
    assert.equal(v2.seen(), 2);
    assert.equal(await by("c3"), "v1");
    const early = await Promise.race([v2.app.serving.then(() => true), new Promise((r) => setTimeout(() => r(false), 300))]);
    assert.equal(early, false, "v2 does not take over by itself");

    // v3 takes over by itself once it has caught up.
    const v3 = start("v3", "caught-up");
    await within(v3.app.ready, "v3 takes over");
    assert.equal(await by("c4"), "v3");

    // An operator switches to v2 (caught up meanwhile): v2's app sees it serve.
    for (let i = 0; ; i++) {
      try {
        await v2.k.apply({ ...v2.app.manifest, switch: "caught-up" });
        break;
      } catch (err) {
        if (!waitsForSwitch(err) || i > 100) throw err;
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    await within(v2.app.ready, "v2 serves");
    assert.equal(await by("c5"), "v2");

    // v1's app restarts (switch caught-up): v1 served before, so it runs beside and does not
    // take the service back, although it catches up.
    v1.app.stop();
    await running[0];
    const v1b = start("v1", "caught-up");
    await within(v1b.app.connected, "v1 connects again");
    await eventually(() => v1b.seen() >= 5);
    assert.equal(v1b.seen(), 5);
    const back = await Promise.race([v1b.app.serving.then(() => true), new Promise((r) => setTimeout(() => r(false), 1_500))]);
    assert.equal(back, false, "a restarted v1 does not switch back by itself");
    assert.equal(await by("c6"), "v2");

    // Only an operator's rollback switches back to it.
    await v1b.k.apply({ ...v1b.app.manifest, switch: "now" });
    await within(v1b.app.ready, "v1 serves again");
    assert.equal(await by("c7"), "v1");
  } finally {
    for (const a of apps) a.app.stop();
    await Promise.all(running);
    for (const a of apps) a.k.close();
    client.close();
    await kernel.dispose();
  }
});
