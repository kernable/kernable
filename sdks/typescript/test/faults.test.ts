// Faults around listeners: a first connect that fails, and a batch that fails while the next one
// is already on its way. Nothing may be skipped, and nothing from before a reactor existed may be
// replayed.

import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(what: string, done: () => boolean): Promise<void> {
  for (let i = 0; i < 250 && !done(); i++) await sleep(20);
  assert.ok(done(), what);
}

test("a new reactor whose first connect fails still starts at the newest event", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  const worker = kernable.worker(counter);
  const running = [worker.run({ reconnectMs: 50 })];
  const seen: number[] = [];
  const reactor = kernable.listener(counters.reactor("mailer", { reconnectMs: 50 }).on("Incremented", (data: { by: number }) => {
    seen.push(data.by);
  }));
  try {
    await worker.ready;
    for (const by of [1, 2, 3]) await increment("c1", by);

    // The kernel is away when the reactor connects for the first time.
    await kernel.kill();
    running.push(reactor.run());
    await sleep(200);
    await kernel.start();
    await reactor.ready;
    await until("the worker is back", () => true);
    await increment("c1", 4);
    await until("the new event arrives", () => seen.length > 0);
    await sleep(100);
    assert.deepEqual(seen, [4], "only what happened after it existed");
  } finally {
    worker.stop();
    reactor.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a batch that fails stops the batches behind it, so none of its events is skipped", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  let open!: () => void;
  const gate = new Promise<void>((resolve) => (open = resolve));
  let failed = false;
  const Totals = counters.readModel("totals", { store: `sqlite:${join(kernel.dir, "totals.db")}`, reconnectMs: 50 })
    .on("Incremented", async (data: { by: number }, evt, ctx) => {
      if (data.by === 1 && !failed) {
        await gate; // the next batch arrives meanwhile
        failed = true;
        throw new Error("boom");
      }
      ctx.store!.set("totals", evt.aggregateId, (ctx.store!.get<number>("totals", evt.aggregateId) ?? 0) + data.by);
    });
  const worker = kernable.worker(counter);
  const model = kernable.listener(Totals);
  const running = [worker.run(), model.run()];
  try {
    await Promise.all([worker.ready, model.ready]);
    await increment("c1", 1);
    await increment("c1", 2);
    await sleep(200);
    open();
    await until("both events handled", () => model.store.get<number>("totals", "c1") === 3);
  } finally {
    worker.stop();
    model.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a command handler that changes its state changes neither the initial state nor others", async () => {
  const boxes = domain("boxes");
  const box = boxes.aggregate("Box", { items: [] as string[] })
    .apply("Opened", (s) => void s.items.push("opened"))
    .command("Open", (s) => {
      if (s.items.length > 0) throw new Rejected("already open", "invalid_state");
      s.items.push("opened"); // changes its copy only
      return [{ type: "Opened" }];
    })
    .command("Fail", () => {
      throw Object.create(null); // cannot even be shown as text
    });
  const kernel = new TestKernel(["boxes.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const submit = (id: string, type: string) =>
    kernable.submit({ domain: "boxes", aggregateType: "Box", aggregateId: id, type, wait: "committed" });
  const worker = kernable.worker(box);
  const running = worker.run();
  try {
    await worker.ready;
    for (const id of ["b1", "b2", "b3"]) assert.ok((await submit(id, "Open")).committed, id);
    const failed = await submit("b4", "Fail");
    assert.ok(!failed.committed && failed.code === "handler_error", JSON.stringify(failed));
    assert.ok((await submit("b5", "Open")).committed, "the worker is still there");
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});
