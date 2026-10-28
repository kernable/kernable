// Persistent read models: data and position are stored together per batch, a restart continues
// after the stored position, a failed batch leaves no trace, and a new schema rebuilds.

import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain, type ListenerHandle } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

test("a read model with a store continues where it left off and rebuilds on a new schema", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const url = `sqlite:${join(kernel.dir, "totals.db")}`;
  let handled = 0;
  let failOn = -1;
  const totals = (schema?: string) => {
    const Totals = counters.readModel("totals", { store: url, schema, reconnectMs: 50 })
      .on("Incremented", (data: { by: number }, evt, ctx) => {
        handled++;
        const total = (ctx.store!.get<number>("totals", evt.aggregateId) ?? 0) + data.by;
        ctx.store!.set("totals", evt.aggregateId, total);
        if (total === failOn) throw new Error("boom"); // after writing: the write must not stick
      })
      .query("GetTotal", (data: { id: string }, _qry, ctx) => {
        const total = ctx.store!.get<number>("totals", data.id);
        if (total === undefined) throw new Rejected("no counter", "not_found");
        return total;
      });
    const model = kernable.listener(Totals);
    models.push(model);
    return model;
  };
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  const total = (id: string) => kernable.query<number>({ domain: "counters", type: "GetTotal", data: { id } });

  const worker = kernable.worker(counter);
  const running = [worker.run()];
  const models: ListenerHandle[] = [];
  try {
    await worker.ready;
    let model = totals();
    running.push(model.run());
    await model.ready;
    for (const by of [1, 2, 3]) await increment("c1", by);
    await increment("c2", 5);
    assert.equal(await kernable.query({ domain: "counters", type: "GetTotal", data: { id: "c1" }, minPosition: 4 }), 6);
    assert.equal(handled, 4);
    model.stop();

    // A new process with the same store: nothing is replayed, the data is there.
    await increment("c1", 4);
    model = totals();
    running.push(model.run());
    await model.ready;
    assert.equal(await kernable.query({ domain: "counters", type: "GetTotal", data: { id: "c1" }, minPosition: 5 }), 10);
    assert.equal(handled, 5, "only the event after the stored position");
    assert.deepEqual(model.store.all("totals"), [["c1", 10], ["c2", 5]]);

    // A failing handler rolls its batch back; delivered again, it counts once.
    failOn = 13;
    await increment("c1", 3);
    await new Promise((r) => setTimeout(r, 150));
    failOn = -1;
    for (let i = 0; i < 100 && model.position < 6; i++) await new Promise((r) => setTimeout(r, 20));
    assert.equal(await kernable.query({ domain: "counters", type: "GetTotal", data: { id: "c1" }, minPosition: 6 }), 13);
    model.stop();

    // A new schema clears the store and rebuilds from the first event.
    handled = 0;
    model = totals("2");
    running.push(model.run());
    await model.ready;
    assert.equal(await kernable.query({ domain: "counters", type: "GetTotal", data: { id: "c1" }, minPosition: 6 }), 13);
    assert.equal(handled, 6);
    assert.equal(await total("c2"), 5);
    model.stop();
  } finally {
    worker.stop();
    for (const m of models) m.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
