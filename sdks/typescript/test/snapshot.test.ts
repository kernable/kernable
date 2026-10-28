// Snapshots: the worker folds only the events after the latest snapshot, and a changed state
// format (schema) falls back to all events.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("snapshots shorten replays and a new schema rebuilds from the events", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  let applied = 0;

  // v1 keeps the state as an object, with what `apply` learns about the last event: every
  // increment is by 1, so the version equals n, whether folded from history or from a snapshot.
  const v1 = domain("counters").aggregate("Counter", { n: 0, version: 0, id: "c1" }, { snapshotEvery: 3 })
    .apply("Incremented", (s, data: { by: number }, evt) => (applied++, { n: s.n + data.by, version: evt.version, id: evt.aggregateId }))
    .command("Increment", (s, data: { by: number }) => {
      if (s.version !== s.n || s.id !== "c1") throw new Rejected(`event meta ${JSON.stringify(s)}`);
      if (s.n + data.by > 20) throw new Rejected("limit");
      return [{ type: "Incremented", data: { by: data.by } }];
    });
  // v2 keeps it as a number: old snapshots do not fit.
  const v2 = domain("counters").aggregate("Counter", 0, { snapshotEvery: 3, schema: "2" })
    .apply("Incremented", (n, data: { by: number }) => n + data.by)
    .command("Increment", (n, data: { by: number }) => {
      if (n + data.by > 20) throw new Rejected("limit");
      return [{ type: "Incremented", data: { by: data.by } }];
    });
  const increment = (by: number) => kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by } });

  let worker = kernable.worker(v1);
  let running = worker.run({ reconnectMs: 100 });
  try {
    await worker.ready;
    for (let i = 0; i < 10; i++) assert.ok((await increment(1)).committed);
    // Without snapshots, folding the history alone would apply 0 + 1 + ... + 9 = 45 events.
    assert.ok(applied <= 25, `applied ${applied} events`);
    worker.stop();
    await running;

    worker = kernable.worker(v2);
    running = worker.run({ reconnectMs: 100 });
    await worker.ready;
    assert.equal((await increment(11)).committed, false); // 10 + 11 > 20: the state is right
    assert.ok((await increment(10)).committed);
    // The v1 snapshot was dropped (the kernel's snapshot store is checked in the Rust tests).
    assert.equal((await increment(1)).committed, false); // 21 > 20
    assert.ok((await increment(0)).committed); // version 12: a v2 snapshot
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});
