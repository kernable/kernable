// Persistent read models: data and position are stored together per batch, a restart continues
// after the stored position, a failed batch leaves no trace, and a new schema rebuilds.

import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain, type ListenerHandle, type Personal } from "../src/index.ts";
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
    // Another process's definition: a domain has one listener of a name.
    const Totals = domain("counters").readModel("totals", { store: url, schema, reconnectMs: 50 })
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

test("a listener closes its store when it stops and opens it again on its next run", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const path = join(kernel.dir, "sums.db");
  const sums = domain("counters").readModel("sums", { store: `sqlite:${path}` })
    .on("Incremented", (data: { by: number }, evt, ctx) => ctx.store!.set("sums", evt.aggregateId, (ctx.store!.get<number>("sums", evt.aggregateId) ?? 0) + data.by));
  const worker = kernable.worker(counter);
  const first = kernable.listener(sums);
  const running = [worker.run(), first.run()];
  let second: ListenerHandle | undefined;
  try {
    await Promise.all([worker.ready, first.ready]);
    await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 2 } });
    const store = first.store;
    assert.deepEqual(store.all("sums"), [["c1", 2]]);
    first.stop();
    await running[1];
    assert.throws(() => store.all("sums"), /not open/, "the store is closed");
    assert.throws(() => first.store, /has no store/);
    assert.ok(!existsSync(`${path}-wal`), "the database is released");

    second = kernable.listener(sums);
    running.push(second.run());
    await second.ready;
    assert.deepEqual(second.store.all("sums"), [["c1", 2]]);
  } finally {
    worker.stop();
    first.stop();
    second?.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("an erased value is in no file of a read model's store once the erasure is acknowledged", async () => {
  const kernel = new TestKernel(["people.*"]);
  await kernel.start();
  const people = domain("people");
  people.command("Register").personal(["email"], (_d, id) => id);
  people.event("Registered", 1).personal(["email"], (_d, id) => id);
  people.event("Erased", 1).erases((_d, id) => id);
  const person = people.aggregate("Person", {})
    .command("Register", (_s, data: { email: string }) => [{ type: "Registered", data: { email: data.email } }])
    .command("Erase", () => [{ type: "Erased" }]);
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey }).use(people);
  const emails = kernable.listener(people.readModel("emails", { store: `sqlite:${join(kernel.dir, "emails.db")}` })
    .on("Registered", (data: { email: Personal<string> }, evt, ctx) => ctx.store!.set("emails", evt.aggregateId, { email: data.email }))
    .on("Erased", (_data, evt, ctx) => ctx.store!.remove("emails", evt.aggregateId)));
  const worker = kernable.worker(person);
  const running = [worker.run(), emails.run()];
  const send = (type: string, data?: object) => kernable.submit({ domain: "people", aggregateType: "Person", aggregateId: "p1", type, data });
  // Every file of the store: the database, its write-ahead log and its shared memory.
  const inStore = (value: string) =>
    readdirSync(kernel.dir).filter((f) => f.startsWith("emails.db")).some((f) => readFileSync(join(kernel.dir, f)).includes(value));
  try {
    await Promise.all([worker.ready, emails.ready]);
    assert.ok((await send("Register", { email: "ann@example.com" })).committed);
    assert.deepEqual(emails.store.get("emails", "p1"), { email: "ann@example.com" });
    assert.ok(inStore("ann@example.com"), "the value is on disk before the erasure");

    assert.ok((await send("Erase")).committed);
    assert.equal(emails.store.get("emails", "p1"), undefined);
    assert.ok(!inStore("ann@example.com"), "the erased value stays on disk");
  } finally {
    worker.stop();
    emails.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("an erasing event is never parked: the reactor tries until it gets through", async () => {
  const kernel = new TestKernel(["people.*"]);
  await kernel.start();
  const people = domain("people");
  people.command("Register").personal(["email"], (_d, id) => id);
  people.event("Registered", 1).personal(["email"], (_d, id) => id);
  people.event("Erased", 1).erases((_d, id) => id);
  const person = people.aggregate("Person", {})
    .command("Register", (_s, data: { email: string }) => [{ type: "Registered", data: { email: data.email } }])
    .command("Erase", () => [{ type: "Erased" }]);
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey }).use(people);
  let failures = 0;
  // Parks at the first failure, but not an erasure: that must get through, or the email would stay.
  const emails = kernable.listener(people.reactor("emails", { parkAfter: 1, reconnectMs: 20, store: "memory:" })
    .on("Registered", (data: { email: Personal<string> }, evt, ctx) => ctx.store!.set("emails", evt.aggregateId, { email: data.email }))
    .on("Erased", (_data, evt, ctx) => {
      if (failures++ < 2) throw new Rejected("not yet");
      ctx.store!.remove("emails", evt.aggregateId);
    }));
  const worker = kernable.worker(person);
  const running = [worker.run(), emails.run()];
  const send = (type: string, data?: object) => kernable.submit({ domain: "people", aggregateType: "Person", aggregateId: "p1", type, data });
  try {
    await Promise.all([worker.ready, emails.ready]);
    assert.ok((await send("Register", { email: "ann@example.com" })).committed);
    assert.ok((await send("Erase")).committed);
    assert.equal(failures, 3, "tried until it got through");
    assert.equal(emails.store.get("emails", "p1"), undefined);
    assert.equal(kernel.cli("listeners", "parked", kernel.store).out, "", "nothing parked");
  } finally {
    worker.stop();
    emails.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
