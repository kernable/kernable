// Unique keys, facts and requirements: the worker sends what an aggregate's state yields, and
// the kernel checks it atomically at commit. A key is held by one aggregate at a time, within
// its tenant or across tenants; a command commits only while the facts it requires are held; a
// personal key is freed by erasing its subject; a reindex gives existing aggregates their keys.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain, type Aggregate, type Outcome } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

type Customer = { number?: string; handle?: string; email?: string | null; active: boolean };

/** The crm domain; `keys`: with the unique keys and the fact (else the aggregate before them). */
function crm(keys = true) {
  const crm = domain("crm");
  // The person a customer's email belongs to.
  const person = (number?: string) => (number ? `person-${number}` : undefined);
  crm.event("Registered", 1).personal(["email"], (d: Customer) => person(d.number)!);
  crm.event("PersonForgotten", 1).erases((d: { subject: string }) => d.subject);
  const customer = crm.aggregate<Customer>("Customer", { active: false })
    .apply("Registered", (s, data: Customer) => ({ ...s, ...data, active: true }))
    .apply("Renumbered", (s, data: { number: string }) => { s.number = data.number; })
    .apply("Deactivated", (s) => { s.active = false; })
    .command("Register", (_s, data: Customer) => [{ type: "Registered", data }])
    .command("Renumber", (_s, data: { number: string }) => [{ type: "Renumbered", data }])
    .command("Deactivate", () => [{ type: "Deactivated" }]);
  if (keys) {
    customer
      .unique("number", (s) => s.number)
      .unique("handle", (s) => s.handle, { scope: "global" })
      .unique("email", (s) => s.email?.toLowerCase(), { scope: "global", personal: (s) => person(s.number) })
      .fact("active", (s) => s.active);
  }
  const order = crm.aggregate("Order", {})
    .command("Place", () => [{ type: "Placed" }], {
      requires: (data: { customer: string }) => [{ domain: "crm", aggregateType: "Customer", aggregateId: data.customer, fact: "active" }],
    });
  // Erases a person from another aggregate: the customer itself runs no command.
  const privacy = crm.aggregate("Privacy", {})
    .command("Forget", (_s, data: { subject: string }) => [{ type: "PersonForgotten", data }]);
  return { crm, customer, order, privacy };
}

async function withKernel(f: (kernable: Kernable, run: (...aggs: Aggregate<any>[]) => Promise<() => Promise<void>>) => Promise<void>) {
  const kernel = new TestKernel(["crm.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const running: (() => Promise<void>)[] = [];
  const run = async (...aggs: Aggregate<any>[]) => {
    const worker = kernable.worker(...aggs);
    const done = worker.run({ reconnectMs: 100 });
    await worker.ready;
    const stop = async () => {
      worker.stop();
      await done;
    };
    running.push(stop);
    return stop;
  };
  try {
    await f(kernable, run);
  } finally {
    for (const stop of running) await stop();
    kernable.close();
    await kernel.dispose();
  }
}

/** `committed` or the rejection's code and message. */
function result(o: Outcome): string {
  return o.committed ? "committed" : `${o.code}: ${o.message}`;
}

test("a unique key is held by one aggregate within its tenant, a global one across tenants", async () => {
  const { customer } = crm();
  await withKernel(async (kernable, run) => {
    await run(customer);
    const send = async (id: string, type: string, data: object, tenant = "t1") =>
      result(await kernable.submit({ domain: "crm", aggregateType: "Customer", aggregateId: id, type, data, tenant, wait: "committed" }));

    assert.equal(await send("k1", "Register", { number: "A-1", handle: "ann" }), "committed");
    assert.equal(await send("k2", "Register", { number: "A-1" }), "unique_taken: crm.Customer key number is taken");
    assert.equal(await send("k2", "Register", { number: "A-1" }, "t2"), "committed", "another tenant");
    assert.equal(await send("k3", "Register", { number: "A-3", handle: "ann" }, "t2"), "unique_taken: crm.Customer key handle is taken");

    // A changed value frees the old one in the same commit.
    assert.equal(await send("k1", "Renumber", { number: "A-2" }), "committed");
    assert.equal(await send("k4", "Register", { number: "A-1" }), "committed");
    assert.equal(await send("k5", "Register", { number: "A-2" }), "unique_taken: crm.Customer key number is taken");
  });
});

test("a command commits only while the aggregates it requires hold the fact", async () => {
  const { customer, order } = crm();
  await withKernel(async (kernable, run) => {
    await run(customer, order);
    const send = async (aggregateType: string, id: string, type: string, data: object = {}) =>
      result(await kernable.submit({ domain: "crm", aggregateType, aggregateId: id, type, data, wait: "committed" }));

    assert.equal(await send("Order", "o1", "Place", { customer: "k1" }), "requirement_missing: crm.Customer k1 does not hold active");
    assert.equal(await send("Customer", "k1", "Register", { number: "A-1" }), "committed");
    assert.equal(await send("Order", "o1", "Place", { customer: "k1" }), "committed");
    assert.equal(await send("Customer", "k1", "Deactivate"), "committed");
    assert.equal(await send("Order", "o2", "Place", { customer: "k1" }), "requirement_missing: crm.Customer k1 does not hold active");
  });
});

test("erasing its subject frees a personal key", async () => {
  const { customer, privacy } = crm();
  await withKernel(async (kernable, run) => {
    await run(customer, privacy);
    const send = async (aggregateType: string, id: string, type: string, data: object) =>
      result(await kernable.submit({ domain: "crm", aggregateType, aggregateId: id, type, data, wait: "committed" }));

    assert.equal(await send("Customer", "k1", "Register", { number: "A-1", email: "Ann@example.com" }), "committed");
    assert.equal(await send("Customer", "k2", "Register", { number: "A-2", email: "ann@example.com" }), "unique_taken: crm.Customer key email is taken");
    assert.equal(await send("Privacy", "p1", "Forget", { subject: "person-A-1" }), "committed");
    assert.equal(await send("Customer", "k2", "Register", { number: "A-2", email: "ann@example.com" }), "committed");
  });
});

test("a reindex gives aggregates from before a key was declared the key", async () => {
  await withKernel(async (kernable, run) => {
    const send = async (id: string, number: string) =>
      result(await kernable.submit({ domain: "crm", aggregateType: "Customer", aggregateId: id, type: "Register", data: { number }, wait: "committed" }));
    const before = await run(crm(false).customer);
    assert.equal(await send("k1", "A-1"), "committed");
    await before();

    await run(crm().customer);
    const outcomes = await kernable.reindex({ domain: "crm", aggregateType: "Customer", aggregateIds: ["k1", "k9"] });
    // No events: k9 never existed, so it holds nothing.
    assert.deepEqual([...outcomes], [["k1", { committed: true, events: [] }], ["k9", { committed: true, events: [] }]]);
    assert.equal(await send("k2", "A-1"), "unique_taken: crm.Customer key number is taken");
  });
});

test("two aggregate types of one domain, each in its own worker, are reindexed each by its own command", async () => {
  // Before their keys: a customer and a supplier with the same number (unique per type).
  const before = () => {
    const d = domain("crm");
    const def = (name: string) => d.aggregate<{ number?: string }>(name, {})
      .apply("Registered", (s, data: { number: string }) => { s.number = data.number; })
      .command(`Register${name}`, (_s, data: { number: string }) => [{ type: "Registered", data }]);
    return { customer: def("Customer"), supplier: def("Supplier") };
  };
  const after = () => {
    const { customer, supplier } = before();
    return { customer: customer.unique("number", (s) => s.number), supplier: supplier.unique("number", (s) => s.number) };
  };
  await withKernel(async (kernable, run) => {
    const send = async (aggregateType: string, id: string, number: string) =>
      result(await kernable.submit({ domain: "crm", aggregateType, aggregateId: id, type: `Register${aggregateType}`, data: { number }, wait: "committed" }));
    const old = before();
    const stopCustomers = await run(old.customer);
    const stopSuppliers = await run(old.supplier);
    assert.equal(await send("Customer", "k1", "N-1"), "committed");
    assert.equal(await send("Supplier", "s1", "N-1"), "committed");
    await stopCustomers();
    await stopSuppliers();

    // Each worker runs only its own type's reindex command; neither leases the other's.
    const keyed = after();
    await run(keyed.customer);
    await run(keyed.supplier);
    for (let round = 0; round < 2; round++) {
      const [customers, suppliers] = await Promise.all([
        kernable.reindex({ domain: "crm", aggregateType: "Customer", aggregateIds: ["k1"] }),
        kernable.reindex({ domain: "crm", aggregateType: "Supplier", aggregateIds: ["s1"] }),
      ]);
      assert.deepEqual([...customers, ...suppliers], [["k1", { committed: true, events: [] }], ["s1", { committed: true, events: [] }]]);
    }
    assert.equal(await send("Customer", "k2", "N-1"), "unique_taken: crm.Customer key number is taken");
    assert.equal(await send("Supplier", "s2", "N-1"), "unique_taken: crm.Supplier key number is taken");
  });
});
