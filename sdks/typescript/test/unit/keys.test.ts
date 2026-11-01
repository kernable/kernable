// Unique keys and facts: declared on the aggregate, computed from a state without a kernel.

import assert from "node:assert/strict";
import { test } from "node:test";

import { CommandContext, domain, reindexType } from "../../src/index.ts";

type Customer = { id?: string; number?: string; emails: string[]; status: string };

const customers = () =>
  domain("crm")
    .aggregate<Customer>("Customer", { emails: [], status: "new" })
    .apply("Registered", (s, data: { id: string; number: string; email: string }) => {
      s.id = data.id;
      s.number = data.number;
      s.emails.push(data.email);
      s.status = "active";
    })
    .unique("number", (s) => s.number)
    .unique("email", (s) => s.emails.map((e) => e.toLowerCase()), { scope: "global", personal: (s) => s.id })
    .fact("active", (s) => s.status === "active")
    .fact("vip", () => false);

test("index returns the keys and facts a state yields", () => {
  const customer = customers();
  assert.deepEqual(customer.index(customer.initial), { keys: [], facts: [] });
  const state = customer.fold([{ type: "Registered", schema: 1, data: { id: "k1", number: "A-1", email: "Ann@example.com" } }]);
  assert.deepEqual(customer.index(state), {
    keys: [
      { name: "number", value: "A-1", scope: "tenant" },
      { name: "email", value: "ann@example.com", scope: "global", subject: "k1" },
    ],
    facts: ["active"],
  });
  // Each value once; a personal key without a subject holds nothing.
  assert.deepEqual(customer.index({ ...state, emails: ["a@x", "A@X", "b@x"] }).keys.map((k) => k.value), ["A-1", "a@x", "b@x"]);
  assert.deepEqual(customer.index({ ...state, id: "" }).keys.map((k) => k.name), ["number"]);
  assert.throws(() => customer.index({ ...state, number: 7 as unknown as string }), /key number of Customer: return a string/);
});

test("aggregates with keys or facts run a built-in reindex command of their type, which produces no events", async () => {
  const customer = customers();
  assert.equal(reindexType("Customer"), "_reindex_Customer");
  assert.deepEqual([...customer.handlers.keys()].filter((t) => t.startsWith("_")), ["_reindex_Customer"]);
  assert.deepEqual(await customer.handle(customer.initial, "_reindex_Customer", {}, new CommandContext("c1")), []);
  assert.deepEqual([...domain("crm").aggregate("Plain", {}).handlers.keys()], []);
});

test("requirements are declared with the command", () => {
  const order = domain("crm")
    .aggregate("Order", {})
    .command("Place", () => [], {
      requires: (data: { customer: string }) => [{ domain: "crm", aggregateType: "Customer", aggregateId: data.customer, fact: "active" }],
    });
  assert.deepEqual(order.requirements.get("Place")?.({ customer: "k1" }, {}), [
    { domain: "crm", aggregateType: "Customer", aggregateId: "k1", fact: "active" },
  ]);
});

test("a user command named like a reindex command, a name declared twice and malformed names throw at definition", () => {
  const agg = () => domain("crm").aggregate("Customer", { n: "" });
  // Reserved for every aggregate type: another one of the domain may be named so.
  for (const type of ["_reindex_Customer", "_reindex_Order"]) {
    assert.throws(() => agg().command(type, () => []), /command types starting with _reindex_ are reserved/);
  }
  assert.doesNotThrow(() => agg().command("_reindex", () => []).command("Reindex", () => []));
  assert.throws(() => agg().unique("number", (s) => s.n).unique("number", (s) => s.n), /Customer has a key number already/);
  assert.throws(() => agg().fact("open", () => true).fact("open", () => true), /Customer has a fact open already/);
  for (const name of ["", "a.b", "x".repeat(129)]) {
    assert.throws(() => agg().unique(name, (s) => s.n), /a key name is non-empty, without dots and at most 128 bytes/);
    assert.throws(() => agg().fact(name, () => true), /a fact name is non-empty/);
  }
  // A key and a fact may share a name: the kernel keeps them apart.
  assert.doesNotThrow(() => agg().unique("open", (s) => s.n).fact("open", () => true));
});
