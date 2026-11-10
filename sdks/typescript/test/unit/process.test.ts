// Processes (process.ts): the builder produces the shared model of design/processes.md's example,
// resolves the order written into explicit `next`s, and checks what it can before the kernel.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { Command, Event, Query, domain, value, type Duration } from "../../src/index.ts";
import { membershipMessage, processMessage } from "../../src/signing.ts";

const fixture = JSON.parse(readFileSync(new URL("../../../../proto/test-vectors/process-model.json", import.meta.url), "utf8"));
const vectors = JSON.parse(readFileSync(new URL("../../../../proto/test-vectors/signing.json", import.meta.url), "utf8"));

class OrderSubmitted extends Event {
  total!: number;
}
class OrderApproved extends Event {}
class Confirm extends Command {}
class Ship extends Command {}
class NotifyDelay extends Command {}
class Cancel extends Command {
  reason!: string;
}
class NeedsApproval extends Query<boolean> {
  orderId!: string;
}

const shop = domain("shop");

// The process first: classes are resolved when the model is built, not when it is written.
const fulfilment = shop.process("fulfilment")
  .start(OrderSubmitted, { key: "aggregateId", data: { orderId: "aggregateId", total: "data.total" } })
  .command("confirm", Confirm, { aggregate: "case.orderId" })
  .decide("large", { when: "case.total > 1000", then: "approval", otherwise: "paid" })
  .query("approval", NeedsApproval, { data: { orderId: "case.orderId" }, paths: { true: "approved", false: "paid" } })
  .wait("approved", OrderApproved, { key: "aggregateId" })
  .wait("paid", "payments.PaymentReceived", { key: "data.orderId", data: { paidAt: "data.at" }, timeout: "3d", onTimeout: "cancel" })
  .command("ship", Ship, { aggregate: "case.orderId", rejected: { out_of_stock: "notify" }, next: "done" })
  .command("notify", NotifyDelay, { aggregate: "case.orderId", next: "done" })
  .command("cancel", Cancel, { aggregate: "case.orderId", data: { reason: value("unpaid") } })
  .end("done");

shop.aggregate("order", {})
  .apply(OrderSubmitted, () => {})
  .apply(OrderApproved, () => {})
  .command(Confirm, () => [])
  .command(Ship, () => [])
  .command(NotifyDelay, () => [])
  .command(Cancel, () => []);
shop.readModel("approvals").query(NeedsApproval, () => false);

test("the design's example builds the shared model", () => {
  assert.deepEqual(fulfilment.model(), fixture.model);
  assert.equal(fulfilment.qualified, "shop.fulfilment");
  assert.deepEqual(shop.processes, [fulfilment]);
  // Key order as in the spec, for readability (the kernel canonicalizes).
  assert.equal(JSON.stringify(fulfilment.model()), JSON.stringify(fixture.model));
});

test("names work like classes; unqualified ones are of the process's domain", () => {
  const byName = domain("shop").process("fulfilment")
    .start("OrderSubmitted", { key: "aggregateId", data: { orderId: "aggregateId", total: "data.total" } })
    .command("confirm", "order.Confirm", { aggregate: "case.orderId" })
    .decide("large", { cases: [{ when: "case.total > 1000", then: "approval" }], otherwise: "paid" })
    .query("approval", "shop.NeedsApproval", { data: { orderId: "case.orderId" }, paths: [[true, "approved"], [false, "paid"]] })
    .wait("approved", "shop.OrderApproved", { key: "aggregateId" })
    .wait("paid", "payments.PaymentReceived", { key: "data.orderId", data: { paidAt: "data.at" }, timeout: "72h", onTimeout: "cancel" })
    .command("ship", "shop.order.Ship", { aggregate: "case.orderId", rejected: { out_of_stock: "notify" }, next: "done" })
    .command("notify", "order.NotifyDelay", { aggregate: "case.orderId", next: "done" })
    .command("cancel", "order.Cancel", { aggregate: "case.orderId", data: { reason: value("unpaid") } })
    .end("done");
  assert.deepEqual(byName.model(), fixture.model);
});

test("timers, query answers, durations and several decisions", () => {
  const model = domain("billing").process("dunning")
    .start("InvoiceIssued", { key: "aggregateId" })
    .timer("grace", { after: "15m" })
    .timer("due", { at: "case.dueAt" }, { next: "check" })
    .end("unused")
    .query("check", "GetInvoice", { answer: "status", paths: { paid: "paid", "2": "paid", "1.5": "paid", "007": "paid" }, into: "invoice", otherwise: "remind" })
    .decide("remind", { cases: [{ when: "case.invoice.total > 100", then: "paid" }, { when: "not case.vip", then: "paid" }], otherwise: "paid" })
    .wait("late", "Paid", { key: "data.invoiceId", timeout: "30s", onTimeout: "paid" })
    .end("paid")
    .model();
  assert.deepEqual(model, {
    name: "billing.dunning",
    start: { on: "billing.InvoiceIssued", key: "aggregateId" },
    steps: [
      { name: "grace", kind: "timer", afterMs: 900_000, next: "due" },
      { name: "due", kind: "timer", at: "case.dueAt", next: "check" },
      { name: "unused", kind: "end" },
      {
        name: "check", kind: "query", query: "billing.GetInvoice", answer: "status",
        paths: [{ value: 2, next: "paid" }, { value: "paid", next: "paid" }, { value: 1.5, next: "paid" }, { value: "007", next: "paid" }],
        into: "invoice", otherwise: "remind",
      },
      { name: "remind", kind: "decide", when: [{ if: "case.invoice.total > 100", next: "paid" }, { if: "not case.vip", next: "paid" }], otherwise: "paid" },
      { name: "late", kind: "wait", event: "billing.Paid", key: "data.invoiceId", timeoutMs: 30_000, onTimeout: "paid", next: "paid" },
      { name: "paid", kind: "end" },
    ],
  });
});

test("local checks name the process, the step and the reason", () => {
  const d = domain("shop");
  const start = (name: string) => d.process(name).start("OrderSubmitted", { key: "aggregateId" });
  assert.throws(() => d.process("nostart").end("done").model(), /^Error: process shop\.nostart: no start/);
  assert.throws(() => start("nosteps").model(), /process shop\.nosteps: no steps/);
  assert.throws(() => start("twice").start("OrderSubmitted", { key: "aggregateId" }), /process shop\.twice: start is given already/);
  assert.throws(() => start("dup").command("a", "order.Confirm", { aggregate: "case.id" }).end("a").model(), /process shop\.dup, step "a": a second step of this name/);
  assert.throws(() => start("badname").end("a b").model(), /process shop\.badname, step "a b": a step name has letters/);
  assert.throws(() => start("target").command("a", "order.Confirm", { aggregate: "case.id", next: "nope" }).end("done").model(), /process shop\.target, step "a": next names an unknown step "nope"/);
  assert.throws(() => start("rejected").command("a", "order.Confirm", { aggregate: "case.id", rejected: { gone: "nope" } }).end("done").model(), /step "a": rejected\.gone names an unknown step "nope"/);
  assert.throws(() => start("timeout").wait("w", "Paid", { key: "aggregateId", timeout: "1d", onTimeout: "nope" }).end("done").model(), /step "w": onTimeout names an unknown step "nope"/);
  assert.throws(() => start("then").decide("d", { when: "case.x", then: "nope", otherwise: "done" }).end("done").model(), /step "d": then names an unknown step "nope"/);
  assert.throws(() => start("otherwise").decide("d", { when: "case.x", then: "done", otherwise: "nope" }).end("done").model(), /step "d": otherwise names an unknown step "nope"/);
  assert.throws(() => start("cases").decide("d", { cases: [], otherwise: "done" }).end("done").model(), /step "d": a decision needs a condition/);
  assert.throws(() => start("paths").query("q", "Get", { paths: { true: "nope" } }).end("done").model(), /step "q": paths\[true\] names an unknown step "nope"/);
  assert.throws(() => start("nopaths").query("q", "Get", { paths: {} }).end("done").model(), /step "q": a query step needs paths/);
  assert.throws(() => start("last").command("a", "order.Confirm", { aggregate: "case.id" }).model(), /process shop\.last, step "a": the last step is not an end/);
  for (const bad of ["3", "3w", "0s", "-1d", "1.5h", " 3d", ""]) {
    assert.throws(() => start(`dur${bad.replace(/[^a-z0-9]/g, "_")}`).timer("t", { after: bad as Duration }).end("done").model(), /step "t": ".*" is no duration/, bad);
  }
  assert.throws(() => start("pair").wait("w", "Paid", { key: "aggregateId", timeout: "1d" }).end("done").model(), /step "w": timeout and onTimeout go together/);
  assert.throws(() => start("pair2").wait("w", "Paid", { key: "aggregateId", onTimeout: "done" }).end("done").model(), /step "w": timeout and onTimeout go together/);
  assert.throws(() => start("timer").timer("t", {} as never).end("done").model(), /step "t": a timer waits either/);
  assert.throws(() => start("cmdtype").command("a", "Confirm", { aggregate: "case.id" }).end("done").model(), /step "a": "Confirm" is no command type/);
  assert.throws(() => start("evtype").wait("w", "a.b.C", { key: "aggregateId" }).end("done").model(), /step "w": "a\.b\.C" is no type/);
  class Unbound extends Command {}
  assert.throws(() => start("unbound").command("a", Unbound, { aggregate: "case.id" }).end("done").model(), /Unbound is not handled by an aggregate yet/);
  assert.throws(() => d.process("dup"), /shop has a process dup already/);
  assert.throws(() => d.process("a.b"), /invalid process name/);
  assert.throws(() => value(undefined), /literal/);
});

test("a query step names its own targets: no implicit successor", () => {
  const model = domain("shop").process("poll")
    .start("Started", { key: "aggregateId" })
    .query("q", "Get", { paths: { true: "done" }, otherwise: "q" })
    .end("done")
    .model();
  assert.deepEqual(model.steps[0], { name: "q", kind: "query", query: "shop.Get", paths: [{ value: true, next: "done" }], otherwise: "q" });
});

test("process registrations and memberships with processes sign as specified", () => {
  // Msg: u32 BE length-prefixed fields, integers 8 bytes BE (SIGNING.md).
  const field = (b: Buffer) => { const len = Buffer.alloc(4); len.writeUInt32BE(b.length); return Buffer.concat([len, b]); };
  const i64 = (n: number) => { const b = Buffer.alloc(8); b.writeBigInt64BE(BigInt(n)); return b; };
  const model = Buffer.from(JSON.stringify(fulfilment.model()));
  assert.deepEqual(processMessage(model, 1700000000000), Buffer.concat([field(Buffer.from("kernable.v1.process")), field(model), i64(1700000000000)]));
  if (vectors.process?.input?.model_hex) {
    assert.equal(processMessage(Buffer.from(vectors.process.input.model_hex ?? "", "hex"), vectors.process.input.issued_at).toString("hex"), vectors.process.message);
  }

  // Without processes as before; with them, `processes`, the count and the names appended.
  const m = vectors.membership.input;
  assert.equal(membershipMessage({ ...m, processes: [] }).toString("hex"), vectors.membership.message);
  const names = ["shop.fulfilment", "shop.returns"];
  assert.deepEqual(
    membershipMessage({ ...m, processes: names }),
    Buffer.concat([membershipMessage(m), field(Buffer.from("processes")), i64(2), ...names.map((n) => field(Buffer.from(n)))]),
  );
});

test("a decision without otherwise goes on to the step written next", () => {
  const p = domain("d1").process("p")
    .start("Started", { key: "aggregateId" })
    .decide("big", { when: "case.total > 10", then: "done" })
    .command("note", "order.Note", { aggregate: "case.id" })
    .end("done");
  const decide = (p.model().steps as { name: string; otherwise?: string }[]).find((s) => s.name === "big");
  assert.equal(decide?.otherwise, "note");
});

test("cancelOn ends a case at an end step, whatever step it is at", () => {
  const model = domain("accounts").process("onboarding")
    .start("AccountOpened", { key: "aggregateId" })
    .wait("email", "EmailConfirmed", { key: "aggregateId", next: "onboarded" })
    .end("onboarded")
    .end("closed")
    .cancelOn("AccountClosed", { key: "aggregateId", end: "closed" })
    .model();
  assert.deepEqual(model.cancelOn, [{ event: "accounts.AccountClosed", key: "aggregateId", end: "closed" }]);
  // Without cancelOn, the model has no such field.
  assert.equal("cancelOn" in fulfilment.model(), false);
  const to = (end: string) => () => domain("accounts").process("p")
    .start("AccountOpened", { key: "aggregateId" })
    .wait("email", "EmailConfirmed", { key: "aggregateId", next: "done" })
    .end("done")
    .cancelOn("AccountClosed", { key: "aggregateId", end })
    .model();
  assert.throws(to("nowhere"), /unknown step "nowhere"/);
  assert.throws(to("email"), /"email", which is no end step/);
});
