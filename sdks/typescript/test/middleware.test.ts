// Middleware: a listener checks commands and queries before the kernel accepts or routes
// them, from its own state, and the kernel refuses them while it is away; after them, it sees
// their outcome. It checks file and cache requests before they run, too.

import assert from "node:assert/strict";
import { test } from "node:test";

import { status } from "@grpc/grpc-js";

import { Kernable, Rejected, domain, type Check, type Outcome, type QueryOutcome } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a middleware checks commands and queries from its own state and fails closed", async () => {
  const kernel = new TestKernel(["support.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const as = (senderIdentity: string) => ({ context: { senderIdentity } });

  const support = domain("support");
  const ticket = support.aggregate("Ticket", { open: false })
    .apply("Opened", () => ({ open: true }))
    .command("Open", () => [{ type: "Opened" }])
    .command("Ban", (_s, data: { identity: string }) => [{ type: "Banned", data }])
    .command("Crash", () => []); // the middleware fails on it before it runs
  const tickets = new Set<string>();
  const model = kernable.listener(support.readModel("tickets")
    .on("Opened", (_data, evt) => void tickets.add(evt.aggregateId))
    .query("CountTickets", () => tickets.size));

  // Refuses banned identities, from the events it has processed.
  const banned = new Set<string>();
  const checked: Check[] = [];
  const guard = kernable.listener(support.readModel("guard")
    .on("Banned", (data: { identity: string }) => void banned.add(data.identity))
    .middleware({ types: ["support.*"] }, (check) => {
      checked.push(check);
      if (banned.has(check.request.senderIdentity ?? "")) throw new Rejected("banned", "forbidden");
      if (check.type === "Crash") throw new Error("boom");
    }));

  const worker = kernable.worker(ticket);
  let running = [worker.run({ reconnectMs: 100 }), model.run(), guard.run()];
  try {
    await Promise.all([worker.ready, model.ready, guard.ready]);
    const open = (id: string, who: string) =>
      kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: id, type: "Open", ...as(who) });

    assert.ok((await open("t1", "mallory")).committed);
    await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: "t1", type: "Ban", data: { identity: "mallory" }, ...as("admin") });
    // The ban settled, so the middleware has it before the next check.
    assert.deepEqual(await open("t2", "mallory"), { committed: false, code: "forbidden", message: "banned" });
    assert.ok((await open("t3", "alice")).committed);
    assert.deepEqual(
      checked.slice(0, 2).map((c) => [c.kind, c.domain, c.type, c.aggregateId, c.request.senderIdentity]),
      [["command", "support", "Open", "t1", "mallory"], ["command", "support", "Ban", "t1", "admin"]],
    );

    // Queries pass the same chain.
    assert.equal(await kernable.query({ domain: "support", type: "CountTickets", ...as("alice") }), 2);
    await assert.rejects(kernable.query({ domain: "support", type: "CountTickets", ...as("mallory") }), /banned/);
    assert.equal(checked.at(-1)?.kind, "query");

    // A failing middleware refuses, too.
    const crash = await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: "t4", type: "Crash" });
    assert.equal(crash.committed ? "committed" : crash.code, "middleware_error");

    // While the middleware is away, everything it checks is refused.
    guard.stop();
    await running[2];
    await new Promise((r) => setTimeout(r, 100));
    await assert.rejects(open("t5", "alice"), /middleware guard is not connected/);
  } finally {
    worker.stop();
    model.stop();
    guard.stop();
    await Promise.all(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a middleware after requests observes commands and may refuse a query's response", async () => {
  const kernel = new TestKernel(["support.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });

  const support = domain("support");
  const ticket = support.aggregate("Ticket", { open: false })
    .apply("Opened", () => ({ open: true }))
    .command("Open", (state) => {
      if (state.open) throw new Rejected("already open", "invalid_state");
      return [{ type: "Opened" }];
    });
  const tickets = new Set<string>();
  const model = kernable.listener(support.readModel("tickets")
    .on("Opened", (_data, evt) => void tickets.add(evt.aggregateId))
    .query("ListTickets", () => [...tickets].sort()));

  const seen: [Check, Outcome | QueryOutcome | undefined][] = [];
  const audit = kernable.listener(support.readModel("audit")
    .middleware({ types: ["*"] }, (check) => {
      if (check.request.senderIdentity === "mallory") throw new Rejected("banned", "forbidden");
    })
    .middleware({ types: ["*"], when: "after" }, (check, outcome) => {
      seen.push([check, outcome]);
      // Only the response of a query can still be refused.
      if (check.kind === "query" && outcome && "answered" in outcome && outcome.answered) {
        if ((outcome.data as string[]).includes("secret")) throw new Rejected("hidden", "hidden");
      }
      if (check.kind === "command") throw new Rejected("ignored for commands");
    }));

  const worker = kernable.worker(ticket);
  const running = [worker.run({ reconnectMs: 100 }), model.run(), audit.run()];
  const after = async (n: number) => {
    for (let i = 0; i < 100 && seen.length < n; i++) await new Promise((r) => setTimeout(r, 20));
    return seen.splice(0).map(([c, o]) => [c.kind, c.type, o] as const);
  };
  try {
    await Promise.all([worker.ready, model.ready, audit.ready]);
    const open = (id: string, senderIdentity = "alice") =>
      kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: id, type: "Open", context: { senderIdentity } });

    const opened = await open("t1");
    assert.ok(opened.committed);
    const [[kind, type, outcome]] = await after(1);
    assert.deepEqual([kind, type], ["command", "Open"]);
    assert.ok(outcome && "committed" in outcome && outcome.committed && outcome.events[0].type === "Opened");

    // Rejected by the aggregate or refused before: it still hears of it, and refuses nothing.
    assert.equal((await open("t1")).committed, false);
    assert.equal((await open("t2", "mallory")).committed, false);
    assert.deepEqual((await after(2)).map(([, , o]) => o), [
      { committed: false, code: "invalid_state", message: "already open" },
      { committed: false, code: "forbidden", message: "banned" },
    ]);

    // A query's response passes it, or not.
    assert.deepEqual(await kernable.query({ domain: "support", type: "ListTickets" }), ["t1"]);
    assert.deepEqual((await after(1))[0][2], { answered: true, data: ["t1"] });
    assert.ok((await open("secret")).committed);
    await after(1);
    await assert.rejects(kernable.query({ domain: "support", type: "ListTickets" }), /hidden/);
    assert.deepEqual((await after(1))[0][2], { answered: true, data: ["secret", "t1"] });

    assert.throws(() => support.readModel("x").middleware({ types: ["*"] }, () => {}).middleware({ types: ["*"] }, () => {}), /already/);
  } finally {
    worker.stop();
    model.stop();
    audit.stop();
    await Promise.all(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a middleware checks file and cache requests before they run", async () => {
  const kernel = new TestKernel(["shop.*"], [], { stores: ["shop.*"] });
  await kernel.start();
  const alice = new Kernable({ url: kernel.url, key: kernel.appKey, context: { senderIdentity: "alice" } });
  const stranger = new Kernable({ url: kernel.url, key: kernel.appKey });

  const checked: Check[] = [];
  const guard = alice.listener(domain("shop").readModel("guard").middleware({ types: ["shop.*"] }, (check) => {
    checked.push(check);
    if (check.request.senderIdentity !== "alice") throw new Rejected("unknown sender", "forbidden");
  }));
  const running = guard.run();
  try {
    await guard.ready;
    const denied = { code: status.PERMISSION_DENIED, details: "forbidden: unknown sender" };
    await assert.rejects(stranger.data.put("shop", "invoices/o-1.pdf", "x"), denied);
    await assert.rejects(stranger.cache.set("shop", "rate/eur", "1.08"), denied);
    await alice.data.put("shop", "invoices/o-1.pdf", "%PDF", { contentType: "application/pdf" });
    await alice.cache.set("shop", "rate/eur", "1.08");
    // A request's own context replaces the client's default.
    await assert.rejects(alice.data.get("shop", "invoices/o-1.pdf", { context: { senderIdentity: "mallory" } }), denied);
    assert.equal(Buffer.from((await stranger.cache.get("shop", "rate/eur", { context: { senderIdentity: "alice" } }))!).toString(), "1.08");

    assert.deepEqual(
      checked.map((c) => [c.kind, c.domain, c.type, c.key, c.tenant, c.data, c.request.senderIdentity]),
      [
        ["data", "shop", "PutData", "invoices/o-1.pdf", "default", undefined, undefined],
        ["cache", "shop", "SetCache", "rate/eur", "default", undefined, undefined],
        ["data", "shop", "PutData", "invoices/o-1.pdf", "default", undefined, "alice"],
        ["cache", "shop", "SetCache", "rate/eur", "default", undefined, "alice"],
        ["data", "shop", "GetData", "invoices/o-1.pdf", "default", undefined, "mallory"],
        ["cache", "shop", "GetCache", "rate/eur", "default", undefined, "alice"],
      ],
    );
    // Only alice's put went through.
    assert.equal(Buffer.from((await alice.data.get("shop", "invoices/o-1.pdf"))!.data).toString(), "%PDF");
  } finally {
    guard.stop();
    await running;
    alice.close();
    stranger.close();
    await kernel.dispose();
  }
});
