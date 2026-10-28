// Middleware: a listener checks commands and queries before the kernel accepts or routes
// them, from its own state, and the kernel refuses them while it is away.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, Rejected, domain, type Check } from "../src/index.ts";
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
    .command("Ban", (_s, data: { identity: string }) => [{ type: "Banned", data }]);
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
