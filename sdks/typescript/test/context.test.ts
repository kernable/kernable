// The request context travels from submit to the command handler, onto the events, to the
// listeners, and on to the commands a reactor causes.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain, type RequestContext, type StoredEvent } from "../src/index.ts";
import { derivedId } from "../src/signing.ts";
import { TestKernel } from "./kernel.ts";

test("request context reaches handlers, events and follow-up commands", async () => {
  const kernel = new TestKernel(["support.*"]);
  await kernel.start();
  const service: RequestContext = { senderIdentity: "svc-1", attributes: { channel: "api" } };
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey, context: service });

  const seenByHandler: RequestContext[] = [];
  const seenByApply: RequestContext[] = [];
  const support = domain("support");
  const ticket = support.aggregate("Ticket", { open: false })
    .apply("Opened", (_s, _data, _evt, ctx) => (seenByApply.push(ctx.request), { open: true }))
    .command("Open", (_s, _data, cmd, ctx) => {
      assert.equal(cmd.commandId, "open-t1");
      seenByHandler.push(ctx.request);
      // Deterministic helpers: the same values on every run of this command.
      const at = ctx.now().getTime();
      assert.ok(Math.abs(at - Date.now()) < 5000 && ctx.deadline.getTime() > at);
      return [{ type: "Opened", data: { at, ids: [ctx.id(), ctx.id()] } }];
    })
    .command("Note", (_s, _data, _cmd, ctx) => (seenByHandler.push(ctx.request), [{ type: "Noted" }]));

  const replayed: boolean[] = [];
  const seenByListener: RequestContext[] = [];
  const notes = kernable.listener(support.reactor("notes").on("Opened", async (_data, evt, ctx) => {
    replayed.push(ctx.replaying);
    seenByListener.push(ctx.request);
    await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: evt.aggregateId, type: "Note", id: `note-${evt.aggregateId}`, context: ctx.follow(), wait: "committed" });
  }));
  const worker = kernable.worker(ticket);
  const running = [worker.run({ reconnectMs: 100 }), notes.run()];
  try {
    await Promise.all([worker.ready, notes.ready]);
    const opened = await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: "t1", type: "Open", id: "open-t1", context: { ...service, senderSession: "s-9", traceId: "tr-1" } });
    assert.ok(opened.committed);

    const { ids } = opened.committed ? (opened.events[0].data as { ids: string[] }) : { ids: [] };
    assert.deepEqual(ids, [derivedId("open-t1", 0), derivedId("open-t1", 1)]);

    const request = { ...service, senderSession: "s-9", traceId: "tr-1", correlationId: "open-t1" };
    assert.deepEqual(opened.events[0].request, request);
    // The reactor's follow-up keeps sender and correlation and names its cause.
    const followUp = { ...request, causationId: "open-t1" };
    assert.deepEqual(seenByHandler, [request, followUp]);
    assert.deepEqual(replayed, [false]);
    assert.deepEqual(seenByListener, [request]);
    // Folding the history for the follow-up gives `apply` the request that caused the event.
    assert.deepEqual(seenByApply, [request]);

    const events: StoredEvent[] = [];
    const sub = kernable.subscribe({}, (e) => void events.push(e));
    for (let i = 0; i < 100 && events.length < 2; i++) await new Promise((r) => setTimeout(r, 20));
    sub.stop();
    assert.deepEqual(events.map((e) => e.request), [request, followUp]);

    // An expected version that no longer matches rejects the command.
    const stale = await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: "t1", type: "Note", context: { expectedVersion: 1 } });
    assert.deepEqual(stale.committed ? "committed" : stale.code, "version_mismatch");

    // A request's own context replaces the client's: an anonymous one does not act as the service.
    const anonymous = await kernable.submit({ domain: "support", aggregateType: "Ticket", aggregateId: "t1", type: "Note", context: {} });
    assert.ok(anonymous.committed);
    assert.deepEqual(anonymous.events[0].request, { correlationId: anonymous.events[0].commandId });
    assert.match(kernel.verify(), /\nOK/);
  } finally {
    worker.stop();
    notes.stop();
    await Promise.all(running);
    kernable.close();
    await kernel.dispose();
  }
});
