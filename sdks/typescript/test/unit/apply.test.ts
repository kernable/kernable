// apply: a handler changes a copy of the state in place, or returns a state that replaces it.

import assert from "node:assert/strict";
import { test } from "node:test";

import { CommandContext, Rejected, domain } from "../../src/index.ts";
import { derivedId } from "../../src/signing.ts";

type Account = { email?: string; tags: string[]; status: string };

const account = domain("crm").aggregate<Account>("Account", { tags: [], status: "new" })
  .apply("Opened", (s, data: { email: string }) => { s.email = data.email; s.tags.push("open"); })
  .apply("Erased", (s) => { delete s.email; })
  .apply("Reset", () => ({ tags: [], status: "reset" }));

const event = (type: string, data: unknown = {}) => ({ type, schema: 1, data });

test("apply changes a copy in place, deletes, or replaces", () => {
  const opened = account.fold([event("Opened", { email: "a@example.com" })]);
  assert.deepEqual(opened, { email: "a@example.com", tags: ["open"], status: "new" });
  assert.deepEqual(account.initial, { tags: [], status: "new" }, "the initial state is untouched");

  const erased = account.fold([event("Erased")], opened);
  assert.deepEqual(erased, { tags: ["open"], status: "new" });
  assert.equal(opened.email, "a@example.com", "the state before is untouched");

  assert.deepEqual(account.fold([event("Opened", { email: "b@example.com" }), event("Reset")]), { tags: [], status: "reset" });
});

test("fold passes what is known of the event and its request context", () => {
  const seen = domain("crm").aggregate("Account", [] as unknown[])
    .apply("Opened", (s, _data, evt, ctx) => [...s, evt.aggregateType, evt.aggregateId, evt.version, ctx.request]);
  assert.deepEqual(seen.fold([{ ...event("Opened"), aggregateId: "a1", request: { senderIdentity: "alice" } }, event("Opened")]), [
    "Account", "a1", 0, { senderIdentity: "alice" },
    "Account", "", 0, {},
  ]);
});

test("a command handler can be called directly, e.g. in a unit test", async () => {
  const crm = domain("crm");
  const account = crm.aggregate<Account>("Account", { tags: [], status: "new" })
    .command("Close", (s, _data, cmd, ctx) => {
      if (!s.email) throw new Rejected("no account", "not_found");
      return [{ type: "Closed", data: { id: ctx.id(), by: cmd.commandId, what: `${cmd.domain}.${cmd.aggregateType}.${cmd.type}`, at: ctx.now() } }];
    });
  const ctx = new CommandContext("close-1", {}, new Date(0));
  await assert.rejects(account.handle({ tags: [], status: "new" }, "Close", undefined, ctx), (err) => err instanceof Rejected && err.code === "not_found");
  const opened = { email: "a@example.com", tags: [], status: "new" };
  const [closed] = await account.handle(opened, "Close", {}, ctx);
  assert.deepEqual(closed, { type: "Closed", data: { id: derivedId("close-1", 0), by: "close-1", what: "crm.Account.Close", at: new Date(0) } });
  // As in a real command: IDs go on in order, the time stays, the deadline is 30 s later.
  assert.deepEqual([ctx.id(), ctx.now(), ctx.deadline], [derivedId("close-1", 1), new Date(0), new Date(30_000)]);
  await assert.rejects(account.handle(opened, "Open", {}, ctx), /no handler for Open on Account/);
});
