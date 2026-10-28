// apply: a handler changes a copy of the state in place, or returns a state that replaces it.

import assert from "node:assert/strict";
import { test } from "node:test";

import { domain } from "../src/index.ts";

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
