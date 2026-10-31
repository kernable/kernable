// Unit tests of the tutorial's Order aggregate, without a kernel: fold events into state, then
// call a command handler on that state.
import assert from "node:assert/strict";
import { test } from "node:test";

import { CommandContext, Rejected } from "@kernable/sdk";

import { orderAggregate } from "../shop/ts/domain.ts";

// #region test
/** Runs `type` on the state after `history`, as a worker would. */
function decide(history: { type: string; data?: unknown }[], type: string, data: unknown = {}) {
  const state = orderAggregate.fold(history.map((e) => ({ schema: 1, data: {}, ...e })));
  return orderAggregate.handle(state, type, data, new CommandContext("test", {}, new Date("2026-01-01T00:00:00Z")));
}

test("a draft order with items can be submitted", async () => {
  const placed = { type: "OrderPlaced", data: { customerId: "c1" } };
  const added = { type: "ItemAdded", data: { sku: "apple", qty: 2 } };
  assert.deepEqual(await decide([placed, added], "Submit"), [{ type: "OrderSubmitted" }]);
});

test("an empty order cannot be submitted", async () => {
  const placed = { type: "OrderPlaced", data: { customerId: "c1" } };
  await assert.rejects(async () => decide([placed], "Submit"), (err: Rejected) => err.code === "invalid_state");
});
// #endregion test
