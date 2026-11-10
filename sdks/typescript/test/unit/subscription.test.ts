// A subscription whose stream cannot be opened reconnects, and one that fails beyond that ends
// with an error line: its `done` never rejects.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Subscription } from "../../src/index.ts";

const key = { id: "k", sign: () => ({ key_id: "k", sig: Buffer.alloc(0) }) };

test("a stream that cannot be opened is retried until stop(), and done resolves", async () => {
  let opened = 0;
  const clients = {
    subscribe: () => {
      opened++;
      throw new Error("channel closed");
    },
  };
  const reasons: string[] = [];
  const log = {
    reconnect: async (_what: string, err: Error) => {
      reasons.push(err.message);
      await new Promise((resolve) => setTimeout(resolve, 5));
    },
    print: () => assert.fail("no error line"),
  };
  const sub = new Subscription(clients as never, key as never, { reconnectMs: 1 }, () => {}, log as never);
  for (let i = 0; i < 100 && opened < 3; i++) await new Promise((resolve) => setTimeout(resolve, 5));
  sub.stop();
  await sub.done;
  assert.ok(opened >= 3, "tried again");
  assert.equal(reasons[0], "channel closed");
});

test("a failure outside the stream ends the subscription with an error line, not a rejection", async () => {
  const clients = {
    subscribe: () => {
      throw new Error("channel closed");
    },
  };
  const lines: string[] = [];
  const log = {
    reconnect: async () => {
      throw new Error("log failed");
    },
    print: (level: string, what: string, line: string) => lines.push(`${level} ${what} ${line}`),
  };
  const sub = new Subscription(clients as never, key as never, {}, () => {}, log as never);
  await sub.done;
  assert.deepEqual(lines, ["error subscription ended: log failed"]);
});
