// Connection lines on stderr, and KERNABLE_LOG silencing them.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

/** The `kernable ...` lines a worker and a listener print until both are connected. */
async function connectedLines(level: string): Promise<string[]> {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const before = process.env.KERNABLE_LOG;
  process.env.KERNABLE_LOG = level;
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  if (before === undefined) delete process.env.KERNABLE_LOG;
  else process.env.KERNABLE_LOG = before;

  const shop = domain("shop");
  const order = shop.aggregate("Order", {}).command("Place", () => []).command("Cancel", () => []);
  const orders = shop.readModel("orders").on("Placed", () => {}).on("billing.*", () => {}).query("GetOrder", () => null);
  const lines: string[] = [];
  const print = console.error;
  console.error = (line: string) => void lines.push(line);
  const worker = kernable.worker(order);
  const listener = kernable.listener(orders);
  const running = [worker.run(), listener.run()];
  try {
    await Promise.all([worker.ready, listener.ready]);
  } finally {
    for (const s of [worker, listener]) s.stop();
    await Promise.allSettled(running);
    console.error = print;
    kernable.close();
    await kernel.dispose();
  }
  return lines.sort();
}

test("a worker and a listener log where they connected and what they handle", async () => {
  const id = "\\([0-9a-f]{12}…\\)";
  const url = "unix:/tmp/kn-[^ ]+";
  const [listener, worker, ...rest] = await connectedLines("info");
  assert.deepEqual(rest, []);
  assert.match(listener, new RegExp(`^kernable listener orders: connected to ${url} as app ${id}, at position 0, reads billing\\.\\*, shop\\.Placed, answers shop\\.GetOrder$`));
  assert.match(worker, new RegExp(`^kernable worker: connected to ${url} as app ${id}, runs shop\\.Cancel, shop\\.Place$`));

  assert.deepEqual(await connectedLines("warn"), []);
});
