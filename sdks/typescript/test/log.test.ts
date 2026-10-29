// Connection lines on stderr, an app's overview, and KERNABLE_LOG silencing them.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

/** The lines a worker and a listener print until both are connected, without their time. */
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
  return lines.map((line) => line.replace(/^\d\d:\d\d:\d\d  /, "")).sort();
}

test("a worker and a listener log where they connected and what they handle", async () => {
  const id = "\\([0-9a-f]{12}…\\)";
  const url = "unix:/tmp/kn-[^ ]+";
  const [listener, worker, ...rest] = await connectedLines("info");
  assert.deepEqual(rest, []);
  assert.match(listener, new RegExp(`^listener orders  connected to ${url} · key app ${id} · from 0 · reads shop\\.Placed, billing\\.\\* · answers shop\\.GetOrder$`));
  assert.match(worker, new RegExp(`^worker  connected to ${url} · key app ${id} · runs shop\\.Cancel, shop\\.Place$`));

  assert.deepEqual(await connectedLines("warn"), []);
});

test("an app prints one overview once everything is connected", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const before = process.env.KERNABLE_LOG;
  process.env.KERNABLE_LOG = "info"; // `make check` runs with warn
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  if (before === undefined) delete process.env.KERNABLE_LOG;
  else process.env.KERNABLE_LOG = before;
  const shop = domain("shop");
  shop.aggregate("Order", {}).command("Place", () => []);
  shop.readModel("orders").on("Placed", () => {}).query("GetOrder", () => null);
  const lines: string[] = [];
  const print = console.error;
  console.error = (line: string) => void lines.push(line);
  const app = kernable.app({ service: "shop", version: "1.0.0", description: "Orders", domains: [shop] });
  const running = app.run();
  try {
    await app.ready;
    await new Promise((resolve) => setTimeout(resolve, 20));
  } finally {
    app.stop();
    await Promise.allSettled([running]);
    console.error = print;
    kernable.close();
    await kernel.dispose();
  }
  assert.equal(lines.length, 1, "one overview, no connection lines");
  const overview = lines[0].split("\n");
  assert.equal(overview[0], "kernable · shop 1.0.0 · Orders");
  assert.match(overview[1], /^ {2}kernel {6}unix:\/tmp\/kn-[^ ]+ · key app \([0-9a-f]{12}…\)$/);
  assert.equal(overview[2], "  membership  applied: 1 worker, 1 listener");
  assert.deepEqual(overview.slice(3, 8), [
    "",
    "  worker      Order   runs Place",
    "  listener    orders  from 0 · reads Placed",
    "                      answers GetOrder",
    "",
  ]);
  assert.match(overview[8], /^ {2}ready in \d+ ms$/);
});
