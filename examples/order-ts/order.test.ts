// End to end against the real kernel binary (`cargo build` first): TS signs, Rust verifies.

import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Kernable, Key, type Worker } from "../../sdks/typescript/src/index.ts";
import { order } from "./order.ts";

const BIN = new URL("../../target/debug/", import.meta.url).pathname;
const dir = mkdtempSync("/tmp/kn-"); // short: Unix socket paths are limited to ~104 bytes
const url = `unix:${join(dir, "k.sock")}`;

function newKey(name: string): Key {
  const seed = randomBytes(32).toString("hex");
  writeFileSync(join(dir, `${name}.key`), seed);
  const key = Key.fromHex(seed);
  writeFileSync(join(dir, `${name}.pub`), key.id);
  return key;
}

const kernelKey = newKey("kernel");
const app = newKey("app"); // submits commands and runs the worker
writeFileSync(
  join(dir, "kernable.toml"),
  `db = "k.db"
listen = "${url}"
kernel_key = "kernel.key"
checkpoint_interval_ms = 50

[clients]
"${app.id}" = ["*"]

[workers]
"${app.id}" = ["PlaceOrder", "AddItem", "CancelOrder", "ShipOrder"]
`,
);

const kernel = spawn(join(BIN, "kernable-kernel"), [join(dir, "kernable.toml")], { stdio: "inherit" });
const kernable = new Kernable({ url, key: join(dir, "app.key"), tenant: "t1" });
let worker: Worker;

before(async () => {
  for (let i = 0; i < 100 && !existsSync(join(dir, "k.sock")); i++) await new Promise((r) => setTimeout(r, 20));
  worker = kernable.worker(order);
  void worker.run();
  await worker.ready;
});

after(() => {
  worker.stop();
  kernable.close();
  kernel.kill();
  rmSync(dir, { recursive: true, force: true });
});

const submit = (aggregateId: string, type: string, data?: unknown) =>
  kernable.submit({ aggregateType: "Order", aggregateId, type, data });

test("order lifecycle", async () => {
  const placed = await submit("o1", "PlaceOrder", { customerId: "k-1" });
  assert.ok(placed.committed);
  assert.deepEqual(placed.events.map((e) => [e.version, e.type, e.data]), [[1, "OrderPlaced", { customerId: "k-1" }]]);

  assert.ok((await submit("o1", "AddItem", { sku: "apple", qty: 2 })).committed);
  assert.ok((await submit("o1", "AddItem", { sku: "apple", qty: 1 })).committed);
  const shipped = await submit("o1", "ShipOrder");
  assert.ok(shipped.committed);
  assert.equal(shipped.events[0].version, 4);

  const late = await submit("o1", "CancelOrder");
  assert.deepEqual(late, { committed: false, code: "rejected", message: "Cannot cancel a shipped order" });
});

test("business rules reject invalid commands", async () => {
  const empty = await submit("o2", "ShipOrder");
  assert.equal(empty.committed, false);
  await submit("o2", "PlaceOrder", { customerId: "k-2" });
  const bad = await submit("o2", "AddItem", { sku: "pear", qty: 0 });
  assert.deepEqual(bad, { committed: false, code: "rejected", message: "Quantity must be positive" });
});

test("resubmitting a command returns the original outcome", async () => {
  const first = await kernable.submit({ aggregateType: "Order", aggregateId: "o3", type: "PlaceOrder", id: "fixed-id", data: { customerId: "k-3" } });
  const again = await kernable.submit({ aggregateType: "Order", aggregateId: "o3", type: "PlaceOrder", id: "fixed-id", data: { customerId: "k-3" } });
  assert.deepEqual(again, first);
});

test("the Rust verifier accepts the log written via the TS SDK", async () => {
  await new Promise((r) => setTimeout(r, 120)); // let the kernel write a checkpoint
  const out = execFileSync(join(BIN, "kernable"), ["verify", join(dir, "k.db"), join(dir, "kernel.pub")], { encoding: "utf8" });
  assert.match(out, /OK$/m);
  assert.ok(kernelKey.id);
});
