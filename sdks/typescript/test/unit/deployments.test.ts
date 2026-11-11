// Deployments without a kernel: the option, the signed membership, the worker's declared
// schemas, and what a listener does with an event newer than its code.

import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, domain, type ListenerHandle } from "../../src/index.ts";
import type { WireEvent } from "../../src/rpc.ts";
import { Key, eventHash, membershipMessage } from "../../src/signing.ts";

const v = JSON.parse(readFileSync(new URL("../../../../proto/test-vectors/signing.json", import.meta.url), "utf8"));
const hex = (s: string) => Buffer.from(s, "hex");

/** A client with a fresh key, not connected to anything. */
function client(options: { deployment?: string } = {}): { kernable: Kernable; done: () => void } {
  const dir = mkdtempSync(join(tmpdir(), "kn-dep-"));
  const key = join(dir, "app.key");
  writeFileSync(key, "11".repeat(32));
  const kernable = new Kernable({ url: `unix:${join(dir, "none.sock")}`, key, ...options });
  return { kernable, done: () => (kernable.close(), rmSync(dir, { recursive: true, force: true })) };
}

test("a membership with a deployment signs as the test vector", () => {
  const key = Key.fromHex(v.key.seed);
  const m = v.membership_deployment;
  const message = membershipMessage(m.input);
  assert.equal(message.toString("hex"), m.message);
  assert.equal(key.sign(message).sig.toString("hex"), m.signature);
  // Without one, the layout is the one before.
  assert.equal(membershipMessage({ ...m.input, deployment: "" }).toString("hex"), v.membership_processes.message);
});

test("an event's hash includes its deployment when set", () => {
  const e = v.event_hash.input;
  const event = {
    ...e,
    payload: hex(e.payload_hex),
    hash: Buffer.alloc(0),
    producer_sig: { key_id: e.producer_key_id, sig: hex(e.producer_sig_hex) },
    ...v.event_hash_deployment.input,
  };
  assert.equal(eventHash(hex(e.prev_hex), event).toString("hex"), v.event_hash_deployment.hash);
});

test("the deployment option is checked at construction", () => {
  for (const ok of ["v1", "2026-11-11.1", "a_b", "x".repeat(128)]) client({ deployment: ok }).done();
  for (const bad of ["x".repeat(129), "v 1", "v1@x", "ü"]) assert.throws(() => client({ deployment: bad }), /a deployment has 1 to 128 letters/);
  const before = process.env.KERNABLE_DEPLOYMENT;
  process.env.KERNABLE_DEPLOYMENT = "bad/one";
  try {
    assert.throws(() => client(), /a deployment has/);
  } finally {
    if (before === undefined) delete process.env.KERNABLE_DEPLOYMENT;
    else process.env.KERNABLE_DEPLOYMENT = before;
  }
});

test("a worker declares every event type it knows, also those it only applies", () => {
  const shop = domain("shop");
  shop.event("ItemAdded", 2).upcast(1, (d) => d);
  shop.event("Archived", 3).upcast(1, (d) => d).upcast(2, (d) => d);
  const cart = shop.aggregate("Cart", 0)
    .apply("ItemAdded", (n) => n + 1)
    .apply("Cleared", () => 0) // applied only, never produced here
    .command("AddItem", () => [{ type: "ItemAdded", data: {} }]);
  const { kernable, done } = client({ deployment: "v1" });
  try {
    const schemas = (kernable.worker(cart) as unknown as { eventSchemas(): { event_type: string; schema: number }[] }).eventSchemas();
    assert.deepEqual(
      new Map(schemas.map((s) => [s.event_type, s.schema])),
      new Map([["shop.ItemAdded", 2], ["shop.Cleared", 1], ["shop.Archived", 3]]),
    );
  } finally {
    done();
  }
});

function wireEvent(position: number, type: string, schema: number, erases = ""): WireEvent {
  return {
    global_position: position,
    tenant_id: "default",
    domain: "shop",
    aggregate_type: "Cart",
    aggregate_id: "c1",
    version: position,
    event_type: type,
    payload: Buffer.from(JSON.stringify({ qty: 1 })),
    command_id: `cmd-${position}`,
    created_at: 0,
    hash: Buffer.alloc(0),
    schema,
    vault_refs: [],
    erases,
    deployment: schema > 1 ? "v2" : "v1",
  };
}

type Batch = { parked: { position: number; code: string; message: string }[]; skipped: { position: number; event_type: string; schema: number }[] };

async function handle(h: ListenerHandle, events: WireEvent[]): Promise<Batch> {
  return (h as unknown as { handleBatch(b: unknown): Promise<Batch> }).handleBatch({ events, up_to: events.at(-1)!.global_position, vault: [] });
}

test("a listener skips or parks an event newer than its code, and handles the rest", async () => {
  const shop = domain("shop"); // knows ItemAdded only at schema 1
  const { kernable, done } = client({ deployment: "v1" });
  try {
    const seen: [string, number, string][] = [];
    const record = (_data: unknown, evt: { type: string; position: number; deployment: string }) => void seen.push([evt.type, evt.position, evt.deployment]);
    const events = [wireEvent(1, "ItemAdded", 1), wireEvent(2, "ItemAdded", 2), wireEvent(3, "Other", 5), wireEvent(4, "ItemAdded", 1)];

    // A read model skips it.
    const carts = kernable.listener(shop.readModel("carts").on("ItemAdded", record).on("Other", record, { raw: true }));
    assert.deepEqual(await handle(carts, events), { parked: [], skipped: [{ position: 2, event_type: "shop.ItemAdded", schema: 2 }] });
    assert.equal(carts.skipped, 0, "counted once acknowledged");
    // A raw handler gets every schema as stored.
    assert.deepEqual(seen, [["ItemAdded", 1, "v1"], ["Other", 3, "v2"], ["ItemAdded", 4, "v1"]]);

    // A reactor that parks parks it at once.
    seen.length = 0;
    const mailer = kernable.listener(shop.reactor("mailer").on("ItemAdded", record));
    const r = await handle(mailer, events);
    assert.deepEqual(r.skipped, []);
    assert.deepEqual(r.parked.map((p) => [p.position, p.code]), [[2, "unknown_event"]]);
    assert.match(r.parked[0].message, /shop\.ItemAdded schema 2/);
    assert.deepEqual(seen.map(([, p]) => p), [1, 4]);

    // A reactor that cannot park (live, or a replicated instance) skips it.
    for (const [name, options] of [["live", { live: true }], ["replicated", { instance: "node-a", single: false }]] as const) {
      const h = kernable.listener(shop.reactor(name, options).on("ItemAdded", () => {}));
      assert.deepEqual((await handle(h, events)).skipped.map((s) => s.position), [2]);
    }
  } finally {
    done();
  }
});

test("an event newer than the code is parked, erasing or not; listeners that cannot park skip it", async () => {
  // The same fixture in the three SDKs.
  const shop = domain("shop"); // reads every type at schema 1
  const { kernable, done } = client({ deployment: "v1" });
  try {
    const events = [
      wireEvent(1, "ItemAdded", 1),
      wireEvent(2, "ItemAdded", 2),
      wireEvent(3, "CustomerForgotten", 2, "customer-7"),
      wireEvent(4, "ItemAdded", 1),
    ];
    const seen: number[] = [];
    const record = (_data: unknown, evt: { position: number }) => void seen.push(evt.position);
    const mails = kernable.listener(shop.reactor("mails", { parkAfter: 3 }).on("ItemAdded", record).on("CustomerForgotten", record));
    const r = await handle(mails, events);
    assert.deepEqual(r.skipped, []);
    assert.deepEqual(r.parked.map((p) => [p.position, p.code]), [[2, "unknown_event"], [3, "unknown_event"]]);
    assert.match(r.parked[1].message, /shop\.CustomerForgotten schema 2/);
    assert.deepEqual(seen, [1, 4]);

    for (const listener of [
      shop.reactor("live", { live: true }),
      shop.reactor("replicated", { parkAfter: 3, instance: "node-a", single: false }),
      shop.readModel("carts"),
    ]) {
      const h = kernable.listener(listener.on("ItemAdded", () => {}).on("CustomerForgotten", () => {}));
      const b = await handle(h, events);
      assert.deepEqual(b.parked, []);
      assert.deepEqual(b.skipped.map((s) => s.position), [2, 3]);
    }
  } finally {
    done();
  }
});
