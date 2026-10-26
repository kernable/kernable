// Acceptance tests of the reference application (MVP_PLAN.md §3): identity in Python, orders in
// TypeScript, one kernel, all over TCP with mutual TLS. Needs `cargo build` and the Python SDK
// venv (`make py`).

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { test } from "node:test";

import { Kernable, type StoredEvent } from "../../sdks/typescript/src/index.ts";
import { TestKernel, freePort } from "../../sdks/typescript/test/kernel.ts";
import { ORDER_EVENTS, project, type OrderRow } from "./orders/order.ts";

const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
const COMMANDS = ["RegisterAccount", "ChangeEmail", "DeactivateAccount", "PlaceOrder", "AddItem", "SubmitOrder", "RecordConfirmation", "CancelOrder"];

/** One service process that can be killed and started again. */
class Service {
  private process?: ChildProcess;
  readonly url: string;
  private readonly command: string;
  private readonly args: string[];

  private readonly env: Record<string, string>;

  constructor(port: number, command: string, args: string[], env: Record<string, string>) {
    this.url = `http://127.0.0.1:${port}`;
    this.command = command;
    this.args = args;
    this.env = env;
  }

  async start(): Promise<void> {
    this.process = spawn(this.command, this.args, {
      stdio: ["ignore", "ignore", "inherit"],
      env: { ...process.env, ...this.env, PYTHONPATH: `${ROOT}sdks/python` },
    });
    for (let i = 0; i < 250; i++) {
      const ok = await fetch(`${this.url}/health`).then((r) => r.ok, () => false);
      if (ok) return;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`${this.args[0]} did not become healthy`);
  }

  async kill(): Promise<void> {
    const p = this.process;
    if (!p || p.exitCode !== null || p.signalCode !== null) return;
    p.kill("SIGKILL");
    await once(p, "exit");
  }
}

interface Shop {
  kernel: TestKernel;
  identity: Service;
  orders: Service;
  call(method: string, path: string, body?: unknown): Promise<[number, any]>;
  dispose(): Promise<void>;
}

async function shop(): Promise<Shop> {
  const kernel = new TestKernel(COMMANDS, "", { tls: true, metrics: true });
  await kernel.start();
  const tls = kernel.tlsFiles!;
  const env = { KERNABLE_TLS_CA: tls.ca, KERNABLE_TLS_CERT: tls.cert, KERNABLE_TLS_KEY: tls.key };
  const identityPort = await freePort();
  const identityArgs = [`${ROOT}examples/shop/identity/identity.py`, kernel.url, kernel.appKey, String(identityPort)];
  const identity = new Service(identityPort, PYTHON, identityArgs, env);
  await identity.start();
  const port = await freePort();
  const ordersArgs = [`${ROOT}examples/shop/orders/service.ts`, kernel.url, kernel.appKey, String(port), identity.url];
  const orders = new Service(port, process.execPath, ordersArgs, env);
  await orders.start();
  return {
    kernel,
    identity,
    orders,
    async call(method, path, body) {
      const res = await fetch(`${orders.url}${path}`, { method, body: body === undefined ? undefined : JSON.stringify(body) });
      return [res.status, await res.json()];
    },
    async dispose() {
      await orders.kill();
      await identity.kill();
      await kernel.dispose();
    },
  };
}

async function until<T>(get: () => Promise<T>, ok: (v: T) => boolean, what: string): Promise<T> {
  let v = await get();
  for (let i = 0; i < 250 && !ok(v); i++) {
    await new Promise((r) => setTimeout(r, 20));
    v = await get();
  }
  assert.ok(ok(v), `timed out waiting for ${what}: ${JSON.stringify(v)}`);
  return v;
}

test("every response shows its own write, across both languages", async () => {
  const s = await shop();
  try {
    // Written by the API, executed by the Python worker, read from the Python read model.
    assert.deepEqual(await s.call("POST", "/accounts", { id: "a1", email: "ann@example.com", name: "Ann" }), [
      200,
      { id: "a1", status: "active", email: "ann@example.com", name: "Ann" },
    ]);
    assert.equal((await s.call("POST", "/accounts", { id: "a1", email: "ann@example.com" }))[0], 409);
    assert.equal((await s.call("POST", "/accounts", { id: "a2", email: "nope" }))[0], 422);

    assert.deepEqual(await s.call("POST", "/orders", { id: "o1", customerId: "a1" }), [
      200,
      { id: "o1", customerId: "a1", status: "draft", items: {} },
    ]);
    assert.equal((await s.call("POST", "/orders", { id: "o2", customerId: "nobody" }))[0], 422);
    assert.deepEqual((await s.call("POST", "/orders/o1/items", { sku: "apple", qty: 2 }))[1].items, { apple: 2 });
    assert.equal((await s.call("POST", "/orders/o1/submit"))[0], 200);

    // The reactor confirms the order by email, read from the other domain's read model.
    const confirmed = await until(() => s.call("GET", "/orders/o1"), ([, o]) => o.status === "confirmed", "confirmation");
    assert.equal(confirmed[1].confirmedTo, "ann@example.com");
    assert.deepEqual((await s.call("POST", "/accounts/a1/email", { email: "ann@example.org" }))[1].email, "ann@example.org");
    assert.deepEqual((await s.call("GET", "/stats"))[1], { confirmations: 1, unknownAccounts: 0 });
  } finally {
    await s.dispose();
  }
});

test("kill -9 of each process loses and duplicates nothing", async () => {
  const s = await shop();
  try {
    const victims: [string, () => Promise<void>][] = [
      ["identity", async () => (await s.identity.kill(), await s.identity.start())],
      ["orders", async () => (await s.orders.kill(), await s.orders.start())],
      ["kernel", async () => (await s.kernel.kill("SIGKILL"), await s.kernel.start())],
    ];
    let n = 0;
    const order = async () => {
      n++;
      assert.equal((await s.call("POST", "/accounts", { id: `a${n}`, email: `c${n}@example.com` }))[0], 200);
      assert.equal((await s.call("POST", "/orders", { id: `o${n}`, customerId: `a${n}` }))[0], 200);
      assert.equal((await s.call("POST", `/orders/o${n}/items`, { sku: "x", qty: n }))[0], 200);
      assert.equal((await s.call("POST", `/orders/o${n}/submit`))[0], 200);
    };
    for (const [, restart] of victims) {
      await order();
      assert.equal((await s.call("GET", "/stats"))[1].unknownAccounts, 0);
      await restart();
      await order();
    }

    // Every order is confirmed exactly once, and the read model matches a replay of the log.
    for (let i = 1; i <= n; i++) {
      await until(() => s.call("GET", `/orders/o${i}`), ([, o]) => o.status === "confirmed", `confirmation of o${i}`);
    }
    const kernable = new Kernable({ url: s.kernel.url, key: s.kernel.appKey, tls: s.kernel.tlsFiles });
    const events: StoredEvent[] = [];
    const replay = kernable.subscribe({ types: ORDER_EVENTS }, (e) => void events.push(e));
    await until(async () => events.filter((e) => e.type === "ConfirmationSent").length, (c) => c >= n, "replay");
    await new Promise((r) => setTimeout(r, 200));
    replay.stop();
    kernable.close();
    const confirmations = events.filter((e) => e.type === "ConfirmationSent").map((e) => e.aggregateId);
    assert.deepEqual(confirmations.sort(), Array.from({ length: n }, (_, i) => `o${i + 1}`).sort());

    const rows = new Map<string, OrderRow>();
    for (const e of events) project(rows, e);
    for (const [id, row] of rows) assert.deepEqual((await s.call("GET", `/orders/${id}`))[1], row);
    assert.match(s.kernel.verify(), /OK/);

    // Every listener has caught up, and the metrics show it.
    const metrics = await (await fetch(s.kernel.metricsUrl!)).text();
    for (const listener of ["accounts", "orders", "confirmations"]) {
      assert.match(metrics, new RegExp(`^kernable_listener_lag\\{listener="${listener}"\\} 0$`, "m"));
      assert.match(metrics, new RegExp(`^kernable_listener_connected\\{listener="${listener}"\\} 1$`, "m"));
    }
  } finally {
    await s.dispose();
  }
});
