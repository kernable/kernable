// Acceptance tests of the reference application: identity in Python, orders in
// TypeScript, one kernel, all over TCP with mutual TLS. Needs `cargo build` and the Python SDK
// venv (`make py`).

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, type StoredEvent } from "../../sdks/typescript/src/index.ts";
import { TestKernel, freePort } from "../../sdks/typescript/test/kernel.ts";
import { ORDER_EVENTS, project, type OrderRow } from "./orders/order.ts";

const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
const COMMANDS = ["identity.*", "orders.*"];
const PASSWORD = "correct horse";

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
  /** Calls the API as the account `as` (the `x-identity` header); without it, anonymously. */
  call(method: string, path: string, body?: unknown, as?: string): Promise<[number, any]>;
  dispose(): Promise<void>;
}

async function shop(): Promise<Shop> {
  const kernel = new TestKernel(COMMANDS, "", { tls: true, metrics: true });
  await kernel.start();
  const tls = kernel.tlsFiles!;
  const env = { KERNABLE_TLS_CA: tls.ca, KERNABLE_TLS_CERT: tls.cert, KERNABLE_TLS_KEY: tls.key };
  const identityPort = await freePort();
  const identityArgs = [`${ROOT}examples/shop/identity/identity.py`, kernel.url, kernel.appKey, String(identityPort), `${kernel.dir}/identity`];
  const identity = new Service(identityPort, PYTHON, identityArgs, env);
  await identity.start();
  const port = await freePort();
  const ordersArgs = [`${ROOT}examples/shop/orders/service.ts`, kernel.url, kernel.appKey, String(port)];
  const orders = new Service(port, process.execPath, ordersArgs, env);
  await orders.start();
  return {
    kernel,
    identity,
    orders,
    async call(method, path, body, as) {
      const headers = as ? { "x-identity": as } : undefined;
      const res = await fetch(`${orders.url}${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
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

const LISTENERS = ["accounts", "rbac", "orders", "confirmations"];

/**
 * The events of the orders read model, replayed from the log by a fresh listener (which, unlike a
 * subscription, gets the vault values it may read), until `done` holds.
 */
async function replay(s: Shop, done: (events: StoredEvent[]) => boolean): Promise<StoredEvent[]> {
  const kernable = new Kernable({ url: s.kernel.url, key: s.kernel.appKey, tls: s.kernel.tlsFiles });
  const events: StoredEvent[] = [];
  const model = kernable.readModel(`replay-${Date.now()}`);
  for (const type of [...ORDER_EVENTS, "identity.AccountRegistered"]) model.on(type, (e) => void events.push(e));
  const running = model.run();
  try {
    await until(async () => events, done, "replay");
    await new Promise((r) => setTimeout(r, 200));
    return events;
  } finally {
    model.stop();
    await running;
    kernable.close();
  }
}

/** Waits until every listener is connected again: until then, the rbac middleware refuses. */
async function reconnected(s: Shop): Promise<void> {
  const metrics = () => fetch(s.kernel.metricsUrl!).then((r) => r.text());
  const all = (m: string) => LISTENERS.every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
  await until(metrics, all, "listeners to reconnect");
}

test("every response shows its own write, across both languages", async () => {
  const s = await shop();
  try {
    // Written by the API, executed by the Python worker, read from the Python read model.
    assert.deepEqual(await s.call("POST", "/accounts", { id: "a1", email: "ann@example.com", name: "Ann", password: PASSWORD }, "a1"), [
      200,
      { id: "a1", status: "active", email: "ann@example.com", name: "Ann" },
    ]);
    assert.equal((await s.call("POST", "/accounts", { id: "a1", email: "ann@example.com", password: PASSWORD }, "a1"))[0], 409);
    assert.equal((await s.call("POST", "/accounts", { id: "a2", email: "nope", password: PASSWORD }, "a2"))[0], 422);

    assert.deepEqual(await s.call("POST", "/orders", { id: "o1", customerId: "a1" }, "a1"), [
      200,
      { id: "o1", customerId: "a1", status: "draft", items: {} },
    ]);
    assert.equal((await s.call("POST", "/orders", { id: "o2", customerId: "nobody" }, "nobody"))[0], 403);
    assert.deepEqual((await s.call("POST", "/orders/o1/items", { sku: "apple", qty: 2 }, "a1"))[1].items, { apple: 2 });
    assert.equal((await s.call("POST", "/orders/o1/submit", undefined, "a1"))[0], 200);

    // The reactor confirms the order by email, read from the other domain's read model.
    const confirmed = await until(() => s.call("GET", "/orders/o1", undefined, "a1"), ([, o]) => o.status === "confirmed", "confirmation");
    assert.equal(confirmed[1].confirmedTo, "ann@example.com");
    assert.deepEqual((await s.call("POST", "/accounts/a1/email", { email: "ann@example.org" }, "a1"))[1].email, "ann@example.org");
    assert.deepEqual((await s.call("GET", "/stats"))[1], { confirmations: 1, unknownAccounts: 0 });
  } finally {
    await s.dispose();
  }
});

test("the rbac middleware (Python) guards both domains' commands and queries", async () => {
  const s = await shop();
  try {
    assert.equal((await s.call("POST", "/accounts", { id: "a1", email: "ann@example.com", password: PASSWORD }, "a1"))[0], 200);
    assert.equal((await s.call("POST", "/accounts", { id: "b1", email: "bob@example.com", password: PASSWORD }, "b1"))[0], 200);
    assert.equal((await s.call("POST", "/orders", { id: "o1", customerId: "a1" }, "a1"))[0], 200);

    // Someone else's order and account: neither readable nor changeable.
    assert.equal((await s.call("GET", "/orders/o1", undefined, "b1"))[0], 403);
    assert.equal((await s.call("POST", "/orders/o1/items", { sku: "x", qty: 1 }, "b1"))[0], 403);
    assert.equal((await s.call("POST", "/orders", { id: "o2", customerId: "a1" }, "b1"))[0], 403);
    assert.equal((await s.call("POST", "/accounts/a1/email", { email: "evil@example.com" }, "b1"))[0], 403);
    // Anonymous: nothing but registering.
    assert.equal((await s.call("GET", "/orders/o1"))[0], 403);
    // The owner still can, and the refused commands left no trace.
    assert.deepEqual((await s.call("GET", "/orders/o1", undefined, "a1"))[1].items, {});
    assert.equal((await s.call("GET", "/accounts/a1", undefined, "b1"))[0], 403);
    assert.equal((await s.call("GET", "/accounts/a1", undefined, "a1"))[1].email, "ann@example.com");

    const metrics = await (await fetch(s.kernel.metricsUrl!)).text();
    const count = (outcome: string) => Number(new RegExp(`^kernable_middleware_total\\{outcome="${outcome}"\\} (\\d+)$`, "m").exec(metrics)?.[1]);
    assert.equal(count("rejected"), 6);
    assert.ok(count("passed") > 0);
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
      ["kernel", async () => (await s.kernel.kill("SIGKILL"), await s.kernel.start(), await reconnected(s))],
    ];
    let n = 0;
    const order = async () => {
      n++;
      const as = `a${n}`;
      assert.equal((await s.call("POST", "/accounts", { id: as, email: `c${n}@example.com`, password: PASSWORD }, as))[0], 200);
      assert.equal((await s.call("POST", "/orders", { id: `o${n}`, customerId: as }, as))[0], 200);
      assert.equal((await s.call("POST", `/orders/o${n}/items`, { sku: "x", qty: n }, as))[0], 200);
      assert.equal((await s.call("POST", `/orders/o${n}/submit`, undefined, as))[0], 200);
    };
    for (const [name, restart] of victims) {
      await order();
      assert.equal((await s.call("GET", "/stats"))[1].unknownAccounts, 0);
      if (name === "identity") {
        // The rbac middleware is away: orders are refused too, not let through.
        await s.identity.kill();
        assert.equal((await s.call("POST", `/orders/o${n}/items`, { sku: "y", qty: 1 }, `a${n}`))[0], 503);
      }
      await restart();
      await order();
    }

    // Every order is confirmed exactly once, and the read model matches a replay of the log.
    for (let i = 1; i <= n; i++) {
      await until(() => s.call("GET", `/orders/o${i}`, undefined, `a${i}`), ([, o]) => o.status === "confirmed", `confirmation of o${i}`);
    }
    const events = await replay(s, (events) => events.filter((e) => e.type === "ConfirmationSent").length >= n);
    const confirmations = events.filter((e) => e.type === "ConfirmationSent").map((e) => e.aggregateId);
    assert.deepEqual(confirmations.sort(), Array.from({ length: n }, (_, i) => `o${i + 1}`).sort());
    // Each confirmation names the command that submitted the order as its cause.
    for (const c of events.filter((e) => e.type === "ConfirmationSent")) {
      const submitted = events.find((e) => e.type === "OrderSubmitted" && e.aggregateId === c.aggregateId)!;
      assert.equal(c.request.causationId, submitted.commandId);
      assert.equal(c.request.correlationId, submitted.request.correlationId);
    }

    const rows = new Map<string, OrderRow>();
    for (const e of events) project(rows, e);
    for (const [id, row] of rows) assert.deepEqual((await s.call("GET", `/orders/${id}`, undefined, row.customerId))[1], row);
    assert.match(s.kernel.verify(), /OK/);

    // Every listener has caught up, and the metrics show it. All reads went through the kernel.
    const metrics = await (await fetch(s.kernel.metricsUrl!)).text();
    const answered = Number(/^kernable_queries_total\{outcome="answered"\} (\d+)$/m.exec(metrics)?.[1]);
    assert.ok(answered > 0, "no queries answered through the kernel"); // counted since the kernel's restart
    for (const listener of LISTENERS) {
      assert.match(metrics, new RegExp(`^kernable_listener_lag\\{listener="${listener}"\\} 0$`, "m"));
      assert.match(metrics, new RegExp(`^kernable_listener_connected\\{listener="${listener}"\\} 1$`, "m"));
    }
  } finally {
    await s.dispose();
  }
});

test("personal data lives in the vault, and erasing an account erases it in both domains", async () => {
  const s = await shop();
  try {
    const account = { id: "a1", email: "ann@example.com", name: "Ann", password: PASSWORD };
    assert.equal((await s.call("POST", "/accounts", account, "a1"))[0], 200);
    // The password is a secret: the handler checks it, nothing keeps it.
    assert.equal((await s.call("POST", "/accounts/a1/sign-in", { password: "wrong password" }, "a1"))[0], 401);
    assert.deepEqual(await s.call("POST", "/accounts/a1/sign-in", { password: PASSWORD }, "a1"), [200, { signedIn: true }]);
    assert.equal((await s.call("POST", "/orders", { id: "o1", customerId: "a1" }, "a1"))[0], 200);
    assert.equal((await s.call("POST", "/orders/o1/items", { sku: "apple", qty: 1 }, "a1"))[0], 200);
    assert.equal((await s.call("POST", "/orders/o1/submit", undefined, "a1"))[0], 200);
    const confirmed = await until(() => s.call("GET", "/orders/o1", undefined, "a1"), ([, o]) => o.status === "confirmed", "confirmation");
    assert.equal(confirmed[1].confirmedTo, "ann@example.com");

    // Neither the email nor the password is in any of the kernel's files in clear.
    const files = readdirSync(s.kernel.dir, { withFileTypes: true }).filter((f) => f.isFile());
    for (const f of files) {
      const content = readFileSync(join(s.kernel.dir, f.name));
      assert.ok(!content.includes("ann@example.com") && !content.includes(PASSWORD), `${f.name} holds personal data`);
    }

    // Erasing the account: it can no longer act, and the log reads null where its data was, in
    // the identity domain and in the orders domain alike.
    assert.deepEqual(await s.call("DELETE", "/accounts/a1", undefined, "a1"), [200, { erased: true }]);
    assert.equal((await s.call("GET", "/accounts/a1", undefined, "a1"))[0], 403);
    const events = await replay(s, (events) => events.some((e) => e.type === "AccountErased"));
    const registered = events.find((e) => e.type === "AccountRegistered")!;
    assert.deepEqual([(registered.data as any).email, registered.redacted.sort()], [null, ["email", "name", "passwordHash"]]);
    const rows = new Map<string, OrderRow>();
    for (const e of events) project(rows, e);
    assert.equal(rows.get("o1")!.confirmedTo, null);
    assert.match(s.kernel.verify(), /OK/);
  } finally {
    await s.dispose();
  }
});
