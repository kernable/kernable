// Runs the tutorial "Build a shop" as written: for every part, the shop's processes and the
// part's script, in TypeScript, Python and Rust and (part 5) mixed, against a real kernel, and
// compares the output with what the tutorial shows.

import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { Kernable, type StoredEvent } from "@kernable/sdk";

import { TestKernel, freePort } from "../../sdks/typescript/test/kernel.ts";
import { shopDomain } from "./shop/ts/domain.ts";

const HERE = new URL("./shop/", import.meta.url).pathname;
const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
// Without the colors `node --test` forces on its children when run in a terminal.
const { FORCE_COLOR: _, ...INHERITED } = process.env;
const ENV = { ...INHERITED, PYTHONPATH: `${ROOT}sdks/python:${ROOT}kits/python` };
type Lang = "ts" | "py" | "rs";
const LANGUAGES = { ts: "TypeScript", py: "Python", rs: "Rust" } as const;

// The Rust shop's binaries (docs/examples/shop/rs), built once like `cargo build` in the tutorial.
execFileSync("cargo", ["build", "-q", "--workspace"], { cwd: ROOT, stdio: "inherit" });

/** The command line of a tutorial file; Python and Rust files use underscores. */
function command(lang: Lang, name: string): [string, string[]] {
  if (lang === "ts") return [process.execPath, ["--conditions=source", "--no-warnings", `${HERE}ts/${name}.ts`]];
  if (lang === "py") return [PYTHON, [`${HERE}py/${name.replace(/-/g, "_")}.py`]];
  return [`${ROOT}target/debug/${name.replace(/-/g, "_")}`, []];
}

/** The shop's processes by the part that introduces them, with the listeners they register. */
const PROCESSES: [part: number, name: string, listeners: string[]][] = [
  [2, "worker", []],
  [3, "read-models", ["customers", "orders"]],
  [4, "confirmations", ["confirmations"]],
  [6, "guard", ["guard"]],
];

async function run(part: number, langs: Record<string, Lang>, client: Lang, expected = `${HERE}${client}/part${part}.out`) {
  const kernel = new TestKernel(["shop.*"], [], { metrics: true, processes: part >= 10 });
  await kernel.start();
  // Part 10: an unpaid order is cancelled after 2 seconds instead of 3 days.
  const env = { ...ENV, KERNABLE_URL: kernel.url, PAY_WITHIN: "2s" };
  const running: ChildProcess[] = [];
  try {
    const listeners: string[] = [];
    for (const [since, name, registers] of PROCESSES.filter(([since]) => since <= part)) {
      void since;
      const [cmd, args] = command(langs[name] ?? client, name);
      running.push(spawn(cmd, args, { cwd: kernel.dir, env, stdio: ["ignore", "ignore", "inherit"] }));
      listeners.push(...registers);
    }
    const up = (m: string) => m.includes("kernable_worker_sessions 1") && listeners.every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 500 && !up(await fetch(kernel.metricsUrl!).then((r) => r.text())); i++) await new Promise((r) => setTimeout(r, 20));
    if (part >= 10) {
      // Registered once the worker is up, as a deploy would.
      const [cmd, args] = command(langs.fulfilment ?? client, "fulfilment");
      assert.equal(execFileSync(cmd, args, { cwd: kernel.dir, env, encoding: "utf8" }), "shop.fulfilment version 1\n");
    }
    const [cmd, args] = command(client, `part${part}`);
    const out = execFileSync(cmd, args, { cwd: kernel.dir, env, encoding: "utf8" });
    assert.equal(out, readFileSync(expected, "utf8"));
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    for (const p of running) p.kill();
    await kernel.dispose();
  }
}

for (const lang of ["ts", "py", "rs"] as const) {
  for (const part of [2, 3, 4, 6, 7, 10]) {
    test(`part ${part} in ${LANGUAGES[lang]}`, () => run(part, {}, lang));
  }
}

/**
 * Part 9: the shop's worker and read models, and its API with the kit; no guard (the kit's
 * middleware decides) and no confirmations reactor. The API listens on a free port.
 */
async function part9(lang: Lang) {
  const KIT = ["tenancy.*", "account.*", "identity.*", "group.*", "invitation.*"];
  const kernel = new TestKernel(["shop.*", ...KIT], [], { metrics: true });
  // As `--dev` grants app.key everything: here also the kit's reserved orders and its privileged follow-ups.
  assert.ok(kernel.cli(
    "grant", "root.key", "app.pub", `run=${["shop.*", ...KIT].join(",")}`,
    ...["submit", "subscribe", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage", "reserved_order", "cache"].map((a) => `${a}=*`),
    "privileged=bypass_authorization",
  ).ok);
  await kernel.start();
  const port = await freePort();
  const env = { ...ENV, KERNABLE_URL: kernel.url, PORT: String(port), API_URL: `http://127.0.0.1:${port}` };
  const running: ChildProcess[] = [];
  try {
    for (const name of ["worker", "read-models", "api"]) {
      const [cmd, args] = command(lang, name);
      running.push(spawn(cmd, args, { cwd: kernel.dir, env, stdio: ["ignore", "ignore", "inherit"] }));
    }
    const listening = () => fetch(`http://127.0.0.1:${port}/me`).then((r) => r.status === 401, () => false);
    for (let i = 0; i < 1000 && !(await listening()); i++) await new Promise((r) => setTimeout(r, 20));
    const up = (m: string) => m.includes("kernable_worker_sessions") && ["orders", "authorization", "own-orders"].every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 500 && !up(await fetch(kernel.metricsUrl!).then((r) => r.text())); i++) await new Promise((r) => setTimeout(r, 20));
    const [cmd, args] = command(lang, "part9");
    const out = execFileSync(cmd, args, { cwd: kernel.dir, env, encoding: "utf8" });
    assert.equal(out, readFileSync(`${HERE}${lang}/part9.out`, "utf8"));
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    for (const p of running) p.kill();
    await kernel.dispose();
  }
}

for (const lang of ["ts", "py", "rs"] as const) {
  test(`part 9 in ${LANGUAGES[lang]}: accounts and sign-in with the kit`, () => part9(lang));
}

test("part 5: the same shop with every process in any language", async () => {
  await run(4, { worker: "py", "read-models": "ts", confirmations: "rs" }, "ts");
  await run(4, { worker: "rs", "read-models": "py", confirmations: "ts" }, "py");
  await run(4, { worker: "ts", "read-models": "rs", confirmations: "py" }, "rs");
});

/**
 * Part 4's shop, one process per language, and `kill -9` of each process and of the kernel in
 * turn, with orders before and after each: every order is confirmed exactly once, each
 * confirmation names its submit as its cause, and the log verifies.
 */
test("kill -9 of each process and of the kernel loses and duplicates nothing", async () => {
  const kernel = new TestKernel(["shop.*"], [], { metrics: true });
  await kernel.start();
  const env = { ...ENV, KERNABLE_URL: kernel.url };
  const langs: Record<string, Lang> = { worker: "py", "read-models": "ts", confirmations: "rs" };
  const running = new Map<string, ChildProcess>();
  const start = (name: string) => {
    const [cmd, args] = command(langs[name], name);
    running.set(name, spawn(cmd, args, { cwd: kernel.dir, env, stdio: ["ignore", "ignore", "inherit"] }));
  };
  const kill = async (name: string) => {
    const p = running.get(name)!;
    p.kill("SIGKILL");
    if (p.exitCode === null && p.signalCode === null) await once(p, "exit");
  };
  const metrics = () => fetch(kernel.metricsUrl!).then((r) => r.text(), () => "");
  const up = async () => {
    const ok = (m: string) => m.includes("kernable_worker_sessions 1") && ["customers", "orders", "confirmations"].every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 1000 && !ok(await metrics()); i++) await new Promise((r) => setTimeout(r, 20));
    assert.ok(ok(await metrics()), "every process connected");
  };
  let client = new Kernable({ url: kernel.url, key: kernel.appKey });
  let n = 0;
  const order = async () => {
    n++;
    const send = (aggregateType: string, aggregateId: string, type: string, data?: object) =>
      client.submit({ domain: "shop", aggregateType, aggregateId, type, data, id: `${type}-${aggregateId}-${n}` });
    await send("customer", `c${n}`, "Register", { email: `c${n}@example.com`, name: `C${n}` });
    await send("order", `o${n}`, "Place", { customerId: `c${n}` });
    await send("order", `o${n}`, "AddItem", { sku: "apple", qty: n });
    await send("order", `o${n}`, "Submit");
  };
  try {
    for (const name of Object.keys(langs)) start(name);
    await up();
    for (const victim of ["worker", "read-models", "confirmations", "kernel"]) {
      await order();
      if (victim === "kernel") {
        client.close();
        await kernel.kill("SIGKILL");
        await kernel.start();
        client = new Kernable({ url: kernel.url, key: kernel.appKey });
      } else {
        await kill(victim);
        start(victim);
      }
      await up();
      await order();
    }

    for (let i = 1; i <= n; i++) {
      const row = await client.query<{ status: string; items: Record<string, number> }>({ domain: "shop", type: "GetOrder", data: { id: `o${i}` } });
      assert.equal(row.status, "confirmed", `o${i}`);
      assert.deepEqual(row.items, { apple: i });
    }
    // The log, read again from the start by a new listener.
    const events: StoredEvent[] = [];
    const replay = shopDomain.readModel("replay");
    for (const type of ["OrderSubmitted", "OrderConfirmed"]) replay.on(type, (data, evt, ctx) => void events.push({ ...evt, data, request: ctx.request }));
    const reader = client.listener(replay);
    const reading = reader.run();
    for (let i = 0; i < 500 && events.filter((e) => e.type === "OrderConfirmed").length < n; i++) await new Promise((r) => setTimeout(r, 20));
    await new Promise((r) => setTimeout(r, 200));
    reader.stop();
    await reading;
    const confirmed = events.filter((e) => e.type === "OrderConfirmed");
    assert.deepEqual(confirmed.map((e) => e.aggregateId).sort(), Array.from({ length: n }, (_, i) => `o${i + 1}`).sort());
    for (const c of confirmed) {
      const submitted = events.find((e) => e.type === "OrderSubmitted" && e.aggregateId === c.aggregateId)!;
      assert.equal(c.request.causationId, submitted.commandId);
      assert.equal(c.request.correlationId, submitted.request.correlationId);
    }
    assert.match(kernel.verify(), /\nOK\n$/);
    const m = await metrics();
    for (const l of ["customers", "orders", "confirmations"]) assert.match(m, new RegExp(`^kernable_listener_lag\\{listener="${l}"\\} 0$`, "m"));
  } finally {
    for (const p of running.values()) p.kill("SIGKILL");
    client.close();
    await kernel.dispose();
  }
});

/** The keys each process of the production shop uses (part 8). */
const SERVICE_KEYS: Record<string, string> = { worker: "worker", "read-models": "read-models", confirmations: "confirmations", guard: "guard", part7: "api" };

/**
 * The shop with every process and part 7's script, on a kernel set up and started by the
 * tutorial's own script: `dev.sh` (part 1) or `production.sh` (part 8), with only the addresses
 * replaced by free ports.
 */
async function deployed(lang: Lang, production: boolean) {
  const dir = execFileSync("mktemp", ["-d", "/tmp/kn-shop-XXXX"], { encoding: "utf8" }).trim();
  const bin = `${ROOT}target/debug`;
  const kernable = (...args: string[]) => execFileSync(`${bin}/kernable`, args, { cwd: dir, encoding: "utf8" }).trim();
  const [port, metrics] = [await freePort(), await freePort()];
  const script = readFileSync(`${HERE}${production ? "production.sh" : "dev.sh"}`, "utf8")
    .replace("0.0.0.0:7233", `127.0.0.1:${port}`)
    .replace("127.0.0.1:9233", `127.0.0.1:${metrics}`)
    // exec, so that signals reach the kernel; the development kernel moves off its default port
    // and serves metrics, to tell when it is ready.
    .replace("\nkernable start ", production ? "\nexec kernable start " : `\nexec kernable start --listen 127.0.0.1:${port} --metrics-listen 127.0.0.1:${metrics} `);
  const kernel = spawn("sh", ["-e", "-c", script], { cwd: dir, env: { ...INHERITED, PATH: `${bin}:${process.env.PATH}` }, stdio: ["ignore", "ignore", "inherit"] });
  /** A process's environment, as part 8 sets it: its own key and certificate. */
  const env = (name: string) => {
    if (!production) return { ...ENV, KERNABLE_URL: `127.0.0.1:${port}` };
    const key = SERVICE_KEYS[name];
    return {
      ...ENV,
      KERNABLE_URL: `127.0.0.1:${port}`,
      KERNABLE_KEY: `keys/${key}.key`,
      KERNABLE_TLS_CA: "certs/ca.tls.crt",
      KERNABLE_TLS_CERT: `certs/${key}.tls.crt`,
      KERNABLE_TLS_KEY: `certs/${key}.tls.key`,
    };
  };
  const running: ChildProcess[] = [];
  const ready = () => fetch(`http://127.0.0.1:${metrics}/ready`).then((r) => r.status, () => 0);
  try {
    for (let i = 0; i < 250 && (await ready()) !== 200; i++) await new Promise((r) => setTimeout(r, 20));
    for (const [, name] of PROCESSES) {
      const [cmd, args] = command(lang, name);
      running.push(spawn(cmd, args, { cwd: dir, env: env(name), stdio: ["ignore", "ignore", "inherit"] }));
    }
    const listeners = PROCESSES.flatMap(([, , l]) => l);
    const up = (m: string) => m.includes("kernable_worker_sessions 1") && listeners.every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 500 && !up(await fetch(`http://127.0.0.1:${metrics}/metrics`).then((r) => r.text())); i++) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.equal(await ready(), 200);
    const [cmd, args] = command(lang, "part7");
    assert.equal(execFileSync(cmd, args, { cwd: dir, env: env("part7"), encoding: "utf8" }), readFileSync(`${HERE}${lang}/part7.out`, "utf8"));
    assert.match(kernable("verify", production ? "sqlite:data/shop.db" : "sqlite:kernable.db", production ? "keys/kernel.pub" : "kernel.pub"), /\nOK$/);
    for (const p of running) p.kill();
    kernel.kill("SIGTERM");
    const [code] = await new Promise<[number | null]>((resolve) => kernel.on("exit", (c) => resolve([c])));
    assert.equal(code, 0, "the kernel drains and stops cleanly");
  } finally {
    for (const p of running) p.kill();
    kernel.kill("SIGKILL");
  }
}

for (const lang of ["ts", "py", "rs"] as const) {
  test(`part 1 in ${LANGUAGES[lang]}: the whole shop on the development kernel`, () => deployed(lang, false));
  test(`part 8 in ${LANGUAGES[lang]}: the whole shop on the production kernel, over mutual TLS`, () => deployed(lang, true));

  test(`part 8 in ${LANGUAGES[lang]}: a read model with a store survives a restart`, async () => {
    const kernel = new TestKernel(["shop.*"], [], { metrics: true });
    await kernel.start();
    const start = () => {
      const [cmd, args] = command(lang, "customers-persistent");
      return spawn(cmd, args, { cwd: kernel.dir, env: { ...ENV, KERNABLE_URL: kernel.url }, stdio: ["ignore", "ignore", "inherit"] });
    };
    const client = new Kernable({ url: kernel.url, key: kernel.appKey });
    const [wcmd, wargs] = command(lang, "worker");
    const worker = spawn(wcmd, wargs, { cwd: kernel.dir, env: { ...ENV, KERNABLE_URL: kernel.url }, stdio: "ignore" });
    let model = start();
    const connected = async (yes: boolean) => {
      for (let i = 0; i < 500; i++) {
        const m = await fetch(kernel.metricsUrl!).then((r) => r.text());
        if (m.includes(`kernable_listener_connected{listener="customers"} ${yes ? 1 : 0}`) && m.includes("kernable_worker_sessions 1")) return;
        await new Promise((r) => setTimeout(r, 20));
      }
    };
    try {
      await connected(true);
      await client.submit({ domain: "shop", aggregateType: "customer", aggregateId: "c1", type: "Register", data: { email: "ann@example.com", name: "Ann" } });
      model.kill();
      await connected(false);
      model = start();
      await connected(true);
      const row = await client.query({ domain: "shop", type: "GetCustomer", data: { id: "c1" } });
      assert.deepEqual(row, { id: "c1", email: "ann@example.com", name: "Ann" });
      assert.ok(readFileSync(`${kernel.dir}/customers.db`).length > 0, "the rows live in customers.db");
    } finally {
      model.kill();
      worker.kill();
      client.close();
      await kernel.dispose();
    }
  });
}
