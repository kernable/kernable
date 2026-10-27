// Runs the tutorial "Build a shop" as written: for every part, the shop's processes and the
// part's script, in TypeScript and in Python and (part 5) mixed, against a real kernel, and
// compares the output with what the tutorial shows.

import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { test } from "node:test";

import { Kernable } from "@kernable/sdk";

import { TestKernel, freePort } from "../../sdks/typescript/test/kernel.ts";

const HERE = new URL("./shop/", import.meta.url).pathname;
const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
const ENV = { ...process.env, PYTHONPATH: `${ROOT}sdks/python` };
type Lang = "ts" | "py";

/** The command line of a tutorial file; Python files use underscores. */
function command(lang: Lang, name: string): [string, string[]] {
  return lang === "ts"
    ? [process.execPath, ["--conditions=source", "--no-warnings", `${HERE}ts/${name}.ts`]]
    : [PYTHON, [`${HERE}py/${name.replace(/-/g, "_")}.py`]];
}

/** The shop's processes by the part that introduces them, with the listeners they register. */
const PROCESSES: [part: number, name: string, listeners: string[]][] = [
  [2, "worker", []],
  [3, "read-models", ["customers", "orders"]],
  [4, "confirmations", ["confirmations"]],
  [6, "guard", ["guard"]],
];

async function run(part: number, langs: Record<string, Lang>, client: Lang, expected = `${HERE}${client}/part${part}.out`) {
  const kernel = new TestKernel(["shop.*"], "", { metrics: true });
  await kernel.start();
  const running: ChildProcess[] = [];
  try {
    const listeners: string[] = [];
    for (const [since, name, registers] of PROCESSES.filter(([since]) => since <= part)) {
      void since;
      const [cmd, args] = command(langs[name] ?? client, name);
      running.push(spawn(cmd, args, { cwd: kernel.dir, env: ENV, stdio: ["ignore", "ignore", "inherit"] }));
      listeners.push(...registers);
    }
    const up = (m: string) => m.includes("kernable_worker_sessions 1") && listeners.every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 500 && !up(await fetch(kernel.metricsUrl!).then((r) => r.text())); i++) await new Promise((r) => setTimeout(r, 20));
    const [cmd, args] = command(client, `part${part}`);
    const out = execFileSync(cmd, args, { cwd: kernel.dir, env: ENV, encoding: "utf8" });
    assert.equal(out, readFileSync(expected, "utf8"));
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    for (const p of running) p.kill();
    await kernel.dispose();
  }
}

for (const lang of ["ts", "py"] as const) {
  for (const part of [2, 3, 4, 6, 7]) {
    test(`part ${part} in ${lang === "ts" ? "TypeScript" : "Python"}`, () => run(part, {}, lang));
  }
}

test("part 5: the same shop with every process in either language", async () => {
  await run(4, { worker: "py", "read-models": "ts", confirmations: "py" }, "ts");
  await run(4, { worker: "ts", "read-models": "py", confirmations: "ts" }, "py");
});

/**
 * The shop with every process and part 7's script, on a kernel started from the tutorial's
 * own config: `kernable.dev.toml` (part 1) or the production `kernable.toml` (part 8), with keys
 * and certificates made as the tutorial makes them.
 */
async function deployed(lang: Lang, production: boolean) {
  const dir = execFileSync("mktemp", ["-d", "/tmp/kn-shop-XXXX"], { encoding: "utf8" }).trim();
  const kernable = (...args: string[]) => execFileSync(`${ROOT}target/debug/kernable`, args, { cwd: dir, encoding: "utf8" }).trim();
  const keys = production ? "keys/" : "";
  for (const sub of production ? ["data", "keys"] : []) mkdirSync(`${dir}/${sub}`);
  kernable("keys", "new", `${keys}kernel`);
  kernable("keys", "new", `${keys}vault`);
  const app = kernable("keys", "new", `${keys}app`);
  if (production) kernable("certs", "new", "certs", "app");
  const [port, metrics] = [await freePort(), await freePort()];
  const toml = readFileSync(`${HERE}${production ? "kernable.toml" : "kernable.dev.toml"}`, "utf8")
    .replaceAll("<app>", app)
    .replace("0.0.0.0:7233", `127.0.0.1:${port}`)
    .replace("127.0.0.1:9233", `127.0.0.1:${metrics}`);
  writeFileSync(`${dir}/kernable.toml`, toml);
  const tls = {
    KERNABLE_URL: `127.0.0.1:${port}`,
    KERNABLE_KEY: "keys/app.key",
    KERNABLE_TLS_CA: "certs/ca.tls.crt",
    KERNABLE_TLS_CERT: "certs/app.tls.crt",
    KERNABLE_TLS_KEY: "certs/app.tls.key",
  };
  const env = production ? { ...ENV, ...tls } : ENV;
  const kernel = spawn(`${ROOT}target/debug/kernable-kernel`, ["kernable.toml"], { cwd: dir, stdio: "ignore" });
  const running: ChildProcess[] = [];
  const ready = () => fetch(`http://127.0.0.1:${metrics}/ready`).then((r) => r.status, () => 0);
  try {
    for (let i = 0; i < 250 && (await ready()) !== 200; i++) await new Promise((r) => setTimeout(r, 20));
    for (const [, name] of PROCESSES) {
      const [cmd, args] = command(lang, name);
      running.push(spawn(cmd, args, { cwd: dir, env, stdio: ["ignore", "ignore", "inherit"] }));
    }
    const listeners = PROCESSES.flatMap(([, , l]) => l);
    const up = (m: string) => m.includes("kernable_worker_sessions 1") && listeners.every((l) => m.includes(`kernable_listener_connected{listener="${l}"} 1`));
    for (let i = 0; i < 500 && !up(await fetch(`http://127.0.0.1:${metrics}/metrics`).then((r) => r.text())); i++) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.equal(await ready(), 200);
    const [cmd, args] = command(lang, "part7");
    assert.equal(execFileSync(cmd, args, { cwd: dir, env, encoding: "utf8" }), readFileSync(`${HERE}${lang}/part7.out`, "utf8"));
    assert.match(kernable("verify", production ? "sqlite:data/shop.db" : "sqlite:shop.db", `${keys}kernel.pub`), /\nOK$/);
    for (const p of running) p.kill();
    kernel.kill("SIGTERM");
    const [code] = await new Promise<[number | null]>((resolve) => kernel.on("exit", (c) => resolve([c])));
    assert.equal(code, 0, "the kernel drains and stops cleanly");
  } finally {
    for (const p of running) p.kill();
    kernel.kill("SIGKILL");
  }
}

for (const lang of ["ts", "py"] as const) {
  test(`part 1 in ${lang === "ts" ? "TypeScript" : "Python"}: the whole shop on the development kernel`, () => deployed(lang, false));
  test(`part 8 in ${lang === "ts" ? "TypeScript" : "Python"}: the whole shop on the production kernel, over mutual TLS`, () => deployed(lang, true));

  test(`part 8 in ${lang === "ts" ? "TypeScript" : "Python"}: a read model with a store survives a restart`, async () => {
    const kernel = new TestKernel(["shop.*"], "", { metrics: true });
    await kernel.start();
    const start = () => {
      const [cmd, args] = command(lang, "customers-persistent");
      return spawn(cmd, args, { cwd: kernel.dir, env: ENV, stdio: ["ignore", "ignore", "inherit"] });
    };
    const client = new Kernable({ url: kernel.url, key: kernel.appKey });
    const [wcmd, wargs] = command(lang, "worker");
    const worker = spawn(wcmd, wargs, { cwd: kernel.dir, env: ENV, stdio: "ignore" });
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
      await client.submit({ domain: "shop", aggregateType: "Customer", aggregateId: "c1", type: "Register", data: { email: "ann@example.com", name: "Ann" } });
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
