// Runs the getting-started tutorial's files against a real kernel, the way a reader would, and
// compares the output with what the tutorial shows (submit.out).

import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { TestKernel } from "../../sdks/typescript/test/kernel.ts";

const HERE = new URL("./getting-started/", import.meta.url).pathname;
const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
const node = (file: string) => [process.execPath, ["--conditions=source", "--no-warnings", `${HERE}${file}`]] as const;
const uuids = (s: string) => s.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "<uuid>");

test("the getting-started tutorial runs as written", async () => {
  const kernel = new TestKernel(["shop.*"], "", { metrics: true });
  await kernel.start();
  const processes: ChildProcess[] = [];
  const run = (command: string, args: readonly string[]) => {
    const p = spawn(command, args, { cwd: kernel.dir, stdio: ["ignore", "ignore", "inherit"], env: { ...process.env, PYTHONPATH: `${ROOT}sdks/python` } });
    processes.push(p);
  };
  try {
    run(...node("cart.ts"));
    run(PYTHON, [`${HERE}carts.py`]);
    // Both are up once the kernel counts the worker session and the listener.
    for (let i = 0; i < 250; i++) {
      const metrics = await fetch(kernel.metricsUrl!).then((r) => r.text());
      if (metrics.includes("kernable_worker_sessions 1") && metrics.includes('kernable_listener_connected{listener="carts"} 1')) break;
      await new Promise((r) => setTimeout(r, 20));
    }
    const [cmd, args] = node("submit.ts");
    const out = execFileSync(cmd, args, { cwd: kernel.dir, encoding: "utf8" });
    assert.equal(uuids(out), uuids(readFileSync(`${HERE}submit.out`, "utf8")));
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    for (const p of processes) p.kill();
    await kernel.dispose();
  }
});
