// Runs the getting-started tutorial's files against a real kernel, the way a reader would, and
// compares the output with what the tutorial shows: the comments under each print in submit.*.

import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { TestKernel } from "../../sdks/typescript/test/kernel.ts";

const HERE = new URL("./getting-started/", import.meta.url).pathname;
const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;
const node = (file: string) => [process.execPath, ["--conditions=source", "--no-warnings", `${HERE}${file}`]] as const;
// Without the colors `node --test` forces on its children when run in a terminal.
const { FORCE_COLOR: _, ...INHERITED } = process.env;
// The tutorial's files use the default address; the test kernel listens elsewhere.
const ENV = { ...INHERITED, PYTHONPATH: `${ROOT}sdks/python` };
// What submit.* print: the comment lines right under each print, without the comment marker.
const expected = (file: string) => {
  const lines = readFileSync(`${HERE}${file}`, "utf8").split("\n").map((l) => l.trim());
  const out: string[] = [];
  lines.forEach((line, i) => {
    if (!/^(console\.log|print|println!)\(/.test(line)) return;
    for (let j = i + 1; /^(\/\/|#) /.test(lines[j]); j++) out.push(lines[j].replace(/^(\/\/|#) /, ""));
  });
  return out.map((l) => `${l}\n`).join("");
};
const uuids = (s: string) => s.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "<uuid>");

// The worker (step 3), the read model (step 4) and the client (step 5) come in three languages
// each; every run below mixes all three, and each language takes each role once.
execFileSync("cargo", ["build", "-q", "--workspace"], { cwd: ROOT, stdio: "inherit" });
const PROGRAMS = {
  TypeScript: (name: string) => node(`${name}.ts`),
  Python: (name: string) => [PYTHON, [`${HERE}${name}.py`]] as const,
  Rust: (name: string) => [`${ROOT}target/debug/${name}`, []] as const,
};
const EXTENSIONS = { TypeScript: "ts", Python: "py", Rust: "rs" };
const RUNS = [
  ["TypeScript", "Python", "Rust"],
  ["Python", "Rust", "TypeScript"],
  ["Rust", "TypeScript", "Python"],
] as const;

for (const [worker, reader, client] of RUNS) test(`the getting-started tutorial runs as written, worker in ${worker}, read model in ${reader}, client in ${client}`, async () => {
  const kernel = new TestKernel(["shop.*"], [], { metrics: true });
  await kernel.start();
  const processes: ChildProcess[] = [];
  const run = (command: string, args: readonly string[]) => {
    const p = spawn(command, args, { cwd: kernel.dir, stdio: ["ignore", "ignore", "inherit"], env: { ...ENV, KERNABLE_URL: kernel.url } });
    processes.push(p);
  };
  try {
    run(...PROGRAMS[worker]("cart"));
    run(...PROGRAMS[reader]("carts"));
    // Both are up once the kernel counts the worker session and the listener.
    for (let i = 0; i < 250; i++) {
      const metrics = await fetch(kernel.metricsUrl!).then((r) => r.text());
      if (metrics.includes("kernable_worker_sessions 1") && metrics.includes('kernable_listener_connected{listener="carts"} 1')) break;
      await new Promise((r) => setTimeout(r, 20));
    }
    const [cmd, args] = PROGRAMS[client]("submit");
    const out = execFileSync(cmd, args, { cwd: kernel.dir, env: { ...ENV, KERNABLE_URL: kernel.url }, encoding: "utf8" });
    assert.equal(uuids(out), uuids(expected(`submit.${EXTENSIONS[client]}`)));
    assert.match(kernel.verify(), /\nOK\n$/);
  } finally {
    for (const p of processes) p.kill();
    await kernel.dispose();
  }
});
