// The code of the how-to guides: the domain tests in Python (the TypeScript ones run as
// how-to/*.test.ts).
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { test } from "node:test";

const ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON = process.env.KERNABLE_PYTHON ?? `${ROOT}sdks/python/.venv/bin/python`;

test("the Python domain tests pass", () => {
  const out = execFileSync(PYTHON, [new URL("./how-to/test_order.py", import.meta.url).pathname], {
    env: { ...process.env, PYTHONPATH: `${ROOT}sdks/python` },
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  assert.equal(out, "");
});
