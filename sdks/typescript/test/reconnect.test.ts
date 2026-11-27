// A kernel on this machine is retried within two seconds after it went away (a restart is over in
// seconds); one elsewhere backs off to 30 s.

import assert from "node:assert/strict";
import { test } from "node:test";

import { isLocal } from "../src/index.ts";

test("kernels on this machine are taken as local", () => {
  for (const url of ["unix:/srv/run/kernel.sock", "http://127.0.0.1:7233", "127.0.0.1:7233", "http://localhost:7233",
    "http://[::1]:7233", "http://127.0.0.1:7233,http://127.0.0.1:7235"]) {
    assert.ok(isLocal(url), url);
  }
  for (const url of ["https://kernel.example.com:7233", "http://10.0.0.5:7233", "http://127.0.0.1:7233,https://b.example:7233",
    "http://localhost.example.com:7233"]) {
    assert.ok(!isLocal(url), url);
  }
});
