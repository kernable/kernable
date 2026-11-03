// Files and the cache of a domain: kept by the kernel apart from the event log, behind per-domain
// grants.

import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { test } from "node:test";

import { status } from "@grpc/grpc-js";

import { Kernable } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

async function withKernel(fn: (kernable: Kernable) => Promise<void>) {
  const kernel = new TestKernel(["shop.*"], [], { stores: ["shop.*"] });
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  try {
    await fn(kernable);
  } finally {
    kernable.close();
    await kernel.dispose();
  }
}

test("files are put, read and deleted", () =>
  withKernel(async (kernable) => {
    await kernable.data.put("shop", "notes/o-1.txt", "Grüße");
    const text = await kernable.data.get("shop", "notes/o-1.txt");
    assert.equal(Buffer.from(text!.data).toString("utf8"), "Grüße");
    assert.equal(text!.contentType, "text/plain; charset=utf-8");

    const pdf = new Uint8Array([0x25, 0x50, 0x44, 0x46]);
    // The put answers the data's SHA-256, for an event to name; a get answers it too.
    const sha256 = createHash("sha256").update(pdf).digest("hex");
    assert.equal(await kernable.data.put("shop", "invoices/o-1.pdf", pdf, { contentType: "application/pdf" }), sha256);
    assert.deepEqual(await kernable.data.get("shop", "invoices/o-1.pdf"), { data: Buffer.from(pdf), contentType: "application/pdf", sha256 });
    await kernable.data.put("shop", "raw", pdf);
    assert.equal((await kernable.data.get("shop", "raw"))!.contentType, "application/octet-stream");

    // Files are per tenant.
    assert.equal(await kernable.data.get("shop", "invoices/o-1.pdf", { tenant: "other" }), undefined);

    assert.equal(await kernable.data.delete("shop", "invoices/o-1.pdf"), true);
    assert.equal(await kernable.data.delete("shop", "invoices/o-1.pdf"), false);
    assert.equal(await kernable.data.get("shop", "invoices/o-1.pdf"), undefined);
    assert.equal(await kernable.data.get("shop", "missing"), undefined);
  }));

test("files are listed by prefix, page after page", () =>
  withKernel(async (kernable) => {
    for (const key of ["docs/b", "docs/a", "other", `docs/${"x".repeat(300)}`]) await kernable.data.put("shop", key, key);
    const docs = await kernable.data.list("shop", "docs/");
    assert.deepEqual(
      docs.files.map((f) => [f.key, f.size]),
      [["docs/a", 6], ["docs/b", 6], [`docs/${"x".repeat(300)}`, 305]],
    );
    assert.equal(docs.next, undefined);
    const after = await kernable.data.list("shop", "docs/", { after: "docs/a" });
    assert.deepEqual(after.files.map((f) => f.key).slice(0, 1), ["docs/b"]);
    assert.equal((await kernable.data.list("shop")).files.length, 4);
    assert.equal((await kernable.data.list("shop", "", { tenant: "other" })).files.length, 0);
  }));

test("a file of 5 MiB goes both ways", () =>
  withKernel(async (kernable) => {
    const big = randomBytes(5 << 20);
    await kernable.data.put("shop", "exports/all.bin", big);
    const got = await kernable.data.get("shop", "exports/all.bin");
    assert.ok(big.equals(got!.data));
  }));

test("cache values expire and can be deleted", () =>
  withKernel(async (kernable) => {
    await kernable.cache.set("shop", "rate/eur", "1.08", { ttlMs: 200 });
    await kernable.cache.set("shop", "token", new Uint8Array([1, 2, 3]));
    assert.equal(Buffer.from((await kernable.cache.get("shop", "rate/eur"))!).toString("utf8"), "1.08");
    assert.deepEqual(await kernable.cache.get("shop", "token"), Buffer.from([1, 2, 3]));

    await new Promise((r) => setTimeout(r, 400));
    assert.equal(await kernable.cache.get("shop", "rate/eur"), undefined);
    assert.ok(await kernable.cache.get("shop", "token"), "a value without a TTL stays");

    assert.equal(await kernable.cache.delete("shop", "token"), true);
    assert.equal(await kernable.cache.delete("shop", "token"), false);
    assert.equal(await kernable.cache.get("shop", "token"), undefined);
  }));

test("files and the cache of a domain without a grant are refused", () =>
  withKernel(async (kernable) => {
    const denied = { code: status.PERMISSION_DENIED };
    await assert.rejects(kernable.data.put("billing", "x", "y"), denied);
    await assert.rejects(kernable.data.get("billing", "x"), denied);
    await assert.rejects(kernable.cache.set("billing", "x", "y"), denied);
    await assert.rejects(kernable.data.put("shop", "", "y"), { code: status.INVALID_ARGUMENT });
  }));
