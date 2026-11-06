// The shared fixtures (kits/fixtures): given these events, when this command, then these
// events (or this rejection), and these unique keys and facts. Every language's kit runs the same
// cases, so the kits write the same events.

import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { test } from "node:test";

import { CommandContext, Rejected, type Aggregate } from "@kernable/sdk";

import { kitDomains } from "../../src/kit.ts";

interface Case {
  name: string;
  aggregate: string;
  aggregateId: string;
  tenant: string;
  now: number;
  given: { type: string; data: unknown }[];
  when: { type: string; data: unknown };
  then?: { type: string; data: unknown }[];
  rejected?: string;
  index?: { keys: unknown[]; facts: string[] };
}

// The fixtures: next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
const DIR = [new URL("../../fixtures/", import.meta.url), new URL("../../../fixtures/", import.meta.url)].find((u) => existsSync(u))!;
const files = readdirSync(DIR).filter((f) => f.endsWith(".json") && f !== "authorization.json");
const aggregates = new Map(kitDomains().flatMap((d) => d.aggregates.map((a) => [`${d.name}.${a.name}`, a as Aggregate<unknown>])));

/** Whether `actual` matches `expected`, where `"<string>"`, `"<number>"`, `"<argon2>"` and `"<any>"` stand for values that differ each run. */
function matches(expected: unknown, actual: unknown, path = "$"): string | null {
  if (typeof expected === "string" && /^<\w+>$/.test(expected)) {
    const ok =
      expected === "<any>" ||
      (expected === "<string>" && typeof actual === "string" && actual.length > 0) ||
      (expected === "<number>" && typeof actual === "number") ||
      (expected === "<argon2>" && typeof actual === "string" && actual.startsWith("$argon2id$"));
    return ok ? null : `${path}: ${JSON.stringify(actual)} is not ${expected}`;
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) return `${path}: ${JSON.stringify(actual)} is not ${JSON.stringify(expected)}`;
    for (let i = 0; i < expected.length; i++) {
      const m = matches(expected[i], actual[i], `${path}[${i}]`);
      if (m) return m;
    }
    return null;
  }
  if (expected !== null && typeof expected === "object") {
    if (actual === null || typeof actual !== "object") return `${path}: ${JSON.stringify(actual)} is no object`;
    const keys = (o: object) => Object.keys(o).sort().join(",");
    if (keys(expected) !== keys(actual)) return `${path}: fields ${keys(actual)}, expected ${keys(expected)}`;
    for (const k of Object.keys(expected)) {
      const m = matches((expected as Record<string, unknown>)[k], (actual as Record<string, unknown>)[k], `${path}.${k}`);
      if (m) return m;
    }
    return null;
  }
  return Object.is(expected, actual) ? null : `${path}: ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`;
}

for (const file of files) {
  const { domain, cases } = JSON.parse(readFileSync(new URL(file, DIR), "utf8")) as { domain: string; cases: Case[] };
  for (const c of cases) {
    test(`${domain}: ${c.name}`, async () => {
      const aggregate = aggregates.get(`${domain}.${c.aggregate}`);
      assert.ok(aggregate, `no aggregate ${domain}.${c.aggregate}`);
      const meta = { aggregateId: c.aggregateId, tenant: c.tenant, domain, aggregateType: c.aggregate };
      const state = aggregate.fold(c.given.map((e, i) => ({ ...meta, type: e.type, schema: 1, data: e.data, version: i + 1 })));
      const ctx = new CommandContext(`fixture-${c.name}`, {}, new Date(c.now));
      let produced: { type: string; data?: unknown }[];
      try {
        produced = await aggregate.handle(state, c.when.type, c.when.data ?? {}, ctx, { aggregateId: c.aggregateId, tenant: c.tenant });
      } catch (err) {
        if (!(err instanceof Rejected)) throw err;
        assert.equal(err.code, c.rejected, `rejected with ${err.code}: ${err.message}`);
        return;
      }
      assert.equal(c.rejected, undefined, `expected rejected ${c.rejected}, got ${JSON.stringify(produced)}`);
      const plain = JSON.parse(JSON.stringify(produced.map((e) => ({ type: e.type, data: e.data ?? {} }))));
      const mismatch = matches(c.then, plain);
      assert.equal(mismatch, null, mismatch ?? "");
      if (c.index) {
        const after = aggregate.fold(plain.map((e: { type: string; data: unknown }, i: number) => ({ ...meta, type: e.type, schema: 1, data: e.data, version: c.given.length + i + 1 })), state);
        const index = JSON.parse(JSON.stringify(aggregate.index(after)));
        const m = matches(c.index, index);
        assert.equal(m, null, m ?? "");
      }
    });
  }
}
