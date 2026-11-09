// The shared authorization fixtures (kits/fixtures/authorization.json): the middleware learns
// a world of events, then decides on a check: allowed, or refused with a code. Every language's
// kit decides the same way.

import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";

import { Rejected, type Check, type StoredEventMeta } from "@kernable/sdk";

import { authorizationListener } from "../../src/authorization/middleware.ts";
import { MemoryKeyValues } from "../../src/authorization/policy.ts";
import { kitDomains, relation } from "../../src/kit.ts";

interface FixtureEvent { domain: string; aggregateType: string; type: string; tenant: string; aggregateId: string; data: unknown }
interface Fixtures {
  relations: { type: string; name: string; event: string; field: string }[];
  world: FixtureEvent[];
  cases: { name: string; extra?: FixtureEvent[]; check: Omit<Check, "id">; expect: string }[];
}

// The fixtures: next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
const file = [new URL("../../fixtures/authorization.json", import.meta.url), new URL("../../../fixtures/authorization.json", import.meta.url)].find((u) => existsSync(u))!;
const fixtures = JSON.parse(readFileSync(file, "utf8")) as Fixtures;
for (const r of fixtures.relations) relation(r.type, r.name).set(r.event, (e) => e.data[r.field]);
kitDomains();

function learn(store: MemoryKeyValues, events: FixtureEvent[]): void {
  events.forEach((e, i) => {
    const entry = authorizationListener.handlers.get(`${e.domain}.${e.type}`);
    if (!entry) return; // the middleware does not care about this event
    const meta = { ...e, version: 1, commandId: `c${i}`, position: i + 1, createdAt: new Date(0), schema: 1, redacted: [] } as unknown as StoredEventMeta;
    void entry.handler(structuredClone(e.data), meta, { store, request: {} } as never);
  });
}

for (const c of fixtures.cases) {
  test(c.name, async () => {
    const store = new MemoryKeyValues();
    learn(store, [...fixtures.world, ...(c.extra ?? [])]);
    const middleware = authorizationListener.middlewares.get("before")!;
    let outcome = "allow";
    try {
      await middleware.handler({ id: "", ...c.check } as Check, undefined, { store } as never);
    } catch (err) {
      if (!(err instanceof Rejected)) throw err;
      outcome = err.code;
    }
    assert.equal(outcome, c.expect);
  });
}

// A process's identity (identity.processes) passes like a service's: in the platform tenant, with
// its API key, and only with a platform group; before one, its commands are refused.
test("a process identity signs in with its API key and acts only with a platform group", async () => {
  const ev = (type: string, aggregateId: string, data: unknown, domain = "identity", aggregateType = "identity") =>
    ({ domain, aggregateType, type, tenant: "platform", aggregateId, data });
  const processIdentity = [
    ev("IdentityCreated", "i-proc", { kind: "process", accountId: null, displayName: "shop.fulfilment", title: null, email: null }),
    ev("ApiKeyCreated", "i-proc", { keyId: "kp", hash: "h", label: "process", expiresAt: null }),
  ];
  const granted = [
    ev("GroupCreated", "g-fulfil", { name: "g-fulfil", description: "", permissions: [{ pattern: "shop.PlaceOrder" }] }, "group", "group"),
    ev("MembershipGranted", "i-proc", { membershipId: "mp", group: "g-fulfil", scope: null, expiresAt: null }),
  ];
  const check = (request: Record<string, string>) =>
    ({ id: "", kind: "command", tenant: "t1", domain: "shop", type: "PlaceOrder", aggregateType: "order", aggregateId: "o9", data: {}, request }) as unknown as Check;
  const decide = async (events: FixtureEvent[], request = { senderTenant: "platform", senderIdentity: "i-proc", senderSession: "kp" }) => {
    const store = new MemoryKeyValues();
    learn(store, [...fixtures.world, ...events]);
    try {
      await authorizationListener.middlewares.get("before")!.handler(check(request), undefined, { store } as never);
      return "allow";
    } catch (err) {
      if (!(err instanceof Rejected)) throw err;
      return err.code;
    }
  };
  assert.equal(await decide(processIdentity), "forbidden", "no permissions of its own");
  assert.equal(await decide([...processIdentity, ...granted]), "allow");
  assert.equal(await decide([...processIdentity, ...granted, ev("ApiKeyRevoked", "i-proc", { keyId: "kp" })]), "unauthenticated", "its key revoked");
  assert.equal(await decide([...processIdentity, ...granted], { senderTenant: "platform", senderIdentity: "i-proc", senderSession: "k1" }), "unauthenticated", "another identity's key");
});
