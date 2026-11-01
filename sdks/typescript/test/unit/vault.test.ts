// The vault: personal fields and secrets are declared on the domain.

import assert from "node:assert/strict";
import { test } from "node:test";

import { identityDomain } from "../identity.ts";

test("command definitions take unqualified types", () => {
  const { identity } = identityDomain();
  assert.ok(identity.commands.get("RegisterAccount"));
  assert.throws(() => identity.command("shop.Register"), /unqualified/);
});
