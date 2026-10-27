// The vault: personal fields and secrets are declared on the domain; the SDK keeps them out of
// the signed log, handlers and allowed listeners see them in clear, and an erasing event deletes
// them for good.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain, type StoredEvent } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

function identityDomain() {
  const identity = domain("identity");
  identity.command("RegisterAccount").personal(["email"], (d: { id: string }) => d.id).secret(["password"]);
  identity.event("AccountRegistered", 1).personal(["email"], (d: { accountId: string }) => d.accountId);
  identity.event("AccountErased", 1).erases((d: { accountId: string }) => d.accountId);
  const account = identity.aggregate("Account", { email: null as string | null, passwordHash: "" })
    .apply("AccountRegistered", (_s, d: { email: string; passwordHash: string }) => ({ email: d.email, passwordHash: d.passwordHash }))
    .apply("AccountErased", () => ({ email: null, passwordHash: "" }))
    .command("RegisterAccount", (_s, d: { id: string; email: string; password: string }) => [
      // The handler gets the secret in clear and keeps only a hash.
      { type: "AccountRegistered", data: { accountId: d.id, email: d.email, passwordHash: sha256(d.password) } },
    ])
    .command("Login", (s, d: { password: string }) => {
      if (sha256(d.password) !== s.passwordHash) throw new Rejected("wrong password", "denied");
      return [{ type: "LoggedIn" }];
    })
    .command("EraseAccount", (_s, d: { id: string }) => [{ type: "AccountErased", data: { accountId: d.id } }]);
  identity.command("Login").secret(["password"]);
  return { identity, account };
}

test("personal fields and secrets live in the vault and an erasing event deletes them", async () => {
  const kernel = new TestKernel(["identity.*"]);
  await kernel.start();
  const { identity, account } = identityDomain();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey }).use(identity);
  const emails = new Map<string, string>();
  const accounts = kernable.readModel("accounts").use(identity)
    .on("identity.AccountRegistered", (e) => void emails.set(e.aggregateId, (e.data as { email: string }).email))
    .on("identity.AccountErased", (e) => void emails.delete(e.aggregateId))
    .query("identity.GetEmail", (d: { id: string }) => {
      if (!emails.has(d.id)) throw new Rejected("no such account", "not_found");
      return emails.get(d.id);
    });
  const worker = kernable.worker(account);
  const running = [worker.run(), accounts.run()];
  const send = (type: string, data: object) =>
    kernable.submit({ domain: "identity", aggregateType: "Account", aggregateId: "a1", type, data });
  try {
    await Promise.all([worker.ready, accounts.ready]);
    const registered = await send("RegisterAccount", { id: "a1", email: "max@firma.de", password: "correct horse" });
    assert.ok(registered.committed);
    // The submitter is no reader of what the vault keeps: it gets references only.
    assert.deepEqual(registered.events[0].data, { accountId: "a1", email: null, passwordHash: sha256("correct horse") });
    assert.deepEqual(registered.events[0].redacted, ["email"]);
    assert.equal(await kernable.query({ domain: "identity", type: "GetEmail", data: { id: "a1" } }), "max@firma.de");
    assert.deepEqual(await send("Login", { password: "wrong" }), { committed: false, code: "denied", message: "wrong password" });
    assert.ok((await send("Login", { password: "correct horse" })).committed);

    // Neither the email nor the password is in any file of the kernel in clear.
    const files = readdirSync(kernel.dir, { withFileTypes: true })
      .filter((f) => f.isFile())
      .map((f) => readFileSync(join(kernel.dir, f.name)));
    for (const secret of ["max@firma.de", "correct horse"]) {
      assert.ok(!files.some((b) => b.includes(secret)), `${secret} is stored in clear`);
    }

    // Erasing: the read model drops the account; a rebuilt one reads null where the email was.
    assert.ok((await send("EraseAccount", { id: "a1" })).committed);
    await assert.rejects(kernable.query({ domain: "identity", type: "GetEmail", data: { id: "a1" } }), /no such account/);
    const replayed: StoredEvent[] = [];
    const rebuilt = kernable.readModel("rebuilt").use(identity).on("identity.AccountRegistered", (e) => void replayed.push(e));
    running.push(rebuilt.run());
    await rebuilt.ready;
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(replayed.length, 1);
    assert.deepEqual([(replayed[0].data as { email: unknown }).email, replayed[0].redacted], [null, ["email"]]);
    rebuilt.stop();
  } finally {
    worker.stop();
    accounts.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("command definitions take unqualified types", () => {
  const { identity } = identityDomain();
  assert.ok(identity.commands.get("RegisterAccount"));
  assert.throws(() => identity.command("shop.Register"), /unqualified/);
});
