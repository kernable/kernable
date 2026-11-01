// The vault: personal fields and secrets are declared on the domain; the SDK keeps them out of
// the signed log, handlers and allowed listeners see them in clear, and an erasing event deletes
// them for good.

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { identityDomain, sha256 } from "./identity.ts";
import { TestKernel } from "./kernel.ts";

test("personal fields and secrets live in the vault and an erasing event deletes them", async () => {
  const kernel = new TestKernel(["identity.*"]);
  await kernel.start();
  const { identity, account } = identityDomain();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey }).use(identity);
  const emails = new Map<string, string>();
  const accounts = kernable.listener(identity.readModel("accounts")
    .on("AccountRegistered", (data: { email: string }, evt) => void emails.set(evt.aggregateId, data.email))
    .on("AccountErased", (_data, evt) => void emails.delete(evt.aggregateId))
    .query("GetEmail", (data: { id: string }) => {
      if (!emails.has(data.id)) throw new Rejected("no such account", "not_found");
      return emails.get(data.id);
    }));
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
    const replayed: [unknown, string[]][] = [];
    const rebuilt = kernable.listener(identity.readModel("rebuilt")
      .on("AccountRegistered", (data: { email: unknown }, evt) => void replayed.push([data.email, evt.redacted])));
    running.push(rebuilt.run());
    await rebuilt.ready;
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(replayed.length, 1);
    assert.deepEqual(replayed[0], [null, ["email"]]);
    rebuilt.stop();
  } finally {
    worker.stop();
    accounts.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a personal field of a command that its event does not declare personal rejects the command", async () => {
  const kernel = new TestKernel(["people.*"]);
  await kernel.start();
  const people = domain("people");
  people.command("Register").personal(["email"], (_d, id) => id);
  people.command("Leak").personal(["email"], (_d, id) => id);
  people.event("Registered", 1).personal(["email"], (_d, id) => id);
  // `Leaked` forgets to declare `email`: written as is, it would go into the signed log in clear.
  const person = people.aggregate("Person", {})
    .command("Register", (_s, data: { email: string }) => [{ type: "Registered", data: { email: data.email } }])
    .command("Leak", (_s, data: { email: string }) => [{ type: "Leaked", data: { email: data.email, other: 1 } }]);
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey }).use(people);
  const worker = kernable.worker(person);
  const running = [worker.run()];
  const send = (type: string) =>
    kernable.submit({ domain: "people", aggregateType: "Person", aggregateId: "p1", type, data: { email: "ann@example.com" }, wait: "committed" });
  try {
    await worker.ready;
    assert.deepEqual(await send("Leak"), {
      committed: false,
      code: "personal_in_clear",
      message: 'Leak declares email personal, but Leaked does not: it would be stored in clear (declare it with Domain.event("Leaked", …).personal)',
    });
    assert.ok((await send("Register")).committed);
  } finally {
    worker.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
