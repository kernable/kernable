// The thin slice: the kit end to end against a real kernel (`cargo build` first). A shop
// app declares a relation; people register, verify, sign in, open a tenant, form groups, and the
// authorization middleware decides.

import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Command, Event, Kernable, Rejected, domain, type RequestContext } from "@kernable/sdk";

import { TestKernel } from "../../../sdks/typescript/test/kernel.ts";
import { FindAccountByEmail } from "../src/account/index.ts";
import { KitApi } from "../src/api/api.ts";
import { newSigningKeys, type Claims } from "../src/api/tokens.ts";
import { bootstrap } from "../src/bootstrap.ts";
import { CreateGroup } from "../src/group/index.ts";
import { CreateIdentity, GrantMembership } from "../src/identity/index.ts";
import { kitDomains, kitSchemas, relation } from "../src/kit.ts";
import { MemoryMailer, useMailer } from "../src/shared/mail.ts";
import { CreateTenant } from "../src/tenancy/index.ts";

// --- A small app: orders that belong to whoever placed them --------------------------------------

const shop = domain("shop");
class OrderPlaced extends Event {
  customer!: string;
}
class OrderCancelled extends Event {}
class PlaceOrder extends Command {}
class CancelOrder extends Command {}
type Order = { status: "none" | "placed" | "cancelled"; customer?: string };
shop
  .aggregate<Order>("order", { status: "none" })
  .apply(OrderPlaced, (_s, d) => ({ status: "placed", customer: d.customer }))
  .apply(OrderCancelled, (s) => void (s.status = "cancelled"))
  .command(PlaceOrder, (s, _d, _cmd, ctx) => {
    if (s.status !== "none") throw new Rejected("The order exists", "exists");
    return [OrderPlaced.new({ customer: ctx.request.senderIdentity! })];
  })
  .command(CancelOrder, (s) => {
    if (s.status !== "placed") throw new Rejected("Not placed", "invalid_state");
    return [OrderCancelled.new()];
  });
relation("shop.order", "owner").set("shop.OrderPlaced", (e) => e.data.customer);

// --- Setup ---------------------------------------------------------------------------------------

const mailer = new MemoryMailer();
useMailer(mailer);
const PASSWORD = "correct horse battery";
let kernel: TestKernel;
let kit: Kernable;
let app: ReturnType<Kernable["app"]>;
let api: KitApi;
let tenant: string;

before(async () => {
  kernel = new TestKernel(["tenancy.*", "account.*", "identity.*", "group.*", "shop.*"]);
  await kernel.start();
  const cli = (...args: string[]) => assert.ok(kernel.cli(...args).ok, args.join(" "));
  cli("keys", "new", "kit");
  cli(
    "grant", "root.key", "kit.pub",
    "run=tenancy.*,account.*,identity.*,group.*,shop.*",
    ...["submit", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage", "reserved_order"].map((a) => `${a}=*`),
    "privileged=bypass_authorization",
  );
  const options = { url: kernel.url, key: join(kernel.dir, "kit.key") };
  kit = new Kernable(options).use(...kitSchemas());
  app = new Kernable(options).app({ service: "kit", domains: [...kitDomains(), shop] });
  void app.run();
  await app.ready;
  const { api: service } = await bootstrap(kit, { signUp: { tenants: "active", accounts: "open" } });
  api = new KitApi({ kernable: kit, keys: newSigningKeys(), service });
});

after(async () => {
  app?.stop();
  kit?.close();
  await kernel?.dispose();
});

async function person(email: string): Promise<{ claims: Claims; accountId: string }> {
  await api.register({ email, name: email.split("@")[0], password: PASSWORD });
  const mail = mailer.last(email);
  assert.ok(mail, `a verification mail to ${email}`);
  assert.equal(await api.verifyEmail(mail.token), true);
  const signedIn = await api.signIn({ email, password: PASSWORD, ip: "203.0.113.7" });
  assert.ok(signedIn.ok, JSON.stringify(signedIn));
  return { claims: api.check(signedIn.accessToken)!, accountId: signedIn.accountId };
}

function send(command: Command, aggregateId: string, context: RequestContext) {
  return kit.submit(command, { aggregateId, tenant, context });
}

// --- The slice -----------------------------------------------------------------------------------

test("register, verify, sign in, open a tenant, and let groups and relations decide", async () => {
  // Signing in before verifying fails without saying more; a wrong password counts as a failure.
  await api.register({ email: "early@example.com", name: "Early", password: PASSWORD });
  assert.deepEqual(await api.signIn({ email: "early@example.com", password: PASSWORD }), { ok: false, reason: "not_verified" });
  assert.deepEqual(await api.signIn({ email: "nobody@example.com", password: PASSWORD }), { ok: false, reason: "wrong_password" });

  const alice = await person("alice@example.com");
  const bob = await person("bob@example.com");
  const carol = await person("carol@example.com");

  // Alice opens a tenant herself (self-service sign-up is on); the kit makes her its owner.
  tenant = crypto.randomUUID();
  const asAccount = await api.contextFor(alice.claims);
  const created = await kit.submit(CreateTenant.new({ slug: "acme", name: "Acme", owner: { accountId: alice.accountId }, status: "active" }), {
    aggregateId: tenant, tenant, context: asAccount,
  });
  assert.ok(created.committed, JSON.stringify(created));
  const asAlice = await api.contextFor(alice.claims, tenant);
  assert.ok(asAlice.senderIdentity);

  // A slug is taken across tenants.
  const other = crypto.randomUUID();
  const taken = await kit.submit(CreateTenant.new({ slug: "acme", name: "Acme 2", owner: { accountId: bob.accountId }, status: "active" }), {
    aggregateId: other, tenant: other, context: await api.contextFor(bob.claims),
  });
  assert.equal(taken.committed, false);

  // Bob and Carol join (the invitation comes later: here the kit's reactor path, privileged).
  const join = async (accountId: string) => {
    const identityId = crypto.randomUUID();
    const out = await send(CreateIdentity.new({ accountId }), identityId, { bypassAuthorization: true });
    assert.ok(out.committed, JSON.stringify(out));
    return identityId;
  };
  await join(bob.accountId);
  await join(carol.accountId);
  const asBob = await api.contextFor(bob.claims, tenant);
  const asCarol = await api.contextFor(carol.claims, tenant);

  // Without a group, Bob may do nothing in the shop.
  const refused = await send(PlaceOrder.new(), "o0", asBob);
  assert.equal(refused.committed, false);
  assert.equal(!refused.committed && refused.code, "forbidden");

  // Alice (an owner) forms "customers": place orders, cancel only your own.
  const customers = crypto.randomUUID();
  const group = await send(CreateGroup.new({
    name: "Customers", permissions: [{ pattern: "shop.PlaceOrder" }, { pattern: "shop.CancelOrder", when: "owner" }],
  }), customers, asAlice);
  assert.ok(group.committed, JSON.stringify(group));
  for (const who of [asBob, asCarol]) {
    const granted = await send(GrantMembership.new({ membershipId: crypto.randomUUID(), group: customers }), who.senderIdentity!, asAlice);
    assert.ok(granted.committed, JSON.stringify(granted));
  }

  // Bob places an order; Carol may not cancel it, Bob may.
  assert.ok((await send(PlaceOrder.new(), "o1", asBob)).committed);
  const notHers = await send(CancelOrder.new(), "o1", asCarol);
  assert.equal(!notHers.committed && notHers.code, "forbidden");
  assert.ok((await send(CancelOrder.new(), "o1", asBob)).committed);

  // No escalation: Bob may not grant memberships at all, nor hand out groups.
  const escalate = await send(GrantMembership.new({ membershipId: crypto.randomUUID(), group: customers }), asCarol.senderIdentity!, asBob);
  assert.equal(!escalate.committed && escalate.code, "forbidden");

  // Nobody signed in: refused; another tenant's identity: refused.
  const anonymous = await send(PlaceOrder.new(), "o2", {});
  assert.equal(!anonymous.committed && anonymous.code, "unauthenticated");
  const wrongTenant = await send(PlaceOrder.new(), "o2", { ...asBob, senderTenant: other });
  assert.equal(!wrongTenant.committed && wrongTenant.code, "forbidden");

  // Signing out ends the session at once, although the access token is still valid.
  await api.signOut(bob.claims);
  const afterSignOut = await send(PlaceOrder.new(), "o3", asBob);
  assert.equal(!afterSignOut.committed && afterSignOut.code, "unauthenticated");

  // Lookups are the API's only: a signed-in person cannot ask for accounts by email.
  await assert.rejects(
    kit.query(FindAccountByEmail.new({ email: "alice@example.com" }), { tenant: "platform", context: asAlice }),
    (err: Error & { code?: string }) => (err as Rejected).code === "forbidden",
  );
});
