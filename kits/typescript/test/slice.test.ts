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
import { AddSsoDomain, CreateSso, DeleteAccount, EnforceSso, GetSso, SuspendAccount } from "../src/account/index.ts";
import { CreateServiceIdentity, DeleteIdentity, GetIdentity, GrantMembership, RevokeApiKey } from "../src/identity/index.ts";
import { CreateInvitation, EraseInvitation, ListInvitations } from "../src/invitation/index.ts";
import { kitDomains, kitSchemas, relation, useAuthorizationStore } from "../src/kit.ts";
import { MemoryMailer, useMailer } from "../src/shared/mail.ts";
import { totpCode } from "../src/shared/secrets.ts";
import { Authenticator } from "./authenticator.ts";
import { OidcProvider } from "./oidc-provider.ts";
import { CreateTenant, DeleteTenant, GetTenant, SuspendTenant } from "../src/tenancy/index.ts";

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
/** The API's own identity and key, from the bootstrap. */
let service: { identityId: string; keyId: string };
let tenant: string;
/** From the first test, for the second. */
let acme: { alice: RequestContext; bobIdentity: string };

before(async () => {
  kernel = new TestKernel(["tenancy.*", "account.*", "identity.*", "group.*", "invitation.*", "shop.*"]);
  await kernel.start();
  const cli = (...args: string[]) => assert.ok(kernel.cli(...args).ok, args.join(" "));
  cli("keys", "new", "kit");
  cli(
    "grant", "root.key", "kit.pub",
    "run=tenancy.*,account.*,identity.*,group.*,invitation.*,shop.*",
    ...["submit", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage", "reserved_order", "cache"].map((a) => `${a}=*`),
    "privileged=bypass_authorization",
  );
  const options = { url: kernel.url, key: join(kernel.dir, "kit.key") };
  kit = new Kernable(options).use(...kitSchemas());
  useAuthorizationStore(`sqlite:${join(kernel.dir, "authorization.db")}`);
  await startApp();
  const booted = await bootstrap(kit, {
    signUp: { tenants: "active", accounts: "open" },
    admin: { email: "admin@example.com", name: "Admin", password: PASSWORD },
  });
  service = booted.api;
  api = new KitApi({ kernable: kit, keys: newSigningKeys(), service });
});

after(async () => {
  app?.stop();
  kit?.close();
  await kernel?.dispose();
});

async function startApp(): Promise<void> {
  app = new Kernable({ url: kernel.url, key: join(kernel.dir, "kit.key") }).app({ service: "kit", domains: [...kitDomains(), shop] });
  void app.run();
  await app.ready;
}

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

  // Alice invites Bob (signed in already) and Dana (new: she registers by accepting).
  const invite = async (email: string) => {
    const out = await send(CreateInvitation.new({ email }), crypto.randomUUID(), asAlice);
    assert.ok(out.committed, JSON.stringify(out));
    return mailer.last(email)!.token;
  };
  const bobsInvitation = await invite("bob@example.com");
  // Carol may not use Bob's invitation: it is for another address.
  assert.deepEqual(await api.acceptInvitation(bobsInvitation, { claims: carol.claims }), { ok: false, reason: "wrong_email" });
  assert.ok((await api.acceptInvitation(bobsInvitation, { claims: bob.claims })).ok);
  // A used invitation is gone.
  assert.equal((await api.acceptInvitation(bobsInvitation, { claims: bob.claims })).ok, false);
  const carolsInvitation = await invite("carol@example.com");
  assert.ok((await api.acceptInvitation(carolsInvitation, { claims: carol.claims })).ok);
  const danasInvitation = await invite("dana@example.com");
  const dana = await api.acceptInvitation(danasInvitation, { name: "Dana", password: PASSWORD });
  assert.ok(dana.ok, JSON.stringify(dana));
  const danaSignedIn = await api.signIn({ email: "dana@example.com", password: PASSWORD });
  assert.ok(danaSignedIn.ok, "Dana signs in without verifying: the invitation proved her email");
  const asDana = await api.contextFor(api.check(danaSignedIn.accessToken)!, tenant);
  assert.ok(asDana.senderIdentity);
  // Members may not hand out owner invitations.
  const ownerInvite = await send(CreateInvitation.new({ email: "eve@example.com", owner: true }), crypto.randomUUID(), await api.contextFor(bob.claims, tenant));
  assert.equal(!ownerInvite.committed && ownerInvite.code, "forbidden");
  const asBob = await api.contextFor(bob.claims, tenant);
  const asCarol = await api.contextFor(carol.claims, tenant);
  acme = { alice: asAlice, bobIdentity: asBob.senderIdentity! };

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

test("suspended and deleted: sessions end at once, and a deleted tenant leaves no identity, invitation or slug behind", async () => {
  const admin = await api.signIn({ email: "admin@example.com", password: PASSWORD });
  assert.ok(admin.ok);
  const asAdmin = await api.contextFor(api.check(admin.accessToken)!, "platform");
  const asPlatform = (id: string, t = "platform") => ({ aggregateId: id, tenant: t, context: asAdmin });

  // An account the platform suspends is signed out at once, and cannot sign in.
  const eve = await person("eve@example.com");
  assert.ok((await kit.submit(SuspendAccount.new({ reason: "abuse" }), asPlatform(eve.accountId))).committed);
  const evesTenant = crypto.randomUUID();
  const refused = await kit.submit(CreateTenant.new({ slug: "eves", name: "Eve's", owner: { accountId: eve.accountId } }), {
    aggregateId: evesTenant, tenant: evesTenant, context: await api.contextFor(eve.claims),
  });
  assert.equal(!refused.committed && refused.code, "unauthenticated");
  assert.deepEqual(await api.signIn({ email: "eve@example.com", password: PASSWORD }), { ok: false, reason: "not_active" });
  // Deleting it frees the address: Eve can register again.
  assert.ok((await kit.submit(DeleteAccount.new(), asPlatform(eve.accountId))).committed);
  await person("eve@example.com");

  // Alice suspends Acme for herself, the platform deletes it.
  const open = crypto.randomUUID();
  assert.ok((await kit.submit(CreateInvitation.new({ email: "frank@example.com" }), { aggregateId: open, tenant, context: acme.alice })).committed);
  assert.ok((await kit.submit(SuspendTenant.new({ reason: "owner_requested" }), { aggregateId: tenant, tenant, context: acme.alice })).committed);
  const deleted = await kit.submit(DeleteTenant.new(), asPlatform(tenant, tenant));
  assert.ok(deleted.committed, JSON.stringify(deleted));

  // The cleanup erased Bob's identity and the open invitation (nothing left to delete or erase) ...
  const internal = { bypassAuthorization: true };
  const again = await kit.submit(DeleteIdentity.new(), { aggregateId: acme.bobIdentity, tenant, context: internal });
  assert.equal(!again.committed && again.code, "not_found");
  const erased = await kit.submit(EraseInvitation.new(), { aggregateId: open, tenant, context: internal });
  assert.equal(!erased.committed && erased.code, "not_found");
  // ... the read side forgot the tenant, and its slug is free.
  await assert.rejects(kit.query(GetTenant.new({ tenantId: tenant }), { tenant, context: asAdmin }), (e: Rejected) => e.code === "not_found");
  assert.equal((await kit.query(ListInvitations.new(), { tenant, context: internal })).length, 0);
  const reuse = crypto.randomUUID();
  const bobAgain = await api.signIn({ email: "bob@example.com", password: PASSWORD });
  assert.ok(bobAgain.ok);
  const acme2 = await kit.submit(CreateTenant.new({ slug: "acme", name: "Acme again", owner: { accountId: bobAgain.accountId } }), {
    aggregateId: reuse, tenant: reuse, context: await api.contextFor(api.check(bobAgain.accessToken)!),
  });
  assert.ok(acme2.committed, JSON.stringify(acme2));
});

test("the middleware resumes from its store after a restart, and decides as before", async () => {
  const grace = await person("grace@example.com");
  const graces = crypto.randomUUID();
  const opened = await kit.submit(CreateTenant.new({ slug: "graces", name: "Grace's", owner: { accountId: grace.accountId } }), {
    aggregateId: graces, tenant: graces, context: await api.contextFor(grace.claims),
  });
  assert.ok(opened.committed, JSON.stringify(opened));
  const asGrace = await api.contextFor(grace.claims, graces);
  const order = (id: string) => kit.submit(PlaceOrder.new(), { aggregateId: id, tenant: graces, context: asGrace });
  assert.ok((await order("g1")).committed);

  app.stop();
  await new Promise((r) => setTimeout(r, 200));
  // The knowledge is in the store file, with the position it continues from.
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(join(kernel.dir, "authorization.db"), { readOnly: true });
  const position = Number((db.prepare("SELECT value FROM meta WHERE key = 'position'").get() as { value: string }).value);
  const stored = db.prepare("SELECT value FROM data WHERE ns = 'tenants' AND key = ?").get(graces) as { value: string } | undefined;
  db.close();
  assert.ok(position > 0, "a stored position");
  assert.ok(stored && JSON.parse(stored.value).owners.length === 1, "Grace's tenant and its owner in the store");
  await startApp();
  // The owner still may, an ended session still may not: both from the stored knowledge.
  assert.ok((await order("g2")).committed);
  await api.signOut(grace.claims);
  const after = await order("g3");
  assert.equal(!after.committed && after.code, "unauthenticated");
});

test("account flows: refresh and reuse, password change and reset, email change, a taken address", async () => {
  const email = "frank@example.com";
  await person(email);
  const signIn = async (address = email, password = PASSWORD) => {
    const s = await api.signIn({ email: address, password });
    assert.ok(s.ok, JSON.stringify(s));
    return s;
  };
  const one = await signIn();
  const two = await signIn();
  const three = await signIn();

  // A refresh rotates the token; the old one again is a reuse and ends the session.
  const refreshed = await api.refresh(one.refreshToken);
  assert.ok(refreshed.ok && api.check(refreshed.accessToken)?.sid === one.sessionId);
  assert.deepEqual(await api.refresh(one.refreshToken), { ok: false, reason: "reuse" });
  assert.equal((await api.refresh(refreshed.ok ? refreshed.refreshToken : "")).ok, false, "the session ended with the reuse");

  // A password change keeps its own session and ends the others.
  assert.deepEqual(await api.changePassword(api.check(two.accessToken)!, "wrong password!", "another horse battery"), { ok: false, reason: "wrong_password" });
  assert.deepEqual(await api.changePassword(api.check(two.accessToken)!, PASSWORD, "another horse battery"), { ok: true });
  const twoAgain = await api.refresh(two.refreshToken);
  assert.ok(twoAgain.ok, "the changing session stays");
  assert.equal((await api.refresh(three.refreshToken)).ok, false, "the other session ended");
  await signIn(email, "another horse battery");

  // A reset by mail sets a new password and ends every session; asking for nobody sends nothing.
  const before = mailer.sent.length;
  await api.requestPasswordReset("nobody@example.com");
  assert.equal(mailer.sent.length, before);
  await api.requestPasswordReset(email);
  const reset = mailer.last(email)!;
  assert.equal(reset.purpose, "reset");
  assert.deepEqual(await api.resetPassword(reset.token, "third horse battery"), { ok: true });
  assert.equal((await api.refresh(twoAgain.ok ? twoAgain.refreshToken : "")).ok, false, "every session ended");
  assert.deepEqual(await api.signIn({ email, password: "another horse battery" }), { ok: false, reason: "wrong_password" });
  const four = await signIn(email, "third horse battery");

  // A new address is confirmed from that address; the old one then signs in no more.
  assert.deepEqual(await api.requestEmailChange(api.check(four.accessToken)!, "frank@new.example"), { ok: true });
  const change = mailer.last("frank@new.example")!;
  assert.equal(change.purpose, "email_change");
  assert.deepEqual(await api.confirmEmailChange(change.token), { ok: true });
  await signIn("frank@new.example", "third horse battery");
  assert.deepEqual(await api.signIn({ email, password: "third horse battery" }), { ok: false, reason: "wrong_password" });

  // Registering with a taken address answers as usual; the account's owner gets a mail.
  const again = await api.register({ email: "frank@new.example", name: "Not Frank", password: PASSWORD });
  assert.ok("accountId" in again);
  assert.equal(mailer.last("frank@new.example")!.purpose, "account_exists");
});

test("a second factor: TOTP with recovery codes; and a service with an API key", async () => {
  const email = "henry@example.com";
  const first = await person(email);
  const totp = await api.startTotp(first.claims, "Kit", email);
  assert.ok(totp.ok && totp.uri.startsWith("otpauth://totp/"));
  const step = Math.floor(Date.now() / 30_000);
  const confirmed = await api.confirmTotp(first.claims, totpCode(totp.secret, step));
  assert.ok(confirmed.ok && confirmed.recoveryCodes.length === 10);

  // Signing in now needs the second factor before anything else.
  const signedIn = await api.signIn({ email, password: PASSWORD });
  assert.ok(signedIn.ok && signedIn.secondFactor);
  const claims = api.check(signedIn.accessToken)!;
  const henrys = crypto.randomUUID();
  const open = () => kit.submit(CreateTenant.new({ slug: "henrys", name: "Henry's", owner: { accountId: claims.sub } }), {
    aggregateId: henrys, tenant: henrys, context: { senderAccount: claims.sub, senderSession: claims.sid },
  });
  const refused = await open();
  assert.equal(!refused.committed && refused.code, "mfa_required");
  // The confirming code counts once; the next step's code, or a recovery code, proves it.
  assert.deepEqual(await api.proveSecondFactor(claims, totpCode(totp.secret, step)), { ok: false, reason: "wrong_code" });
  const proved = await api.proveSecondFactor(claims, confirmed.recoveryCodes[0]);
  assert.ok(proved.ok);
  assert.ok((await open()).committed);
  const again = await api.signIn({ email, password: PASSWORD });
  assert.ok(again.ok);
  assert.ok((await api.proveSecondFactor(api.check(again.accessToken)!, totpCode(totp.secret, step + 1))).ok);
  assert.deepEqual(await api.proveSecondFactor(api.check(again.accessToken)!, confirmed.recoveryCodes[0]), { ok: false, reason: "wrong_code" });

  // Henry, now an owner, gives a service an API key, and a group to act in.
  const asHenry = { senderAccount: claims.sub, senderSession: claims.sid, senderTenant: henrys, senderIdentity: (await api.contextFor(claims, henrys)).senderIdentity };
  const service = crypto.randomUUID();
  assert.ok((await kit.submit(CreateServiceIdentity.new({ displayName: "Billing" }), { aggregateId: service, tenant: henrys, context: asHenry })).committed);
  const orders = crypto.randomUUID();
  assert.ok((await kit.submit(CreateGroup.new({ name: "Orders", permissions: [{ pattern: "shop.PlaceOrder" }] }), { aggregateId: orders, tenant: henrys, context: asHenry })).committed);
  assert.ok((await kit.submit(GrantMembership.new({ membershipId: "svc", group: orders }), { aggregateId: service, tenant: henrys, context: asHenry })).committed);
  const created = await api.createApiKey(asHenry, henrys, service, "billing");
  assert.ok(created.ok);
  const asService = await api.checkApiKey(created.key);
  assert.deepEqual(asService, { senderTenant: henrys, senderIdentity: service, senderSession: created.keyId });
  assert.equal(await api.checkApiKey(`${created.key}x`), null, "a wrong key");
  const byKey = (id: string) => kit.submit(PlaceOrder.new(), { aggregateId: id, tenant: henrys, context: asService! });
  assert.ok((await byKey("h1")).committed);

  // Revoked: refused at once, even with the API's cached context.
  assert.ok((await kit.submit(RevokeApiKey.new({ keyId: created.keyId }), { aggregateId: service, tenant: henrys, context: asHenry })).committed);
  const revoked = await byKey("h2");
  assert.equal(!revoked.committed && revoked.code, "unauthenticated");
});

test("passkeys: added against an issued challenge, signing in without a password, a challenge counting once", async () => {
  const email = "ivy@example.com";
  const ivy = await person(email);
  const device = new Authenticator("localhost", "http://localhost:3000", ivy.accountId);
  const creation = await api.passkeyRegistrationOptions(ivy.claims, email);
  assert.deepEqual(await api.addPasskey(ivy.claims, device.register("bm90LWlzc3VlZA") as never), { ok: false, reason: "invalid_challenge" });
  assert.deepEqual(await api.addPasskey(ivy.claims, device.register(creation.challenge) as never, "Laptop"), { ok: true });

  // Without a password: the passkey names the account and counts as two factors.
  const request = await api.passkeySignInOptions();
  const answer = device.assert(request.challenge);
  const signedIn = await api.signInWithPasskey(answer as never);
  assert.ok(signedIn.ok && signedIn.accountId === ivy.accountId, JSON.stringify(signedIn));
  assert.deepEqual(api.check(signedIn.accessToken)!.fac, ["passkey"]);
  // The same answer again: its challenge is used up.
  assert.deepEqual(await api.signInWithPasskey(answer as never), { ok: false, reason: "wrong_password" });
  // A passkey answer from another site's page fails.
  const other = await api.passkeySignInOptions();
  assert.equal((await api.signInWithPasskey(device.assert(other.challenge, "http://evil.example") as never)).ok, false);

  // The passkey session acts like any other.
  const claims = api.check(signedIn.accessToken)!;
  const ivys = crypto.randomUUID();
  const opened = await kit.submit(CreateTenant.new({ slug: "ivys", name: "Ivy's", owner: { accountId: claims.sub } }), {
    aggregateId: ivys, tenant: ivys, context: { senderAccount: claims.sub, senderSession: claims.sid },
  });
  assert.ok(opened.committed, JSON.stringify(opened));
});

test("OAuth and SSO: a social login registers or signs in, links on request; a tenant's SSO verified by DNS and enforced", async () => {
  const social = new OidcProvider();
  const company = new OidcProvider();
  await social.start();
  await company.start();
  const txt = new Map<string, string>();
  const sso = new KitApi({
    kernable: kit, keys: newSigningKeys(), service,
    providers: { social: { issuer: social.issuer, clientId: social.clientId, clientSecret: social.clientSecret } },
    redirectUri: "http://localhost:3000/callback", insecureIssuers: true,
    resolveTxt: async (name) => (txt.has(name) ? [[txt.get(name)!]] : []),
  });
  try {
    // A social login of a new person registers and links them; the second time signs them in.
    const jack = { sub: "s-jack", email: "jack@example.com", email_verified: true };
    const start = async (by: { provider: string } | { email: string }, link?: Parameters<KitApi["oidcStart"]>[1]) => {
      const s = await sso.oidcStart(by, link);
      assert.ok(s.ok, JSON.stringify(s));
      return s.url;
    };
    const first = await sso.oidcFinish(social.login(await start({ provider: "social" }), jack));
    assert.ok(first.ok && "accountId" in first, JSON.stringify(first));
    const second = await sso.oidcFinish(social.login(await start({ provider: "social" }), jack));
    assert.ok(second.ok && "accountId" in second && second.accountId === first.accountId);
    // A callback used once does not work twice.
    const url = social.login(await start({ provider: "social" }), jack);
    assert.ok((await sso.oidcFinish(url)).ok);
    assert.deepEqual(await sso.oidcFinish(url), { ok: false, reason: "invalid_state" });

    // A social login whose email has an account is not linked by itself: sign in and link it.
    const kate = await person("kate@example.com");
    const kateSocial = { sub: "s-kate", email: "kate@example.com", email_verified: true };
    assert.deepEqual(await sso.oidcFinish(social.login(await start({ provider: "social" }), kateSocial)), { ok: false, reason: "not_linked" });
    assert.deepEqual(await sso.oidcFinish(social.login(await start({ provider: "social" }, kate.claims), kateSocial)), { ok: true, linked: true });
    const kateIn = await sso.oidcFinish(social.login(await start({ provider: "social" }), kateSocial));
    assert.ok(kateIn.ok && "accountId" in kateIn && kateIn.accountId === kate.accountId);

    // Liam's tenant sets up SSO: the domain proven by DNS, then enforced.
    const liam = await person("liam@lumen.example");
    const lumen = crypto.randomUUID();
    assert.ok((await kit.submit(CreateTenant.new({ slug: "lumen", name: "Lumen", owner: { accountId: liam.accountId } }), {
      aggregateId: lumen, tenant: lumen, context: await api.contextFor(liam.claims),
    })).committed);
    const asLiam = await api.contextFor(liam.claims, lumen);
    const ssoId = crypto.randomUUID();
    const at = (c: Parameters<typeof kit.submit>[0]) => kit.submit(c as never, { aggregateId: ssoId, tenant: lumen, context: asLiam });
    assert.ok((await at(CreateSso.new({ issuer: company.issuer, clientId: company.clientId, clientSecret: company.clientSecret }))).committed);
    assert.ok((await at(AddSsoDomain.new({ domain: "lumen.example" }))).committed);
    assert.deepEqual(await sso.verifySsoDomain(lumen, ssoId, "lumen.example"), { ok: false, reason: "not_verified" });
    const config = await kit.query(GetSso.new({ ssoId }), { tenant: lumen, context: asLiam });
    txt.set("_kernable-sso.lumen.example", config.domains[0].txt);
    assert.deepEqual(await sso.verifySsoDomain(lumen, ssoId, "lumen.example"), { ok: true });
    assert.ok((await at(EnforceSso.new({ enforced: true }))).committed);

    // Mia of lumen.example joins with a password; in Lumen only an SSO session counts.
    const mia = await person("mia@lumen.example");
    await kit.submit(CreateInvitation.new({ email: "mia@lumen.example" }), { aggregateId: crypto.randomUUID(), tenant: lumen, context: asLiam });
    assert.ok((await api.acceptInvitation(mailer.last("mia@lumen.example")!.token, { claims: mia.claims })).ok);
    const asMia = await api.contextFor(mia.claims, lumen);
    const own = (context: typeof asMia) => kit.query(GetIdentity.new({ identityId: context.senderIdentity! }), { tenant: lumen, context });
    await assert.rejects(own(asMia), (e: Rejected) => e.code === "sso_required");
    // Through the company's provider: linked to her account by the verified domain and email.
    const miaSso = await sso.oidcFinish(company.login(await start({ email: "mia@lumen.example" }), { sub: "c-mia", email: "mia@lumen.example", email_verified: true }));
    assert.ok(miaSso.ok && "accountId" in miaSso && miaSso.accountId === mia.accountId, JSON.stringify(miaSso));
    const asMiaSso = await api.contextFor(sso.check(miaSso.accessToken)!, lumen);
    assert.equal((await own(asMiaSso)).id, asMia.senderIdentity);
    // The owner keeps a password session, so a broken SSO can be mended.
    assert.ok((await own(asLiam)).id);
  } finally {
    social.stop();
    company.stop();
  }
});
