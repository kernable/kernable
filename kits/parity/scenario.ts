// Writes scenario.json: the steps every language's parity driver runs (see README.md). The
// steps follow the TypeScript kit's slice (test/slice.test.ts) and add more checks; `expect`
// holds what a step must answer (fields named must match, arrays whole).
//
//   node kits/parity/scenario.ts

import { writeFileSync } from "node:fs";

type Step = Record<string, unknown> & { op: string };
const steps: Step[] = [];
const PW = "correct horse battery";
const add = (s: Step) => void steps.push(s);
const note = (text: string) => add({ op: "value", value: null, note: text });

const ok = { ok: true };
const committed = { committed: true };
const rejected = (code: string) => ({ committed: false, code });
const refused = (reason: string) => ({ ok: false, reason });
const queryRefused = (code: string) => ({ ok: false, code });

/** A person registers, verifies by mail and signs in: `<as>In` (the sign-in), `<as>` (claims). */
function person(as: string, email: string) {
  add({ op: "register", email, name: email.split("@")[0], password: PW, as: `${as}Reg`, expect: ok });
  add({ op: "mail", to: email, as: `${as}Mail`, expect: { purpose: "verify" } });
  add({ op: "verifyEmail", token: `$${as}Mail.token`, expect: true });
  add({ op: "signIn", email, password: PW, ip: "203.0.113.7", as: `${as}In`, expect: ok });
  add({ op: "check", token: `$${as}In.accessToken`, as });
}
const submit = (command: string, aggregateId: unknown, tenant: unknown, context: unknown, data?: unknown, expect?: unknown, as?: string) =>
  add({ op: "submit", command, aggregateId, tenant, context, ...(data ? { data } : {}), ...(expect ? { expect } : {}), ...(as ? { as } : {}) });
const query = (q: string, tenant: unknown, context: unknown, data?: unknown, expect?: unknown, as?: string) =>
  add({ op: "query", query: q, tenant, context, ...(data ? { data } : {}), ...(expect ? { expect } : {}), ...(as ? { as } : {}) });
const uuid = (as: string) => add({ op: "uuid", as });
const ctx = (as: string, claims: string, tenant?: unknown) => add({ op: "contextFor", claims, ...(tenant !== undefined ? { tenant } : {}), as });
const internal = { bypassAuthorization: true };
const PLATFORM = "platform";
/** A new tenant `<as>` owned by `owner` (claims var), with `as<Name>` the owner's context in it. */
function tenant(as: string, slug: string, name: string, owner: string, ownerCtx: string) {
  uuid(as);
  ctx(`${owner}Account`, `$${owner}`);
  submit("tenancy.tenant.CreateTenant", `$${as}`, `$${as}`, `$${owner}Account`, { slug, name, owner: { accountId: `$${owner}.sub` }, status: "active" }, committed);
  ctx(ownerCtx, `$${owner}`, `$${as}`);
}

// --- Setup ---------------------------------------------------------------------------------------

add({ op: "bootstrap", signUp: { tenants: "active", accounts: "open" }, admin: { email: "admin@example.com", name: "Admin", password: PW }, as: "booted" });

// --- 1. register, verify, sign in, open a tenant, groups and relations decide -------------------

note("1. register, verify, sign in, open a tenant; groups and relations decide");
add({ op: "register", email: "early@example.com", name: "Early", password: PW, expect: ok });
add({ op: "signIn", email: "early@example.com", password: PW, expect: refused("not_verified") });
add({ op: "signIn", email: "nobody@example.com", password: PW, expect: refused("wrong_password") });
person("alice", "alice@example.com");
person("bob", "bob@example.com");
person("carol", "carol@example.com");
tenant("acme", "acme", "Acme", "alice", "asAlice");
uuid("other");
ctx("bobAccount", "$bob");
submit("tenancy.tenant.CreateTenant", "$other", "$other", "$bobAccount", { slug: "acme", name: "Acme 2", owner: { accountId: "$bob.sub" }, status: "active" }, { committed: false });
submit("tenancy.tenant.CreateTenant", "$other", "$other", "$bobAccount", { slug: "Not A Slug", name: "Acme 2", owner: { accountId: "$bob.sub" }, status: "active" }, rejected("invalid_slug"));
submit("tenancy.tenant.CreateTenant", "$other", "$other", "$bobAccount", { slug: "admin", name: "Acme 2", owner: { accountId: "$bob.sub" }, status: "active" }, { committed: false });
submit("tenancy.tenant.CreateTenant", "$other", "$other", "$bobAccount", { slug: "acme-two", name: "  ", owner: { accountId: "$bob.sub" }, status: "active" }, rejected("invalid_name"));
function invite(as: string, email: string, tenantRef = "$acme", context = "$asAlice", extra: Record<string, unknown> = {}) {
  uuid(`${as}Id`);
  submit("invitation.invitation.CreateInvitation", `$${as}Id`, tenantRef, context, { email, ...extra }, committed);
  add({ op: "mail", to: email, as: `${as}Mail`, expect: { purpose: "invitation" } });
}
invite("bobInv", "bob@example.com");
add({ op: "acceptInvitation", token: "$bobInvMail.token", claims: "$carol", expect: refused("wrong_email") });
add({ op: "acceptInvitation", token: "$bobInvMail.token", claims: "$bob", expect: ok });
add({ op: "acceptInvitation", token: "$bobInvMail.token", claims: "$bob", expect: { ok: false } });
invite("carolInv", "carol@example.com");
add({ op: "acceptInvitation", token: "$carolInvMail.token", claims: "$carol", expect: ok });
invite("danaInv", "dana@example.com");
add({ op: "peekInvitation", token: { tamper: "$danaInvMail.token", how: "secret" }, expect: null });
add({ op: "peekInvitation", token: "$danaInvMail.token" });
add({ op: "acceptInvitation", token: "$danaInvMail.token", name: "Dana", password: "short", expect: refused("weak_password") });
add({ op: "acceptInvitation", token: "$danaInvMail.token", name: "Dana", password: PW, expect: ok });
add({ op: "signIn", email: "dana@example.com", password: PW, as: "danaIn", expect: ok });
add({ op: "check", token: "$danaIn.accessToken", as: "dana" });
ctx("asDana", "$dana", "$acme");
ctx("asBob", "$bob", "$acme");
ctx("asCarol", "$carol", "$acme");
uuid("eveInvId");
submit("invitation.invitation.CreateInvitation", "$eveInvId", "$acme", "$asBob", { email: "eve@example.com", owner: true }, rejected("forbidden"));
submit("shop.order.PlaceOrder", "o0", "$acme", "$asBob", undefined, rejected("forbidden"));
uuid("customers");
submit("group.group.CreateGroup", "$customers", "$acme", "$asAlice", { name: "Customers", permissions: [{ pattern: "shop.PlaceOrder" }, { pattern: "shop.CancelOrder", when: "owner" }] }, committed);
uuid("m1");
uuid("m2");
submit("identity.identity.GrantMembership", "$asBob.senderIdentity", "$acme", "$asAlice", { membershipId: "$m1", group: "$customers" }, committed);
submit("identity.identity.GrantMembership", "$asCarol.senderIdentity", "$acme", "$asAlice", { membershipId: "$m2", group: "$customers" }, committed);
submit("shop.order.PlaceOrder", "o1", "$acme", "$asBob", undefined, committed);
submit("shop.order.PlaceOrder", "o1", "$acme", "$asBob", undefined, rejected("exists"));
submit("shop.order.CancelOrder", "o1", "$acme", "$asCarol", undefined, rejected("forbidden"));
submit("shop.order.CancelOrder", "o1", "$acme", "$asBob", undefined, committed);
uuid("m3");
submit("identity.identity.GrantMembership", "$asCarol.senderIdentity", "$acme", "$asBob", { membershipId: "$m3", group: "$customers" }, rejected("forbidden"));
submit("shop.order.PlaceOrder", "o2", "$acme", {}, undefined, rejected("unauthenticated"));
submit("shop.order.PlaceOrder", "o2", "$acme", { merge: ["$asBob", { senderTenant: "$other" }] }, undefined, rejected("forbidden"));
query("group.ListGroups", "$acme", "$asAlice");
query("identity.ListIdentities", "$acme", "$asAlice");
query("invitation.ListInvitations", "$acme", "$asAlice");
query("tenancy.GetTenant", "$acme", "$asAlice", { tenantId: "$acme" });
query("identity.GetIdentity", "$acme", "$asBob", { identityId: "$asBob.senderIdentity" });
add({ op: "signOut", claims: "$bob" });
submit("shop.order.PlaceOrder", "o3", "$acme", "$asBob", undefined, rejected("unauthenticated"));
invite("bobInv2", "bob@example.com");
add({ op: "acceptInvitation", token: "$bobInv2Mail.token", claims: "$bob", expect: refused("unauthenticated") });
query("account.FindAccountByEmail", PLATFORM, "$asAlice", { email: "alice@example.com" }, queryRefused("forbidden"));

// --- 2. suspended and deleted ---------------------------------------------------------------------

note("2. suspended and deleted: sessions end at once; a deleted tenant leaves nothing behind");
add({ op: "signIn", email: "admin@example.com", password: PW, as: "adminIn", expect: ok });
add({ op: "check", token: "$adminIn.accessToken", as: "admin" });
ctx("asAdmin", "$admin", PLATFORM);
person("eve", "eve@example.com");
submit("account.account.SuspendAccount", "$eve.sub", PLATFORM, "$asAdmin", { reason: "abuse" }, committed);
uuid("evesTenant");
ctx("eveAccount", "$eve");
submit("tenancy.tenant.CreateTenant", "$evesTenant", "$evesTenant", "$eveAccount", { slug: "eves", name: "Eve's", owner: { accountId: "$eve.sub" } }, rejected("unauthenticated"));
add({ op: "signIn", email: "eve@example.com", password: PW, expect: refused("not_active") });
submit("account.account.DeleteAccount", "$eve.sub", PLATFORM, "$asAdmin", undefined, committed);
person("eve2", "eve@example.com");
uuid("openInv");
submit("invitation.invitation.CreateInvitation", "$openInv", "$acme", "$asAlice", { email: "frank@example.com" }, committed);
add({ op: "mail", to: "frank@example.com", as: "frankInvMail" });
submit("tenancy.tenant.SuspendTenant", "$acme", "$acme", "$asAlice", { reason: "owner_requested" }, committed);
submit("shop.order.PlaceOrder", "o4", "$acme", "$asAlice", undefined, { committed: false });
submit("tenancy.tenant.DeleteTenant", "$acme", "$acme", "$asAdmin", undefined, committed);
submit("identity.identity.DeleteIdentity", "$asBob.senderIdentity", "$acme", internal, undefined, rejected("not_found"));
submit("invitation.invitation.EraseInvitation", "$openInv", "$acme", internal, undefined, rejected("not_found"));
query("tenancy.GetTenant", "$acme", "$asAdmin", { tenantId: "$acme" }, queryRefused("not_found"));
query("invitation.ListInvitations", "$acme", internal, undefined, { ok: true, value: [] });
add({ op: "signIn", email: "bob@example.com", password: PW, as: "bobIn2", expect: ok });
add({ op: "check", token: "$bobIn2.accessToken", as: "bob2" });
uuid("reuse");
ctx("bob2Account", "$bob2");
submit("tenancy.tenant.CreateTenant", "$reuse", "$reuse", "$bob2Account", { slug: "acme", name: "Acme again", owner: { accountId: "$bob2.sub" } }, committed);

// --- 3. the middleware resumes from its store after a restart --------------------------------------

note("3. the host restarts; the middleware decides as before");
person("grace", "grace@example.com");
tenant("graces", "graces", "Grace's", "grace", "asGrace");
submit("shop.order.PlaceOrder", "g1", "$graces", "$asGrace", undefined, committed);
add({ op: "restartHost" });
submit("shop.order.PlaceOrder", "g2", "$graces", "$asGrace", undefined, committed);
add({ op: "signOut", claims: "$grace" });
submit("shop.order.PlaceOrder", "g3", "$graces", "$asGrace", undefined, rejected("unauthenticated"));

// --- 4. account flows ------------------------------------------------------------------------------

note("4. account flows: refresh and reuse, password change and reset, email change, a taken address");
person("frank", "frank@example.com");
add({ op: "signIn", email: "frank@example.com", password: PW, as: "one", expect: ok });
add({ op: "signIn", email: "frank@example.com", password: PW, as: "two", expect: ok });
add({ op: "signIn", email: "frank@example.com", password: PW, as: "three", expect: ok });
add({ op: "refresh", token: "$one.refreshToken", as: "oneRefreshed", expect: ok });
add({ op: "check", token: "$oneRefreshed.accessToken", expect: { sid: "$one.sessionId" } });
add({ op: "refresh", token: { concat: ["$one.sessionId", ".1.made-up-secret"] }, expect: refused("invalid_token") });
add({ op: "refresh", token: { concat: ["$one.sessionId", ".x.made-up-secret"] }, expect: refused("invalid_token") });
add({ op: "refresh", token: "$one.refreshToken", expect: refused("reuse") });
add({ op: "refresh", token: "$oneRefreshed.refreshToken", expect: { ok: false } });
add({ op: "check", token: "$two.accessToken", as: "twoClaims" });
add({ op: "changePassword", claims: "$twoClaims", current: "wrong password!", next: "another horse battery", expect: refused("wrong_password") });
add({ op: "changePassword", claims: "$twoClaims", current: PW, next: "short", expect: { ok: false } });
add({ op: "changePassword", claims: "$twoClaims", current: PW, next: "another horse battery", expect: ok });
add({ op: "refresh", token: "$two.refreshToken", as: "twoAgain", expect: ok });
add({ op: "refresh", token: "$three.refreshToken", expect: { ok: false } });
add({ op: "signIn", email: "frank@example.com", password: "another horse battery", expect: ok });
add({ op: "mailCount", as: "before" });
add({ op: "requestPasswordReset", email: "nobody@example.com" });
add({ op: "mailCount", expect: "$before" });
add({ op: "requestPasswordReset", email: "frank@example.com" });
add({ op: "mail", to: "frank@example.com", as: "reset", expect: { purpose: "reset" } });
add({ op: "mailCount", as: "mailed" });
add({ op: "requestPasswordReset", email: "frank@example.com" });
add({ op: "mailCount", expect: "$mailed" });
add({ op: "resetPassword", token: { tamper: "$reset.token", how: "lastChar" }, expect: { ok: false } });
add({ op: "resetPassword", token: "$reset.token", next: "third horse battery", expect: ok });
add({ op: "resetPassword", token: "$reset.token", next: "fourth horse battery", expect: { ok: false } });
add({ op: "refresh", token: "$twoAgain.refreshToken", expect: { ok: false } });
add({ op: "signIn", email: "frank@example.com", password: "another horse battery", expect: refused("wrong_password") });
add({ op: "signIn", email: "frank@example.com", password: "third horse battery", as: "four", expect: ok });
add({ op: "check", token: "$four.accessToken", as: "fourClaims" });
add({ op: "requestEmailChange", claims: "$fourClaims", email: "alice@example.com", expect: { ok: true } });
add({ op: "requestEmailChange", claims: "$fourClaims", email: "frank@new.example", expect: ok });
add({ op: "mail", to: "frank@new.example", as: "change", expect: { purpose: "email_change" } });
add({ op: "confirmEmailChange", token: "$change.token", expect: ok });
add({ op: "confirmEmailChange", token: "$change.token", expect: { ok: false } });
add({ op: "signIn", email: "frank@new.example", password: "third horse battery", expect: ok });
add({ op: "signIn", email: "frank@example.com", password: "third horse battery", expect: refused("wrong_password") });
add({ op: "register", email: "frank@new.example", name: "Not Frank", password: PW, expect: ok });
add({ op: "mail", to: "frank@new.example", expect: { purpose: "account_exists" } });
add({ op: "mailCount", as: "told" });
add({ op: "register", email: "frank@new.example", name: "Not Frank", password: PW, expect: ok });
add({ op: "mailCount", expect: "$told" });
add({ op: "register", email: "not an address", name: "X", password: PW, expect: { ok: false } });
add({ op: "register", email: "weak@example.com", name: "Weak", password: "short", expect: refused("weak_password") });
person("gus", "gus@example.com");
for (let i = 0; i < 5; i++) add({ op: "signIn", email: "gus@example.com", password: "wrong horse battery", expect: refused("wrong_password") });
add({ op: "signIn", email: "gus@example.com", password: PW, expect: refused("wrong_password") });
add({ op: "signIn", email: "nobody-at-all@example.com", password: PW, expect: refused("wrong_password") });

// --- 5. a second factor, and a service with an API key ------------------------------------------

note("5. TOTP with recovery codes; a service with an API key");
person("henry", "henry@example.com");
add({ op: "startTotp", claims: "$henry", issuer: "Kit", label: "henry@example.com", as: "totp", expect: ok });
add({ op: "totpStep", as: "step" });
add({ op: "confirmTotp", claims: "$henry", code: { totp: "$totp.secret", at: "$step" }, as: "confirmed", expect: ok });
add({ op: "signIn", email: "henry@example.com", password: PW, as: "henryIn", expect: { ok: true, secondFactor: true } });
add({ op: "check", token: "$henryIn.accessToken", as: "henry2" });
uuid("henrys");
submit("tenancy.tenant.CreateTenant", "$henrys", "$henrys", { senderAccount: "$henry2.sub", senderSession: "$henry2.sid" }, { slug: "henrys", name: "Henry's", owner: { accountId: "$henry2.sub" } }, rejected("mfa_required"));
add({ op: "joinWithInvitationLink", claims: "$henry2", token: { concat: ["link_", "$henrys", ".l1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"] }, expect: refused("mfa_required") });
add({ op: "proveSecondFactor", claims: "$henry2", code: { totp: "$totp.secret", at: "$step" }, expect: refused("wrong_code") });
add({ op: "proveSecondFactor", claims: "$henry2", code: "000000", expect: refused("wrong_code") });
add({ op: "proveSecondFactor", claims: "$henry2", code: "$confirmed.recoveryCodes.0", expect: ok });
submit("tenancy.tenant.CreateTenant", "$henrys", "$henrys", { senderAccount: "$henry2.sub", senderSession: "$henry2.sid" }, { slug: "henrys", name: "Henry's", owner: { accountId: "$henry2.sub" } }, committed);
add({ op: "signIn", email: "henry@example.com", password: PW, as: "henryIn3", expect: ok });
add({ op: "check", token: "$henryIn3.accessToken", as: "henry3" });
add({ op: "proveSecondFactor", claims: "$henry3", code: { totp: "$totp.secret", at: "$step", plus: 1 }, expect: ok });
add({ op: "proveSecondFactor", claims: "$henry3", code: "$confirmed.recoveryCodes.0", expect: refused("wrong_code") });
ctx("asHenry", "$henry2", "$henrys");
uuid("svc");
submit("identity.identity.CreateServiceIdentity", "$svc", "$henrys", "$asHenry", { displayName: "Billing" }, committed);
uuid("ordersGroup");
submit("group.group.CreateGroup", "$ordersGroup", "$henrys", "$asHenry", { name: "Orders", permissions: [{ pattern: "shop.PlaceOrder" }] }, committed);
submit("identity.identity.GrantMembership", "$svc", "$henrys", "$asHenry", { membershipId: "svc", group: "$ordersGroup" }, committed);
add({ op: "createApiKey", context: "$asHenry", tenant: "$henrys", identityId: "$svc", label: "billing", as: "apiKey", expect: ok });
add({ op: "checkApiKey", key: "$apiKey.key", as: "asService", expect: { senderTenant: "$henrys", senderIdentity: "$svc", senderSession: "$apiKey.keyId" } });
add({ op: "checkApiKey", key: { concat: ["$apiKey.key", "x"] }, expect: null });
submit("shop.order.PlaceOrder", "h1", "$henrys", "$asService", undefined, committed);
query("identity.GetIdentity", "$henrys", "$asHenry", { identityId: "$svc" });
submit("identity.identity.RevokeApiKey", "$svc", "$henrys", "$asHenry", { keyId: "$apiKey.keyId" }, committed);
submit("shop.order.PlaceOrder", "h2", "$henrys", "$asService", undefined, rejected("unauthenticated"));
// The API caches a checked key for a minute; the middleware refused the revoked key above anyway.
add({ op: "checkApiKey", key: "$apiKey.key", note: "cached" });

// --- 6. passkeys -----------------------------------------------------------------------------------

note("6. passkeys: added against an issued challenge, signing in without a password");
person("ivy", "ivy@example.com");
add({ op: "authenticator", as: "device", userId: "$ivy.sub" });
add({ op: "passkeyRegistrationOptions", claims: "$ivy", userName: "ivy@example.com", as: "creation" });
add({ op: "passkeyRegister", device: "device", challenge: "bm90LWlzc3VlZA", as: "wrongReg" });
add({ op: "addPasskey", claims: "$ivy", response: "$wrongReg", expect: refused("invalid_challenge") });
add({ op: "passkeyRegister", device: "device", challenge: "$creation.challenge", as: "reg" });
add({ op: "addPasskey", claims: "$ivy", response: "$reg", label: "Laptop", expect: ok });
add({ op: "passkeySignInOptions", as: "request" });
add({ op: "passkeyAssert", device: "device", challenge: "$request.challenge", as: "answer" });
add({ op: "signInWithPasskey", response: "$answer", as: "ivyIn", expect: { ok: true, accountId: "$ivy.sub" } });
add({ op: "check", token: "$ivyIn.accessToken", expect: { fac: ["passkey"] } });
add({ op: "signInWithPasskey", response: "$answer", expect: refused("wrong_password") });
add({ op: "passkeySignInOptions", as: "other" });
add({ op: "passkeyAssert", device: "device", challenge: "$other.challenge", origin: "http://evil.example", as: "evil" });
add({ op: "signInWithPasskey", response: "$evil", expect: { ok: false } });
add({ op: "check", token: "$ivyIn.accessToken", as: "ivyPk" });
uuid("ivys");
submit("tenancy.tenant.CreateTenant", "$ivys", "$ivys", { senderAccount: "$ivyPk.sub", senderSession: "$ivyPk.sid" }, { slug: "ivys", name: "Ivy's", owner: { accountId: "$ivyPk.sub" } }, committed);
query("account.GetAccount", PLATFORM, { senderAccount: "$ivyPk.sub", senderSession: "$ivyPk.sid" }, { accountId: "$ivyPk.sub" });

// --- 7. OAuth and SSO -------------------------------------------------------------------------------

note("7. OAuth and SSO: a social login registers or signs in, links on request; a tenant's SSO verified by DNS and enforced");
add({ op: "oidcProvider", as: "social" });
add({ op: "oidcProvider", as: "company" });
add({ op: "kitApi", as: "sso", providers: { social: "social" }, redirectUri: "http://localhost:3000/callback" });
function oidc(provider: string, by: unknown, user: unknown, as: string, expect?: unknown, link?: string) {
  add({ op: "oidcStart", api: "sso", by, ...(link ? { link } : {}), as: `${as}Start`, expect: ok });
  add({ op: "oidcLogin", provider, url: `$${as}Start.url`, user, as: `${as}Cb` });
  add({ op: "oidcFinish", api: "sso", callback: `$${as}Cb`, as, ...(expect ? { expect } : {}) });
}
const jack = { sub: "s-jack", email: "jack@example.com", email_verified: true };
oidc("social", { provider: "social" }, jack, "jack1", ok);
oidc("social", { provider: "social" }, jack, "jack2", { ok: true, accountId: "$jack1.accountId" });
oidc("social", { provider: "social" }, jack, "jack3", ok);
add({ op: "oidcFinish", api: "sso", callback: "$jack3Cb", expect: refused("invalid_state") });
person("kate", "kate@example.com");
const kateSocial = { sub: "s-kate", email: "kate@example.com", email_verified: true };
oidc("social", { provider: "social" }, kateSocial, "kate1", refused("not_linked"));
oidc("social", { provider: "social" }, kateSocial, "kate2", { ok: true, linked: true }, "$kate");
oidc("social", { provider: "social" }, kateSocial, "kate3", { ok: true, accountId: "$kate.sub" });
submit("tenancy.tenant.ChangeSignUpPolicy", PLATFORM, PLATFORM, internal, { tenants: "active", accounts: "off" }, committed);
oidc("social", { provider: "social" }, { sub: "s-nora", email: "nora@example.com", email_verified: true }, "nora1", refused("sign_up_closed"));
oidc("social", { provider: "social" }, jack, "jack4", { ok: true, accountId: "$jack1.accountId" });
add({ op: "register", email: "closed@example.com", name: "Closed", password: PW });
submit("tenancy.tenant.ChangeSignUpPolicy", PLATFORM, PLATFORM, internal, { tenants: "active", accounts: "open" }, committed);
person("liam", "liam@lumen.example");
tenant("lumen", "lumen", "Lumen", "liam", "asLiam");
uuid("ssoId");
add({ op: "oidcIssuer", provider: "company", as: "companyConfig" });
submit("account.sso.CreateSso", "$ssoId", "$lumen", "$asLiam", { issuer: "$companyConfig.issuer", clientId: "$companyConfig.clientId", clientSecret: "$companyConfig.clientSecret" }, committed);
submit("account.sso.AddSsoDomain", "$ssoId", "$lumen", "$asLiam", { domain: "lumen.example" }, committed);
add({ op: "verifySsoDomain", api: "sso", tenant: "$lumen", ssoId: "$ssoId", domain: "lumen.example", expect: refused("not_verified") });
query("account.GetSso", "$lumen", "$asLiam", { ssoId: "$ssoId" }, { ok: true }, "ssoConfig");
add({ op: "setTxt", name: "_kernable-sso.lumen.example", value: "$ssoConfig.value.domains.0.txt" });
add({ op: "verifySsoDomain", api: "sso", tenant: "$lumen", ssoId: "$ssoId", domain: "lumen.example", expect: ok });
submit("account.sso.EnforceSso", "$ssoId", "$lumen", "$asLiam", { enforced: true }, committed);
person("mia", "mia@lumen.example");
invite("miaInv", "mia@lumen.example", "$lumen", "$asLiam");
add({ op: "acceptInvitation", token: "$miaInvMail.token", claims: "$mia", expect: refused("sso_required") });
oidc("company", { email: "mia@lumen.example" }, { sub: "c-mia", email: "mia@lumen.example", email_verified: true }, "miaSso", { ok: true, accountId: "$mia.sub" });
add({ op: "check", api: "sso", token: "$miaSso.accessToken", as: "miaSsoClaims" });
add({ op: "acceptInvitation", token: "$miaInvMail.token", claims: "$miaSsoClaims", expect: ok });
ctx("asMia", "$mia", "$lumen");
query("identity.GetIdentity", "$lumen", "$asMia", { identityId: "$asMia.senderIdentity" }, queryRefused("sso_required"));
add({ op: "contextFor", api: "sso", claims: "$miaSsoClaims", tenant: "$lumen", as: "asMiaSso" });
query("identity.GetIdentity", "$lumen", "$asMiaSso", { identityId: "$asMia.senderIdentity" }, { ok: true, value: { id: "$asMia.senderIdentity" } });
query("identity.GetIdentity", "$lumen", "$asLiam", { identityId: "$asLiam.senderIdentity" }, ok);
person("nils", "nils@lumen.example");
invite("nilsInv", "nils@lumen.example", "$lumen", "$asLiam", { owner: true });
add({ op: "acceptInvitation", token: "$nilsInvMail.token", claims: "$nils", expect: { ok: true, accountId: "$nils.sub", tenant: "$lumen" } });
oidc("company", { email: "mia@lumen.example" }, { sub: "c-evil", email: "kate@example.com", email_verified: true }, "takeover", refused("email_not_in_domain"));
oidc("company", { email: "mia@lumen.example" }, { sub: "c-zed", email: "zed@elsewhere.example", email_verified: true }, "stranger", refused("email_not_in_domain"));
add({ op: "serviceContext", api: "sso", as: "ssoService" });
query("account.FindAccountByEmail", PLATFORM, "$ssoService", { email: "zed@elsewhere.example" }, queryRefused("not_found"));
add({ op: "oidcStart", api: "sso", by: { email: "someone@unknown.example" }, expect: { ok: false } });
add({ op: "oidcStart", api: "sso", by: { provider: "nope" }, expect: { ok: false } });

// --- 8. lists and Explain ---------------------------------------------------------------------------

note("8. lists show what one may see; Explain says why");
person("nora", "nora@example.com");
tenant("noras", "noras", "Nora's", "nora", "asNora");
uuid("buyers");
submit("group.group.CreateGroup", "$buyers", "$noras", "$asNora", { name: "Buyers", permissions: [{ pattern: "shop.PlaceOrder" }, { pattern: "shop.ListOrders" }, { pattern: "shop.GetOrder", when: "owner" }] }, committed);
for (const who of ["oscar", "pia"]) {
  person(who, `${who}@example.com`);
  invite(`${who}Inv`, `${who}@example.com`, "$noras", "$asNora", { groups: [{ group: "$buyers" }] });
  add({ op: "acceptInvitation", token: `$${who}InvMail.token`, claims: `$${who}`, expect: ok });
  ctx(`as${who}`, `$${who}`, "$noras");
  submit("shop.order.PlaceOrder", `o-${who}`, "$noras", `$as${who}`, undefined, committed);
}
query("shop.ListOrders", "$noras", "$asoscar", undefined, { ok: true, value: ["o-oscar"] });
query("shop.ListOrders", "$noras", "$aspia", undefined, { ok: true, value: ["o-pia"] });
query("shop.ListOrders", "$noras", "$asNora", undefined, { ok: true, value: ["o-oscar", "o-pia"] });
query("group.Explain", "$noras", "$asoscar", { identityId: "$asoscar.senderIdentity", type: "shop.GetOrder", aggregateType: "order", aggregateId: "o-oscar" }, { ok: true, value: { allowed: true } });
query("group.Explain", "$noras", "$asoscar", { identityId: "$asoscar.senderIdentity", type: "shop.GetOrder", aggregateType: "order", aggregateId: "o-pia" }, { ok: true, value: { allowed: false, code: "forbidden" } });
query("group.Explain", "$noras", "$asNora", { identityId: "$aspia.senderIdentity", type: "shop.PlaceOrder" }, { ok: true, value: { allowed: true } });
query("group.Explain", "$noras", "$asoscar", { identityId: "$aspia.senderIdentity", type: "shop.PlaceOrder" }, queryRefused("forbidden"));
query("group.ListGroups", "$noras", "$asNora");
query("identity.ListIdentities", "$noras", "$asNora");
query("identity.ListIdentities", "$noras", "$asoscar");

// --- 9. invitation links --------------------------------------------------------------------------

note("9. invitation links; a query about one order is decided on that order");
person("olga", "link-olga@example.com");
tenant("olgas", "olgas-shop", "Olga's shop", "olga", "asOlga");
uuid("olgaCustomers");
submit("group.group.CreateGroup", "$olgaCustomers", "$olgas", "$asOlga", { name: "Customers", permissions: [{ pattern: "shop.PlaceOrder" }, { pattern: "shop.GetOrder", when: "owner" }] }, committed);
add({ op: "createInvitationLink", context: "$asOlga", tenant: "$olgas", groups: [{ group: "$olgaCustomers" }], label: "website", maxUses: 2, as: "link", expect: ok });
add({ op: "peekInvitationLink", token: "$link.token", expect: { tenant: "$olgas", linkId: "$link.linkId", label: "website", usable: true } });
add({ op: "peekInvitationLink", token: { tamper: "$link.token", how: "lastChar" }, expect: null });
person("lpia", "link-pia@example.com");
person("lquinn", "link-quinn@example.com");
person("lrosa", "link-rosa@example.com");
add({ op: "joinWithInvitationLink", claims: "$lpia", token: "$link.token", expect: { ok: true, tenant: "$olgas" } });
add({ op: "joinWithInvitationLink", claims: "$lpia", token: "$link.token", expect: refused("exists") });
add({ op: "joinWithInvitationLink", claims: "$lquinn", token: "$link.token", expect: { ok: true, tenant: "$olgas" } });
add({ op: "joinWithInvitationLink", claims: "$lrosa", token: "$link.token", expect: refused("used_up") });
person("lsven", "link-sven@example.com");
add({ op: "signOut", claims: "$lsven" });
add({ op: "joinWithInvitationLink", claims: "$lsven", token: "$link.token", expect: refused("unauthenticated") });
add({ op: "peekInvitationLink", token: "$link.token", expect: { usable: false } });
add({ op: "joinWithInvitationLink", claims: "$lrosa", token: { tamper: "$link.token", how: "lastChar" }, expect: refused("invalid_token") });
ctx("asLpia", "$lpia", "$olgas");
ctx("asLquinn", "$lquinn", "$olgas");
submit("shop.order.PlaceOrder", "p1", "$olgas", "$asLpia", undefined, committed);
query("shop.GetOrder", "$olgas", "$asLpia", { id: "p1" }, { ok: true, value: { customer: "$asLpia.senderIdentity" } });
query("shop.GetOrder", "$olgas", "$asLquinn", { id: "p1" }, queryRefused("forbidden"));
query("shop.GetOrder", "$olgas", "$asOlga", { id: "nope" }, queryRefused("not_found"));
query("group.Explain", "$olgas", "$asOlga", { identityId: "$asLquinn.senderIdentity", type: "shop.GetOrder", aggregateType: "order", aggregateId: "p1" }, { ok: true, value: { allowed: false } });
add({ op: "createInvitationLink", context: "$asLpia", tenant: "$olgas", groups: [{ group: "$olgaCustomers" }], expect: refused("forbidden") });
add({ op: "createInvitationLink", context: "$asOlga", tenant: "$olgas", groups: [{ group: "$olgaCustomers" }], as: "second", expect: ok });
submit("invitation.link.RevokeInvitationLink", "$second.linkId", "$olgas", "$asOlga", undefined, committed);
add({ op: "peekInvitationLink", token: "$second.token", expect: null });
add({ op: "joinWithInvitationLink", claims: "$lrosa", token: "$second.token", expect: refused("invalid_token") });

// --- 10. accepting and joining check the tenant ----------------------------------------------------

note("10. accepting and joining check the tenant too: suspended, a second factor required");
person("tara", "join-tara@example.com");
tenant("taras", "taras", "Tara's", "tara", "asTara");
person("ugo", "join-ugo@example.com");
add({ op: "createInvitationLink", context: "$asTara", tenant: "$taras", groups: [], as: "taraLink", expect: ok });
invite("ugoInv", "join-ugo@example.com", "$taras", "$asTara");
submit("tenancy.tenant.RequireMfa", "$taras", "$taras", "$asTara", { required: true }, committed);
add({ op: "joinWithInvitationLink", claims: "$ugo", token: "$taraLink.token", expect: refused("mfa_required") });
add({ op: "acceptInvitation", token: "$ugoInvMail.token", claims: "$ugo", expect: refused("mfa_required") });
submit("tenancy.tenant.RequireMfa", "$taras", "$taras", internal, { required: false }, committed);
invite("veraInv", "join-vera@example.com", "$taras", "$asTara");
submit("tenancy.tenant.SuspendTenant", "$taras", "$taras", "$asTara", { reason: "owner_requested" }, committed);
add({ op: "joinWithInvitationLink", claims: "$ugo", token: "$taraLink.token", expect: refused("tenant_suspended") });
add({ op: "acceptInvitation", token: "$ugoInvMail.token", claims: "$ugo", expect: refused("tenant_suspended") });
add({ op: "acceptInvitation", token: "$veraInvMail.token", name: "Vera", password: PW, expect: refused("tenant_not_active") });
add({ op: "serviceContext", as: "apiService" });
query("account.FindAccountByEmail", PLATFORM, "$apiService", { email: "join-vera@example.com" }, queryRefused("not_found"));
submit("tenancy.tenant.ReactivateTenant", "$taras", "$taras", "$asTara", undefined, committed);
add({ op: "acceptInvitation", token: "$ugoInvMail.token", claims: "$ugo", expect: ok });
add({ op: "acceptInvitation", token: "$veraInvMail.token", name: "Vera", password: PW, expect: ok });
query("account.FindAccountByEmail", PLATFORM, "$apiService", { email: "join-vera@example.com" }, ok);

// --- 11. a process's identity ----------------------------------------------------------------------

note("11. a process gets an identity and a key, and acts only with a platform group");
add({ op: "processIdentityId", process: "shop.dispatch", as: "procId" });
add({ op: "waitIdentityKey", identityId: "$procId", as: "procKey" });
query("identity.GetIdentity", PLATFORM, internal, { identityId: "$procId" }, { ok: true, value: { kind: "process", displayName: "shop.dispatch", memberships: [] } });
submit("shop.parcel.RequestParcel", "parcel-1", "$graces", internal, undefined, committed);
add({ op: "waitCase", process: "shop.dispatch", case: "parcel-1#1", state: "incident", expect: { state: "incident" } });
submit("group.group.CreateGroup", "g-dispatch", PLATFORM, internal, { name: "Dispatch", permissions: [{ pattern: "shop.ShipParcel" }] }, committed);
submit("identity.identity.GrantMembership", "$procId", PLATFORM, internal, { membershipId: "dispatch", group: "g-dispatch" }, committed);
add({ op: "retryCase", process: "shop.dispatch", case: "parcel-1#1", tenant: "$graces" });
add({ op: "waitCase", process: "shop.dispatch", case: "parcel-1#1", state: "ended", expect: { state: "ended" } });
add({ op: "processAct", process: "shop.dispatch", context: { senderTenant: PLATFORM, senderIdentity: "$procId", senderSession: "$procKey" }, expect: { changed: false } });

// --- 12. what is left: every list ------------------------------------------------------------------

note("12. the read side at the end");
for (const [t, c] of [["$graces", "$asGrace"], ["$henrys", "$asHenry"], ["$lumen", "$asLiam"], ["$noras", "$asNora"], ["$olgas", "$asOlga"], ["$taras", "$asTara"]]) {
  query("tenancy.GetTenant", t, c, { tenantId: t });
  query("group.ListGroups", t, c);
  query("identity.ListIdentities", t, c);
  query("invitation.ListInvitations", t, c);
}
query("identity.GetIdentity", PLATFORM, internal, { identityId: "$procId" });

writeFileSync(new URL("./scenario.json", import.meta.url), `${JSON.stringify({ steps }, null, 1)}\n`);
console.log(`${steps.length} steps`);
