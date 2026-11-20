// The kit parity runner for TypeScript (see ../../parity/README.md). Run by ../../parity/run.ts:
//
//   node --conditions=source test/parity.ts host     # the kit's domains and the parity shop, until SIGTERM
//   node --conditions=source test/parity.ts driver   # runs the scenario through KitApi, writes the transcript
//
// Both read PARITY_URL, PARITY_KEY, PARITY_DIR; the driver also PARITY_SCENARIO, PARITY_OUT,
// PARITY_KERNABLE (the kernel binary) and PARITY_STORE.

import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Command, Event, Kernable, Query, Rejected, domain, type RequestContext } from "@kernable/sdk";

import { KitApi } from "../src/api/api.ts";
import { newSigningKeys } from "../src/api/tokens.ts";
import { bootstrap } from "../src/bootstrap.ts";
import { processIdentityId } from "../src/identity/index.ts";
import { kitDomains, kitSchemas, relation, useAuthorizationStore, visibility } from "../src/kit.ts";
import { useMailer, type Mail } from "../src/shared/mail.ts";
import { totpCode } from "../src/shared/secrets.ts";
import { Authenticator } from "./authenticator.ts";
import { OidcProvider } from "./oidc-provider.ts";

const env = (name: string) => {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is not set`);
  return v;
};
const URL_ = env("PARITY_URL");
const KEY = env("PARITY_KEY");
const DIR = env("PARITY_DIR");

// --- The parity shop: the same in every language ----------------------------------------------

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

class ListOrders extends Query<string[]> {}
const orderRows = new Map<string, { tenant: string; id: string }>();
const ordersModel = shop.readModel("shop.orders").on(OrderPlaced, (_d, evt) => void orderRows.set(`${evt.tenant}/${evt.aggregateId}`, { tenant: evt.tenant, id: evt.aggregateId }));
const visible = visibility(ordersModel);
ordersModel.query(ListOrders, (_d, qry, ctx) =>
  [...orderRows.values()].filter((o) => o.tenant === qry.tenant && visible(qry.tenant, ctx.request, "shop.GetOrder", { type: "shop.order", id: o.id })).map((o) => o.id).sort());

class GetOrder extends Query<{ customer: string }> {
  static target = { aggregateType: "order", field: "id" };
  id!: string;
}
const customerOf = new Map<string, string>();
shop.readModel("shop.order-details")
  .on(OrderPlaced, (d, evt) => void customerOf.set(`${evt.tenant}/${evt.aggregateId}`, d.customer))
  .query(GetOrder, (d, qry) => {
    const customer = customerOf.get(`${qry.tenant}/${d.id}`);
    if (!customer) throw new Rejected("No such order", "not_found");
    return { customer };
  });

class ParcelRequested extends Event {}
class ParcelShipped extends Event {}
class RequestParcel extends Command {}
class ShipParcel extends Command {}
shop
  .aggregate("parcel", { requested: false, shipped: false })
  .apply(ParcelRequested, (s) => void (s.requested = true))
  .apply(ParcelShipped, (s) => void (s.shipped = true))
  .command(RequestParcel, () => [ParcelRequested.new()])
  .command(ShipParcel, (s) => {
    if (s.shipped) throw new Rejected("Shipped already", "invalid_state");
    return [ParcelShipped.new()];
  });
shop.process("dispatch")
  .start(ParcelRequested, { key: "aggregateId", data: { parcelId: "aggregateId" } })
  .command("ship", ShipParcel, { aggregate: "case.parcelId" })
  .end("done");

// Mails go to a file both processes see: the host's reactors send, the driver reads.
const MAILS = join(DIR, "mails.jsonl");
class FileMailer {
  async send(mail: Mail): Promise<void> {
    appendFileSync(MAILS, `${JSON.stringify({ to: mail.to, subject: mail.subject, text: mail.text, purpose: mail.purpose, token: mail.token })}\n`);
  }
}
const readMails = (): Mail[] => (existsSync(MAILS) ? readFileSync(MAILS, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)) : []);

// --- Host ----------------------------------------------------------------------------------------

async function host(): Promise<void> {
  useMailer(new FileMailer());
  useAuthorizationStore(`sqlite:${join(DIR, "authorization.db")}`);
  const app = new Kernable({ url: URL_, key: KEY }).app({ service: "kit", domains: [...kitDomains(), shop] });
  void app.run();
  await app.ready;
  process.stdout.write("ready\n");
  const stop = () => {
    app.stop();
    setTimeout(() => process.exit(0), 200);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

// --- Driver --------------------------------------------------------------------------------------

type Json = any;
type Step = { op: string; as?: string; expect?: Json; [k: string]: Json };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A request context as JSON (camelCase, empty fields left out), the same in every language. */
function contextJson(c: RequestContext | null | undefined): Json {
  if (!c) return null;
  const out: Record<string, unknown> = {};
  for (const k of ["senderTenant", "senderIdentity", "senderAccount", "senderSession", "onBehalfOf"] as const) if (c[k]) out[k] = c[k];
  if (c.bypassAuthorization) out.bypassAuthorization = true;
  return out;
}
const toContext = (j: Json): RequestContext => ({ ...(j ?? {}) });

/** Whether `actual` has every field `expected` names, with the same value (arrays whole). */
function matches(expected: Json, actual: Json): boolean {
  if (expected === null || typeof expected !== "object" || Array.isArray(expected)) return JSON.stringify(expected) === JSON.stringify(actual);
  if (actual === null || typeof actual !== "object") return false;
  return Object.entries(expected).every(([k, v]) => matches(v, actual[k]));
}

async function driver(): Promise<void> {
  const scenario = JSON.parse(readFileSync(env("PARITY_SCENARIO"), "utf8")) as { steps: Step[] };
  const kernableBin = env("PARITY_KERNABLE");
  const store = env("PARITY_STORE");
  const kit = new Kernable({ url: URL_, key: KEY }).use(...kitSchemas(), shop);
  const vars: Record<string, Json> = {};
  const apis = new Map<string, KitApi>();
  const devices = new Map<string, Authenticator>();
  const providers = new Map<string, OidcProvider>();
  const txt = new Map<string, string>();
  const claims = new Map<string, unknown>();
  let consumed = 0;
  let service: { identityId: string; keyId: string } | undefined;

  const lookup = (path: string): Json => {
    const [head, ...rest] = path.split(".");
    if (!(head in vars)) throw new Error(`unknown variable ${head}`);
    let v = vars[head];
    for (const p of rest) v = v == null ? undefined : v[p];
    return v;
  };
  const resolve = (v: Json): Json => {
    if (typeof v === "string") return v.startsWith("$") ? lookup(v.slice(1)) : v;
    if (Array.isArray(v)) return v.map(resolve);
    if (v && typeof v === "object") {
      if ("concat" in v) return (v.concat as Json[]).map(resolve).join("");
      if ("merge" in v) return Object.assign({}, ...(v.merge as Json[]).map(resolve));
      if ("tamper" in v) {
        const s = resolve(v.tamper) as string;
        if (v.how === "lastChar") return s.slice(0, -1) + (s.endsWith("A") ? "B" : "A");
        if (v.how === "secret") return `${s.slice(0, s.lastIndexOf("."))}.not-the-secret`;
        throw new Error(`unknown tamper ${v.how}`);
      }
      if ("totp" in v) return totpCode(resolve(v.totp), resolve(v.at) + (v.plus ?? 0));
      return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, resolve(x)]));
    }
    return v;
  };
  const api = (s: Step) => {
    const a = apis.get(s.api ?? "api");
    if (!a) throw new Error(`no KitApi ${s.api ?? "api"}`);
    return a;
  };
  /** Claims: from a `check` step's variable (its JSON) or an access token checked by the step's API. */
  const claimsOf = (s: Step, ref: Json) => {
    const v = resolve(ref);
    if (typeof v === "string") return api(s).check(v);
    return v;
  };

  const run = async (s: Step): Promise<Json> => {
    const p = (k: string) => resolve(s[k]);
    switch (s.op) {
      case "uuid": return crypto.randomUUID();
      case "value": return p("value");
      case "bootstrap": {
        const booted = await bootstrap(kit, { signUp: p("signUp"), admin: p("admin") });
        service = booted.api;
        apis.set("api", new KitApi({ kernable: kit, keys: newSigningKeys(), service }));
        return booted;
      }
      case "kitApi": {
        const providerConfig: Record<string, { issuer: string; clientId: string; clientSecret: string }> = {};
        for (const [name, handle] of Object.entries((s.providers ?? {}) as Record<string, string>)) {
          const pr = providers.get(handle)!;
          providerConfig[name] = { issuer: pr.issuer, clientId: pr.clientId, clientSecret: pr.clientSecret };
        }
        apis.set(s.as!, new KitApi({
          kernable: kit, keys: newSigningKeys(), service: service!,
          providers: providerConfig, redirectUri: p("redirectUri"), insecureIssuers: true,
          resolveTxt: async (name) => (txt.has(name) ? [[txt.get(name)!]] : []),
        }));
        return null;
      }
      case "register": return api(s).register({ email: p("email"), name: p("name"), password: p("password") });
      case "mail": {
        const to = p("to");
        for (let i = 0; i < 400; i++) {
          const mails = readMails();
          const found = mails.findLastIndex((m) => m.to === to);
          if (found >= 0 && (s.old || found >= consumed)) {
            consumed = Math.max(consumed, found + 1);
            const m = mails[found];
            return { to: m.to, subject: m.subject, text: m.text, purpose: m.purpose, token: m.token };
          }
          await sleep(25);
        }
        throw new Error(`no new mail to ${to}`);
      }
      case "mailCount": {
        await sleep(s.settleMs ?? 300);
        const n = readMails().length;
        consumed = Math.max(consumed, n);
        return n;
      }
      case "verifyEmail": return api(s).verifyEmail(p("token"));
      case "signIn": return api(s).signIn({ email: p("email"), password: p("password"), ...(s.ip ? { ip: p("ip") } : {}) });
      case "check": return api(s).check(p("token"));
      case "contextFor": return contextJson(await api(s).contextFor(claimsOf(s, s.claims), s.tenant === undefined ? undefined : p("tenant")));
      case "refresh": return api(s).refresh(p("token"));
      case "signOut": await api(s).signOut(claimsOf(s, s.claims)); return null;
      case "startTotp": return api(s).startTotp(claimsOf(s, s.claims), p("issuer"), p("label"));
      case "totpStep": return Math.floor(Date.now() / 30_000);
      case "confirmTotp": return api(s).confirmTotp(claimsOf(s, s.claims), p("code"));
      case "proveSecondFactor": return api(s).proveSecondFactor(claimsOf(s, s.claims), p("code"));
      case "createApiKey": return api(s).createApiKey(toContext(p("context")), p("tenant"), p("identityId"), p("label") ?? "");
      case "checkApiKey": return contextJson(await api(s).checkApiKey(p("key")));
      case "changePassword": return api(s).changePassword(claimsOf(s, s.claims), p("current"), p("next"));
      case "requestPasswordReset": await api(s).requestPasswordReset(p("email")); return null;
      case "resetPassword": return api(s).resetPassword(p("token"), p("next"));
      case "requestEmailChange": return api(s).requestEmailChange(claimsOf(s, s.claims), p("email"));
      case "confirmEmailChange": return api(s).confirmEmailChange(p("token"));
      case "peekInvitation": return api(s).peekInvitation(p("token"));
      case "acceptInvitation":
        return api(s).acceptInvitation(p("token"), s.claims !== undefined ? { claims: claimsOf(s, s.claims) as never } : { name: p("name"), password: p("password") });
      case "createInvitationLink":
        return api(s).createInvitationLink(toContext(p("context")), p("tenant"), { groups: p("groups") ?? [], label: p("label"), maxUses: p("maxUses") });
      case "peekInvitationLink": return api(s).peekInvitationLink(p("token"));
      case "joinWithInvitationLink": return api(s).joinWithInvitationLink(claimsOf(s, s.claims), p("token"));
      case "verifySsoDomain": return api(s).verifySsoDomain(p("tenant"), p("ssoId"), p("domain"));
      case "serviceContext": return contextJson(api(s).serviceContext);
      case "authenticator": devices.set(s.as!, new Authenticator("localhost", "http://localhost:3000", p("userId"))); return null;
      case "passkeyRegistrationOptions": return api(s).passkeyRegistrationOptions(claimsOf(s, s.claims), p("userName"));
      case "passkeyRegister": return devices.get(s.device)!.register(p("challenge"));
      case "passkeyAssert": return devices.get(s.device)!.assert(p("challenge"), s.origin === undefined ? undefined : p("origin"));
      case "addPasskey": return api(s).addPasskey(claimsOf(s, s.claims), p("response"), s.label === undefined ? undefined : p("label"));
      case "passkeySignInOptions": return api(s).passkeySignInOptions();
      case "signInWithPasskey": return api(s).signInWithPasskey(p("response"));
      case "oidcProvider": {
        const pr = new OidcProvider();
        await pr.start();
        providers.set(s.as!, pr);
        return null;
      }
      case "oidcIssuer": {
        const pr = providers.get(s.provider)!;
        return { issuer: pr.issuer, clientId: pr.clientId, clientSecret: pr.clientSecret };
      }
      case "waitIdentityKey": {
        for (let i = 0; i < 400; i++) {
          try {
            const identity = (await kit.query({ domain: "identity", type: "GetIdentity", data: { identityId: p("identityId") }, tenant: "platform", context: { bypassAuthorization: true } })) as { apiKeys: { keyId: string }[] };
            if (identity.apiKeys[0]?.keyId) return identity.apiKeys[0].keyId;
          } catch {
            // not there yet
          }
          await sleep(25);
        }
        throw new Error("no key for the identity");
      }
      case "setTxt": txt.set(p("name"), p("value")); return null;
      case "oidcStart": {
        const by = p("by");
        return api(s).oidcStart(by, s.link === undefined ? undefined : claimsOf(s, s.link));
      }
      case "oidcLogin": return providers.get(s.provider)!.login(p("url"), p("user"));
      case "oidcFinish": return api(s).oidcFinish(p("callback"));
      case "submit": {
        const [d, aggregateType, type] = (s.command as string).split(".");
        const out = await kit.submit({ domain: d, aggregateType, type, aggregateId: p("aggregateId"), data: p("data") ?? {}, tenant: p("tenant"), context: toContext(p("context")) });
        return out.committed
          ? { committed: true, events: out.events.map((e) => ({ type: e.type, data: e.data })) }
          : { committed: false, code: out.code, message: out.message };
      }
      case "query": {
        const [d, type] = (s.query as string).split(".");
        try {
          const value = await kit.query({ domain: d, type, data: p("data") ?? {}, tenant: p("tenant"), context: toContext(p("context")) });
          return { ok: true, value };
        } catch (e) {
          if (e instanceof Rejected) return { ok: false, code: e.code, message: e.message };
          throw e;
        }
      }
      case "processIdentityId": return processIdentityId(p("process"));
      case "processAct": return kit.processAct(p("process"), toContext(p("context")));
      case "waitCase": {
        const process_ = p("process");
        const caseId = p("case");
        for (let i = 0; i < 400; i++) {
          const out = execFileSync(kernableBin, ["processes", "cases", store, process_, "--state", p("state")], { encoding: "utf8" });
          if (out.split("\n").some((l) => l.includes(` ${caseId} `))) return { state: p("state") };
          await sleep(25);
        }
        return { state: "not reached" };
      }
      case "retryCase": {
        execFileSync(kernableBin, ["processes", "retry", URL_, KEY, p("process"), p("case"), "--tenant", p("tenant")], { encoding: "utf8" });
        return null;
      }
      case "restartHost": {
        writeFileSync(join(DIR, "restart"), "");
        for (let i = 0; i < 1200 && existsSync(join(DIR, "restart")); i++) await sleep(25);
        if (existsSync(join(DIR, "restart"))) throw new Error("the host did not restart");
        return null;
      }
      default:
        throw new Error(`unknown op ${s.op}`);
    }
  };

  const transcript: Json[] = [];
  for (const [i, s] of scenario.steps.entries()) {
    let result: Json;
    try {
      result = await run(s);
      if (result === undefined) result = null;
      if (result instanceof Map) result = Object.fromEntries(result);
      result = JSON.parse(JSON.stringify(result));
    } catch (e) {
      result = { error: e instanceof Rejected ? e.code : String((e as Error).message ?? e) };
    }
    if (s.as) {
      vars[s.as] = result;
      if (s.op === "check") claims.set(s.as, result);
    }
    const entry: Json = { step: i, op: s.op, ...(s.note ? { note: s.note } : {}), result };
    if (s.expect !== undefined && !matches(resolve(s.expect), result)) entry.unexpected = resolve(s.expect);
    transcript.push(entry);
  }
  for (const pr of providers.values()) pr.stop();
  writeFileSync(env("PARITY_OUT"), JSON.stringify(transcript, null, 1));
  kit.close();
  process.exit(0);
}

if (process.argv[2] === "host") await host();
else if (process.argv[2] === "driver") await driver();
else throw new Error("usage: parity.ts host|driver");
