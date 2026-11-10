// An example API on node:http over the kit's API part: how a framework wires `KitApi` in.
// Carry the routes over to Express, Fastify or whatever you use; the kit's functions stay.
//
//   KERNABLE_URL=… KERNABLE_KEY=kit.key node example/server.ts      (127.0.0.1, port 3000 or PORT)
//
// Requests carry `Authorization: Bearer <access token>` (or `ApiKey <key>`) and, inside a tenant,
// `X-Tenant: <tenant ID>`; the server turns them into the request context the middleware decides on.

import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import type { Kernable, RequestContext } from "@kernable/sdk";

import { KitApi } from "../src/api/api.ts";
import { PLATFORM_TENANT } from "../src/shared/constants.ts";

type Json = Record<string, any>;

/** Where a request of `domain` goes: accounts and sessions live in the platform tenant, the rest in the caller's. */
function tenantFor(domain: string, context: RequestContext): string | undefined {
  return domain === "account" ? PLATFORM_TENANT : context.senderTenant;
}

async function body(req: IncomingMessage): Promise<Json> {
  let raw = "";
  for await (const chunk of req) raw += chunk;
  try {
    return raw ? (JSON.parse(raw) as Json) : {};
  } catch {
    return {};
  }
}

function send(res: ServerResponse, status: number, value: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(value));
}

/** The routes; `kernable` submits the app's own commands with the caller's context. */
export function exampleServer(api: KitApi, kernable: Kernable): Server {
  /** The caller's request context, from the bearer token or API key and the tenant header; null if none. */
  async function caller(req: IncomingMessage): Promise<RequestContext | null> {
    const auth = req.headers.authorization ?? "";
    if (auth.startsWith("ApiKey ")) return api.checkApiKey(auth.slice(7));
    const claims = auth.startsWith("Bearer ") ? api.check(auth.slice(7)) : null;
    if (!claims) return null;
    const tenant = req.headers["x-tenant"];
    try {
      return await api.contextFor(claims, typeof tenant === "string" ? tenant : undefined);
    } catch {
      return null; // no identity in that tenant
    }
  }

  return createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const route = `${req.method} ${url.pathname}`;
    const ip = req.socket.remoteAddress;
    try {
      const b = req.method === "POST" ? await body(req) : {};
      switch (route) {
        case "POST /register":
          await api.register({ email: b.email, name: b.name, password: b.password });
          return send(res, 202, { message: "Check your email" }); // the same whether or not the address was taken
        case "POST /verify":
          return send(res, (await api.verifyEmail(b.token)) ? 200 : 400, {});
        case "POST /sign-in": {
          const r = await api.signIn({ email: b.email, password: b.password, ip, userAgent: req.headers["user-agent"] });
          return send(res, r.ok ? 200 : 401, r.ok ? { accessToken: r.accessToken, refreshToken: r.refreshToken, secondFactor: r.secondFactor } : { error: r.reason });
        }
        case "POST /refresh": {
          const r = await api.refresh(b.refreshToken, ip);
          return send(res, r.ok ? 200 : 401, r);
        }
        case "POST /password-reset":
          await api.requestPasswordReset(b.email);
          return send(res, 202, { message: "Check your email" });
        case "POST /password-reset/confirm":
          return send(res, (await api.resetPassword(b.token, b.password)).ok ? 200 : 400, {});
        case "POST /invitations/accept": {
          const claims = api.check((req.headers.authorization ?? "").replace(/^Bearer /, ""));
          const r = await api.acceptInvitation(b.token, claims ? { claims } : { name: b.name, password: b.password });
          return send(res, r.ok ? 200 : 400, r);
        }
      }
      // Everything below needs a signed-in caller.
      const claims = api.check((req.headers.authorization ?? "").replace(/^Bearer /, ""));
      switch (route) {
        case "POST /second-factor": {
          if (!claims) return send(res, 401, { error: "unauthenticated" });
          const r = await api.proveSecondFactor(claims, b.code);
          return send(res, r.ok ? 200 : 401, r);
        }
        case "POST /sign-out":
          if (claims) await api.signOut(claims);
          return send(res, 204, null);
        case "POST /tenants": {
          // Opens a tenant the caller owns (self-service, as the sign-up policy allows).
          if (!claims) return send(res, 401, { error: "unauthenticated" });
          const tenant = randomUUID();
          const outcome = await kernable.submit(
            { domain: "tenancy", aggregateType: "tenant", type: "CreateTenant", aggregateId: tenant, tenant,
              data: { slug: b.slug, name: b.name, owner: { accountId: claims.sub } }, context: await api.contextFor(claims) },
          );
          return send(res, outcome.committed ? 201 : 403, outcome.committed ? { tenant } : { error: outcome.code });
        }
        case "POST /invitation-links/join": {
          if (!claims) return send(res, 401, { error: "unauthenticated" });
          const r = await api.joinWithInvitationLink(claims, b.token);
          return send(res, r.ok ? 200 : 400, r);
        }
      }
      // Below: as the caller, in the tenant of `X-Tenant`.
      const context = await caller(req);
      if (!context) return send(res, 401, { error: "unauthenticated" });
      switch (route) {
        case "GET /me":
          return send(res, 200, context);
        case "POST /invitation-links": {
          const r = await api.createInvitationLink(context, context.senderTenant!, { groups: b.groups ?? [], label: b.label, maxUses: b.maxUses, expiresAt: b.expiresAt });
          return send(res, r.ok ? 201 : 403, r.ok ? { token: r.token, linkId: r.linkId } : { error: r.reason });
        }
      }
      // POST /commands/<domain>/<aggregateType>/<Type>: the app's commands, as the caller; `account`
      // ones go to the platform tenant, where every account lives.
      const command = /^POST \/commands\/([\w-]+)\/([\w-]+)\/(\w+)$/.exec(route);
      if (command) {
        const outcome = await kernable.submit({
          domain: command[1], aggregateType: command[2], type: command[3], aggregateId: b.aggregateId ?? randomUUID(),
          tenant: tenantFor(command[1], context), data: b.data ?? {}, context,
        });
        return send(res, outcome.committed ? 200 : 403, outcome.committed ? { events: outcome.events.map((e) => e.type) } : { error: outcome.code });
      }
      // POST /queries/<domain>/<Type>: the app's queries, as the caller.
      const query = /^POST \/queries\/([\w-]+)\/(\w+)$/.exec(route);
      if (query) {
        try {
          return send(res, 200, await kernable.query({ domain: query[1], type: query[2], data: b.data ?? {}, tenant: tenantFor(query[1], context), context }));
        } catch (err) {
          const code = (err as { code?: string }).code;
          if (!code) throw err;
          return send(res, code === "not_found" ? 404 : 403, { error: code });
        }
      }
      send(res, 404, { error: "not_found" });
    } catch (err) {
      send(res, 500, { error: (err as Error).message });
    }
  });
}

// Run directly: the kit as one service, the bootstrap (it may run again), and the server.
if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const { Kernable } = await import("@kernable/sdk");
  const { kitDomains, kitSchemas } = await import("../src/kit.ts");
  const { bootstrap } = await import("../src/bootstrap.ts");
  const { newSigningKeys, signingKeysFrom } = await import("../src/api/tokens.ts");
  const app = new Kernable().app({ service: "kit", domains: kitDomains() });
  void app.run();
  await app.ready;
  const kernable = new Kernable().use(...kitSchemas());
  const { api: service } = await bootstrap(kernable, { signUp: { tenants: "active", accounts: "open" } });
  // The access token key: a PEM file in production (KIT_SIGNING_KEY), new on every start otherwise.
  const keys = process.env.KIT_SIGNING_KEY ? signingKeysFrom(process.env.KIT_SIGNING_KEY) : newSigningKeys();
  const port = Number(process.env.PORT ?? 3000);
  // Loopback only: put it behind your reverse proxy (TLS, rate limits) to serve it.
  exampleServer(new KitApi({ kernable, keys, service }), kernable).listen(port, "127.0.0.1", () => process.stderr.write(`example server on 127.0.0.1:${port}\n`));
}
