// Orders service (TypeScript) and the shop's public HTTP API: the Order worker, the `orders`
// read model, the `confirmations` reactor and the endpoints. The API reads both read models:
// its own in memory, the accounts read model through the identity service.
//
// Usage: node service.ts <kernel-url> <key> <http-port> <identity-url>
// Mutual TLS: set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { Kernable, NotSettled, type SubmitOptions } from "../../../sdks/typescript/src/index.ts";
import { ORDER_EVENTS, order, project, type OrderRow } from "./order.ts";

const [url, key, port, identity] = process.argv.slice(2);
const env = process.env;
const tls = env.KERNABLE_TLS_CA ? { ca: env.KERNABLE_TLS_CA, cert: env.KERNABLE_TLS_CERT!, key: env.KERNABLE_TLS_KEY! } : undefined;
const kernable = new Kernable({ url, key, tenant: "shop", tls });

// ---- read model: in memory, rebuilt from the first event on every start ----

const orders = new Map<string, OrderRow>();
const ordersModel = kernable.readModel("orders");
for (const type of ORDER_EVENTS) ordersModel.on(type, (e) => project(orders, e));

async function account(id: string): Promise<{ email: string; status: string } | undefined> {
  const res = await fetch(`${identity}/accounts/${encodeURIComponent(id)}`);
  if (res.status === 404) return undefined;
  if (!res.ok) throw new Error(`identity service: ${res.status}`);
  return res.json();
}

// ---- reactor: runs after both read models (order 10 > 0) ----

const stats = { confirmations: 0, unknownAccounts: 0 };
const confirmations = kernable.reactor("confirmations").on("OrderSubmitted", async (e, ctx) => {
  const row = orders.get(e.aggregateId)!;
  const customer = await account(row.customerId); // throws while identity is down: retried
  if (!customer) {
    stats.unknownAccounts++; // must never happen: accounts ran first
    return;
  }
  // A reactor must not wait for settling: that would include waiting for itself. The fixed
  // command ID makes a redelivered event harmless; `follow()` keeps sender and correlation.
  await kernable.submit({
    aggregateType: "Order",
    aggregateId: e.aggregateId,
    type: "RecordConfirmation",
    data: { email: customer.email },
    id: `confirm-${e.aggregateId}`,
    context: ctx.follow(),
    wait: "committed",
  });
  stats.confirmations++;
});

// ---- HTTP API ----

type Reply = [status: number, body: unknown];

async function run(options: SubmitOptions, show: () => Promise<unknown> | unknown): Promise<Reply> {
  try {
    const outcome = await kernable.submit(options);
    if (!outcome.committed) return [outcome.code === "exists" ? 409 : 422, { error: outcome.message }];
    return [200, await show()]; // settled: the read models already show the change
  } catch (err) {
    if (err instanceof NotSettled) return [202, { accepted: true }];
    throw err;
  }
}

const orderCommand = (id: string, type: string, data?: unknown) =>
  run({ aggregateType: "Order", aggregateId: id, type, data }, () => orders.get(id));

async function route(method: string, path: string, body: any): Promise<Reply> {
  let m: RegExpMatchArray | null;
  if (method === "GET" && path === "/health") return [200, { ok: true }]; // listening means ready
  if (method === "GET" && path === "/stats") return [200, stats];
  if (method === "POST" && path === "/accounts") {
    return run({ aggregateType: "Account", aggregateId: body.id, type: "RegisterAccount", data: body }, () => account(body.id));
  }
  if (method === "POST" && (m = path.match(/^\/accounts\/([^/]+)\/email$/))) {
    const id = m[1];
    return run({ aggregateType: "Account", aggregateId: id, type: "ChangeEmail", data: body }, () => account(id));
  }
  if (method === "POST" && path === "/orders") {
    const customer = await account(body.customerId);
    if (customer?.status !== "active") return [422, { error: "Unknown or inactive customer" }];
    return orderCommand(body.id, "PlaceOrder", { customerId: body.customerId });
  }
  if (method === "GET" && (m = path.match(/^\/orders\/([^/]+)$/))) {
    const row = orders.get(m[1]);
    return row ? [200, row] : [404, { error: "not found" }];
  }
  if (method === "POST" && (m = path.match(/^\/orders\/([^/]+)\/items$/))) return orderCommand(m[1], "AddItem", body);
  if (method === "POST" && (m = path.match(/^\/orders\/([^/]+)\/submit$/))) return orderCommand(m[1], "SubmitOrder");
  if (method === "POST" && (m = path.match(/^\/orders\/([^/]+)\/cancel$/))) return orderCommand(m[1], "CancelOrder");
  return [404, { error: "no such endpoint" }];
}

async function handle(req: IncomingMessage, res: ServerResponse) {
  let reply: Reply;
  try {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    reply = await route(req.method ?? "GET", req.url ?? "/", raw ? JSON.parse(raw) : {});
  } catch (err) {
    reply = [503, { error: (err as Error).message }];
  }
  res.writeHead(reply[0], { "content-type": "application/json" }).end(JSON.stringify(reply[1]));
}

const worker = kernable.worker(order);
void worker.run();
void ordersModel.run();
void confirmations.run();
await Promise.all([worker.ready, ordersModel.ready, confirmations.ready]);
createServer((req, res) => void handle(req, res)).listen(Number(port), "127.0.0.1");
