// Orders service (TypeScript) and the shop's public HTTP API: the Order worker, the `orders`
// read model, the `confirmations` reactor and the endpoints. Every read goes through the
// kernel as a query, also of the read model in this very process, so that signatures,
// allowlists and the `rbac` middleware (identity service) always apply.
//
// Who calls is the `x-identity` header (a stand-in for real authentication); it becomes the
// request context's `senderIdentity`. The service itself acts as `svc-orders`.
//
// Usage: node service.ts <kernel-url> <key> <http-port>
// Mutual TLS: set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { Kernable, NotSettled, Rejected, type RequestContext, type SubmitOptions } from "../../../sdks/typescript/src/index.ts";
import { ORDER_EVENTS, order, project, type OrderRow } from "./order.ts";

const [url, key, port] = process.argv.slice(2);
const env = process.env;
const tls = env.KERNABLE_TLS_CA ? { ca: env.KERNABLE_TLS_CA, cert: env.KERNABLE_TLS_CERT!, key: env.KERNABLE_TLS_KEY! } : undefined;
const kernable = new Kernable({ url, key, tenant: "shop", tls, context: { senderIdentity: "svc-orders" } });

// ---- read model: in memory, rebuilt from the first event on every start ----

const ordersModel = (() => {
  const orders = new Map<string, OrderRow>();
  const model = kernable.readModel("orders");
  for (const type of ORDER_EVENTS) model.on(type, (e) => project(orders, e));
  return model.query<{ id: string }>("orders.GetOrder", ({ id }) => {
    const row = orders.get(id);
    if (!row) throw new Rejected(`no order ${id}`, "not_found");
    return row;
  });
})();

// ---- queries, through the kernel ----

/** The answer of a query, or `undefined` if the read model has no such entry. */
async function find<T>(domain: string, type: string, id: string, context?: RequestContext): Promise<T | undefined> {
  try {
    return await kernable.query<T>({ domain, type, data: { id }, context });
  } catch (err) {
    if (err instanceof Rejected && err.code === "not_found") return undefined;
    throw err;
  }
}

const accountOf = (id: string, as?: RequestContext) => find<{ email: string; status: string }>("identity", "GetAccount", id, as);
const orderOf = (id: string, as?: RequestContext) => find<OrderRow>("orders", "GetOrder", id, as);

// ---- reactor: runs after both read models (order 10 > 0) ----

const stats = { confirmations: 0, unknownAccounts: 0 };
const confirmations = kernable.reactor("confirmations").on("orders.OrderSubmitted", async (e, ctx) => {
  const row = (await orderOf(e.aggregateId))!;
  const customer = await accountOf(row.customerId); // throws while identity is down: retried
  if (!customer) {
    stats.unknownAccounts++; // must never happen: accounts ran first
    return;
  }
  // The submit settles on the read models before this reactor, not on the reactor itself. The
  // fixed command ID makes a redelivered event harmless; `follow()` keeps sender and correlation.
  await kernable.submit({
    domain: "orders",
    aggregateType: "Order",
    aggregateId: e.aggregateId,
    type: "RecordConfirmation",
    data: { email: customer.email },
    id: `confirm-${e.aggregateId}`,
    context: ctx.follow(),
  });
  stats.confirmations++;
});

// ---- HTTP API ----

type Reply = [status: number, body: unknown];

const STATUS: Record<string, number> = { exists: 409, forbidden: 403 };

async function run(options: SubmitOptions, show: () => Promise<unknown> | unknown): Promise<Reply> {
  try {
    const outcome = await kernable.submit(options);
    if (!outcome.committed) return [STATUS[outcome.code] ?? 422, { error: outcome.message }];
    return [200, await show()]; // settled: the read models already show the change
  } catch (err) {
    if (err instanceof NotSettled) return [202, { accepted: true }];
    throw err;
  }
}

async function route(method: string, path: string, body: any, as: RequestContext): Promise<Reply> {
  const account = (id: string) => accountOf(id, as);
  const getOrder = (id: string) => orderOf(id, as);
  const orderCommand = (id: string, type: string, data?: unknown) =>
    run({ domain: "orders", aggregateType: "Order", aggregateId: id, type, data, context: as }, () => getOrder(id));
  let m: RegExpMatchArray | null;
  if (method === "GET" && path === "/health") return [200, { ok: true }]; // listening means ready
  if (method === "GET" && path === "/stats") return [200, stats];
  if (method === "POST" && path === "/accounts") {
    return run({ domain: "identity", aggregateType: "Account", aggregateId: body.id, type: "RegisterAccount", data: body, context: as }, () => account(body.id));
  }
  if (method === "GET" && (m = path.match(/^\/accounts\/([^/]+)$/))) {
    const row = await account(m[1]);
    return row ? [200, row] : [404, { error: "not found" }];
  }
  if (method === "POST" && (m = path.match(/^\/accounts\/([^/]+)\/email$/))) {
    const id = m[1];
    return run({ domain: "identity", aggregateType: "Account", aggregateId: id, type: "ChangeEmail", data: body, context: as }, () => account(id));
  }
  if (method === "POST" && path === "/orders") {
    const customer = await account(body.customerId);
    if (customer?.status !== "active") return [422, { error: "Unknown or inactive customer" }];
    return orderCommand(body.id, "PlaceOrder", { customerId: body.customerId });
  }
  if (method === "GET" && (m = path.match(/^\/orders\/([^/]+)$/))) {
    const row = await getOrder(m[1]);
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
    const as = { senderIdentity: String(req.headers["x-identity"] ?? "") };
    reply = await route(req.method ?? "GET", req.url ?? "/", raw ? JSON.parse(raw) : {}, as);
  } catch (err) {
    const forbidden = err instanceof Rejected && err.code === "forbidden";
    reply = [forbidden ? 403 : 503, { error: (err as Error).message }];
  }
  res.writeHead(reply[0], { "content-type": "application/json" }).end(JSON.stringify(reply[1]));
}

const worker = kernable.worker(order);
void worker.run();
void ordersModel.run();
void confirmations.run();
await Promise.all([worker.ready, ordersModel.ready, confirmations.ready]);
createServer((req, res) => void handle(req, res)).listen(Number(port), "127.0.0.1");
