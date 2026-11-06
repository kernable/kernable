// Lists that show only what one may see: the middleware decides on single requests; a read
// model answering a list filters its rows with the same decision. `visibility(listener)` teaches
// an app's read model what the middleware knows and returns `visible`.
//
//   const orders = shop.readModel("shop.orders");
//   const visible = visibility(orders);
//   orders.query(ListOrders, (_d, qry, ctx) =>
//     rows.filter((o) => visible(qry.tenant, ctx.request, "shop.GetOrder", { type: "shop.order", id: o.id })));

import type { Check, Listener, RequestContext } from "@kernable/sdk";

import { authorizationListener, installRelations, knowledge } from "./middleware.ts";
import { MemoryKeyValues, decide, type KeyValues } from "./policy.ts";

/** The listener's store under a prefix of its own, so the kit's namespaces never meet the app's. */
class Prefixed implements KeyValues {
  private readonly store: KeyValues;
  constructor(store: KeyValues) {
    this.store = store;
  }
  get<T>(ns: string, key: string) {
    return this.store.get<T>(`kit:${ns}`, key);
  }
  set(ns: string, key: string, value: unknown) {
    this.store.set(`kit:${ns}`, key, value);
  }
  remove(ns: string, key: string) {
    this.store.remove(`kit:${ns}`, key);
  }
  all<T>(ns: string) {
    return this.store.all<T>(`kit:${ns}`);
  }
}

/**
 * Adds the middleware's event handlers to `listener` (after its own, for the same events) and
 * returns `visible(tenant, request, type, target)`: whether the asker may `type` (a qualified
 * type, e.g. `shop.GetOrder`) on the target aggregate, as the middleware would decide. Declare
 * the relations before calling it. Kept in the listener's store, if it has one, else in memory.
 */
export function visibility(listener: Listener): (tenant: string, request: RequestContext, type: string, target: { type: string; id: string }) => boolean {
  installRelations();
  const memory = new MemoryKeyValues();
  let current: KeyValues = memory;
  const kv = (store?: KeyValues) => (current = store ? new Prefixed(store) : memory);
  for (const [type, entry] of authorizationListener.handlers) {
    const own = listener.handlers.get(type);
    listener.handlers.set(type, {
      raw: own?.raw ?? false,
      handler: async (data, evt, ctx) => {
        if (own) await own.handler(data, evt, ctx);
        await entry.handler(structuredClone(data), evt, { ...ctx, store: kv(ctx.store) as never });
      },
    });
  }
  return (tenant, request, type, target) => {
    const [domain, name] = type.split(".");
    const check: Check = { kind: "command", id: "", tenant, domain, type: name, aggregateType: target.type.split(".")[1], aggregateId: target.id, data: {}, request };
    const saved = knowledge.kv;
    knowledge.kv = current;
    try {
      return decide(knowledge, check, Date.now()) === null;
    } finally {
      knowledge.kv = saved;
    }
  };
}
