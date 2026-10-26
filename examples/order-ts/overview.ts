// Read model: one row per order, kept up to date from the event stream.
import type { StoredEvent } from "../../sdks/typescript/src/index.ts";

export interface OrderRow {
  customerId: string;
  status: "placed" | "shipped" | "cancelled";
  items: number;
}

export type OrdersOverview = Record<string, OrderRow>;

export const OVERVIEW_EVENTS = ["OrderPlaced", "ItemAdded", "OrderCancelled", "OrderShipped"];

export function project(view: OrdersOverview, e: StoredEvent): OrdersOverview {
  const row = view[e.aggregateId];
  switch (e.type) {
    case "OrderPlaced":
      view[e.aggregateId] = { customerId: (e.data as { customerId: string }).customerId, status: "placed", items: 0 };
      break;
    case "ItemAdded":
      row.items += (e.data as { qty: number }).qty;
      break;
    case "OrderCancelled":
      row.status = "cancelled";
      break;
    case "OrderShipped":
      row.status = "shipped";
      break;
  }
  return view;
}
