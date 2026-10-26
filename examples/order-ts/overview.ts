// Read model: one row per order, kept up to date from the event stream. Its state is private:
// everyone else reads it through queries (orders.GetOrder, orders.ListOrders) via the kernel.
import { Rejected, type Kernable, type Listener, type StoredEvent } from "../../sdks/typescript/src/index.ts";

export interface OrderRow {
  customerId: string;
  status: "placed" | "shipped" | "cancelled";
  items: number;
}

export type OrdersOverview = Record<string, OrderRow>;

export const OVERVIEW_EVENTS = ["orders.OrderPlaced", "orders.ItemAdded", "orders.OrderCancelled", "orders.OrderShipped"];

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

/** The `overview` read model with its queries. */
export function overviewModel(kernable: Kernable, options = {}): Listener {
  const view: OrdersOverview = {};
  const model = kernable.readModel("overview", options);
  for (const type of OVERVIEW_EVENTS) model.on(type, (e) => void project(view, e));
  return model
    .query<{ id: string }>("orders.GetOrder", ({ id }) => {
      if (!view[id]) throw new Rejected(`no order ${id}`, "not_found");
      return view[id];
    })
    .query("orders.ListOrders", () => view);
}
