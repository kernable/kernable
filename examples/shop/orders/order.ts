// The Order aggregate and the `orders` read model's projection.
import { Rejected, domain, type StoredEvent } from "../../../sdks/typescript/src/index.ts";

type Status = "none" | "draft" | "submitted" | "confirmed" | "cancelled";

interface Order {
  status: Status;
  items: number;
}

export const order = domain("orders").aggregate<Order>("Order", { status: "none", items: 0 })
  .apply("OrderPlaced", () => ({ status: "draft", items: 0 }))
  .apply("ItemAdded", (s, e: { qty: number }) => ({ ...s, items: s.items + e.qty }))
  .apply("OrderSubmitted", (s) => ({ ...s, status: "submitted" }))
  .apply("ConfirmationSent", (s) => ({ ...s, status: "confirmed" }))
  .apply("OrderCancelled", (s) => ({ ...s, status: "cancelled" }))

  .command("PlaceOrder", (s, cmd: { customerId: string }) => {
    if (s.status !== "none") throw new Rejected("Order already exists", "exists");
    return [{ type: "OrderPlaced", data: { customerId: cmd.customerId } }];
  })
  .command("AddItem", (s, cmd: { sku: string; qty: number }) => {
    if (s.status !== "draft") throw new Rejected(`Cannot add items to a ${s.status} order`, "invalid_state");
    if (!(cmd.qty > 0)) throw new Rejected("Quantity must be positive", "invalid");
    return [{ type: "ItemAdded", data: { sku: cmd.sku, qty: cmd.qty } }];
  })
  .command("SubmitOrder", (s) => {
    if (s.status !== "draft") throw new Rejected(`Cannot submit a ${s.status} order`, "invalid_state");
    if (s.items === 0) throw new Rejected("Cannot submit an empty order", "invalid");
    return [{ type: "OrderSubmitted" }];
  })
  .command("RecordConfirmation", (s, cmd: { email: string }) => {
    if (s.status !== "submitted") throw new Rejected(`Cannot confirm a ${s.status} order`, "invalid_state");
    return [{ type: "ConfirmationSent", data: { email: cmd.email } }];
  })
  .command("CancelOrder", (s) => {
    if (s.status !== "draft" && s.status !== "submitted") throw new Rejected(`Cannot cancel a ${s.status} order`, "invalid_state");
    return [{ type: "OrderCancelled" }];
  });

export interface OrderRow {
  id: string;
  customerId: string;
  status: Exclude<Status, "none">;
  items: Record<string, number>;
  confirmedTo?: string;
}

export const ORDER_EVENTS = ["orders.OrderPlaced", "orders.ItemAdded", "orders.OrderSubmitted", "orders.ConfirmationSent", "orders.OrderCancelled"];

export function project(rows: Map<string, OrderRow>, e: StoredEvent): void {
  const data = e.data as any;
  const row = rows.get(e.aggregateId)!;
  switch (e.type) {
    case "OrderPlaced":
      rows.set(e.aggregateId, { id: e.aggregateId, customerId: data.customerId, status: "draft", items: {} });
      break;
    case "ItemAdded":
      row.items[data.sku] = (row.items[data.sku] ?? 0) + data.qty;
      break;
    case "OrderSubmitted":
      row.status = "submitted";
      break;
    case "ConfirmationSent":
      row.status = "confirmed";
      row.confirmedTo = data.email;
      break;
    case "OrderCancelled":
      row.status = "cancelled";
      break;
  }
}
