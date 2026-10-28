// The `order` aggregate and the `orders` read model's projection.
import { Rejected, domain, type StoredEventMeta } from "../../../sdks/typescript/src/index.ts";

type Status = "none" | "draft" | "submitted" | "confirmed" | "cancelled";

interface Order {
  status: Status;
  items: number;
}

export const ordersDomain = domain("orders");
// The customer's email is personal data of the customer (an identity account): the vault keeps it,
// and erasing the account erases it here too.
ordersDomain.command("RecordConfirmation").personal(["email"], (d: { customerId: string }) => d.customerId);
ordersDomain.event("ConfirmationSent", 1).personal(["email"], (d: { customerId: string }) => d.customerId);

export const orderAggregate = ordersDomain.aggregate<Order>("order", { status: "none", items: 0 })
  .apply("OrderPlaced", () => ({ status: "draft", items: 0 }))
  .apply("ItemAdded", (s, data: { qty: number }) => { s.items += data.qty; })
  .apply("OrderSubmitted", (s) => { s.status = "submitted"; })
  .apply("ConfirmationSent", (s) => { s.status = "confirmed"; })
  .apply("OrderCancelled", (s) => { s.status = "cancelled"; })

  .command("PlaceOrder", (s, data: { customerId: string }) => {
    if (s.status !== "none") throw new Rejected("Order already exists", "exists");
    return [{ type: "OrderPlaced", data: { customerId: data.customerId } }];
  })
  .command("AddItem", (s, data: { sku: string; qty: number }) => {
    if (s.status !== "draft") throw new Rejected(`Cannot add items to a ${s.status} order`, "invalid_state");
    if (!(data.qty > 0)) throw new Rejected("Quantity must be positive", "invalid");
    return [{ type: "ItemAdded", data: { sku: data.sku, qty: data.qty } }];
  })
  .command("SubmitOrder", (s) => {
    if (s.status !== "draft") throw new Rejected(`Cannot submit a ${s.status} order`, "invalid_state");
    if (s.items === 0) throw new Rejected("Cannot submit an empty order", "invalid");
    return [{ type: "OrderSubmitted" }];
  })
  .command("RecordConfirmation", (s, data: { email: string; customerId: string }) => {
    if (s.status !== "submitted") throw new Rejected(`Cannot confirm a ${s.status} order`, "invalid_state");
    return [{ type: "ConfirmationSent", data: { email: data.email, customerId: data.customerId } }];
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
  /** The email the confirmation went to; null once the customer's data is erased. */
  confirmedTo?: string | null;
}

/** The events of the `orders` read model, a listener of the `orders` domain. */
export const ORDER_EVENTS = ["OrderPlaced", "ItemAdded", "OrderSubmitted", "ConfirmationSent", "OrderCancelled", "identity.AccountErased"];

export function project(rows: Map<string, OrderRow>, data: any, evt: StoredEventMeta): void {
  const row = rows.get(evt.aggregateId)!;
  switch (evt.type) {
    case "OrderPlaced":
      rows.set(evt.aggregateId, { id: evt.aggregateId, customerId: data.customerId, status: "draft", items: {} });
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
    case "AccountErased":
      // Personal data of an erased customer goes from every order (rebuilt, it reads null anyway).
      for (const r of rows.values()) if (r.customerId === evt.aggregateId && r.confirmedTo) r.confirmedTo = null;
      break;
  }
}
