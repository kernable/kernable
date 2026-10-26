import { Rejected, domain } from "../../sdks/typescript/src/index.ts";

type Status = "none" | "placed" | "shipped" | "cancelled";

interface Order {
  status: Status;
  customerId?: string;
  items: Record<string, number>;
}

export const order = domain("orders").aggregate<Order>("Order", { status: "none", items: {} })
  .apply("OrderPlaced", (s, e: { customerId: string }) => ({ ...s, status: "placed", customerId: e.customerId }))
  .apply("ItemAdded", (s, e: { sku: string; qty: number }) => ({
    ...s,
    items: { ...s.items, [e.sku]: (s.items[e.sku] ?? 0) + e.qty },
  }))
  .apply("OrderCancelled", (s) => ({ ...s, status: "cancelled" }))
  .apply("OrderShipped", (s) => ({ ...s, status: "shipped" }))

  .command("PlaceOrder", (s, cmd: { customerId: string }) => {
    if (s.status !== "none") throw new Rejected("Order already exists");
    return [{ type: "OrderPlaced", data: { customerId: cmd.customerId } }];
  })
  .command("AddItem", (s, cmd: { sku: string; qty: number }) => {
    if (s.status !== "placed") throw new Rejected(`Cannot add items to a ${s.status} order`);
    if (!(cmd.qty > 0)) throw new Rejected("Quantity must be positive");
    return [{ type: "ItemAdded", data: cmd }];
  })
  .command("CancelOrder", (s) => {
    if (s.status !== "placed") throw new Rejected(`Cannot cancel a ${s.status} order`);
    return [{ type: "OrderCancelled" }];
  })
  .command("ShipOrder", (s) => {
    if (s.status !== "placed") throw new Rejected(`Cannot ship a ${s.status} order`);
    if (Object.keys(s.items).length === 0) throw new Rejected("Cannot ship an empty order");
    return [{ type: "OrderShipped" }];
  });
