// The read models: customers and orders, in memory, answering queries.
import { Kernable, Rejected } from "@kernable/sdk";

import { shopDomain } from "./domain.ts";

// #region customers
type CustomerRow = { id: string; email: string; name: string };
const customers = new Map<string, CustomerRow>();

const customersReadmodel = shopDomain.readModel("customers")
  .on("CustomerRegistered", (data: { email: string; name: string }, evt) => {
    customers.set(evt.aggregateId, { id: evt.aggregateId, email: data.email, name: data.name });
  })
  .query("GetCustomer", (data: { id: string }) => {
    const row = customers.get(data.id);
    if (!row) throw new Rejected(`No customer ${data.id}`, "not_found");
    return row;
  });
// #endregion customers

// #region orders
type OrderRow = { id: string; customerId: string; status: string; items: Record<string, number>; confirmedTo?: string | null; cancelled?: string };
const orders = new Map<string, OrderRow>();

const ordersReadmodel = shopDomain.readModel("orders")
  .on("OrderPlaced", (data: { customerId: string }, evt) => {
    orders.set(evt.aggregateId, { id: evt.aggregateId, customerId: data.customerId, status: "draft", items: {} });
  })
  .on("ItemAdded", (data: { sku: string; qty: number }, evt) => {
    const items = orders.get(evt.aggregateId)!.items;
    items[data.sku] = (items[data.sku] ?? 0) + data.qty;
  })
  .on("OrderSubmitted", (_data, evt) => void (orders.get(evt.aggregateId)!.status = "submitted"))
  .query("GetOrder", (data: { id: string }) => {
    const row = orders.get(data.id);
    if (!row) throw new Rejected(`No order ${data.id}`, "not_found");
    return row;
  });
// #endregion orders

// #region confirmed
ordersReadmodel.on("OrderConfirmed", (data: { email: string }, evt) => {
  const row = orders.get(evt.aggregateId)!;
  row.status = "confirmed";
  row.confirmedTo = data.email;
});
// #endregion confirmed

// #region erased
// An erased customer's personal data must not outlive the erasure in a read model either.
customersReadmodel.on("CustomerErased", (_data, evt) => void customers.delete(evt.aggregateId));
ordersReadmodel.on("CustomerErased", (_data, evt) => {
  for (const row of orders.values()) if (row.customerId === evt.aggregateId && row.confirmedTo) row.confirmedTo = null;
});
// #endregion erased

// #region fulfilled
ordersReadmodel
  .on("OrderPaid", (_data, evt) => void (orders.get(evt.aggregateId)!.status = "paid"))
  .on("OrderShipped", (_data, evt) => void (orders.get(evt.aggregateId)!.status = "shipped"))
  .on("OrderCancelled", (data: { reason: string }, evt) => {
    const row = orders.get(evt.aggregateId)!;
    row.status = "cancelled";
    row.cancelled = data.reason;
  });
// #endregion fulfilled

// #region run
const kernable = new Kernable();
await Promise.all([kernable.listener(customersReadmodel).run(), kernable.listener(ordersReadmodel).run()]);
// #endregion run
