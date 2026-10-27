// The read models: customers and orders, in memory, answering queries.
import { Rejected } from "@kernable/sdk";

import { connect } from "./connect.ts";

const kernable = connect();

// #region customers
type CustomerRow = { id: string; email: string; name: string };
const customers = new Map<string, CustomerRow>();

const customersModel = kernable.readModel("customers")
  .on("shop.CustomerRegistered", (e) => {
    const { email, name } = e.data as { email: string; name: string };
    customers.set(e.aggregateId, { id: e.aggregateId, email, name });
  })
  .query("shop.GetCustomer", (d: { id: string }) => {
    const row = customers.get(d.id);
    if (!row) throw new Rejected(`No customer ${d.id}`, "not_found");
    return row;
  });
// #endregion customers

// #region orders
type OrderRow = { id: string; customerId: string; status: string; items: Record<string, number>; confirmedTo?: string | null };
const orders = new Map<string, OrderRow>();

const ordersModel = kernable.readModel("orders")
  .on("shop.OrderPlaced", (e) => {
    const { customerId } = e.data as { customerId: string };
    orders.set(e.aggregateId, { id: e.aggregateId, customerId, status: "draft", items: {} });
  })
  .on("shop.ItemAdded", (e) => {
    const { sku, qty } = e.data as { sku: string; qty: number };
    const items = orders.get(e.aggregateId)!.items;
    items[sku] = (items[sku] ?? 0) + qty;
  })
  .on("shop.OrderSubmitted", (e) => void (orders.get(e.aggregateId)!.status = "submitted"))
  .query("shop.GetOrder", (d: { id: string }) => {
    const row = orders.get(d.id);
    if (!row) throw new Rejected(`No order ${d.id}`, "not_found");
    return row;
  });
// #endregion orders

// #region confirmed
ordersModel.on("shop.OrderConfirmed", (e) => {
  const row = orders.get(e.aggregateId)!;
  row.status = "confirmed";
  row.confirmedTo = (e.data as { email: string }).email;
});
// #endregion confirmed

// #region erased
// An erased customer's personal data must not outlive the erasure in a read model either.
customersModel.on("shop.CustomerErased", (e) => void customers.delete(e.aggregateId));
ordersModel.on("shop.CustomerErased", (e) => {
  for (const row of orders.values()) if (row.customerId === e.aggregateId && row.confirmedTo) row.confirmedTo = null;
});
// #endregion erased

// #region run
await Promise.all([customersModel.run(), ordersModel.run()]);
// #endregion run
