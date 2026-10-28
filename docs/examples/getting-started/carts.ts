// #region file
import { Kernable, Rejected, domain } from "@kernable/sdk";

// #region register
const shopDomain = domain("shop");
const cartsReadmodel = shopDomain.readModel("carts");
const rows = new Map<string, { items: number; order?: string }>();
// #endregion register

// #region events
cartsReadmodel.on("ItemAdded", (data: { qty: number }, evt) => {
  const row = rows.get(evt.aggregateId) ?? { items: 0 };
  row.items += data.qty;
  rows.set(evt.aggregateId, row);
});

cartsReadmodel.on("CheckedOut", (data: { orderId: string }, evt) => {
  rows.get(evt.aggregateId)!.order = data.orderId;
});
// #endregion events

// #region queries
cartsReadmodel.query("GetCart", (data: { id: string }) => {
  const row = rows.get(data.id);
  if (!row) throw new Rejected("no such cart", "not_found");
  return row;
});
// #endregion queries

// #region run
const kernable = new Kernable({ key: "app.key" });
await kernable.listener(cartsReadmodel).run();
// #endregion run
// #endregion file
