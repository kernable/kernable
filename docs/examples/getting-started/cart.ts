// #region file
import { Kernable, Rejected, domain } from "@kernable/sdk";

// #region aggregate
const shopDomain = domain("shop");
const cartAggregate = shopDomain.aggregate("cart", { items: 0, checkedOut: false });
// #endregion aggregate

// #region events
cartAggregate.apply("ItemAdded", (state, data: { qty: number }) => { state.items += data.qty; });
cartAggregate.apply("CheckedOut", (state) => { state.checkedOut = true; });
// #endregion events

// #region commands
cartAggregate.command("AddItem", (state, data: { sku: string; qty: number }) => {
  if (state.checkedOut) throw new Rejected("Cart is checked out");
  return [{ type: "ItemAdded", data }];
});

cartAggregate.command("CheckOut", (state, _data, _cmd, ctx) => {
  if (state.items === 0) throw new Rejected("Cart is empty");
  // ctx.now() and ctx.id() stay the same if the command runs again.
  return [{ type: "CheckedOut", data: { at: ctx.now().toISOString(), orderId: ctx.id() } }];
});
// #endregion commands

// #region worker
const kernable = new Kernable({ key: "app.key" });
await kernable.worker(cartAggregate).run();
// #endregion worker
// #endregion file
