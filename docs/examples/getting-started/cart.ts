import { Kernable, Rejected, domain } from "@kernable/sdk";

// #region aggregate
export const cart = domain("shop").aggregate("Cart", { items: 0, checkedOut: false })
  .apply("ItemAdded", (s, e: { qty: number }) => ({ ...s, items: s.items + e.qty }))
  .apply("CheckedOut", (s) => ({ ...s, checkedOut: true }))
  .command("AddItem", (s, cmd: { sku: string; qty: number }) => {
    if (s.checkedOut) throw new Rejected("Cart is checked out");
    return [{ type: "ItemAdded", data: { sku: cmd.sku, qty: cmd.qty } }];
  })
  .command("CheckOut", (s, _cmd, ctx) => {
    if (s.items === 0) throw new Rejected("Cart is empty");
    // ctx.now() and ctx.id() stay the same if the command runs again.
    return [{ type: "CheckedOut", data: { at: ctx.now().toISOString(), orderId: ctx.id() } }];
  });
// #endregion aggregate

// #region worker
const kernable = new Kernable({ url: `unix:${process.cwd()}/k.sock`, key: "app.key" });
await kernable.worker(cart).run();
// #endregion worker
