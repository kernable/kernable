import { domain } from "@kernable/sdk";

// #region upcast
export const shop = domain("shop");
shop.event("ItemAdded", 2).upcast(1, (d: { sku: string; qty: number }) => ({ sku: d.sku, count: d.qty }));
// #endregion upcast
