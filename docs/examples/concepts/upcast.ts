import { domain } from "@kernable/sdk";

// #region upcast
export const shopDomain = domain("shop");
shopDomain.event("ItemAdded", 2).upcast(1, (d: { sku: string; qty: number }) => ({ sku: d.sku, count: d.qty }));
// #endregion upcast
