// group: a named set of permissions per tenant (design/kits.md "group"). The `owners` group
// is virtual: owners live on the Tenant (tenancy). `index.ts` registers the parts; the
// authorization middleware (../authorization/) is a listener of this domain too.

import { domain } from "@kernable/sdk";

export const groupDomain = domain("group");

/**
 * A permission: a type pattern in the grant syntax (`shop.PlaceOrder`, `shop.*`, `*`), optionally
 * tied to a relation of the target aggregate (`when`), which may hold on its parent (`viaParent`).
 */
export interface Permission {
  pattern: string;
  when?: string;
  viaParent?: boolean;
}
