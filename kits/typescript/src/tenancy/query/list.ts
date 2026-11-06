import type { TenantStatus } from "../domain.ts";
import { tenantRows, tenantsReadModel } from "../readmodel/tenants.ts";

tenantsReadModel.query("ListTenants", (d: { status?: TenantStatus; after?: string; limit?: number }) => {
  const limit = Math.min(Math.max(Number(d?.limit) || 50, 1), 500);
  const all = [...tenantRows.values()]
    .filter((t) => !d?.status || t.status === d.status)
    .sort((a, b) => a.slug.localeCompare(b.slug))
    .filter((t) => !d?.after || t.slug > d.after);
  return { tenants: all.slice(0, limit), more: all.length > limit };
});
