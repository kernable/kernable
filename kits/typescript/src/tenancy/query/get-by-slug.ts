import { Rejected } from "@kernable/sdk";

import { tenantRows, tenantsReadModel } from "../readmodel/tenants.ts";

// For subdomains and login pages, through the API only: nothing open lists tenants.
tenantsReadModel.query("GetTenantBySlug", (d: { slug: string }) => {
  const r = [...tenantRows.values()].find((t) => t.slug === d?.slug);
  if (!r) throw new Rejected("No such tenant", "not_found");
  return { id: r.id, name: r.name, status: r.status };
});
