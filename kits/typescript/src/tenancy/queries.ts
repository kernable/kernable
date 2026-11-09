// The tenancy queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import type { SignUpPolicy, TenantStatus } from "./domain.ts";
import { policy, type TenantRow, tenantRows, tenantsReadModel } from "./readmodels.ts";

export class GetTenantBySlug extends Query<{ id: string; name: string; status: TenantStatus }> {
  slug!: string;
}

// For subdomains and login pages, through the API only: nothing open lists tenants.
tenantsReadModel.query(GetTenantBySlug, (d) => {
  const r = [...tenantRows.values()].find((t) => t.slug === d?.slug);
  if (!r) throw new Rejected("No such tenant", "not_found");
  return { id: r.id, name: r.name, status: r.status };
});

export class GetSignUpPolicy extends Query<SignUpPolicy> {}

tenantsReadModel.query(GetSignUpPolicy, () => policy.signUp);

export class GetTenant extends Query<TenantRow> {
  static target = { aggregateType: "tenancy.tenant", field: "tenantId" };
  tenantId!: string;
}

tenantsReadModel.query(GetTenant, (d) => {
  const r = tenantRows.get(d?.tenantId);
  if (!r) throw new Rejected("No such tenant", "not_found");
  return r;
});

export class ListTenants extends Query<{ tenants: TenantRow[]; more: boolean }> {
  status?: TenantStatus;
  /** The last slug of the previous page. */
  after?: string;
  limit?: number;
}

tenantsReadModel.query(ListTenants, (d) => {
  const limit = Math.min(Math.max(Number(d?.limit) || 50, 1), 500);
  const all = [...tenantRows.values()]
    .filter((t) => !d?.status || t.status === d.status)
    .sort((a, b) => a.slug.localeCompare(b.slug))
    .filter((t) => !d?.after || t.slug > d.after);
  return { tenants: all.slice(0, limit), more: all.length > limit };
});
