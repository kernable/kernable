// The tenants read model: one row per tenant; a deleted tenant leaves the read side. Its
// queries are in ../query/.

import { ORDERS } from "../../shared/constants.ts";
import { tenancyDomain, type SignUpPolicy, type Tenant, type TenantStatus, type TenantSuspended } from "../domain.ts";

export interface TenantRow {
  id: string;
  slug: string;
  name: string;
  status: TenantStatus;
  suspension: Tenant["suspension"];
  mfaRequired: boolean;
  owners: string[];
}

export const tenantRows = new Map<string, TenantRow>();
export const policy: { signUp: SignUpPolicy } = { signUp: { tenants: "off", accounts: "off" } };

const update = (id: string, f: (r: TenantRow) => void) => {
  const r = tenantRows.get(id);
  if (r) f(r);
};

export const tenantsReadModel = tenancyDomain
  .readModel("tenants", { order: ORDERS.readModels })
  .on("TenantCreated", (d: { slug: string; name: string; status: TenantStatus }, evt) => {
    tenantRows.set(evt.aggregateId, {
      id: evt.aggregateId, slug: d.slug, name: d.name, status: d.status, suspension: null, mfaRequired: false, owners: [],
    });
  })
  .on("TenantActivated", (_d, evt) => update(evt.aggregateId, (r) => void (r.status = "active")))
  .on("TenantRenamed", (d: { name: string }, evt) => update(evt.aggregateId, (r) => void (r.name = d.name)))
  .on("TenantSlugChanged", (d: { slug: string }, evt) => update(evt.aggregateId, (r) => void (r.slug = d.slug)))
  .on("TenantSuspended", (d: TenantSuspended, evt) => update(evt.aggregateId, (r) => {
    r.status = "suspended";
    r.suspension = { reason: d.reason, at: d.at };
  }))
  .on("TenantReactivated", (_d, evt) => update(evt.aggregateId, (r) => {
    r.status = "active";
    r.suspension = null;
  }))
  .on("TenantDeleted", (_d, evt) => void tenantRows.delete(evt.aggregateId))
  .on("OwnerAdded", (d: { identityId: string }, evt) => update(evt.aggregateId, (r) => void r.owners.push(d.identityId)))
  .on("OwnerRemoved", (d: { identityId: string }, evt) => update(evt.aggregateId, (r) => void (r.owners = r.owners.filter((o) => o !== d.identityId))))
  .on("MfaRequirementChanged", (d: { required: boolean }, evt) => update(evt.aggregateId, (r) => void (r.mfaRequired = d.required)))
  .on("SignUpPolicyChanged", (d: SignUpPolicy) => void (policy.signUp = { tenants: d.tenants, accounts: d.accounts }));
