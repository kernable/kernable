// The tenancy read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { type SignUpPolicy, type SuspensionReason, tenancyDomain, type TenantStatus } from "./domain.ts";
import { MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated, TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended } from "./events.ts";

export interface TenantRow {
  id: string;
  slug: string;
  name: string;
  status: TenantStatus;
  suspension: { reason: SuspensionReason; at: number } | null;
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
  .readModel("tenancy.tenants", { order: ORDERS.readModels })
  .on(TenantCreated, (d, evt) => {
    tenantRows.set(evt.aggregateId, { id: evt.aggregateId, slug: d.slug, name: d.name, status: d.status, suspension: null, mfaRequired: false, owners: [] });
  })
  .on(TenantActivated, (_d, evt) => update(evt.aggregateId, (r) => void (r.status = "active")))
  .on(TenantRenamed, (d, evt) => update(evt.aggregateId, (r) => void (r.name = d.name)))
  .on(TenantSlugChanged, (d, evt) => update(evt.aggregateId, (r) => void (r.slug = d.slug)))
  .on(TenantSuspended, (d, evt) => update(evt.aggregateId, (r) => {
    r.status = "suspended";
    r.suspension = { reason: d.reason, at: d.at };
  }))
  .on(TenantReactivated, (_d, evt) => update(evt.aggregateId, (r) => {
    r.status = "active";
    r.suspension = null;
  }))
  .on(TenantDeleted, (_d, evt) => void tenantRows.delete(evt.aggregateId))
  .on(OwnerAdded, (d, evt) => update(evt.aggregateId, (r) => void r.owners.push(d.identityId)))
  .on(OwnerRemoved, (d, evt) => update(evt.aggregateId, (r) => void (r.owners = r.owners.filter((o) => o !== d.identityId))))
  .on(MfaRequirementChanged, (d, evt) => update(evt.aggregateId, (r) => void (r.mfaRequired = d.required)))
  .on(SignUpPolicyChanged, (d) => void (policy.signUp = { tenants: d.tenants, accounts: d.accounts }));
