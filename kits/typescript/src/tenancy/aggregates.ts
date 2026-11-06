// The tenancy aggregates: state, how each event changes it, unique keys and facts.

import { type Owner, type SignUpPolicy, type SuspensionReason, tenancyDomain, type TenantStatus } from "./domain.ts";
import { MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated, TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended } from "./events.ts";

export interface Tenant {
  id?: string;
  status: TenantStatus;
  slug?: string;
  name?: string;
  owner?: Owner;
  /** Identity IDs: the only place owners are kept; `owners` is a virtual group. */
  owners: string[];
  suspension: { reason: SuspensionReason; at: number } | null;
  mfaRequired: boolean;
  /** Only the platform tenant's matters. */
  signUp: SignUpPolicy;
}

export const tenantAggregate = tenancyDomain
  .aggregate<Tenant>("tenant", {
    status: "none",
    owners: [],
    suspension: null,
    mfaRequired: false,
    signUp: { tenants: "off", accounts: "off" },
  })
  .apply(TenantCreated, (s, d, evt) => {
    s.id = evt.aggregateId;
    s.status = d.status;
    s.slug = d.slug;
    s.name = d.name;
  })
  .apply(TenantActivated, (s) => void (s.status = "active"))
  .apply(TenantRenamed, (s, d) => void (s.name = d.name))
  .apply(TenantSlugChanged, (s, d) => void (s.slug = d.slug))
  .apply(TenantSuspended, (s, d) => {
    s.status = "suspended";
    s.suspension = { reason: d.reason, at: d.at };
  })
  .apply(TenantReactivated, (s) => {
    s.status = "active";
    s.suspension = null;
  })
  .apply(TenantDeleted, (s) => {
    s.status = "deleted";
    s.owners = [];
  })
  .apply(OwnerAdded, (s, d) => void s.owners.push(d.identityId))
  .apply(OwnerRemoved, (s, d) => void (s.owners = s.owners.filter((o) => o !== d.identityId)))
  .apply(MfaRequirementChanged, (s, d) => void (s.mfaRequired = d.required))
  .apply(SignUpPolicyChanged, (s, d) => void (s.signUp = { tenants: d.tenants, accounts: d.accounts }))
  // A slug is unique across tenants while the tenant is not deleted: deleting frees it.
  .unique("slug", (s) => (s.status !== "none" && s.status !== "deleted" ? s.slug : null), { scope: "global" })
  // Lets identity.DeleteIdentity require that no tenant holds the identity as owner.
  .unique("owner", (s) => s.owners)
  .fact("active", (s) => s.status === "active")
  .fact("exists", (s) => s.status !== "none" && s.status !== "deleted");
