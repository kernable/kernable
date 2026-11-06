// The Tenant aggregate: its state, how events change it, its unique keys and facts. The commands
// are in ../command/, one per file.

import { tenancyDomain, type SignUpPolicy, type Tenant, type TenantCreated, type TenantSuspended } from "../domain.ts";

export const tenantAggregate = tenancyDomain
  .aggregate<Tenant>("tenant", {
    status: "none",
    owners: [],
    suspension: null,
    mfaRequired: false,
    signUp: { tenants: "off", accounts: "off" },
  })
  .apply("TenantCreated", (s, d: TenantCreated, evt) => {
    s.id = evt.aggregateId;
    s.status = d.status;
    s.slug = d.slug;
    s.name = d.name;
  })
  .apply("TenantActivated", (s) => void (s.status = "active"))
  .apply("TenantRenamed", (s, d: { name: string }) => void (s.name = d.name))
  .apply("TenantSlugChanged", (s, d: { slug: string }) => void (s.slug = d.slug))
  .apply("TenantSuspended", (s, d: TenantSuspended) => {
    s.status = "suspended";
    s.suspension = { reason: d.reason, at: d.at };
  })
  .apply("TenantReactivated", (s) => {
    s.status = "active";
    s.suspension = null;
  })
  .apply("TenantDeleted", (s) => {
    s.status = "deleted";
    s.owners = [];
  })
  .apply("OwnerAdded", (s, d: { identityId: string }) => void s.owners.push(d.identityId))
  .apply("OwnerRemoved", (s, d: { identityId: string }) => void (s.owners = s.owners.filter((o) => o !== d.identityId)))
  .apply("MfaRequirementChanged", (s, d: { required: boolean }) => void (s.mfaRequired = d.required))
  .apply("SignUpPolicyChanged", (s, d: SignUpPolicy) => void (s.signUp = { tenants: d.tenants, accounts: d.accounts }))
  // A slug is unique across tenants while the tenant is not deleted: deleting frees it.
  .unique("slug", (s) => (s.status !== "none" && s.status !== "deleted" ? s.slug : null), { scope: "global" })
  // Lets identity.DeleteIdentity require that no tenant holds the identity as owner.
  .unique("owner", (s) => s.owners)
  .fact("active", (s) => s.status === "active")
  .fact("exists", (s) => s.status !== "none" && s.status !== "deleted");
