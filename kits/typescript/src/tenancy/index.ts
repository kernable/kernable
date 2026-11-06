// Registers every part of the tenancy domain (a part not imported here does not exist) and
// exports what other domains and clients use.

export { tenancyDomain } from "./domain.ts";
export type * from "./domain.ts";
export * from "./events.ts";
export { tenantAggregate, type Tenant } from "./aggregates.ts";
export { CreateTenant, ActivateTenant, RenameTenant, ChangeTenantSlug, SuspendTenant, ReactivateTenant, DeleteTenant, AddOwner, RemoveOwner, RequireMfa, ChangeSignUpPolicy } from "./commands.ts";
export { tenantsReadModel, type TenantRow } from "./readmodels.ts";
export { GetTenant, GetTenantBySlug, ListTenants, GetSignUpPolicy } from "./queries.ts";
