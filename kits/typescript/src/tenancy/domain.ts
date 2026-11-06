// tenancy: one Tenant per tenant, living under its own tenant ID (design/kits.md "tenancy").
// The domain object, its types and vault declarations (none: a tenant is an organisation).
// `index.ts` registers the parts.

import { domain } from "@kernable/sdk";

export const tenancyDomain = domain("tenancy");

export type TenantStatus = "none" | "pending" | "active" | "suspended" | "deleted";
export type SuspensionReason = "owner_requested" | "unpaid" | "abuse" | "by_platform" | "rejected";
export const SUSPENSION_REASONS: SuspensionReason[] = ["owner_requested", "unpaid", "abuse", "by_platform", "rejected"];

export interface SignUpPolicy {
  /** The status a self-service tenant starts with, or `off`. */
  tenants: "off" | "pending" | "active";
  accounts: "off" | "open";
}

export type Owner = { email: string } | { accountId: string };

export interface Tenant {
  id?: string;
  status: TenantStatus;
  slug?: string;
  name?: string;
  /** Identity IDs: the only place owners are kept; `owners` is a virtual group. */
  owners: string[];
  suspension: { reason: SuspensionReason; at: number } | null;
  mfaRequired: boolean;
  /** Only the platform tenant's matters. */
  signUp: SignUpPolicy;
}

// Event payloads.
export interface TenantCreated { slug: string; name: string; owner: Owner; status: "active" | "pending" }
export interface TenantSuspended { reason: SuspensionReason; at: number }
