// tenancy: one Tenant per tenant, living under its own tenant ID (design/kits.md "tenancy").
// The domain object and the types its parts share; `index.ts` registers the parts.

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
