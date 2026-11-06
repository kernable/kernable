// Checks the tenancy commands share.

import { Rejected, type CommandMeta } from "@kernable/sdk";

import { PLATFORM_TENANT, RESERVED_SLUGS } from "../../shared/constants.ts";
import type { Tenant } from "../domain.ts";

export function validSlug(slug: unknown, tenantId: string): slug is string {
  const shaped = typeof slug === "string" && slug.length >= 3 && slug.length <= 63 && /^[a-z0-9](-?[a-z0-9])*$/.test(slug);
  return shaped && (tenantId === PLATFORM_TENANT || !RESERVED_SLUGS.includes(slug as string));
}

export function slug(value: unknown, tenantId: string): string {
  if (!validSlug(value, tenantId)) {
    throw new Rejected("A slug is 3 to 63 lowercase letters, digits and single dashes, and not reserved", "invalid_slug");
  }
  return value;
}

export function name(value: unknown): string {
  if (typeof value !== "string" || !value.trim() || value.length > 200) throw new Rejected("A tenant needs a name", "invalid_name");
  return value.trim();
}

/** A tenant's aggregate ID is its tenant ID. */
export function itself(cmd: CommandMeta): void {
  if (cmd.aggregateId !== cmd.tenant) throw new Rejected("A tenant lives under its own tenant ID", "invalid_tenant");
}

export function exists(s: Tenant): void {
  if (s.status === "none" || s.status === "deleted") throw new Rejected("No such tenant", "not_found");
}
