// Checks the group commands share.

import { Rejected } from "@kernable/sdk";

import type { Permission } from "../domain.ts";

export function validPattern(p: unknown): p is string {
  return typeof p === "string" && (p === "*" || /^[a-z][\w-]*\.(\*|[A-Za-z]\w*)$/.test(p));
}

/** The permissions, checked and normalised. */
export function permissions(value: unknown): Permission[] {
  if (!Array.isArray(value)) throw new Rejected("permissions is a list", "invalid_permissions");
  return value.map((p: Permission) => {
    if (!p || !validPattern(p.pattern)) throw new Rejected(`Not a type pattern: ${p?.pattern}`, "invalid_permissions");
    if (p.when !== undefined && (typeof p.when !== "string" || !/^[a-z]\w*$/.test(p.when))) {
      throw new Rejected(`Not a relation: ${p.when}`, "invalid_permissions");
    }
    const out: Permission = { pattern: p.pattern };
    if (p.when) out.when = p.when;
    if (p.when && p.viaParent) out.viaParent = true;
    return out;
  });
}

export function name(value: unknown): string {
  if (typeof value !== "string" || !value.trim() || value.length > 100 || value.trim().toLowerCase() === "owners") {
    throw new Rejected("A group needs a name other than owners", "invalid_name");
  }
  return value.trim();
}
