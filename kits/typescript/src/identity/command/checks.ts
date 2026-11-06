// Checks the identity commands share.

import { Rejected } from "@kernable/sdk";

import type { Identity, Scope } from "../domain.ts";

export function active(s: Identity): void {
  if (s.status !== "active") throw new Rejected("The identity is not active", "invalid_state");
}

export function scope(value: unknown): Scope | null {
  if (value === null || value === undefined) return null;
  const sc = value as Scope;
  if (typeof sc.type === "string" && /^[a-z][\w-]*\.[\w-]+$/.test(sc.type) && typeof sc.id === "string" && sc.id) return { type: sc.type, id: sc.id };
  throw new Rejected("A scope is { type: 'domain.aggregate', id }", "invalid_scope");
}
