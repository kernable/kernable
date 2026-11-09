// identity: a person, service or business process in one tenant, holding its group memberships
// (design/kits.md "identity"). The domain object and the types its parts share; `index.ts`
// registers the parts.

import { domain } from "@kernable/sdk";

import { sha256 } from "../shared/secrets.ts";

export const identityDomain = domain("identity");

/** A person (with an account), a service, or a business process the kernel runs (in the platform tenant). */
export type IdentityKind = "person" | "service" | "process";
export type IdentityStatus = "none" | "active" | "suspended" | "deleted";
export interface Scope { type: string; id: string }
export interface Membership {
  membershipId: string;
  group: string;
  scope: Scope | null;
  expiresAt: number | null;
  status: "active" | "suspended";
}
export interface ApiKey { keyId: string; hash: string; label: string; expiresAt: number | null; revoked: boolean }

/** An API key as its holder sends it: `key_<keyId>_<secret>`. */
export function apiKey(keyId: string, secret: string): string {
  return `key_${keyId}_${secret}`;
}

/** The key ID and the whole key's hash, or null if it has another shape. */
export function parseApiKey(value: string): { keyId: string; hash: string } | null {
  const m = /^key_([A-Za-z0-9]{8,32})_(.{32,})$/.exec(value);
  return m ? { keyId: m[1], hash: sha256(value) } : null;
}
