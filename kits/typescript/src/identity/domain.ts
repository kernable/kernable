// identity: a person or service in one tenant, holding its group memberships
// (design/kits.md "identity"). The domain object, its types and vault declarations; `index.ts`
// registers the parts.

import { domain } from "@kernable/sdk";

import { sha256 } from "../shared/secrets.ts";

export const identityDomain = domain("identity");

const own = (_d: unknown, id: string) => `identity:${id}`;
identityDomain.command("CreateIdentity").personal(["displayName", "title", "email"], own);
identityDomain.command("CreateServiceIdentity").personal(["displayName"], own);
identityDomain.command("UpdateProfile").personal(["displayName", "title", "email"], own);
identityDomain.command("CreateApiKey").secret(["secret"]);
identityDomain.event("IdentityCreated", 1).personal(["displayName", "title", "email"], own);
identityDomain.event("ProfileUpdated", 1).personal(["displayName", "title", "email"], own);
identityDomain.event("IdentityDeleted", 1).erases(own);

export type IdentityStatus = "none" | "active" | "suspended" | "deleted";
export interface Scope { type: string; id: string }
export interface Membership {
  membershipId: string;
  group: string;
  /** null: the whole tenant; else one aggregate (or what has it as parent). */
  scope: Scope | null;
  expiresAt: number | null;
  status: "active" | "suspended";
}
export interface ApiKey { keyId: string; hash: string; label: string; expiresAt: number | null; revoked: boolean }

export interface Identity {
  id?: string;
  status: IdentityStatus;
  kind?: "person" | "service";
  accountId?: string;
  memberships: Membership[];
  apiKeys: ApiKey[];
}

// Event payloads.
export interface IdentityCreated { kind: "person" | "service"; accountId: string | null; displayName: string | null; title: string | null; email: string | null }
export type MembershipGranted = Omit<Membership, "status">;

/** An API key as its holder sends it: `key_<keyId>_<secret>`. */
export function apiKey(keyId: string, secret: string): string {
  return `key_${keyId}_${secret}`;
}

/** The key ID and the whole key's hash, or null if it has another shape. */
export function parseApiKey(value: string): { keyId: string; hash: string } | null {
  const m = /^key_([A-Za-z0-9]{8,32})_(.{32,})$/.exec(value);
  return m ? { keyId: m[1], hash: sha256(value) } : null;
}
