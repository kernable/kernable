// invitation: one Invitation per invitation, under its tenant's ID (A77, design/kits.md
// "invitation"). The domain object and the types its parts share; `index.ts` registers the parts.

import { domain } from "@kernable/sdk";

import { sha256 } from "../shared/secrets.ts";

export const invitationDomain = domain("invitation");

export type InvitationStatus = "none" | "open" | "accepted" | "declined" | "revoked";

/** A group the invited person joins, for the whole tenant or one aggregate. */
export interface InvitedGroup { group: string; scope?: { type: string; id: string } | null }

export type LinkStatus = "none" | "open" | "revoked";

/** An invitation link as people get it: `link_<tenantId>.<linkId>.<secret>`. */
export function linkToken(tenant: string, linkId: string, secret: string): string {
  return `link_${tenant}.${linkId}.${secret}`;
}

/** The tenant, the link and the whole token's hash, or null if it has another shape. */
export function parseLinkToken(value: unknown): { tenant: string; linkId: string; hash: string } | null {
  if (typeof value !== "string") return null;
  const m = /^link_([^.\s]+)\.([^.\s]+)\.([A-Za-z0-9_-]{32,})$/.exec(value);
  return m ? { tenant: m[1], linkId: m[2], hash: sha256(value) } : null;
}
