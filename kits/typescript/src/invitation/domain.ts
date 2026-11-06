// invitation: one Invitation per invitation, under its tenant's ID (A77, design/kits.md
// "invitation"). The domain object and the types its parts share; `index.ts` registers the parts.

import { domain } from "@kernable/sdk";

export const invitationDomain = domain("invitation");

export type InvitationStatus = "none" | "open" | "accepted" | "declined" | "revoked";

/** A group the invited person joins, for the whole tenant or one aggregate. */
export interface InvitedGroup { group: string; scope?: { type: string; id: string } | null }
