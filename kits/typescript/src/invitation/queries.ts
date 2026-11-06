// The invitation queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import { parseInvitationToken } from "../shared/secrets.ts";
import { type InvitationRow, invitationRows, invitationsReadModel, rowKey } from "./readmodels.ts";

/** The tenant's invitations; the open ones appear in user lists. */
export class ListInvitations extends Query<InvitationRow[]> {}

invitationsReadModel.query(ListInvitations, (_d, qry) => [...invitationRows.values()].filter((r) => r.tenant === qry.tenant));

/** The API's only: what the acceptance page shows for a token (the token itself is checked on accepting). */
export class PeekInvitation extends Query<{ tenant: string; invitationId: string; email: string | null; owner: boolean; expired: boolean }> {
  token!: string;
}

invitationsReadModel.query(PeekInvitation, (d) => {
  const t = parseInvitationToken(d?.token);
  const r = t ? invitationRows.get(rowKey(t.tenant, t.invitationId)) : undefined;
  if (!t || !r || r.status !== "open") throw new Rejected("The invitation is not valid", "invalid_token");
  return { tenant: r.tenant, invitationId: r.id, email: r.email, owner: r.owner, expired: Date.now() > r.expiresAt };
});
