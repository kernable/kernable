// The invitation queries: each class with its handler.

import { Query, Rejected } from "@kernable/sdk";

import { parseInvitationToken } from "../shared/secrets.ts";
import { parseLinkToken } from "./domain.ts";
import { type InvitationRow, type LinkRow, invitationRows, invitationsReadModel, linkRows, rowKey } from "./readmodels.ts";

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

/** The tenant's invitation links, with how often each was used. */
export class ListInvitationLinks extends Query<LinkRow[]> {}

invitationsReadModel.query(ListInvitationLinks, (_d, qry) => [...linkRows.values()].filter((r) => r.tenant === qry.tenant));

/** The API's only: whether a link token can be used, for the join page (the token is checked on joining). */
export class PeekInvitationLink extends Query<{ tenant: string; linkId: string; label: string; usable: boolean }> {
  token!: string;
}

invitationsReadModel.query(PeekInvitationLink, (d) => {
  const t = parseLinkToken(d?.token);
  const r = t ? linkRows.get(rowKey(t.tenant, t.linkId)) : undefined;
  if (!t || !r || r.status !== "open") throw new Rejected("The link is not valid", "invalid_token");
  const usable = (r.expiresAt === null || Date.now() <= r.expiresAt) && (r.maxUses === null || r.uses < r.maxUses);
  return { tenant: r.tenant, linkId: r.id, label: r.label, usable };
});
