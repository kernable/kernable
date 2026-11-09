// The invitation read models: rows and event handlers. Their queries are in queries.ts.

import { ORDERS } from "../shared/constants.ts";
import { TenantDeleted } from "../tenancy/index.ts";
import { invitationDomain, type InvitationStatus, type InvitedGroup, type LinkStatus } from "./domain.ts";
import {
  InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationLinkCreated, InvitationLinkRevoked,
  InvitationLinkUsed, InvitationResent, InvitationRevoked,
} from "./events.ts";

export interface InvitationRow {
  id: string;
  tenant: string;
  email: string | null;
  groups: InvitedGroup[];
  owner: boolean;
  status: InvitationStatus;
  expiresAt: number;
  invitedBy: string | null;
  invitedAt: number;
}

/** `tenant/invitationId` */
export const invitationRows = new Map<string, InvitationRow>();

export interface LinkRow {
  id: string;
  tenant: string;
  groups: InvitedGroup[];
  label: string;
  status: LinkStatus;
  expiresAt: number | null;
  maxUses: number | null;
  /** How many joined through it. */
  uses: number;
  createdBy: string | null;
  createdAt: number;
}

/** `tenant/linkId` */
export const linkRows = new Map<string, LinkRow>();
export const rowKey = (tenant: string, id: string) => `${tenant}/${id}`;
const status = (s: InvitationStatus) => (_d: unknown, evt: { tenant: string; aggregateId: string }) => {
  const r = invitationRows.get(rowKey(evt.tenant, evt.aggregateId));
  if (r) r.status = s;
};

export const invitationsReadModel = invitationDomain
  .readModel("invitation.invitations", { order: ORDERS.readModels })
  .on(InvitationCreated, (d, evt, ctx) => {
    invitationRows.set(rowKey(evt.tenant, evt.aggregateId), {
      id: evt.aggregateId, tenant: evt.tenant, email: d.email, groups: d.groups, owner: d.owner, status: "open",
      expiresAt: d.expiresAt, invitedBy: ctx.request.senderIdentity ?? null, invitedAt: evt.createdAt.getTime(),
    });
  })
  .on(InvitationResent, (d, evt) => {
    const r = invitationRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (r) r.expiresAt = d.expiresAt;
  })
  .on(InvitationRevoked, status("revoked"))
  .on(InvitationDeclined, status("declined"))
  .on(InvitationAccepted, status("accepted"))
  .on(InvitationErased, (_d, evt) => {
    const r = invitationRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (r) r.email = null;
  })
  .on(InvitationLinkCreated, (d, evt, ctx) => {
    linkRows.set(rowKey(evt.tenant, evt.aggregateId), {
      id: evt.aggregateId, tenant: evt.tenant, groups: d.groups, label: d.label, status: "open", expiresAt: d.expiresAt, maxUses: d.maxUses,
      uses: 0, createdBy: ctx.request.senderIdentity ?? null, createdAt: evt.createdAt.getTime(),
    });
  })
  .on(InvitationLinkRevoked, (_d, evt) => {
    const r = linkRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (r) r.status = "revoked";
  })
  .on(InvitationLinkUsed, (_d, evt) => {
    const r = linkRows.get(rowKey(evt.tenant, evt.aggregateId));
    if (r) r.uses += 1;
  })
  .on(TenantDeleted, (_d, evt) => {
    for (const [k, r] of invitationRows) if (r.tenant === evt.aggregateId) invitationRows.delete(k);
    for (const [k, r] of linkRows) if (r.tenant === evt.aggregateId) linkRows.delete(k);
  });
