// The invitation aggregates: state, how each event changes it, unique keys and facts.

import { invitationDomain, type InvitationStatus, type InvitedGroup } from "./domain.ts";
import {
  InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationResent, InvitationRevoked,
} from "./events.ts";

export interface Invitation {
  status: InvitationStatus;
  erased: boolean;
  email?: string | null;
  groups: InvitedGroup[];
  owner: boolean;
  tokenId?: string;
  /** The token while open; in the vault, so null once used or erased. */
  token: string | null;
  expiresAt: number;
  accountId?: string;
}

export const invitationAggregate = invitationDomain
  .aggregate<Invitation>("invitation", { status: "none", erased: false, groups: [], owner: false, token: null, expiresAt: 0 })
  .apply(InvitationCreated, (s, d) => {
    s.status = "open";
    s.email = d.email;
    s.groups = d.groups;
    s.owner = d.owner;
    s.tokenId = d.tokenId;
    s.token = d.token;
    s.expiresAt = d.expiresAt;
  })
  .apply(InvitationResent, (s, d) => {
    s.tokenId = d.tokenId;
    s.token = d.token;
    s.expiresAt = d.expiresAt;
  })
  .apply(InvitationRevoked, (s) => {
    s.status = "revoked";
    s.token = null;
  })
  .apply(InvitationDeclined, (s) => {
    s.status = "declined";
    s.token = null;
  })
  .apply(InvitationAccepted, (s, d) => {
    s.status = "accepted";
    s.token = null;
    s.accountId = d.accountId;
  })
  .apply(InvitationErased, (s) => {
    s.erased = true;
    s.email = null;
  })
  // One open invitation per email and tenant.
  .unique("email", (s) => (s.status === "open" && s.email ? s.email : null));
